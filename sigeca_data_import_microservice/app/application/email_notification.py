import uuid
import logging
from typing import List

from app.config import Config
from app.infrastructure.smtp_client import SMTPClient
from app.domain.resources import ContactDetailsOfUsesrRepository
from app.infrastructure.jdbc_reader import JDBCReader
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)


template = """Dear Administrator,

The recent synchronization process has finished.

Please review the resources below and make the necessary manual adjustments.

Resources:
{}
Please manually update the resources listed above and verify the changes.
"""

resource_template = """> {facility_name} {facility_code} ({operation})
        {geo_zone_mismatch}
        {facility_type_mismatch}
"""


def _emails_formatter(facilities_response):
    emails = []
    current_email = ""
    max_facilities_per_email = 500
    subject_prefix = "Facilities Sync Completion Notification"

    for i, resource in enumerate(facilities_response):
        resource_info = resource_template.format(
            facility_name=resource["name"],
            facility_code=resource["code"],
            operation=resource["operation"],
            geo_zone_mismatch=f"Geographic Zone: {resource['municipality']}",
            facility_type_mismatch=f"Facility Type: {resource['type']}",
        )

        if (i % max_facilities_per_email) == 0 and i != 0:
            emails.append((f"{subject_prefix} - Part {len(emails) + 1}", current_email))
            current_email = ""

        current_email += resource_info + "\n\n"

    # Add the last email
    if current_email:
        emails.append((f"{subject_prefix} - Part {len(emails) + 1}", current_email))

    formatted_emails = []
    for subject, email_body in emails:
        formatted_emails.append((subject, template.format(email_body.strip())))

    return formatted_emails


def _is_uuid_valid(uuid_string):
    try:
        # Attempt to create a UUID object from the string
        val = uuid.UUID(uuid_string, version=4)
        return True
    except ValueError:
        # If it raises a ValueError, it's not a valid UUID
        return False


def __collect_user_contact_emails():
    config = Config().jdbc_reader
    right_id = Config().sync.email_report_role_right_uuid
    if not right_id or not _is_uuid_valid(right_id):
        raise KeyError(
            f"Email report notification through the role right was triggered but the configured "
            + f"email_report_role_right_uuid (`{right_id}`) is either invalid or has incorrect format."
        )
    jdbc_reader = JDBCReader(config)
    contact_details = ContactDetailsOfUsesrRepository(jdbc_reader).get_all()

    filtered_contacts = contact_details.filter(col("rightid") == right_id).filter(
        "email is not NULL"
    )
    
    if filtered_contacts.count() == 0: 
        logger.warning(
            "Email report notification through the role right was triggered, "+
            F"but no recipients were found eligible for role right id `{right_id}`. " +
            "Is the value correct? ")

    unique_emails = (
        filtered_contacts.select("email")
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    
    return unique_emails
    

def notify_administrator(
    sync_results: List[dict],
    nofify_mailing_list: bool = False,
    notify_users_with_role: bool = False,
):
    emails = _emails_formatter(sync_results)
    smtp_client = SMTPClient(Config().smtp)

    if nofify_mailing_list:
        for receiver in Config().sync.email_report_list:
            for subject, content in emails:
                smtp_client.send_email(receiver, subject, content)

    if notify_users_with_role:
        emails = __collect_user_contact_emails()

        for email in emails:
            for subject, content in emails:
                smtp_client.send_email(email, subject, content)
