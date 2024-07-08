from typing import List

from app.config import Config
from app.infrastructure.smtp_client import SMTPClient


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
            operation=resource['operation'],
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


def notify_administrator(sync_results: List[dict], mailing_list: List[str]):
    emails = _emails_formatter(sync_results)
    smtp_client = SMTPClient(Config().smtp)

    for receiver in mailing_list:
        for subject, content in emails:
            smtp_client.send_email(receiver, subject, content)