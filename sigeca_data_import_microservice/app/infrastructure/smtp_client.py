import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from app.config import SMTPClientConfig


class SMTPClient:
    def __init__(self, config: SMTPClientConfig):
        self._url = config.server_url
        self._port = config.server_port
        self._username = config.username
        self._password = config.password
        self._sender = config.sender

    def send_email(self, address: str, subject: str, message: str):
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self._url, self._port, context=context) as smtp:
                smtp.login(self._username, self._password)
                msg = MIMEMultipart()
                msg["From"] = self._sender
                msg["To"] = address
                msg["Subject"] = subject
                # Attach the email body to the MIME message
                msg.attach(MIMEText(message, "plain"))
                # Send the email
                smtp.send_message(msg)
        except Exception as e:
            logging.error("Failed to send email via SMTP: %s", str(e), exc_info=e)
            raise e
