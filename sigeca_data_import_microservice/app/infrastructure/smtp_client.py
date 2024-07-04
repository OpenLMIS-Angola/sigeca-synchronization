import logging
import smtplib
import ssl


class SMTPClient:
    def __init__(self, server_url: str, server_port: str | int, username: str, password: str, sender: str):
        self._url = server_url
        self._port = server_port
        self._username = username
        self._password = password
        self._sender = sender

    def send_email(self, address: str, subject: str, message: str):
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self._url, self._port, context=context) as smtp:
                smtp.login(self._username, self._password)
                smtp.sendmail(self._sender, address, f"Subject: {subject}\n\n{message}")
        except Exception as e:
            logging.error("Failed to send email via SMTP: %s", str(e), exc_info=e)
