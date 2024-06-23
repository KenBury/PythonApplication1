
# email_sender.py
from base_classes import EmailSender
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText



class EmailSenderImpl(EmailSender):
    def send_email(self, recipient, subject, body):
        # Initialize OutlookEmailSender with specific Outlook server details
        outlook_email_sender = OutlookEmailSender('smtp.office365.com', 587, 'your_email@company.com', 'your_password')
        outlook_email_sender.send_email(recipient, subject, body)

# Usage example
# email_sender = EmailSenderImpl()
#from outlook_email_sender import OutlookEmailSender



class OutlookEmailSender(EmailSender):
    def __init__(self, smtp_server, smtp_port, sender_email, sender_password):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password

    def send_email(self, recipient, subject, body):
        msg = MIMEMultipart()
        msg['From'] = self.sender_email
        msg['To'] = recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.sender_email, self.sender_password)
            server.send_message(msg)
            server.quit()
            print("Email sent successfully.")
        except Exception as e:
            print(f"Failed to send email: {e}")

# Usage example
# email_sender = OutlookEmailSender('smtp.office365.com', 587, 'your_email@company.com', 'your_password')
# email_sender.send_email('recipient@example.com', 'Subject', 'Email body content')