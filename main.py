
# main.py
from html_report import HTMLReportGeneratorImpl
from email_sender import EmailSenderImpl

def main():
    html_report_generator = HTMLReportGeneratorImpl()
    email_sender = EmailSenderImpl()

    # Usage of the HTML report generator and email sender
    data = {...}  # Data for the report
    report = html_report_generator.generate_report(data)
    email_sender.send_email('recipient@example.com', 'Report', report)

if __name__ == "__main__":
    main()