
# main.py
import pandas as pd
from html_report import HTMLReportGeneratorImpl
from email_sender import EmailSenderImpl

def main():
    html_report_generator = HTMLReportGeneratorImpl()
    email_sender = EmailSenderImpl()
    
    
    runbooks_df = pd.read_pickle("C:/Users/kenbu/source/repos/PythonApplication1/runbooks.pkl")
    tasks_df = pd.read_pickle("C:/Users/kenbu/source/repos/PythonApplication1/tasks.pkl")
    comments_df = pd.read_pickle("C:/Users/kenbu/source/repos/PythonApplication1/comments.pkl")
    streams_df = pd.read_pickle("C:/Users/kenbu/source/repos/PythonApplication1/streams.pkl")
    folders_df = pd.read_pickle("C:/Users/kenbu/source/repos/PythonApplication1/folders.pkl")

    
    # Data for the report  
    data = {
        "runbooks": runbooks_df,
        "tasks": tasks_df,
        "comments": comments_df,
        "streams": streams_df,
        "folders": folders_df
    } 
    # Usage of the HTML report generator and email sender
    html_report = html_report_generator.generate_report(data)
    with open('final_report.html', 'w') as file:
        file.write(html_report)
    
    email_sender.send_email('recipient@example.com', 'Report', html_report)

if __name__ == "__main__":
    main()