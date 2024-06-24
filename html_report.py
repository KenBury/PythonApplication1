# html_report.py
from multiprocessing import process
from base_classes import HTMLReportGenerator
import pandas as pd

class HTMLReportGeneratorImpl(HTMLReportGenerator):
    def __init__(self, template_path: str):
        self.template_path = template_path

    def generate_report(self, data: dict) -> str:
        # Generate HTML report using the provided data and template
        report = self._generate_html_report(data)
        return report
    
    def _prepare_data(self, data: dict) -> dict:
        prepared_data = {}
        for key, value in data.items():
            if isinstance(value, pd.DataFrame):
                prepared_data[key] = self._preprocess_dataframe(key, value)
            else:
                prepared_data[key] = value
        return prepared_data

    def _preprocess_dataframe(self, key: str, dataframe: pd.DataFrame) -> pd.DataFrame:
        if key == 'runbooks':
            # Specific preprocessing steps for the 'runbooks' dataframe
            # Example: Renaming columns, filtering rows, etc.
            processed_dataframe = self._process_runbooks_dataframe(dataframe)
        elif key == 'tasks':
            # Specific preprocessing steps for the 'tasks' dataframe
            # Example: Calculating additional columns, removing duplicates, etc.
            processed_dataframe = self._process_tasks_dataframe(dataframe)
        else:
            # Default: return the dataframe as is
            processed_dataframe = dataframe
        return processed_dataframe

    def _process_runbooks_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        # Example of processing for 'runbooks' dataframe
        processed_df = dataframe.rename(columns={'old_column_name': 'new_column_name'})
        return processed_df

    def _process_folders_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        # Example of processing for 'folders' dataframe
 
        name_dict = dataframe.set_index('id')['attributes_name'].to_dict()

        merged_folders_df = dataframe.copy()
        merged_folders_df['folder_name'] = merged_folders_df['relationships_parent_data_id'].apply(lambda x: name_dict.get(x, ''))
 
        def create_subfolder_name(row):
        
            if row['folder_name'] != '':
        
                return row['attributes_name']
        
            else:
        
                return ''
        
 


        merged_folders_df.loc[:, 'subfolder_name'] = merged_folders_df.apply(create_subfolder_name,axis=1)


        def fill_folder_name(row):
        
            if row['folder_name'] != '':
        
                return row['folder_name']
        
            else:
        
                return row['attributes_name']

        merged_folders_df.loc[:, 'folder_name'] = merged_folders_df.apply(fill_folder_name,axis=1)
         
        processed_df = merged_folders_df[['id', 'folder_name', 'subfolder_name']]
         
        return processed_df

    def _generate_html_report(self, data: dict) -> str:
        # Actual logic to generate the HTML report using data and template
        # Example implementation
        html_report = "<html><body>"
        for key, value in data.items():
            html_report += f"<h2>{key}</h2>"
            if isinstance(value, pd.DataFrame):
                html_report += value.to_html()
            else:
                html_report += str(value)
        html_report += "</body></html>"
        return html_report
    
    def _merge_progressive_dataframes(self, data: dict) -> pd.DataFrame:
        processed_data = {}
        for key, value in data.items():
            if isinstance(value, pd.DataFrame):
                processed_data[key] = self._preprocess_dataframe(key, value)

        merged_df = None
        for key, processed_df in processed_data.items():
            if key == 'runbooks':
                merged_df = processed_df.copy()
            elif key == 'streams':
                if merged_df is not None:
                    merged_df = merged_df.merge(processed_df, on='runbook_id', how='left')
            elif key == 'comments':
                if merged_df is not None:
                    merged_df = merged_df.merge(processed_df, on='runbook_id', how='left')
            # Add more conditions for other named dataframes as needed

        return merged_df

import json

import pickle

import re

from datetime import datetime

import pytz

from pprint import pprint

import pandas as pd

import xlsxwriter

from openpyxl import Workbook

from openpyxl.utils.dataframe import dataframe_to_rows

from openpyxl.styles import Font, PatternFill

 

 

 



tasks_df

 

 

# In[4]:

 

 

folders_df.to_excel("folders.xlsx",engine='openpyxl')

 

 

# In[5]:

 

 



 

 

# In[ ]:

 

 

 

 

 

# In[15]:

 

 

runbooks_df

           

 

 

# In[16]:

 

 

runbooks_df.to_excel("runbooks.xlsx",engine='openpyxl')

 

 

# In[17]:

 

 

runbooks_col_lst = [

    "id",

    "attributes_name",

    "attributes_created_at",

    "attributes_start_planned",

    "attributes_start_scheduled",

    "attributes_start_actual",

    "attributes_end_forecast",

    "attributes_end_planned",

    "attributes_end_actual",

    "attributes_auto_start",

    "attributes_run_type",

    "attributes_stage",

    "attributes_timezone",

    "attributes_timing_mode",

    "relationships_folder_data_id",

    "relationships_runbook_type_data_id",

    "relationships_workspace_data_id",

    "meta_tasks_count",

    "meta_completed_tasks_count"

    ]

 

 

# In[18]:

 

 

runbooks_col_rename = {

    "attributes_name": "runbook_name",

    "attributes_created_at": "runbook_created_at",

    "attributes_start_planned": "runbook_start_planned",

    "attributes_start_scheduled": "runbook_start_scheduled",

    "attributes_start_actual": "runbook_start_actual",

    "attributes_end_forecast": "runbook_end_forecast",

    "attributes_end_planned": "runbook_end_planned",

    "attributes_end_actual": "runbook_end_actual",

    "meta_tasks_count": "runbook_tasks_count",

    "meta_completed_tasks_count": "runbook_completed_tasks_count"

}

 

 

# In[19]:

 

 

runbooks_rpt_df = runbooks_df.copy()

runbooks_rpt_df = runbooks_rpt_df[runbooks_col_lst]

runbooks_rpt_df = runbooks_rpt_df.rename(columns=runbooks_col_rename)

 

 

# In[20]:

 

 

runbooks_rpt_df

 

 

# In[ ]:

 

 

 

 

 

# In[21]:

 

 

def create_http_link(row): return f'<a href="https://bankofamerica.cutover.com/#/app/GBT/runbooks/{row["id"]}/current_version/tasks/dashboard/1" target="_blank">{row["runbook_name"]}</a>'

 

 

# In[ ]:

 

 

 

 

 

# In[22]:

 

 

runbooks_rpt_df.loc[:, 'runbook_http_link'] = runbooks_rpt_df.apply(create_http_link,axis=1)

 

 

# In[23]:

 

 

def extract_crq_string(input_string):

    # define regex pattern for 'CRQ' followed by 12 digits

    pattern = r'CRQ\d{12}'

    # search for the pattern in the input string

    match = re.search(pattern,input_string)

    if match:

        return match.group(0)

    else:

        return "CRQ not entered"

   

 

 

# In[24]:

 

 

def create_CRQ_number(row): return extract_crq_string(row["runbook_name"])

 

 

# In[25]:

 

 

runbooks_rpt_df.loc[:, 'CRQ_number'] = runbooks_rpt_df.apply(create_CRQ_number,axis=1)

 

 

# In[26]:

 

 

def extract_datetime_UTC(Z_timestamp_str):

    if Z_timestamp_str and Z_timestamp_str != 'None':

        try:

            timestamp = datetime.strptime(Z_timestamp_str, "%Y-%m-%dT%H:%M:%SZ")

            timestamp = timestamp.replace(tzinfo=pytz.UTC)

            return timestamp

        except ValueError as e:

            print(f"Error extracting timestamp: {e}")

            return None

    else:

        return None

 

 

# In[ ]:

 

 

 

 

 

# In[27]:

 

 

print(extract_datetime_UTC('2024-06-16T19:15:00Z'))

 

 

# In[28]:

 

 

display(runbooks_rpt_df)

 

 

# In[29]:

 

 

comments_df

 

 

# In[30]:

 

 

comments_rpt_df = comments_df.groupby('runbook_id')['comment_attributes_content'].agg(lambda x: '<br>'.join(f'{i+1}. {comment}' for i, comment in enumerate(x))).reset_index()

 

 

# In[31]:

 

 

comments_rpt_df

 

 

# In[32]:

 

 

comments_col_rename = {

    "comment_attributes_content": "comments"

}

comments_rpt_df = comments_rpt_df.rename(columns=comments_col_rename)

 

 

# In[ ]:

 

 

 

 

 

# In[ ]:

 

 

 

 

 

# In[ ]:

 

 

 

 

 

# In[33]:

 

 

comments_df.to_excel("comments.xlsx",engine='openpyxl')

 

 

# In[34]:

 

 

streams_df

 

 

# In[35]:

 

 

streams_rpt_df = streams_df[streams_df['stream_attributes_name'].isin(['Code installation','Technical certification','Business certification'])].copy()

 

 

# In[36]:

 

 

streams_col_lst = [

    "runbook_id",

    "stream_attributes_name",

    "stream_attributes_start_latest_planned",

    "stream_attributes_start_display",

    "stream_attributes_end_planned",

    "stream_attributes_end_latest_planned",

    "stream_attributes_end_display",

    "stream_meta_completed_tasks_count",

    "stream_meta_tasks_count"

            ]

 

 

# In[37]:

 

 

streams_col_rename = {

    "stream_attributes_end_display": "end_display",

    "stream_attributes_end_latest_planned": "end_latest_planned",

    "stream_attributes_end_planned": "end_planned",

    "stream_attributes_name": "stream_name",

    "stream_attributes_start_display": "start_latest_display",

    "stream_attributes_start_latest_planned": "start_latest_planned",

    "stream_meta_tasks_count": "tasks_count",

    "stream_meta_completed_tasks_count": "completed_tasks_count"

}

 

 

# In[38]:

 

 

streams_rpt_df = streams_rpt_df[streams_col_lst]

 

 

# In[39]:

 

 

streams_rpt_df = streams_rpt_df.rename(columns=streams_col_rename)

 

 

# In[40]:

 

 

streams_rpt_df

 

 

# In[ ]:

 

 

 

 

 

# In[41]:

 

 

streams_pivot_lst = [

    "start_latest_planned",

    "start_latest_display",

    "end_display",

    "end_latest_planned",

    "end_planned",

    "tasks_count",

    "completed_tasks_count"

        ]

 

 

# In[42]:

 

 

streams_rpt_df = streams_rpt_df.pivot(index='runbook_id', columns='stream_name', values=streams_pivot_lst)

 

 

# In[43]:

 

 

streams_rpt_df

 

 

# In[44]:

 

 

streams_rpt_df.columns = [f'{col[1].replace(" ", "_")}_{col[0]}' for col in streams_rpt_df.columns]

 

 

# In[ ]:

 

 

 

 

 

# In[45]:

 

 

streams_rpt_df = streams_rpt_df.reset_index()

 

 

# In[ ]:

 

 

 

 

 

# In[46]:

 

 

streams_rpt_df

 

 

# In[ ]:

 

 

 

 

 

# In[47]:

 

 

runbooks_folders_rpt_df = pd.merge(runbooks_rpt_df, folders_rpt_df, left_on='relationships_folder_data_id', right_on = 'id', how='left', suffixes=('', '_folder'))

 

 

# In[ ]:

 

 

 

 

 

# In[48]:

 

 

runbooks_folders_rpt_df.to_excel("runbooks_folders.xlsx",engine='openpyxl')

 

 

# In[49]:

 

 

runbooks_folders_rpt_df

 

 

# In[50]:

 

 

runbooks_folders_streams_rpt_df = pd.merge(runbooks_folders_rpt_df, streams_rpt_df, left_on='id', right_on = 'runbook_id', how='left')

 

 

# In[51]:

 

 

runbooks_folders_streams_rpt_df.fillna('  ', inplace=True)

 

 

# In[52]:

 

 

runbooks_folders_streams_rpt_df

 

 

# In[53]:

 

 

runbooks_folders_streams_rpt_df.to_excel("runbooks_folders_streams.xlsx",engine='openpyxl')

 

 

# In[54]:

 

 

runbooks_folders_streams_comments_rpt_df = pd.merge(runbooks_folders_streams_rpt_df, comments_rpt_df, left_on='id', right_on = 'runbook_id', how='left', suffixes=('', '_comment'))

 

 

# In[55]:

 

 

runbooks_folders_streams_comments_rpt_df

 

 

# In[ ]:

 

 

 

 

 

# In[56]:

 

 

runbooks_folders_streams_comments_rpt_df.to_excel("runbooks_folders_streams_comments.xlsx",engine='openpyxl')

 

 

# In[ ]:

 

 

 

 

 

# In[57]:

 

 

def generate_html_report(df):

    folders = df['folder_name'].unique()

    html_content = """

    <html>

    <head>

    <style>

        .dataframe {

            border-collapse: colapse;

            width: 100%;

            margin-bottom: 1em;

            border: 1px solid black;

        }

        .dataframe th, .dataframe td {

        border: 1px solid black;

        padding: 8px;

        text-align: left;

        }

        .dataframe th {

            background-color: #b3d4ff;

        }

        body {

            font-family: Ariel, sans-serif;

            }

        h2 {

            color: #333;

            {

    </style>

    </head>

    <body>

    """

 

    for folder in folders:

        folder_df = df[df['folder_name'] == folder].sort_values(by='subfolder_name')

        html_content += f'<h2>{folder}</h2>'

        html_content += """

        <table border="1" class="dataframe">

            <thread>

                <tr><th rowspan="2">CRQ</th><th rowspan="2">Subfolder</th><th colspan="4">Runbook</th>

                    <th colspan="3">Code installation</th>

                    <th colspan="3">Technical certification</th>

                    <th colspan="3">Business certification</th>

                    <th rowspan="2">Comments</th></tr>

               <tr><th>Dashboard</th><th>Start</th><th>End</th><th>Tasks complete</th><th>Start</th><th>End</th><th>Tasks complete</th><th>Start</th><th>End</th><th>Tasks complete</th><th>Start</th><th>End</th><th>Tasks complete</th></tr>

            </thread>

            <tbody>

        """

        for _, row in folder_df.iterrows():

            html_content += f"""

            <tr>

                <td>{row['CRQ_number']}</td>

                <td>{row['subfolder_name']}</td>

                <td>{row['runbook_http_link']}</td>

                <td>P:{row['runbook_start_planned']}<br>S:{row['runbook_start_scheduled']}<br>A:{row['runbook_start_actual']}</td>

                <td>P:{row['runbook_end_planned']}<br>F:{row['runbook_end_forecast']}<br>A:{row['runbook_end_actual']}</td>

                <td>{row['runbook_completed_tasks_count']}/{row['runbook_tasks_count']}</td>

                <td>P:{row['Code_installation_start_latest_planned']}<br>D:{row['Code_installation_start_latest_display']}</td>

                <td>P:{row['Code_installation_end_latest_planned']}<br>F:{row['Code_installation_end_planned']}<br>A:{row['Code_installation_end_display']}</td>

                <td>{row['Code_installation_completed_tasks_count']}/{row['Code_installation_tasks_count']}</td>

                <td>P:{row['Technical_certification_start_latest_planned']}<br>D:{row['Technical_certification_start_latest_display']}</td>

                <td>P:{row['Technical_certification_end_latest_planned']}<br>F:{row['Technical_certification_end_planned']}<br>A:{row['Technical_certification_end_display']}</td>

                <td>{row['Technical_certification_completed_tasks_count']}/{row['Technical_certification_tasks_count']}</td>

                <td>P:{row['Business_certification_start_latest_planned']}<br>D:{row['Business_certification_start_latest_display']}</td>

                <td>P:{row['Business_certification_end_latest_planned']}<br>F:{row['Business_certification_end_planned']}<br>A:{row['Business_certification_end_display']}</td>

                <td>{row['Business_certification_completed_tasks_count']}/{row['Business_certification_tasks_count']}</td>

                <td>{row['comments']}</td>

            </tr>

            """

        html_content += """

            </tbody>

        </table>

        <br>

        """

 

    html_content += '</body></html>'

 

    return html_content

       

        

 

 

# In[58]:

 

 

html_report = generate_html_report(runbooks_folders_streams_comments_rpt_df)

 

 

# In[59]:

 

 

with open('final_report.html', 'w') as file:

    file.write(html_report)

 

 

# In[60]:

 

 

print(html_report)

 

 

# In[ ]:

 

 

 

 

 

# In[ ]:

 

 

 