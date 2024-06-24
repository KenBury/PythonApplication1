# html_report.py
from asyncio import proactor_events
from multiprocessing import Process, process
from telnetlib import EL
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
    
    def convert_utc_to_eastern(Z_timestamp_str):
        if not Z_timestamp_str or Z_timestamp_str.lower() == 'none':
            return None
        
        # Convert the timestamp from string to datetime object
        timestamp = datetime.strptime(Z_timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
        
        # Set the timezone to UTC
        timestamp = timestamp.replace(tzinfo=pytz.UTC)
        
        # Convert the timezone to Eastern Time Zone
        eastern = pytz.timezone('US/Eastern')
        timestamp_eastern = timestamp.astimezone(eastern)
        
        # Return the timestamp in a normal datetime string format
        return timestamp_eastern.strftime("%Y-%m-%d %H:%M:%S")

    def _preprocess_dataframe(self, key: str, dataframe: pd.DataFrame) -> pd.DataFrame:
        if key == 'runbooks':
            # Specific preprocessing steps for the 'runbooks' dataframe
            # Example: Renaming columns, filtering rows, etc.
            processed_dataframe = self._process_runbooks_dataframe(dataframe)
        if key == 'comments':
            processed_dataframe = self._process_comments_dataframe(dataframe)
        
        elif key == 'streams':
            # Specific preprocessing steps for the 'streams' dataframe
            # Example: Calculating additional columns, removing duplicates, etc.
            processed_dataframe = self._process_streams_dataframe(dataframe)
        
        elif key == 'folders':
            # Specific preprocessing steps for the 'folders' dataframe
            # Example: Calculating additional columns, removing duplicates, etc.
            processed_dataframe = self._process_folders_dataframe(dataframe)
        else:
            # Default: return the dataframe as is
            processed_dataframe = dataframe
        return processed_dataframe

    def _process_runbooks_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        # Example of processing for 'runbooks' dataframe
        runbooks_column_select = [

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
        
        runbooks_column_rename = {

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

 

        processed_df = dataframe[runbooks_column_select].rename(columns=runbooks_column_rename)
        
        def create_http_link(row): return f'<a href="https://bankofamerica.cutover.com/#/app/GBT/runbooks/{row["id"]}/current_version/tasks/dashboard/1" target="_blank">{row["runbook_name"]}</a>'
        
        processed_df.loc[:, 'runbook_http_link'] = processed_df.apply(create_http_link,axis=1)
        
        def extract_crq_string(input_string):

            # define regex pattern for 'CRQ' followed by 12 digits

            pattern = r'CRQ\d{12}'

            # search for the pattern in the input string

            match = re.search(pattern,input_string)

            if match:

                return match.group(0)

            else:

                return "CRQ not entered"


        def create_CRQ_number(row): return extract_crq_string(row["runbook_name"])



        processed_df.loc[:, 'CRQ_number'] = processed_df.apply(create_CRQ_number,axis=1)
        
        # List of column names to apply the function to
        columns_to_convert = ['timestamp_col1', 'timestamp_col2']



        # Apply the function to each column in the DataFrame
        processed_df[columns_to_convert] = processed_df[columns_to_convert].applymap(convert_utc_to_eastern)

        
        return processed_df
    
    def _process_comments_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        # Group by 'runbook_id' and aggregate comments into an unindexed HTML list
        #comments_rpt_df = comments_df.groupby('runbook_id')['comment_attributes_content'].agg(lambda x: '<br>'.join(f'{i+1}. {comment}' for i, comment in enumerate(x))).reset_index()
        processed_df = dataframe.groupby('runbook_id')['comment_attributes_content'].agg(lambda x: '<ul>' + ''.join(f'<li>{comment}</li>' for comment in x) + '</ul>').reset_index()
        # Refactored the comments_col_rename dictionary variable name to follow snake_case naming convention
        comments_column_rename = {
            "comment_attributes_content": "comments"
            }
        processed_df = processed_df.rename(columns=comments_column_rename)
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
    



    def _process_streams_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:

 

        processed_df = dataframe[dataframe['stream_attributes_name'].isin(['Code installation','Technical certification','Business certification'])].copy()



        streams_column_select = [
        
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

 

 
        streams_column_rename = {
        
            "stream_attributes_end_display": "end_display",
        
            "stream_attributes_end_latest_planned": "end_latest_planned",
        
            "stream_attributes_end_planned": "end_planned",
        
            "stream_attributes_name": "stream_name",
        
            "stream_attributes_start_display": "start_latest_display",
        
            "stream_attributes_start_latest_planned": "start_latest_planned",
        
            "stream_meta_tasks_count": "tasks_count",
        
            "stream_meta_completed_tasks_count": "completed_tasks_count"
        
        }
        
        processed_df = processed_df[streams_column_select].rename(columns=streams_column_rename)
        
        streams_pivot_columns = [

            "start_latest_planned",

            "start_latest_display",

            "end_display",

            "end_latest_planned",

            "end_planned",

            "tasks_count",

            "completed_tasks_count"

            ]



        processed_df = processed_df.pivot(index='runbook_id', columns='stream_name', values=streams_pivot_columns).reset_index()



        processed_df.columns = [f'{col[1].replace(" ", "_")}_{col[0]}' for col in processed_df.columns]


        return processed_df



    def _generate_html_report(self, dataframe: pd.DataFrame) -> str:
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
        folders = dataframe['folder_name'].unique()

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

            folder_df = dataframe[dataframe['folder_name'] == folder].sort_values(by='subfolder_name')

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
            elif key == 'folders':
                if merged_df is not None:
                    #runbooks_folders_rpt_df = pd.merge(runbooks_rpt_df, folders_rpt_df, left_on='relationships_folder_data_id', right_on = 'id', how='left', suffixes=('', '_folder'))
                    merged_df = pd.merge(merged_df, processed_df, left_on='relationships_folder_data_id', right_on = 'id', how='left', suffixes=('', '_folder'))
            elif key == 'streams':
                if merged_df is not None:
                    #runbooks_folders_streams_rpt_df = pd.merge(runbooks_folders_rpt_df, streams_rpt_df, left_on='id', right_on = 'runbook_id', how='left')
                    #runbooks_folders_streams_rpt_df.fillna('  ', inplace=True)
                    merged_df = pd.merge(merged_df, processed_df, left_on='id', right_on = 'runbook_id', how='left')
                    merged_df.fillna('  ', inplace=True)
            elif key == 'comments':
                if merged_df is not None:
                    #runbooks_folders_streams_comments_rpt_df = pd.merge(runbooks_folders_streams_rpt_df, comments_rpt_df, left_on='id', right_on = 'runbook_id', how='left', suffixes=('', '_comment'))
                    merged_df = pd.merge(merged_df, processed_df, left_on='id', right_on = 'runbook_id', how='left', suffixes=('', '_comment'))
        return merged_df


