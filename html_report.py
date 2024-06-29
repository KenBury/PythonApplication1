# html_report.py


from base_classes import HTMLReportGenerator
import pandas as pd
import pytz
from datetime import date, datetime
from datetime import timedelta
import re
import openpyxl



class HTMLReportGeneratorImpl:
    def __init__(self):
        # Initialization code without template_path
        pass

    def generate_report(self, data: dict) -> str:
        # Generate HTML report using the provided data
        merged_df = self._merge_progressive_dataframes(data)
        #print(f'generate report {merged_df.columns}')
        report = self._generate_html_report(merged_df)
        return report
    
    def parse_timestamp(self, timestamp_str):
                formats_to_try = ['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S']
    
                for date_format in formats_to_try:
                    try:
                        parsed_date = datetime.strptime(timestamp_str, date_format)
                        return parsed_date
                    except ValueError:
                        pass
    
                # Handle case where none of the formats matched
                raise ValueError("Timestamp format not recognized")

    
    def convert_utc_to_eastern_formated(self, Z_timestamp_str):
            if not Z_timestamp_str or Z_timestamp_str.lower() == 'none':
                return ""
           
            timestamp = self.parse_timestamp(Z_timestamp_str)
            
        
            # Set the timezone to UTC
            timestamp = timestamp.replace(tzinfo=pytz.UTC)
        
            # Convert the timezone to Eastern Time Zone
            eastern = pytz.timezone('US/Eastern')
            timestamp_eastern = timestamp.astimezone(eastern)
        
            # Return the timestamp in a normal datetime string format
            return timestamp_eastern.strftime("%b %d %H:%M")
 

    def _prepare_data(self, data: dict) -> dict:
        prepared_data = {}
        for key, value in data.items():
            #print(f'prepare_data: {key}')
            if isinstance(value, pd.DataFrame):
                prepared_data[key] = self._preprocess_dataframe(key, value)
            else:
                prepared_data[key] = value
        
        #print('prepared_data processing done')
        return prepared_data
    
  
    def _preprocess_dataframe(self, key: str, dataframe: pd.DataFrame) -> pd.DataFrame:
        if key == 'runbooks':
            # Specific preprocessing steps for the 'runbooks' dataframe
            # Example: Renaming columns, filtering rows, etc.
            processed_dataframe = self._process_runbooks_dataframe(dataframe)
        elif key == 'comments':
            #print('preprocessing comments')
            processed_dataframe = self._process_comments_dataframe(dataframe)
            
        elif key == 'tasks':
            # Specific preprocessing steps for the 'tasks' dataframe
            # Example: Calculating additional columns, removing duplicates, etc.
            processed_dataframe = self._process_tasks_dataframe(dataframe)
        
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
        #print('processing runbooks')
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
            
            "attributes_end_scheduled",

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
            "attributes_run_type": "runbook_run_type",
            "attributes_stage": "runbook_stage",
            "attributes_created_at": "runbook_created_at",

            "attributes_start_planned": "runbook_start_planned",

            "attributes_start_scheduled": "runbook_start_scheduled",

            "attributes_start_actual": "runbook_start_actual",

            "attributes_end_forecast": "runbook_end_forecast",

            "attributes_end_planned": "runbook_end_planned",
            "attributes_end_scheduled": "runbook_end_scheduled",
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
        

        
        
        def format_timedelta(td):
            if isinstance(td, timedelta) and not td.total_seconds() != td.total_seconds():
                   # Determine if the timedelta is positive or negative
                   sign = '+ ' if td.total_seconds() >= 0 else '- '
                   
                   # Use the absolute value of the timedelta for calculation
                   total_seconds = abs(int(td.total_seconds()))

                   # Calculate the number of whole years
                   years = total_seconds // (365 * 86400)
                   total_seconds %= (365 * 86400)

                   # Calculate the number of whole months
                   months = total_seconds // (30 * 86400)
                   total_seconds %= (30 * 86400)

                   # Calculate the number of whole weeks
                   weeks = total_seconds // (7 * 86400)
                   total_seconds %= (7 * 86400)

                   # Calculate the number of whole days
                   days = total_seconds // 86400
                   total_seconds %= 86400

                   # Calculate the number of whole hours
                   hours = total_seconds // 3600
                   total_seconds %= 3600

                   # Calculate the number of whole minutes
                   minutes = total_seconds // 60

                   # Calculate the number of whole seconds
                   seconds = total_seconds % 60

                   # Create a list of (value, unit) tuples with single-character units
                   time_units = [
                       (years, "y"),
                       (months, "m"),
                       (weeks, "w"),
                       (days, "d"),
                       (hours, "h"),
                       (minutes, "m"),
                       (seconds, "s")
                   ]

                   # Find the first non-zero value
                   for i, (value, unit) in enumerate(time_units):
                       if value > 0:
                           first_significant = f"{sign}{value}{unit}"
                           if i + 1 < len(time_units) and time_units[i + 1][0] > 0:
                               next_significant = f"{time_units[i + 1][0]}{time_units[i + 1][1]}"
                               return f"{first_significant} {next_significant}"
                           return first_significant
 
        
        return processed_df
    
    def _process_comments_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        
        # Define the regex pattern
        pattern = r'^Failed to receive message Twilio response.*'

        # Apply the logic to filter and remove rows based on the regex match
        dataframe = dataframe[~dataframe['comment_attributes_content'].apply(lambda x: bool(re.match(pattern, x)))]

        # Reset the index after removing rows
        dataframe.reset_index(drop=True, inplace=True)
        
        # Check for empty dataframe and just add expected columns
        if dataframe.empty:
            dataframe = pd.DataFrame(columns=['runbook_id','comments'])
            return dataframe
        
        # Define the regex pattern
        pattern = r'^Failed to receive message Twilio response.*'

        # Apply the logic to filter and remove rows based on the regex match
        dataframe = dataframe[~dataframe['comment_attributes_content'].apply(lambda x: bool(re.match(pattern, x)))]

        # Reset the index after removing rows
        dataframe.reset_index(drop=True, inplace=True)
        
        # Check for empty dataframe and just add expected columns
        if dataframe.empty:
            dataframe = pd.DataFrame(columns=['runbook_id','comments'])
            return dataframe
        
        
        # Group by 'runbook_id' and aggregate comments into an unindexed HTML list
        # Define a custom aggregation function to format comments as HTML list
        def format_comments_as_html_list(comments):
            html_list = '<ul>' + ''.join([f'<li>{comment}</li>' for comment in comments]) + '</ul>'
            return html_list
        
        
        # Group by 'runbook_id' and aggregate 'comment_attributes_content' as HTML list
        processed_df = dataframe.groupby('runbook_id')['comment_attributes_content'].agg(format_comments_as_html_list).reset_index()

        comments_column_rename = {
            "comment_attributes_content": "comments"
            }
        processed_df = processed_df.rename(columns=comments_column_rename)
        

        return processed_df

    def _process_folders_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        # Example of processing for 'folders' dataframe
        #print('processing folders')
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
    

    def _process_tasks_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:

        #print('processing tasks')

        processed_df = dataframe[dataframe['task_attributes_name'].isin(['Start Code installation','End Code installation','Start Technical certification','End Technical certification','Start Business certification','End Business certification'])].copy()



        tasks_column_select = [
        
            "runbook_id",
        
            "task_attributes_name",
        
            "task_attributes_start_actual",
        
            "task_attributes_start_display",
            "task_attributes_start_planned",
        
            "task_attributes_end_planned",
        
            "task_attributes_end_actual",
        
            "task_attributes_end_display"
        
                    ]

 

 
        tasks_column_rename = {
            
            "runbook_id": "task_runbook_id",
        
            "task_attributes_name": "task_name",
        
            "task_attributes_end_actual": "task_end_actual",
        
            "task_attributes_end_display": "task_end_display",
        
            "task_attributes_end_planned": "task_end_planned",
        
            "task_attributes_start_actual": "task_start_actual",
        
            "task_attributes_start_display": "task_start_display",
        
            "task_attributes_start_planned": "task_start_planned",
        
           
        
        }
        
        processed_df = processed_df[tasks_column_select].rename(columns=tasks_column_rename)
        
        tasks_pivot_columns = [

            "task_end_actual",

            "task_end_display",

            "task_end_planned",

            "task_start_actual",

            "task_start_display",

            "task_start_planned"

            ]



        processed_df = processed_df.pivot(index='task_runbook_id', columns='task_name', values=tasks_pivot_columns).reset_index()



        processed_df.columns = [f'{col[1].replace(" ", "_")}_{col[0]}' for col in processed_df.columns]
        

        
        #print(f'process stream {processed_df.columns}')




        return processed_df


    def _process_streams_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:

        #print('processing streams')

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
            
            "runbook_id": "stream_runbook_id",
                  
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



        processed_df = processed_df.pivot(index='stream_runbook_id', columns='stream_name', values=streams_pivot_columns).reset_index()



        processed_df.columns = [f'{col[1].replace(" ", "_")}_{col[0]}' for col in processed_df.columns]
        
        #print(f'processed stream {processed_df.columns}')


        return processed_df

    def _merge_progressive_dataframes(self, data: dict) -> pd.DataFrame:
        processed_data = {}
        for key, value in data.items():
            if isinstance(value, pd.DataFrame):
                processed_data[key] = self._preprocess_dataframe(key, value)
            else:
                processed_data[key] = value
                
        #print('processed_data processing done')
        

        merged_df = None
        #print('starting merge')
        for key, processed_df in processed_data.items():
            if key == 'runbooks':
                merged_df = processed_df.copy()
                #print(f'after merge {key} {merged_df.columns}')
            elif key == 'folders':
                if merged_df is not None:
                    #runbooks_folders_rpt_df = pd.merge(runbooks_rpt_df, folders_rpt_df, left_on='relationships_folder_data_id', right_on = 'id', how='left', suffixes=('', '_folder'))
                    #print(f'before merge {key} {merged_df.columns}')
                    merged_df = pd.merge(merged_df, processed_df, left_on='relationships_folder_data_id', right_on = 'id', how='left', suffixes=('', '_folder'))
                    #print(f'after merge {key} {merged_df.columns}')
            elif key == 'streams':
                if merged_df is not None:
                    #runbooks_folders_streams_rpt_df = pd.merge(runbooks_folders_rpt_df, streams_rpt_df, left_on='id', right_on = 'runbook_id', how='left')
                    #runbooks_folders_streams_rpt_df.fillna('  ', inplace=True)
                    #print(f'before merge {key} {merged_df.columns}')
                    #print(processed_df.columns)
                    merged_df = pd.merge(merged_df, processed_df, left_on='id', right_on = '_stream_runbook_id', how='left')
                    merged_df.fillna('', inplace=True)
                    #print(f'after merge {key} {merged_df.columns}')
            elif key == 'tasks':
                if merged_df is not None:
                    #print(f'before merge {key} {merged_df.columns}')
                    #print(processed_df.columns)
                    merged_df = pd.merge(merged_df, processed_df, left_on='id', right_on = '_task_runbook_id', how='left')
                    #print(f'after merge {key} {merged_df.columns}')
            elif key == 'comments':
                if merged_df is not None:
                    #print(f'before merge {key} {merged_df.columns}')
                    #runbooks_folders_streams_comments_rpt_df = pd.merge(runbooks_folders_streams_rpt_df, comments_rpt_df, left_on='id', right_on = 'runbook_id', how='left', suffixes=('', '_comment'))
                    merged_df = pd.merge(merged_df, processed_df, left_on='id', right_on = 'runbook_id', how='left', suffixes=('', '_comment'))
                    
                    #print(f'after merge {key} {merged_df.columns}')
        return merged_df

    def _generate_html_report(self, dataframe: pd.DataFrame) -> str:
        # Actual logic to generate the HTML report using data and template
        # Example implementation
        
        
        #print(f'generate html {dataframe.columns}')
        #print(f'generate html {dataframe.head}')
        
        dataframe.to_excel('output/html_df.xlsx', index=False)
       
        folders = dataframe['folder_name'].unique()
        
        #print(f'generate html folders: {folders}')
        
        html_content = """
        <body> 
        <html>

        <head>

        <style>

            .dataframe {

                border-collapse: collapse;

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
            
            #print(f'generate html {folder_df.columns}')

            html_content += f'<h2>{folder}</h2>'

            html_content += """

            <table class="dataframe">

                <thread>

                    <tr><th rowspan="2">CRQ</th><th rowspan="2">Subfolder</th><th colspan="5">Runbook</th>

                        <th colspan="3">Code installation</th>

                        <th colspan="3">Technical certification</th>

                        <th colspan="3">Business certification</th>

                        <th rowspan="2">Comments</th></tr>

                   <tr><th>Dashboard</th><th>Stage</th><th>Start</th><th>End</th><th>Tasks complete</th><th>Start</th><th>End</th><th>Tasks complete</th><th>Start</th><th>End</th><th>Tasks complete</th><th>Start</th><th>End</th><th>Tasks complete</th></tr>

                </thread>

                <tbody>

            """

            for _, row in folder_df.iterrows():
                runbook_run_type = row['runbook_run_type']
                runbook_stage = row['runbook_stage']
                
                runbook_start_planned = self.convert_utc_to_eastern_formated(row['runbook_start_planned'])
                runbook_start_actual = self.convert_utc_to_eastern_formated(row['runbook_start_actual'])
                runbook_end_planned = self.convert_utc_to_eastern_formated(row['runbook_end_planned'])
                runbook_end_actual = self.convert_utc_to_eastern_formated(row['runbook_end_actual'])
                runbook_start_display = f"{runbook_start_actual}" if runbook_start_actual != "" else f"{runbook_start_planned}"
                runbook_end_display = f"{runbook_end_actual}" if runbook_end_actual != "" else f"{runbook_end_planned}"

                runbook_tasks_count = row['runbook_tasks_count']
                runbook_completed_tasks_count = row['runbook_completed_tasks_count']
                
                tasks_count_display = f"{runbook_completed_tasks_count}/{runbook_tasks_count}" if runbook_tasks_count and runbook_completed_tasks_count else f"0/{runbook_tasks_count}"
                
                code_installation_tasks_count = row['Code_installation_tasks_count']
                code_installation_completed_tasks_count = row['Code_installation_completed_tasks_count']
                code_installation_display = f"{code_installation_completed_tasks_count}/{code_installation_tasks_count}" if code_installation_tasks_count != "" and code_installation_completed_tasks_count != "" else f"no stream"
                #code_installation_display = f"{code_installation_completed_tasks_count}/{code_installation_tasks_count}" if code_installation_tasks_count and code_installation_completed_tasks_count else f""
                start_code_installation_task_start_display = self.convert_utc_to_eastern_formated(row['Start_Code_installation_task_start_display'])
                code_installation_start_display  = f"{start_code_installation_task_start_display}" if start_code_installation_task_start_display != "" else f"no start task"
                end_code_installation_task_end_display = self.convert_utc_to_eastern_formated(row['End_Code_installation_task_end_display'])
                code_installation_end_display  = f"{end_code_installation_task_end_display}" if end_code_installation_task_end_display != "" else f"no end task"

                technical_certification_tasks_count = row['Technical_certification_tasks_count']
                technical_certification_completed_tasks_count = row['Technical_certification_completed_tasks_count']
                technical_certification_display = f"{technical_certification_completed_tasks_count}/{technical_certification_tasks_count}" if technical_certification_tasks_count != "" and technical_certification_completed_tasks_count != "" else f"no stream"
                start_technical_certification_task_start_display = self.convert_utc_to_eastern_formated(row['Start_Technical_certification_task_start_display'])
                technical_certification_start_display  = f"{start_technical_certification_task_start_display}" if start_technical_certification_task_start_display != "" else f"no start task"
                end_technical_certification_task_end_display = self.convert_utc_to_eastern_formated(row['End_Technical_certification_task_end_display'])
                technical_certification_end_display  = f"{end_technical_certification_task_end_display}" if end_technical_certification_task_end_display != "" else f"no end task"
                
                business_certification_tasks_count = row['Business_certification_tasks_count']
                business_certification_completed_tasks_count = row['Business_certification_completed_tasks_count']
                business_certification_display = f"{business_certification_completed_tasks_count}/{business_certification_tasks_count}" if business_certification_tasks_count != "" and business_certification_completed_tasks_count != "" else f"no stream"
                start_business_certification_task_start_display = self.convert_utc_to_eastern_formated(row['Start_Business_certification_task_start_display'])
                business_certification_start_display  = f"{start_business_certification_task_start_display}" if start_business_certification_task_start_display != "" else f"no start task"
                end_business_certification_task_end_display = self.convert_utc_to_eastern_formated(row['End_Business_certification_task_end_display'])
                business_certification_end_display  = f"{end_business_certification_task_end_display}" if end_business_certification_task_end_display != "" else f"no end task"
                
                html_content += f"""
                <tr>
                <td>{row['CRQ_number']}</td>
                <td>{row['subfolder_name']}</td>
                <td>{row['runbook_http_link']}</td>
                <td>{row['runbook_run_type']}<br>{row['runbook_stage']}</td>
                <td>{runbook_start_display}</td>
                <td>{runbook_end_display}</td>
                <td>{tasks_count_display}</td>
                <td>{code_installation_start_display}</td>
                <td>{code_installation_end_display}</td>
                <td>{code_installation_display}</td>
                <td>{technical_certification_start_display}</td>
                <td>{technical_certification_end_display}</td>
                <td>{technical_certification_display}</td>
                <td>{business_certification_start_display}</td>
                <td>{business_certification_end_display}</td>
                <td>{business_certification_display}</td>
                <td>{row['comments']}</td>
    </tr>
   """
            html_content += """

                </tbody>

            </table>

            <br>

            """



        html_content += f"""Note: The start and end times are in the Eastern Time Zone and may be forecasted if not actual. This report was generated {datetime.now(pytz.timezone('US/Eastern')).strftime("%b %d %H:%M")}
        </body></html>"""
        
        return html_content
    
  


