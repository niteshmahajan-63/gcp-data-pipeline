import io
import json
import pandas as pd
import numpy as np
from google.cloud import bigquery
from src.dbutils.connection import create_client

'''List all the master tasks here (datasetname.tablename.filetype)
# Note: tablename filename should be same as tablename.'''

taskList = ["sync_merged.biomappings.xlsx"]

# Iterate over all task in the task list
for task in taskList:
    # Get the task details
    taskInfo = task.split('.')
    if len(taskInfo) != 3:
        continue
    fileName = taskInfo[1] + '.' + taskInfo[2]
    datasetName = taskInfo[0]
    targetTable = taskInfo[1]
    filetype = taskInfo[2]
    filePath = f"E:\\nitesh\\Documents\\testclient\masters\\{fileName}"
    if filetype == 'xlsx':
        df = pd.read_excel(filePath)
    elif filetype == 'csv':
        df = pd.read_csv(filePath)
    else:
        print('File type not supported!')
        continue

    try:
        # Get the bigquery client
        client,project_id, sync_ingest_only = create_client()

        # Get the table details
        tablename = f"{project_id}.{datasetName}.{targetTable}"
        table_ref = client.get_table(tablename)
        f = io.StringIO("")
        client.schema_to_json(table_ref.schema, f)
        table_schema = json.loads(f.getvalue())

        # Replace nan with None
        df = df.replace({np.nan: None})

        # Convert dataframe columns type
        for value in table_schema:
            if value['type'] == 'STRING':
                df[value['name']] = df[value['name']].astype('string')
                df[value['name']] = df[value['name']].str.lower()
            if value['type'] == 'TIMESTAMP':
                df[value['name']] = pd.to_datetime(df[value['name']],format= '%Y-%m-%d %H:%M:%S')
            elif value['type'] == 'INTEGER':
                df[value['name']] = df[value['name']].astype('Int64')
            elif value['type'] == 'BOOLEAN':
                bt = {'true': True, 'True': True, 'false': False, 'False': False, '0': False, 0: False, '1': True, 1: True}
                df[value['name']] = df[value['name']].map(bt)

        # adding leading zeros in string
        if targetTable == 'department_master':
            df['departmentcode']= df['departmentcode'].str.zfill(4)
            df['departmentname'] = df['departmentname'].str.lower()
        # Write data to bigquery table
        # Specify bigquery config
        jobConfig = bigquery.LoadJobConfig()
        jobConfig.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE   
        bigqueryJob = client.load_table_from_dataframe(df, table_ref, job_config=jobConfig)
        bigqueryJob.result()
        print(f'Data loaded into table {tablename}')
    except Exception as e:
        print(f'Failed to load data into master table {tablename}')
        print(f'Error: {e}')