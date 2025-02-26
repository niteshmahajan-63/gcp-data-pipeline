import os, sys
from src.dbutils.connection import read_config
from src.fileutils.browser import traverse
from src.pipeline.dml_creator import get_table_name
from src.sync_ingest.utils_ingest.utils import run_query,insert_query, update_query
from src.pipeline.stats import add_stats_for_table
from pathlib import Path
import datetime

filename =os.path.basename(__file__).split('.')[0]
config_file = "config.ini"
config = read_config(config_file)
project_id = config['gcp']['project_id']
tasklist = {'sync_rawinput.patients_json': 'joiningdate', 'sync_rawinput.billings_json': 'bookingdate', 'sync_rawinput.labreports_json': 'authenticateddate'}
taskmapping = {'sync_rawinput.patients_json': 'patient', 'sync_rawinput.billings_json': 'billing', 'sync_rawinput.labreports_json': 'labreport'}
raw_dataset = 'sync_rawinput'
file_filter="raw_input"
raw_watermark = 'sync_metadata.raw_watermark'

class syncRawData():
    def start(self, time, client, queue_obj=None):
        try:
            cur_date = datetime.datetime.now().date()
            start_time = datetime.datetime.now()
            pipelineid = ''
            taskname = 'raw-ingestion'
            type = 'rawingest'
            end_time = None
            total_exec_time = None
            pipelineid = 'testclient'+'-'+cur_date.strftime("%Y%m%d")
            insert_query(client, pipelineid,type,taskname,'in-progress',start_time)

            print('running for raw ingestion....')            
            file_paths = traverse(folder_name = 'dml-queries', filter=file_filter)
            for file_path in file_paths:
                print("===============> running for {}".format(file_path))
                current_filename = Path(file_path).stem
                ruleName = current_filename + "#"+ ('-')
                with open(file_path) as f:
                    file_data = f.read()
                    query = file_data
                    tableName = get_table_name(query)
                    
                    if tableName is not None:
                        add_stats_for_table(client, file_path, 'before', tableName, 0, ruleName)                        

                    # truncate existing table
                    trunc_query = f"""truncate table {tableName}"""
                    print(trunc_query)
                    run_query(client, trunc_query)

                    first_time = datetime.datetime.now()
                    run_query(client, query)
                    difference = datetime.datetime.now() - first_time                
                    # get table count in stats.
                    if tableName is not None:
                        add_stats_for_table(client, file_path, 'after', tableName, int(difference.total_seconds()*1000), ruleName)
                   
                    #update raw watermark table with max date
                    getmaxquery = "SELECT MAX({}) FROM {}".format(tasklist.get(tableName), tableName)
                    # print(getmaxquery)
                    result = run_query(client, getmaxquery)
                    updaterawwatermarkquery = "UPDATE {} SET value = '{}' where taskname='{}'".format(raw_watermark,result[0][0], taskmapping.get(tableName)) 
                    # print(updaterawwatermarkquery)
                    run_query(client, updaterawwatermarkquery)

            end_time = datetime.datetime.now()
            total_exec_time = end_time - start_time
            update_query(client, pipelineid,type,taskname,'success',end_time,total_exec_time)
            print('raw ingestion completed...')

        except Exception as error :
            print('>>>>> {} :: got exception  '.format(filename))
            if queue_obj:
                queue_obj.put(sys.exc_info())
            end_time = datetime.now()
            total_exec_time = end_time - start_time     
            update_query(client, pipelineid,type,taskname,'fail',end_time,total_exec_time)      

            raise error    
        

def moveRawDatatoBackupTable(client):  
    print("#####moving data from raw to backup table start#####")
    for task in tasklist:            
        backuptable = task.replace('_json', '_json_backup')
        # print(task)
        query = f"""insert into {backuptable} select * from {task}"""
        print(query)
        run_query(client, query)
    print("#####moving data from raw to backup table completed#####")    


