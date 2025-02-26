from dmlqueriescreator.constant.loadallconst import querygenerator
from src.pipeline.stats import update_latest_state_filename, create_stats_table, add_stats_for_table, statsTableName
from src.pipeline.dml_creator import run_dml
from src.dbutils.connection import run_query, create_client
from src.sync_ingest.ingestworker import ingestworker
from src.validation.runvalidationtask import validationtask
from datetime import datetime,timedelta
import traceback, sys
from src.sync_ingest.sync_dumptorawdata import moveRawDatatoBackupTable
from src.sync_ingest.sync_sourcedata import moveDumpDatatoBackupTable, updateDumpWatermarkTable
from src.sync_ingest.sync_reload.sync_data_remove import remove_merge_data,remove_dump_raw_data,remove_billing_data,remove_labreport_data, update_watermark
from src.dbutils.connection import read_config
from src.sync_ingest.sync_derivatives import runDerivativeQueries
from src.sync_ingest.utils_ingest.utils import mailauditrecords, update_synccompletionflag_centaliseddb
from pretty_html_table import build_table
config_file = "config.ini"
config = read_config(config_file)

labname = config['gcp']['labname']
runMode = config['gcp']['runmode']
dryRun = False
run_state = {
    'runMode' : runMode,
    'lastFile' : None,
    'ruleName': '',
    'filenames': []
}

def update_run_state(client, run_state):
    update_latest_state_filename(client, run_state)
    
def start(client,sync_ingest_only, skipcount= 0, runMode='start'):
    try:
        print('starting pipeline!!!!!')
        run_state['runMode'] = runMode
        
        
        if config['gcp']['reload_fullmerge'] == 'True':
            print('deleting all merge table data')
            remove_merge_data(client)

        if config['gcp']['reload_dump_raw'] == 'True':
            print('deleting all dump and raw table data') 
            remove_dump_raw_data(client)
        
        if config['gcp']['reload_billing'] == 'True':
            print('deleting all billing table data')
            remove_billing_data(client)
            print('updating dump watermark value for billing!')
            update_watermark(client, 'billing')

        if config['gcp']['reload_labreports'] == 'True': 
            print('deleting all labreport table data')
            remove_labreport_data(client)
            print('updating dump watermark value for labreport!')
            update_watermark(client, 'labreport')
    
   
        if dryRun is not True:
            print('create or update state of the pipeline!!!')
            create_stats_table(client)
            update_run_state(client, run_state)

        validationtask().pre_merge_check()           
               
        status =[]
        status.append({"task": "Raw Load", "completed_at": "Pending", "time_taken": '-'})
        status.append({"task": "Merge Load", "completed_at": "Pending", "time_taken": "-"})
        status.append({"task": "CTA Load", "completed_at": "Pending", "time_taken": '-'})
        status.append({"task": "Derivative Load", "completed_at": "Pending", "time_taken": "-"})
        initial_time = datetime.utcnow()
        ist_offset = timedelta(hours=5, minutes=30)
        initial_time = initial_time + ist_offset
        # raw data load
        ingestworker().startingestworker(client)

        raw_load_completed_at = datetime.utcnow()
        raw_load_completed_at = raw_load_completed_at + ist_offset
        raw_load_time_taken = (raw_load_completed_at - initial_time).seconds // 60
        status[0]['completed_at']= raw_load_completed_at
        status[0]['time_taken']= raw_load_time_taken

        if sync_ingest_only == 'True':
            exit()  
        
        run_dml(client, file_filter="load_constants", skipcount=skipcount, dryRun=dryRun, run_state=run_state)

        run_dml(client, file_filter="pre_merge", skipcount=skipcount, dryRun=dryRun, run_state=run_state)
        
        run_dml(client, file_filter="merge_inputs", skipcount=skipcount, dryRun=dryRun, run_state=run_state)
        
        run_dml(client, file_filter="patients_merge", skipcount=skipcount, dryRun=dryRun, run_state=run_state)

        run_dml(client, file_filter="load_bill_n_bio", skipcount=skipcount, dryRun=dryRun, run_state=run_state)

        merge_load_completed_at = datetime.utcnow()
        merge_load_completed_at = merge_load_completed_at + ist_offset
        merge_load_time_taken = (merge_load_completed_at - raw_load_completed_at).seconds // 60 
        status[1]['completed_at']= merge_load_completed_at
        status[1]['time_taken']= merge_load_time_taken

        # add json data to backup tables
        moveRawDatatoBackupTable(client)
        # move dump data to backup tables
        moveDumpDatatoBackupTable(client) 
        # update last sync date in watermark table
        updateDumpWatermarkTable(client)

        run_dml(client, file_filter="load_cta_full",  skipcount=skipcount, dryRun=dryRun, run_state=run_state)

        
        cta_load_completed_at = datetime.utcnow()
        cta_load_completed_at = cta_load_completed_at + ist_offset
        cta_time_taken = ((cta_load_completed_at - merge_load_completed_at).seconds // 60)
        status[2]['completed_at']= cta_load_completed_at
        status[2]['time_taken']= cta_time_taken

         


        runDerivativeQueries(client)
        derivative_load_completed_at = datetime.utcnow()
        derivative_load_completed_at = derivative_load_completed_at + ist_offset
        derivative_time_taken = ((derivative_load_completed_at - cta_load_completed_at).seconds // 60)
        status[3]['completed_at']= derivative_load_completed_at
        status[3]['time_taken']= derivative_time_taken

        # update mergeprocessed in db
        query = f"""update `sync_metadata.dbtasks` set lastrundate='{datetime.utcnow()}' where taskname = 'mergeprocessed'"""
        run_query(client, query)

        update_synccompletionflag_centaliseddb(datetime.utcnow())

        mailauditdf = mailauditrecords(client)
        if mailauditdf.empty:
            audit_mailbody = ''
            sub_keyword = ' '
            print("No long running task found")
        else:
            mailtext = "<html><p><b>Please find Audit Logs with Processing Time > 15 Minutes:\n\n</b></p></html>"
            audit_tablehtml = build_table(mailauditdf,'red_dark', font_size='small', 
                            font_family='Open Sans, sans-serif',text_align = 'right')

            audit_mailbody = mailtext + audit_tablehtml
            sub_keyword = ' Critical '   

        validationtask().post_merge_status(status, audit_mailbody, sub_keyword)      

        validationtask().start(client)
        
        if dryRun is not True:
            add_stats_for_table(client, 'main.py', 'after', statsTableName, 0, '')

    except Exception as e:
        print(f"An error occurred: {str(e)}")      
        traceback.print_exc(limit=1, file=sys.stdout)
        print("exiting main with exception")
        traceback_message = traceback.format_exc(limit=1)
        validationtask().send_pipeline_failure_alert(status,e,traceback_message, labname = "Testclient")


   
