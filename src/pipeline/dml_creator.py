from src.dbutils.connection import run_query
import re,os
from src.fileutils.browser import traverse, file_skip
# from src.pipeline.target_merge_queries import process_patients_new_to_target_merge_rule
from src.pipeline.stats import add_stats_for_table
import datetime
from src.pipeline.state import pipelineState
from pathlib import Path
# from datetime import datetime
from src.sync_ingest.utils_ingest.utils import insert_query, update_query



def run_dml(client, file_filter=None, skipcount = 0, ruleName='-', dryRun=None, run_state=None):
    file_paths = traverse(folder_name = 'dml-queries', filter=file_filter)
    file_paths = sorted(file_paths, key=extract_number)
    
    itercount = -1
    pipelineState['stepCounter'] = pipelineState['stepCounter']  + 1
    print(f' counter {0} '.format(pipelineState['stepCounter']))
    all_queries = []

    cur_date = datetime.datetime.now().date()
    start_time = datetime.datetime.now()
    pipelineid = ''    
    type = file_filter
    end_time = None
    total_exec_time = None
    pipelineid = 'testclient'+'-'+cur_date.strftime("%Y%m%d")
    taskname = file_filter
    insert_query(client, pipelineid,type,taskname,'in-progress',start_time)
    
    for file_path in file_paths:
        itercount += 1
        if file_skip(filename=file_path):
            continue
        # if run_state and run_state['runMode'] == 'resume' and run_state['lastFile'] is not None:
        if run_state and run_state['runMode'] == 'resume' and run_state['filenames'] is not None and len(run_state['filenames']) > 0:
            current_filename = Path(file_path).stem
            current_filename = current_filename + "#"+ (ruleName or '-')
            print("{} ===============> skipping for {} - {}".format(itercount, file_path, current_filename))

            if current_filename in run_state['filenames']:
                continue
            else:
                print(run_state['filenames'])
                run_state['lastFile'] = None
                run_state['runMode'] = None
            # lastRuleName = run_state['ruleName']
            # if run_state['lastFile'] in file_path and lastRuleName == ruleName:
            #     
            # continue

        now = datetime.datetime.now()
        # current_time = now.strftime("%H:%M:%S")
        print("Current Time =", str(now))

        print("{} ===============> running for {}".format(itercount, file_path))

        if (skipcount > 0 and itercount < skipcount):
            print("skipped the processing")
            continue

        with open(file_path) as f:
            # lines = f.readlines()
            file_data = f.read()
            query = file_data
            # if ruleName is not None:
            #     query = process_rule(query, ruleName, file_path)
            # print(query)
            if dryRun:
                all_queries.append(query)
                continue

            tableName = get_table_name(query)

            # print("tablename ################"+(tableName))

            if tableName is not None and dryRun is not True:
                add_stats_for_table(client, file_path, 'before', tableName, 0, ruleName)

            first_time = datetime.datetime.now()
            run_query(client, query, skip_result=True)
            difference = datetime.datetime.now() - first_time
        
            # get table count in stats.
            # if tableName is not None:
            if tableName is not None and dryRun is not True:
                add_stats_for_table(client, file_path, 'after', tableName, int(difference.total_seconds()*1000), ruleName)
    end_time = datetime.datetime.now()
    total_exec_time = end_time - start_time
    update_query(client, pipelineid,type,taskname,'success',end_time,total_exec_time)
    if dryRun:
        print(all_queries)
    return all_queries


def extract_number(filename):     
    basename = os.path.basename(filename)     
    filename_without_extension = os.path.splitext(basename)[0]     
    return int(filename_without_extension.split('_')[0]) 


def create_sp(file_filter, all_queries, ruleName):

    dmlQueries = ";;;  \r\n ".join(all_queries)
    suffix = "alter" if ruleName and len(ruleName) > 2 else "default"
    procedure_name = '''[s_etl_merged].[step_{}_{}]'''.format(file_filter, suffix)
    print(procedure_name)
    drop_query = '''
     IF OBJECT_ID ( '{}', 'P' ) IS NOT NULL
        DROP PROCEDURE {};
    '''.format(procedure_name, procedure_name)

    run_query(drop_query, skip_result=True)

# @startindexParam [bigint]
    final_query = '''
    CREATE PROCEDURE {}
    
        AS BEGIN
            {};
        END;
    '''.format(procedure_name, dmlQueries)
    print("now running sp create")
    # with open(procedure_name+'.sql', 'w') as f:
    #     f.write(final_query)

    run_query(final_query, skip_result=True)

def get_table_name(query):
    if 'create table' in query or 'insert into' in query:
        texttoSearch = 'create table' if 'create table ' in query else 'insert into' 
        regex = '{} .*`(.*\..*)`'.format(texttoSearch)
        m = re.search(regex, query)
        if m and len(m.groups()) > 0:
            tablename = m.groups()[0]
            return tablename

# def process_rule(query, ruleName, file_path):
#     if ruleName == "patients_new_to_target_merge":
#         return process_patients_new_to_target_merge_rule(query, file_path)
#     return query


def add_raw_table_stats(client):
    file_paths = traverse(folder_name = 'ddl-queries', filter='etl_raw')
    for file_path in file_paths:
        print("===============> running for {}".format(file_path))
        with open(file_path) as f:
            # lines = f.readlines()
            file_data = f.read()
            query = file_data
            tableName = get_table_name(query)
            if tableName is not None:
                add_stats_for_table(client, file_path, 'before', tableName, -1, '')

