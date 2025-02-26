
import json as jsonutil
import time, os, json,requests, io
from src.dbutils.connection import read_config
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
filename =os.path.basename(__file__).split('.')[0]

config_file = "config.ini"
config = read_config(config_file)
project_id = config['gcp']['project_id']

task_table_map ={
    'patient' : f'''{project_id}.sync_rawinput.patients_json''',    
    'billing' : f'''{project_id}.sync_rawinput.billings_json''',
    'labreport' : f'''{project_id}.sync_rawinput.labreports_json'''
}


_float_fields = [
        "totalgrossamount", "totalnetamount", "totalpatientamount", "totaldiscount", "totaltax", "grossamount", "patientamount", "netamount", "discount", "tax", "totalcompanyamount", "refundamount", "depositamount", "balance", "ignoreitemamount", "insuranceamount"
    ]


def get_list(dataarray:dict, pgcolumns: list, meta: dict):
    datalist = []
    for doc in dataarray:
        doc['labname'] = meta.get('labname')
        newdoc ={}
        for k in pgcolumns:
            if not doc.get(k):
                newdoc[k] = None
            else:
                if k in _float_fields and type(doc[k]) != float:
                    newdoc[k] = float(doc[k])
                else:    
                    newdoc[k] = doc[k]
        datalist.append(newdoc)
        
    return datalist


def get_json_string(dataarray:dict, pgcolumns: list, meta: dict):
    output = ''
    for doc in dataarray:
        doc['labname'] = meta.get('labname')
        newdoc ={}
        for k in pgcolumns:
            if not doc.get(k):
                newdoc[k] = None
            else:
                if k in _float_fields and type(doc[k]) != float:
                    newdoc[k] = float(doc[k])
                else:
                    newdoc[k] = doc[k]
        output += json.dumps(newdoc,ensure_ascii=False) + '\n'
    return output

def get_insert_values(dataarray:dict, pgcolumns: list, meta: dict):
    output = []
    for doc in dataarray:
        doc['labname'] = meta.get('labname')
        
        newdoc ={}
        for val in pgcolumns:
            k = val['name']
            if not doc.get(k):
                newdoc[k] = None
            else:
                if k in _float_fields and type(doc[k]) != float:
                    newdoc[k] = float(doc[k])
                else:
                    newdoc[k] = doc[k]
        output.append(newdoc)
    return output

def create_schema(table_schema):
    ljc_schema = []
    for value in table_schema:
        ljc_schema.append(bigquery.SchemaField(value['name'], value['type']))
    return ljc_schema



# def save_result_to_db(data, client, meta:dict = None): 
#         taskname = meta.get('taskname')
#         dataarray = jsonutil.loads('['+data[:-1]+']')
                
#         pgcolumns, tablename = get_table_and_pgcolumn(taskname)
        
#         try:
#             table_ref = client.get_table(tablename)
#             f = io.StringIO("")
#             client.schema_to_json(table_ref.schema, f)
#             table_schema = json.loads(f.getvalue())
#             datalist = get_insert_values(dataarray, table_schema, meta)
#             ljc_schema = create_schema(table_schema)
#             df = pd.DataFrame(datalist)
#             for value in table_schema:
#                 if value['type'] == 'TIMESTAMP':
#                     df[value['name']] = pd.to_datetime(df[value['name']],format= '%Y-%m-%d %H:%M:%S')
#                 elif value['type'] == 'INTEGER':
#                     df[value['name']] = df[value['name']].astype('Int64')
#                 elif value['type'] == 'BOOLEAN':
#                     bt = {'true': True, 'True': True, 'false': False, 'False': False, '0': False, 0: False, '1': True, 1: True}
#                     df[value['name']] = df[value['name']].map(bt)
            
#             job_config = bigquery.LoadJobConfig(
#                 schema=ljc_schema,
#                 autodetect=False
#             )
#             job = client.load_table_from_dataframe(df, tablename, job_config=job_config)
#             job.result()
#             print(f"Data Loaded into {tablename}")
#             return
#         except Exception  as e:
#             raise Exception(f"Errors occurred during insertion: {e}")


# def get_table_and_pgcolumn(taskname:str):
#     pgcolumns= None
#     tablename = task_table_map[taskname]
#     pgcolumns = task_columns_map[taskname]
   
#     return  pgcolumns, tablename


def run_query(client, query, skip_result = False, fetch_all_rows= False):
    try:
        
        query_job = client.query(query)
        query_job.result()
        if query_job.errors and query_job.errors != []:
            raise Exception(f"{query_job.errors[0]['message']}")
        if (query.strip().lower().startswith("select") or query.strip().lower().startswith("with")) and skip_result is False:
            return list(query_job)
        else:
            return None
    except Exception as e:
        raise Exception(f"Query execution failed: {str(e)}")
    

def alertForNoData(client, content):
    labname = 'Testclient'
    subject =f'Sync Pipeline Critical Alert ::{labname} For Source Read'

    contents = f'''Hi Team, <br><br> Today no data found in source for below tasks. <br><br><b> {content}</b> <br>'''
    if 'patients_dump' in content: 
        contents  += f'''<p style="color: red;"><b>Merge pipeline stopped due to no patient data.</b></p>'''
    contents  += f'''<p>Thank you for your attention.</p>
            <p>Best regards,<br>Sync Team</p>'''

    req = {
            "emailList": ['nitesh.xortix@gmail.com'],
            "labname": labname,
            "content": contents,
            "from":"Sync Alerts",
            "subject":subject
        }

    try:
            response = requests.post(
                url='https://mddr7aigo9.execute-api.ap-south-1.amazonaws.com/test/sync/emailalert',
                data=json.dumps(req),
                headers={"content-type": "application/json"}
            )
            if response.status_code == 200:
                print(response.text)
            else:
                print("Error: Request failed with status code", response.status_code)

    except Exception as e:
            print("Error:", str(e))


def insert_query(client, pipelineid,type,taskname,status,start_time,end_time=None,total_exec_time=None,condition_start_value=None,condition_end_value=None,source_datacount=None,copy_datacount=None,error_msg=None,other=None, bulk_loading=None, dryRun=False, queue_obj=None):
    try:
        currentdate = datetime.now().date()
        auditquery_main = f""" insert into `sync_metadata.audit` (pipelineid,type,taskname,starttime,status,currentdate) values ('{pipelineid}','{type}','{taskname}','{start_time}','{status}','{currentdate}') """
        run_query(client, auditquery_main)

    except Exception as error :
        print('>>>>> {} :: got exception in inserting logs in audit table')
        raise error        
    

def update_query(client, pipelineid,type,taskname,status,end_time=None,total_exec_time=None,start_time=None,condition_start_value=None,condition_end_value=None,source_datacount=None,copy_datacount=None,error_msg=None,other=None, bulk_loading=None, dryRun=False, queue_obj=None):
        try:
            ex_status = ''
            if status:
                ex_status =f",status = '{status}'"
            ex_copydatacount = ''
            if copy_datacount:
                ex_copydatacount =f",copy_datacount = '{copy_datacount}'"
            
            currentdate = datetime.now().date()
            auditquery_up = f""" update `sync_metadata.audit` set endtime = '{end_time}', totalexecutiontime = '{total_exec_time}' {ex_status} {ex_copydatacount} where pipelineid = '{pipelineid}' and type = '{type}' and taskname = '{taskname}'"""

            run_query(client, auditquery_up)

        except Exception as error :
            print('>>>>> {} :: got exception in updating logs in audit table')
            raise error    
        
def mailauditrecords(client):
        cur_date = datetime.now().date()
        try:
            print('fetching audit records')
            
            query_text = f"""
            SELECT DISTINCT pipelineid, type, taskname, CAST(FORMAT_TIME('%T', TIME(totalexecutiontime)) AS STRING) AS totalexecutiontime, status FROM `sync_metadata.audit` WHERE currentdate = CURRENT_DATE() AND totalexecutiontime >= TIME '00:15:00';"""
            querydata = run_query(client, query_text)

            mailauditrecords = pd.DataFrame([(row[0], row[1], row[2], row[3], row[4]) for row in querydata], columns=['PipelineID', 'Type', 'Taskname','Timetaken','Status'])

            return mailauditrecords

        except Exception as error :
            print('>>>>> {} :: got exception in fetch audit records for mail..  '.format(filename))
            raise error        
        


def update_synccompletionflag_centaliseddb(time):

    req = {
        "labName":"onprem",
        "taskName":"synccompletion_flag",
        "staticToken":"test_token",
        "data":[
        {
            "lab_name": "Testclient",
            "taskname": "mergeprocessed",
            "lastrundate": str(time),
            "sync_column": str(time)
         
        } ]
    }

    try:
        response = requests.post(
            url='https://betadatasyncapi.hlthclub.in/ssdb/vault/syncV3-service/generic/insert-data',
            data=json.dumps(req),
            headers={"content-type": "application/json"}
        )

        if response.status_code == 200:
            print(response.text)
        else:
            print("Error: Request failed with status code", response.status_code)

    except Exception as e:
        print("Error:", str(e))
