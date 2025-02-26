from google.cloud import bigquery
import configparser
from google.cloud.exceptions import NotFound
import os


def read_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config
def create_client():
    try:
        config_file = "config.ini"
        config = read_config(config_file)
        project_id = config['gcp']['project_id']
        location =config['gcp']['location']
        billtype  = config['gcp']['billtype']
        sync_ingest_only = config['gcp']['sync_ingest_only']

       
        return bigquery.Client(project=project_id,location=location),project_id,sync_ingest_only
    except Exception as e:
        raise Exception(f"Failed to create BigQuery client: {str(e)}")

def run_query(client, query, skip_result = False, fetch_all_rows= False):
    # print(query)
    try:
        
        query_job = client.query(query)

       
        query_job.result()

        if query.strip().lower().startswith("select") and skip_result is False:
            return list(query_job)
        else:
            return None
    except Exception as e:
        raise Exception(f"Query execution failed: {str(e)}")

def readRows(result):
    records = result
    # print("Total rows are:  ", len(records))
    # print("Printing each row")
    return records

def isTableExists(client, tableId=None):
    try :
        client.get_table(tableId)
        return True
    except NotFound:
        return False


    
def createTable(client, table_id, schema):
    # table_id = "project-id.dataset.table_name"
    try:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print( "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id) )

        return True
    except Exception as e:
        print(f'Error creating table: {e}')
        return False    