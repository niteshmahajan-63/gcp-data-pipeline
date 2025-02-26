
import pandas as pd
import datetime, os
from src.sync_ingest.utils_ingest.utils import run_query,insert_query, update_query
filename =os.path.basename(__file__).split('.')[0]

def runDerivativeQueries(client):
    try:
        queryfetch = f"""select * from `sync_metadata.derivative_queries` where enable = true order by priority asc;"""
        derv_output = run_query(client,queryfetch)
        derv_df = pd.DataFrame(derv_output)
        print(derv_df)
        if derv_df.empty:
            return
        cur_date = datetime.datetime.now().date()
        start_time = datetime.datetime.now()
        pipelineid = ''
        taskname = 'derivative'
        type = 'derivative'
        end_time = None
        total_exec_time = None
        pipelineid = 'testclient'+'-'+cur_date.strftime("%Y%m%d")
        insert_query(client, pipelineid,type,taskname,'in-progress',start_time)
        for i in range(len(derv_df.values)):
            create_query = f""" 
                    drop table if exists cg_tg.{derv_df.values[i][0][0]};
                    create table cg_tg.{derv_df.values[i][0][0]} as {derv_df.values[i][0][1]};
                """

            run_query(client, create_query)
            print('{} table loaded successfully'.format(derv_df.values[i][0][0]))

        end_time = datetime.datetime.now()
        total_exec_time = end_time - start_time
        update_query(client, pipelineid,type,taskname,'success',end_time,total_exec_time)    

    except Exception as error :
        print('>>>>> {} :: got exception  '.format(filename))
        
        end_time = datetime.now()
        total_exec_time = end_time - start_time     
        update_query(client, pipelineid,type,taskname,'fail',end_time,total_exec_time)      

        raise error        