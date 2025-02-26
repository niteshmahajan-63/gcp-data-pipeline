from src.dbutils.connection import run_query, isTableExists
import os
from pathlib import Path

statsTableName = 'sync_metadata.dml_stats'

def update_latest_state_filename(client, run_state):
    # query = '''
    # SELECT top 1 filename, tablename, rulename, time 
    # From 
    # [s_pre_merge].[dml_stats]
    # where state='after'

    # order by time desc
    # '''
    query = f'''
    SELECT 
         CONCAT(filename, '#', COALESCE(rulename, '-')) as filename
    From 
        {statsTableName}
    where state='after'
        and 
        time >
        (
            select time 
            From 
            {statsTableName}
            where state='after'
            and filename ='main'
            order by time desc limit 1
        )
    order by time desc
    '''
    print(query)
    rows = run_query(client, query, fetch_all_rows=True)
    if not rows or len(rows) == 0:
        return None
    filenames = []
    for row in rows:
        filenames.append(row[0])
    if len(filenames) == 0:
        print("last step found returning")
        run_state['runMode'] = None
        return
    run_state['filenames'] = filenames


def add_stats_for_table(client, file_path, state, tablename, duration, rulename):
    if tablename == '-' or tablename is None:
        return
    create_stats_table(client)
    # os.path.basename(file_path)
    fileName = Path(file_path).stem
    rulename = rulename if rulename or len(rulename) >1 else '-'
    tablename = tablename or '-'
    
    isexists = isTableExists(client, tablename)
    if isexists:
        query = '''
            INSERT INTO `{}` (tablename, filename, state, duration, rulename, count, time, labname)
            select tablename, filename, state, duration, rulename, count, time, labname
            from
            (
            select '{}' as tablename, '{}' as filename, '{}' as state, {} as duration, '{}' as rulename, count(*) as count,
            DATETIME(CURRENT_TIMESTAMP,"+05:30") as time, 'testclient' as labname
            from `{}`
            group by labname
            union all
            select '{}' as tablename, '{}' as filename, '{}' as state, {} as duration, '{}' as rulename, -1 as count,
            DATETIME(CURRENT_TIMESTAMP,"+05:30") as time, 'testclient' as labname
            ) as a
            order by count desc limit 1
        '''.format(statsTableName, tablename, fileName, state, duration, rulename, tablename, tablename, fileName, state, duration, rulename)
        
        # print(query)
        run_query(client, query, skip_result=True)

    if not isexists and '_dump' in tablename:
        query = '''
        INSERT INTO `{}` (tablename, filename, state, duration, rulename, count, time, labname)
        values ('{}', '{}','{}',{},'{}', -1,DATETIME(CURRENT_TIMESTAMP,"+05:30"), 'testclient')
        '''.format(statsTableName, tablename, fileName, state, duration,rulename)
        # print(query)
        run_query(client, query, skip_result=True)

    return

def create_stats_table(client):
    
    isexists = isTableExists(client, statsTableName)
    if isexists:
        print('Table already exists!!')
    else:
        query = '''
        CREATE TABLE `{}`(
            tablename string(100),
            rulename string(100),
            filename string(256),
            state string(32),
            duration int64,
            count int64,
            time datetime,
            labname string(64)
            );
        '''.format(statsTableName)
        # print(query)
        run_query(client, query, skip_result=True)


    