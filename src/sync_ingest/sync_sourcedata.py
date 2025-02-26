import os, sys,datetime
# from datetime import datetime
from src.dbutils.connection import read_config
from src.pipeline.dml_creator import get_table_name
from src.sync_ingest.utils_ingest.utils import run_query, alertForNoData,insert_query, update_query
from src.pipeline.stats import add_stats_for_table
filename =os.path.basename(__file__).split('.')[0]
config_file = "config.ini"
config = read_config(config_file)
project_id = config['gcp']['project_id']
# tasklist = {'patients_dump': 'updateddate', 'billings_dump': 'bookingdate', 'labreports_dump':'authenticateddate'}
tasklist = {'patients_backup': 'updateddate', 'billings_backup': 'bookingdate', 'labreports_backup':'authenticateddate'}
tasknameMapping = {'patients_dump': 'patient', 'billings_dump': 'billing', 'labreports_dump':'labreport' }
dump_dataset = 'sync_dump'

class syncDumpData():
    def start(self, time, client, queue_obj=None):
        try:
            print('running for dump ingestion....')
            cur_date = datetime.datetime.now().date()
            start_time = datetime.datetime.now()
            pipelineid = ''
            taskname = 'dump-ingestion'
            type = 'dumpingest'
            end_time = None
            total_exec_time = None
            pipelineid = 'testclient'+'-'+cur_date.strftime("%Y%m%d")
            insert_query(client, pipelineid,type,taskname,'in-progress',start_time)
            # move data to backup tables
            # if config['gcp']['reload_dump_raw'] == 'False':
            #     self.moveDumpDatatoBackupTable(client)            

            # drop dump tables
            self.dropDumpTables(client)

            # check last inserted value from db
            taskwiseLastReadDate = self.getLastReadDateFromdb(client)
            print(taskwiseLastReadDate)

            # get task wise data from source and ingest in dump table
            if len(taskwiseLastReadDate):
                for task in taskwiseLastReadDate:
                    print(task.get('taskname'),'-----', task.get('value'))
                    query = self.getQueryBasedOnTaskname(task.get('taskname'),  task.get('value'))
                    print(query)
                    tableName = get_table_name(query)
                    
                    if tableName is not None:
                        add_stats_for_table(client, '--', 'before', tableName, 0, '#-')   

                    first_time = datetime.datetime.now()
                    run_query(client, query)
                    difference = datetime.datetime.now() - first_time                
                    # get table count in stats.
                    if tableName is not None:
                        add_stats_for_table(client, '--', 'after', tableName, int(difference.total_seconds()*1000), '#-')
                          
            # update last sync date in watermark table
            # self.updateDumpWatermarkTable(client)
            
            end_time = datetime.datetime.now()
            total_exec_time = end_time - start_time
            update_query(client, pipelineid,type,taskname,'success',end_time,total_exec_time)
            print('dump ingestion completed...')

        except Exception as error :
            print('>>>>> {} :: got exception  '.format(filename))
            if queue_obj:
                queue_obj.put(sys.exc_info())
            end_time = datetime.now()
            total_exec_time = end_time - start_time     
            update_query(client, pipelineid,type,taskname,'fail',end_time,total_exec_time)      

            raise error    


    def getLastReadDateFromdb(self, client):
        query = f"""select taskname, value from sync_metadata.dump_watermark"""
        lastReadValue =  run_query(client, query)
        return lastReadValue    


    def getQueryBasedOnTaskname(self, taskname, date):
        query = f"""create table """
        if taskname == tasknameMapping['patients_dump']:
            query += f"""`{dump_dataset}.patients_dump` as 
            with a11 as(select ptnt_id,ptnt_cd from `srl-labs-cdp.ORACLE_DATA.PTNT` group by 1,2 having count(distinct concat(ifnull(PTNT_FNM,''),' ',(ifnull(PTNT_LNM,''))))>1) SELECT case when lower(TRIM( NULLIF(a.PTNT_CD, '') )) is null then lower( TRIM(NULLIF(a.PTNT_ID, '') )) else lower(TRIM(NULLIF(a.PTNT_CD, '')) ) end as patientid, case when a11.ptnt_id is not null then true else false end as srcbaddata, case when a11.ptnt_id is not null then 'multiple name on same patientid and encounterid' else null end as srcbaddataremarks, lower(trim(a.PTNT_ID)) as encounterid, lower(trim(a.PTNT_CD)) as sourcepatientid, lower(trim(PTNT_TITLE)) as salutation,lower(trim(concat(trim(ifnull(PTNT_FNM,'')),' ',trim(ifnull(PTNT_LNM,''))))) as name, lower(trim(PTNT_FNM)) as firstname,lower(trim(PTNT_LNM)) as lastname, lower(concat(trim(IFNULL(PTNT_ADD,'')),' || ',trim(IFNULL(PTNT_ADD2,'')))) as address, lower(trim(PTNT_ADD)) as address1,lower(trim(PTNT_ADD2)) as address2, lower(trim(a.BU_ID)) as centreid, lower(trim(b.LCTN_CD)) as centrecode, lower(trim(b.LCTN_NM)) as centrename, lower(trim(PTNT_CITY)) as city, lower(trim(PTNT_STSTE)) as state, lower(trim(PTNT_COUNTRY)) as country, lower(trim(PTNT_ZIP)) as pin, lower(trim(PTNT_GNDR)) as gender, PTNT_DOB as dob, lower(trim(PTNT_EMAIL)) as email, lower(trim(PTNT_PHONE)) as alternatemobile, lower(trim(MOBILE_NO)) as mobile, lower(trim(PTNT_REM)) as remarks, COALESCE(a.CREATE_DT, a.MODIFIED_ON) as joiningdate, MODIFIED_ON as updateddate, UPD_DT as upd_dt, lower(trim(NATIONALITY)) as nationality, lower(trim(IS_VIP)) as vipflag, lower(trim(MARITAL_STATUS)) as marital_status, lower(trim(PTNT_ACTUAL_DOB_FLG)) as ptnt_actual_dob_flg, 'testclient' as labname,TIMESTAMP(CURRENT_TIMESTAMP()) as insertedon FROM `srl-labs-cdp.ORACLE_DATA.PTNT` a left join a11 on a.PTNT_ID=a11.ptnt_id and a.ptnt_cd=a11.ptnt_cd left join (select distinct lab_id ,LCTN_CD,LCTN_NM from `srl-labs-cdp.ORACLE_DATA.TESTCLIENT_CENTRE_LIST`)b on a.BU_ID=b.lab_id WHERE COALESCE(a.MODIFIED_ON, a.CREATE_DT)>TIMESTAMP("{date}") and COALESCE(MODIFIED_ON, CREATE_DT) < TIMESTAMP(CURRENT_DATE())
            
            """       
        elif taskname == tasknameMapping['billings_dump']:
            query += f"""`{dump_dataset}.billings_dump` as 
            with a12 as( select distinct ptnt_id from `srl-labs-cdp.ORACLE_DATA.PTNT` group by 1 having count(distinct ptnt_cd)>1 ),b12 as ( select distinct ptnt_id,ptnt_cd from `srl-labs-cdp.ORACLE_DATA.PTNT` group by 1,2 having count(distinct concat(ifnull(PTNT_FNM,''),' ',ifnull(PTNT_LNM,'')))>1 ) select case when lower(trim(NULLIF(b.PTNT_CD,''))) is null then lower(trim(NULLIF(a.PTNT_PTNT_ID,''))) else lower(trim(NULLIF(b.PTNT_CD,''))) end as patientid, case when b.ptnt_id is null then true else false end as srcbaddata, case when b.ptnt_id is null then 'patientid and encounter id does not exists in ptnt table' else null end as srcbaddataremarks, case when a12.ptnt_id is not null then true else false end as srcbaddata1, case when a12.ptnt_id is not null then 'multiple patientid on same encounterid' else null end as srcbaddataremarks1, case when b12.ptnt_id is not null then true else false end as srcbaddata2, case when b12.ptnt_id is not null then 'multiple name on same patientid and encounterid' else null end as srcbaddataremarks2, lower(trim(b.PTNT_CD)) as sourcepatientid, lower(trim(a.PTNT_PTNT_ID)) as encounterid, lower(trim(a.acc_id)) as bookingid, a.acc_dt as bookingdate, lower(trim(a.BU_ID)) as centreid, lower(trim(c.LCTN_CD)) as centrecode, lower(trim(c.LCTN_NM)) as centrename, lower(trim(a.ACC_TYP)) as channelid, lower(trim(cen.name)) as channel, lower(trim(a.PTNT_RMRK)) as remarks, case when d.parent_PRDCT_ID<>d.prdct_prdct_id then lower(trim(d.parent_PRDCT_ID)) else null end as packageid, case when d.parent_PRDCT_ID<>d.prdct_prdct_id then lower(trim(f.prdct_cd)) else null end as parentproductcode, case when d.parent_PRDCT_ID<>d.prdct_prdct_id then lower(trim(f.NAME)) else null end as packagename, lower(trim(d.prdct_PRDCT_ID)) as testid, lower(trim(g.prdct_cd)) as productcode, lower(trim(g.name)) as testname, lower(trim(g.DEPT_CODE)) as departmentid, lower(trim(dept.departmentname)) as department, e.NETWR as totalnetamount, lower(trim(g4.KTEXT)) as customertype, lower(trim(a1.RSLT_STATUS)) as billstatusid, lower(trim(bill.status)) as billstatus, lower(trim(d.SR_NO)) as itemrow, lower(trim(g3.kunnr)) as customerno, lower(trim(g3.ORT01)) as ort01, concat(lower(trim(ifnull(g3.name1,''))),'',lower(trim(ifnull(g3.name2,'')))) as recipient, lower(trim(a.party_id)) as partyid, lower(trim(c11.LCTN_NM)) as partyname, lower(trim(a.BILL_TO_PARTY_ID)) as billtopartyid, lower(trim(c12.LCTN_NM)) as billtoparty, (cast(g1.KZWI1 as Float64)+ cast(g1.MWSBP as Float64)) As grossamount, case when (cast(g1.KZWI6 as Float64)) >0 then (ifnull(cast(g1.KZWI1 as Float64),0) - (cast(g1.KZWI6 as Float64))) else case when ((ifnull(cast(g1.KZWI1 as Float64),0) - ifnull(cast(g1.NETWR as Float64),0)) >= 0) then (ifnull(cast(g1.KZWI1 as Float64),0) - ifnull(cast(g1.NETWR as Float64),0)) else -(ifnull(cast(g1.KZWI4 as Float64),0)) end end As discount, case when cast(g1.KZWI6 as float64) >0 then cast(g1.KZWI6 as float64) else case when ((ifnull(cast(g1.KZWI1 as float64),0) - ifnull(cast(g1.NETWR as float64),0)) < 0) then (ifnull(cast(g1.KZWI1 as float64),0) + ifnull(cast(g1.KZWI4 as float64),0) + ifnull(cast(g1.MWSBP as float64),0)) else ifnull(cast(g1.NETWR as float64),0) end end as netamount, cast(g1.MWSBP as float64) As tax, CASE WHEN lower(trim(lf.NAME1)) = lower(trim(lf.NAME2)) THEN lower(trim(lf.NAME1)) ELSE concat(lower(trim(lf.NAME1)) ,' ' ,lower(trim(lf.NAME2))) END AS referal1, lower(trim(lf.LIFNR)) AS referalid, Case When lower(trim(lf.LIFNR)) = 'r000005226' then lower(trim(e.ZZRFDRNAME)) else CASE WHEN lower(trim(lf.NAME1)) = lower(trim(lf.NAME2)) THEN lower(trim(lf.NAME1)) ELSE concat(lower(trim(lf.NAME1)) , ' ' , lower(trim(lf.NAME2))) END end as referalname, 'testclient' as labname, TIMESTAMP(CURRENT_TIMESTAMP()) as insertedon FROM `srl-labs-cdp.ORACLE_DATA.ACC` a left join a12 on a.PTNT_PTNT_ID=a12.PTNT_ID left join b12 on a.PTNT_PTNT_ID=b12.PTNT_ID left join `srl-labs-cdp.sync_dump.channel_master` cen on lower(cen.code)=lower(a.ACC_TYP) left join ( select distinct PTNT_ID,PTNT_CD from(select ROW_NUMBER () OVER (PARTITION BY PTNT_ID ORDER BY UPD_DT desc,MODIFIED_ON desc) as rnum, PTNT_ID,PTNT_CD from `srl-labs-cdp.ORACLE_DATA.PTNT` )x where x.rnum=1)b on a.PTNT_PTNT_ID=b.ptnt_id left join (select distinct lab_id ,LCTN_CD,LCTN_NM from `srl-labs-cdp.ORACLE_DATA.TESTCLIENT_CENTRE_LIST`)c on a.BU_ID=c.lab_id left join (select distinct lab_id ,LCTN_CD,LCTN_NM from `srl-labs-cdp.ORACLE_DATA.TESTCLIENT_CENTRE_LIST`)c11 on a.party_id=c11.lab_id left join (select distinct lab_id ,LCTN_CD,LCTN_NM from `srl-labs-cdp.ORACLE_DATA.TESTCLIENT_CENTRE_LIST`)c12 on a.BILL_TO_PARTY_ID=c12.lab_id left join (select distinct PRDCT_PRDCT_ID,ACC_ACC_ID,cast(SR_NO as int64)SR_NO,RSLT_STATUS from `srl-labs-cdp.ORACLE_DATA.ACC_PRDCT`) a1 on a1.ACC_ACC_ID=a.ACC_ID left join `srl-labs-cdp.sync_dump.billstatus_master` bill on lower(bill.code)=lower(a1.RSLT_STATUS) left join `srl-labs-cdp.ORACLE_DATA.ACC_PRDCT_LCTN` d on a1.acc_ACC_ID=d.ACC_ACC_ID and a1.PRDCT_PRDCT_ID=d.PARENT_PRDCT_ID left join (SELECT distinct PRDCT_ID,NAME,PRDCT_CD,PRDCT_TYP FROM `srl-labs-cdp.ORACLE_DATA.PRDCT`) f on d.PARENT_PRDCT_ID=f.PRDCT_ID left join (SELECT distinct PRDCT_ID,NAME,PRDCT_CD,PRDCT_TYP,DEPT_CODE FROM `srl-labs-cdp.ORACLE_DATA.PRDCT`) g on d.PRDCT_PRDCT_ID=g.PRDCT_ID left join `srl-labs-cdp.sync_dump.department_master`dept on lower(dept.departmentcode)=lower(g.DEPT_CODE) left join (select distinct BSTNK,VBELN,NETWR,spart,KUNNR,VKORG,VTWEG,ZZRFDRNAME from`srl-labs-cdp.SAP_DATA.VBAK` where VKORG='SR01')e on a.acc_id=e.BSTNK left join (select distinct VBELN,IF(LENGTH(REGEXP_REPLACE(MATNR,r'[\d.]', '')) = 0,cast((cast(MATNR as int64)*1) as string),MATNR) as MATNR,KZWI1,MWSBP,KZWI6,NETWR,KZWI4,ARKTX from `srl-labs-cdp.SAP_DATA.vbap`) g1 on e.VBELN=g1.VBELN and g1.MATNR=f.PRDCT_CD left join(select distinct spart,KUNNR,VKORG,KDGRP,VTWEG from `srl-labs-cdp.SAP_DATA.knvv`)g2 on e.spart=g2.spart and e.KUNNR = g2.KUNNR and e.VKORG=g2.VKORG and g2.VTWEG=e.VTWEG left join `srl-labs-cdp.SAP_DATA.kna1` g3 on g2.KUNNR=g3.KUNNR left join `srl-labs-cdp.SAP_DATA.T151T` g4 on g3.mandt=g4.mandt and g3.SPRAS=g4.SPRAS and g2.KDGRP=g4.KDGRP left join (select distinct VBELN,LIFNR,POSNR,PARVW from `srl-labs-cdp.SAP_DATA.VBPA`) re1 on re1.VBELN=e.VBELN and (re1.POSNR = '000001') AND (re1.PARVW = 'RD') left join `srl-labs-cdp.SAP_DATA.LFA1` lf on re1.LIFNR = lf.LIFNR and lf.KTOKK = '1600' where a.ACC_DT>TIMESTAMP("{date}") and ACC_DT < TIMESTAMP(CURRENT_DATE()-1)

            """
        elif taskname == tasknameMapping['labreports_dump']:
            query += f"""`{dump_dataset}.labreports_dump` as 
            with a12 as ( select distinct ptnt_id from `srl-labs-cdp.ORACLE_DATA.PTNT` group by 1 having count(distinct ptnt_cd)>1 ), b12 as ( select distinct ptnt_id,ptnt_cd from `srl-labs-cdp.ORACLE_DATA.PTNT` group by 1,2 having count(distinct concat(ifnull(PTNT_FNM,''),' ',ifnull(PTNT_LNM,'')))>1 ) SELECT case when b.ptnt_id is null then true else false end as srcbaddata, case when b.ptnt_id is null then 'patientid and encounter id does not exists in ptnt table' else null end as srcbaddataremarks, case when a12.ptnt_id is not null then true else false end as srcbaddata1, case when a12.ptnt_id is not null then 'multiple patientid on same encounterid' else null end as srcbaddataremarks1, case when b12.ptnt_id is not null then true else false end as srcbaddata2, case when b12.ptnt_id is not null then 'multiple name on same patientid and encounterid' else null end as srcbaddataremarks2, case when lower(trim(NULLIF(b.PTNT_CD,'')))  is null then lower(trim(NULLIF(a1.PTNT_PTNT_ID,''))) else lower(trim(NULLIF(b.PTNT_CD,''))) end as patientid, lower( trim(b.PTNT_CD) ) as sourcepatientid, lower( trim(a1.PTNT_PTNT_ID) ) as encounterid, lower( trim(a.ACC_ID) ) as bookingid, lower( trim(a.PRDCT_ID) ) as testid, lower( trim(a.PRDCT_CD) ) as productcode, lower( trim(a.PRDCT_NAME) ) as testname, lower( trim(a.ELMNT_CD) ) as biomarkercode, lower( trim(a.ELMNT_ID) ) as biomarkerid, lower( trim(a.ELMNT_NAME) ) as biomarkername, lower( trim(a.PARENT_PRDCT_ID) ) as packageid, lower( trim(a.PARENT_PRDCT_NAME) ) as packagename, lower( trim(a.PARENT_PRDCT_CD) ) as parentproductcode, lower( trim(a.RSLT) ) as result, a.RSLT_DT as resultdate, lower( trim(a.PRNT_RNG_TXT) ) as normalrange, lower( trim(a.RANGE_VAL) ) as range_val, lower( trim(a.RSLT_NORMAL_FLAG) ) as indicator, lower( trim(a.NP_CMMNT) ) as comment, lower( trim(a.P_CMMNT) ) as remarks1, lower( trim(a.RMRKS) ) as remarks, lower( trim(a.ELMNT_RSLT_TYP) ) as resulttype, lower( trim(a.ELMNT_MIN_RANGE) ) as rangestart, lower( trim(a.ELMNT_MAX_RANGE) ) as rangeend, lower( trim(a.SR_NO) ) as itemrow, lower( trim(a.CANCELLED_FLAG) ) as cancelledflag, '' as approvedstatus, a.REVIEW_DT as authenticateddate, lower( trim(a.ELMNT_METHOD) ) as method, lower( trim(a.ELMNT_RSLT_UNIT) ) as unit, lower( trim(a.LAB_CTGRY_CD) ) as lab_ctgry_cd, lower( trim(a.LAB_CTGRY_ID) ) as departmentid, lower( trim(a.LAB_CTGRY_NAME) ) as department, lower( trim(a.RSLT_CRTCL_TXT) ) as rslt_crtcl_txt, lower( trim(a.RSLT_STG) ) as rslt_stg, lower( trim(a.DEMOGRAPHIC) ) as demographic, lower( trim(a.LOCATION_ID) ) as centreid, lower( trim(c.LCTN_CD) ) as centrecode, lower( trim(c.LCTN_NM) ) as centrename, lower( trim(a.SERVICED_BY_LAB) ) as processingunitid, lower( trim(c1.LCTN_CD) ) as processingunitcode, lower( trim(c1.LCTN_NM) ) as processingunit, 'testclient' as labname, TIMESTAMP( CURRENT_TIMESTAMP() ) as insertedon FROM `srl-labs-cdp.ORACLE_DATA.RSLT` a inner join `srl-labs-cdp.ORACLE_DATA.ACC` a1 on a.ACC_ID=a1.acc_id and a1.acc_dt>'2022-01-01' left join ( select distinct PTNT_ID,PTNT_CD from(select ROW_NUMBER () OVER (PARTITION BY PTNT_ID ORDER BY UPD_DT desc,MODIFIED_ON desc) as rnum, PTNT_ID,PTNT_CD from `srl-labs-cdp.ORACLE_DATA.PTNT` )x where x.rnum=1)b on a1.PTNT_PTNT_ID=b.ptnt_id left join a12 on a1.PTNT_PTNT_ID=a12.ptnt_id left join b12 on a1.PTNT_PTNT_ID=b12.PTNT_ID left join (select distinct lab_id,LCTN_CD,LCTN_NM from `srl-labs-cdp.ORACLE_DATA.TESTCLIENT_CENTRE_LIST`)c on a.LOCATION_ID=c.lab_id left join (select distinct lab_id,LCTN_CD,LCTN_NM from `srl-labs-cdp.ORACLE_DATA.TESTCLIENT_CENTRE_LIST`)c1 on a.SERVICED_BY_LAB=c1.lab_id WHERE COALESCE(REVIEW_DT, RSLT_DT)>TIMESTAMP("{date}") and COALESCE(REVIEW_DT, RSLT_DT)<TIMESTAMP(CURRENT_DATE()-1)
            """
        return query

    
            
    def dropDumpTables(self, client):
        print('###drop dump tables start###')
        for task in tasknameMapping:
            query = f"""drop table if exists {dump_dataset}.{task}"""
            print(query)
            run_query(client, query)
        print('###drop dump tables completed###')    


def moveDumpDatatoBackupTable(client):
        print("#####moving data from dump to backup table start#####")
        for task in tasknameMapping:            
            backuptable = task.replace('_dump', '_backup')
            # print(task)
            query = f"""insert into {dump_dataset}.{backuptable} select * from {dump_dataset}.{task}"""
            print(query)
            run_query(client, query)
        print("#####moving data from dump to backup table completed#####")


def updateDumpWatermarkTable(client):
        maxid_tasklist = {}
        
        for task in tasklist:            
            query = f"""select max({tasklist[task]}) from {dump_dataset}.{task}"""
            result = run_query(client, query)
            print(task, '>>>>', result[0][0])
            if result[0][0]:
                maxid_tasklist[task] = result[0][0]
            else:
                maxid_tasklist[task] = '0'
        nodata_tasklist = []
        for task in maxid_tasklist:
            if maxid_tasklist[task] == '0':
                nodata_tasklist.append(task)
        if len(nodata_tasklist):
            nodata_tasklist = ', '.join(nodata_tasklist)
            alertForNoData(client, nodata_tasklist)
        if maxid_tasklist['patients_backup'] == '0':
            print('patient dump not available, Stopping the pipeline!')
            exit()
        else:
            for task in maxid_tasklist:
                if maxid_tasklist[task] != '0':
                    taskname  = task.replace('_backup', '_dump')
                    updateDumpTableQuery = f"""update sync_metadata.dump_watermark set value='{maxid_tasklist[task]}' where taskname='{tasknameMapping.get(taskname)}'""" 
                    print(updateDumpTableQuery)
                    run_query(client, updateDumpTableQuery)