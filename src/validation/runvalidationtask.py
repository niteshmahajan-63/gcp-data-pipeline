import math,requests, os, time, json

from datetime import datetime, timedelta
filename =os.path.basename(__file__).split('.')[0]
# outputcolumn='queryid,testname,query,result,ispass'
outputcolumn=['queryid','testname','query','result','ispass', 'start_exec','end_exec','total_exec_time']
mergenumber = None
ist_offset = timedelta(hours=5, minutes=30)
from src.sync_ingest.utils_ingest.utils import run_query,insert_query, update_query


class validationtask():
   
    def start(self,client, dryRun=False):
        start_time = datetime.now()
        pipelineid = ''
        taskname = 'validationtask'
        type = 'validation'
        end_time = None
        total_exec_time = None
        try:
            pipelineid = 'testclient'+'-'+start_time.strftime("%Y%m%d")
            insert_query(client, pipelineid,type,taskname,'in-progress',start_time)
            self.postvalidation(client,dryRun)
            print(f'''validation phase completed successfully''')
            end_time = datetime.now()   
            total_exec_time = end_time - start_time
            update_query(client, pipelineid,type,taskname,'success',end_time,total_exec_time)        

        except Exception as error :
            print('>>>>> {} :: got exception  '.format(filename))
            end_time = datetime.now()
            total_exec_time = end_time - start_time     
            update_query(client, pipelineid,type,taskname,'fail',end_time,total_exec_time)       
            raise error

    

    
    def postvalidation(self,client, dryRun):
        print('running post phase validation queries now')
        self.post_merge_check(client)
        
    

    
    def post_merge_check(self, client):
        query_start_time = datetime.now()  + ist_offset
        # running all comparator queries
        global mergenumber
        mergenumber = int(time.time())
        run_queries_list = []
        resultlist=[]
        reason_queryids_list = []
        reason_note = []
        result_tbl_list = []
        total_query_exec_duration = 0
        
        
        query = f""" with r as (select * from `sync_validation.validationqueries` ORDER BY queryid )
                     select TO_JSON_STRING(r) from r """
        reason_query = f"""  with r as (select array_agg(queryid) queryids, ignorereason from  `sync_validation.validationqueries` where ignorereason is not null
                                group by ignorereason )
                                select TO_JSON_STRING(r) from r """
        merg_lrundate = f""" select date(lastrundate) from  `sync_metadata.dbtasks` where taskname = 'mergeprocessed' """                        

        run_queries_list.append(query)
        record= run_query(client, query)
        run_queries_list.append(reason_query)
        reason_record = run_query(client, reason_query)
        run_queries_list.append(merg_lrundate)
        run_date= run_query(client,merg_lrundate)
        run_date = run_date[0][0] if run_date and run_date[0] and run_date[0][0] else 0
        print(run_date)

        # print(reason_record)
        for reason_items in reason_record:
            for ids in json.loads(reason_items[0])['queryids']:
                reason_queryids_list.append(ids)
        # print(reason_queryids_list)
        for reason_items in reason_record:
            # print(reason_items)
            reason_note.append(str(json.loads(reason_items[0])['queryids'])+'-'+str(json.loads(reason_items[0])['ignorereason']))
        # print(reason_note)    
       

        result=[]
        for row in record:
            result.append(row[0])
        for queryrow in result:
            queryrow = json.loads(queryrow)
            if queryrow['enabled']==False:
                pass
            else:
                start_times=str(datetime.now())
                validationresult=run_query(client,queryrow['query'] or 'select \'query missing\' ')
                complete_times=str(datetime.now())
                start_time = datetime.strptime(start_times, "%Y-%m-%d %H:%M:%S.%f")
                complete_time = datetime.strptime(complete_times, "%Y-%m-%d %H:%M:%S.%f")
                # Calculate the difference between the datetime objects
                time_difference = complete_time - start_time

                # Extract the difference in minutes
                difference_in_minutes = time_difference.total_seconds() / 60
                total_query_exec_duration += difference_in_minutes
                # print("executing query " +queryrow['queryid'])
                print(validationresult,queryrow['queryid'])
                if queryrow['comparison']==False:
                    if validationresult[0][0]==0:
                        ispass=True
                    else:
                        ispass=False
                    resultlist.append([queryrow['queryid'],queryrow['testname'],validationresult[0][0],ispass,queryrow['priority']])
                    result_tbl_list.append([queryrow['queryid'],queryrow['testname'],validationresult[0][0],ispass,start_time.strftime('%Y-%m-%d %H:%M:%S'),complete_time.strftime('%Y-%m-%d %H:%M:%S'),f"{difference_in_minutes:.2f} minutes"])
                    # print(resultlist)
                    
                else:
                    count1=validationresult[0][0] or 0
                    count2=validationresult[1][0] or 0
                    if count1==count2:
                        ispass=True
                        resultlist.append([queryrow['queryid'],queryrow['testname'],str(count1)+'||'+str(count2),ispass,queryrow['priority']])
                        result_tbl_list.append([queryrow['queryid'],queryrow['testname'],validationresult[0][0],ispass,start_time.strftime('%Y-%m-%d %H:%M:%S'),complete_time.strftime('%Y-%m-%d %H:%M:%S'),f"{difference_in_minutes:.2f} minutes"])
                    elif count1!=count2 and queryrow['percenttype'] == True :
                        # print(type(count1))
                        if ((count1/count2)*100) < 10:
                            # print((count1/count2)*100)
                            count1 = round((count1/count2)*100, 2)
                            count2 = '%'
                            ispass = True
                            resultlist.append([queryrow['queryid'],queryrow['testname'],str(count1)+str(count2),ispass,queryrow['priority']])
                            result_tbl_list.append([queryrow['queryid'],queryrow['testname'],validationresult[0][0],ispass,start_time.strftime('%Y-%m-%d %H:%M:%S'),complete_time.strftime('%Y-%m-%d %H:%M:%S'),f"{difference_in_minutes:.2f} minutes"])
                        else:
                            ispass = False
                            resultlist.append([queryrow['queryid'],queryrow['testname'],str(count1) +"||"+str(count2),ispass,queryrow['priority']])
                            result_tbl_list.append([queryrow['queryid'],queryrow['testname'],validationresult[0][0],ispass,start_time.strftime('%Y-%m-%d %H:%M:%S'),complete_time.strftime('%Y-%m-%d %H:%M:%S'),f"{difference_in_minutes:.2f} minutes"])
                    else:
                        ispass=False
                        resultlist.append([queryrow['queryid'],queryrow['testname'],str(count1)+"||"+str(count2),ispass,queryrow['priority']])
                        result_tbl_list.append([queryrow['queryid'],queryrow['testname'],validationresult[0][0],ispass,start_time.strftime('%Y-%m-%d %H:%M:%S'),complete_time.strftime('%Y-%m-%d %H:%M:%S'),f"{difference_in_minutes:.2f} minutes"])
        print(resultlist)
        print(reason_queryids_list)
        print(reason_note)
        # run_date = datetime.now().date()
        print(run_date)    
            
        
        sendmail(resultlist,reason_queryids_list, reason_note,run_date, query_start_time)

        
        self.updateresult(client,result_tbl_list.copy(),result) 
                
        


    def updateresult(self,client, update_list:list,result):
        updatelist = []
        for items in update_list:
            updatelist.append(items[0:7])
        for i in range(0,len(updatelist)):
            updatelist[i].insert(2,json.loads(result[i])['query'])           
        result_list = [dict(zip(outputcolumn, updatelist[i])) for i in range(len(updatelist))]
        print(result_list) 
        query = f'''select max(id) as id from `sync_validation.validationresult`'''
        getresult = run_query(client, query)
        if getresult[0][0] == None:
            starting_id = 1
        else:
            starting_id = getresult[0][0] + 1
        for i, item in enumerate(result_list):
            item['id'] = starting_id + i
        runquery = client.insert_rows_json('sync_validation.validationresult', result_list)  # Make an API request.
        if runquery == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(runquery))     
        return       


    def pre_merge_check(self):
        labname = 'Testclient'
        # start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_utc_time = datetime.utcnow()
        ist_offset = timedelta(hours=5, minutes=30)
        start_time = current_utc_time + ist_offset
        subject = f'Sync Pipeline Alert ::{labname} Pipeline Started'

        contents = f'''
        <style>
        body {{
            font-family: Arial, sans-serif;
        }}
        .container {{
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
            border: 1px solid #ccc;
            border-radius: 5px;
        }}
        .header {{
            text-align: center;
            margin-bottom: 20px;
        }}
        .message {{
            font-size: 16px;
            line-height: 1.5;
        }}
        .pipeline {{
            font-weight: bold;
            color: #0066cc;
        }}
        </style>
        <body>
        <div class="container">
            <div class="header">
            <h1>Pipeline Status Update</h1>
            </div>
            <div class="message">
            <p>Dear Team,</p>
            <p>The pipeline for <span class="pipeline">{labname}</span> has commenced.</p>
            <p>The pipeline was initiated on <strong>{start_time}</strong> IST.</p>
            <p>Please monitor the progress and attend to any potential issues promptly.</p>
            <p>Thank you for your attention.</p>
            <p>Best regards,<br>Sync Team</p>
            </div>
        </div>
        </body>
        '''
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

    def post_merge_status(self,status, audit_mailbody,sub_keyword):
        labname = 'Testclient'
        # end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_utc_time = datetime.utcnow()
        ist_offset = timedelta(hours=5, minutes=30)
        end_time = current_utc_time + ist_offset
        subject =f'Sync Pipeline{sub_keyword}Alert ::{labname} Pipeline Completed'
        status_table = "<table style='border-collapse: collapse; width: 100%; text-align: left;'>"
        status_table += "<tr style='background-color: #f5a623; color: white;'><th style='padding: 10px;'>Task</th><th style='padding: 10px;'>Status</th><th style='padding: 10px;'>Time Taken (minutes)</th></tr>"
        for index, task_info in enumerate(status):
            task = task_info["task"]
            task_time = task_info.get("completed_at", "Pending")
            completed_at = f"Completed at: {task_time}" if isinstance(task_time, datetime) else "Pending"
            time_taken = task_info.get("time_taken", "-")
            if time_taken!="-":
                time_taken= f'{time_taken} minutes'
            row_color = "#f0f0f0" if index % 2 == 0 else "#e0e0e0"
            status_table += f"<tr style='background-color: {row_color};'><td style='padding: 10px;'>{task}</td><td style='padding: 10px;'>{completed_at}</td><td style='padding: 10px;'>{time_taken}</td></tr>"
        status_table += "</table>" if status else "No status information available."
        

        contents = f'''
        <style>
        body {{
            font-family: Arial, sans-serif;
        }}
        .container {{
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
            border: 1px solid #ccc;
            border-radius: 5px;
        }}
        .header {{
            text-align: center;
            margin-bottom: 20px;
        }}
        .message {{
            font-size: 16px;
            line-height: 1.5;
        }}
        .pipeline {{
            font-weight: bold;
            color: #0066cc;
        }}
        </style>
        <body>
        <div class="container">
            <div class="header">
            <h1>Pipeline Status Update</h1>
            </div>
            <div class="message">
            <p>Dear Team,</p>
            <p>The pipeline for <span class="pipeline">{labname}</span> has successfully completed its run.</p>
            <p>The pipeline concluded on <strong>{end_time}</strong>. IST</p>
            {audit_mailbody}<br>
            <p>Status:</p>
            {status_table}<br>
            <p>Validation test suite is running currently, Please review the results in validation Report and address any follow-up actions as needed.</p>
            <p>Thank you for your attention.</p>
            <p>Best regards,<br>Sync Team</p>
        </div>
        </body>
        '''
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

    


    def send_pipeline_failure_alert(self,status,exception,traceback_message, labname='Testclient'):
        #current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_utc_time = datetime.utcnow()
        ist_offset = timedelta(hours=5, minutes=30)
        current_time = current_utc_time + ist_offset
        #traceback_message = traceback.format_exc(limit=10) 
        failflag =0
        Failedmessage = ''
        
        subject = f'Pipeline Failure Alert - {labname}'
        status_table = "<table style='border-collapse: collapse; width: 100%; text-align: left;'>"
        status_table += "<tr style='background-color: #f5a623; color: white;'><th style='padding: 10px;'>Task</th><th style='padding: 10px;'>Status</th><th style='padding: 10px;'>Time Taken (minutes)</th></tr>"
        for index, task_info in enumerate(status):
            task = task_info["task"]
            task_time = task_info.get("completed_at", "Pending")
            if failflag==0 and task_info.get('completed_at')=='Pending':
                Failedmessage=f'Failed at : {task}'
                failflag = 1

            completed_at = f"Completed at: {task_time}" if isinstance(task_time, datetime) else "Pending"
            time_taken = task_info.get("time_taken", "-")
            if time_taken!="-":
                time_taken= f'{time_taken} minutes'
            row_color = "#f0f0f0" if index % 2 == 0 else "#e0e0e0"
            status_table += f"<tr style='background-color: {row_color};'><td style='padding: 10px;'>{task}</td><td style='padding: 10px;'>{completed_at}</td><td style='padding: 10px;'>{time_taken}</td></tr>"
        status_table += "</table>" if status else "No status information available."
        
        
        
        html_content = f'''
        <style>
        body {{
            font-family: Arial, sans-serif;
        }}
        .container {{
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
            border: 1px solid #ccc;
            border-radius: 5px;
        }}
        .header {{
            text-align: center;
            margin-bottom: 20px;
        }}
        .message {{
            font-size: 16px;
            line-height: 1.5;
        }}
        .pipeline {{
            font-weight: bold;
            color: #ff0000;
        }}
        .alert-icon {{
            font-size: 54px;
            text-align: center;
            margin-bottom: 20px;
            color: #ff0000;
        }}
        
        </style>
    
        <body>
        <div class="container">
            <div class="header">
            <h1>Pipeline Failure Alert</h1>
            </div>
            <div class="alert-icon">
            &#9888; 
            </div>
            <div class="message">
            <p>Dear Team,</p>
            <p>The pipeline for <span class="pipeline">{labname}</span> has encountered a failure.</p>
            <p style= 'color: #ff0000;'>{Failedmessage}</p>
            <p>Status:</p>
            {status_table}
            <p>The failure occurred at {current_time} IST.</p>
            <p>Error message: <strong>{str(exception)}</strong></p>
            <p>Traceback:</p>
            <pre>{traceback_message}</pre>
            <p>Please take necessary actions to resolve the issue promptly.</p>
            <p>Thank you for your attention.</p>
            <p>Best regards,<br>Sync Team</p>
            </div>
        </div>
        </body>
        '''


        # Prepare the payload
        req = {
            "emailList": ['nitesh.xortix@gmail.com'],   
            "labname": labname,
            "content": html_content,
            "subject": subject,
            "from":"Sync Alerts"
        }

        # Send the API request
        headers = {"content-type": "application/json"}
        try:
            response = requests.post(
                        url='https://mddr7aigo9.execute-api.ap-south-1.amazonaws.com/test/sync/emailalert',
                        data=json.dumps(req),
                        headers={"content-type": "application/json"}
                    )
            if response.status_code == 200:
                print(response.text)
            else:
                print(f"Failed to send alert. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while sending the alert: {e}")    

def sendmail(result: list, reason_list: list, reason_note: list,run_date, query_start_time):


    # result=[['101', 'daily distinct patientid', '4199|4199', True],
    #  ['','102', 'daily distinct bookingid from bill', '728|728', True]]
    print(reason_list)
    for lst in result:
        if lst[0] in reason_list and lst[3] is False:
            lst[2] = str(lst[2]) + '**'

    for lst in result:
        if lst[0] in reason_list and lst[3] is False:
            lst[3] = True

    tl1 = []
    tl2 = []
    tl3 = []
    tl4 = []
    tl5 = []
    tl6 = []
    tl7 = []

    table_data = result
    for d1 in table_data:
        if (d1[4] == 1):
            tl1.append(d1)
        if (d1[4] == 2):
            tl2.append(d1)
        if (d1[4] == 3):
            tl3.append(d1)
        if (d1[4] == 4):
            tl4.append(d1)
        if (d1[4] == 5):
            tl5.append(d1)    
        if (d1[4] == 6):
            tl6.append(d1)     
        if (d1[4] == 7):
            tl7.append(d1)       
        # success percent   check        
    f_cases = 0
    ttl_count = 0
    for t_f in result:
        if (t_f[3] == False):
            f_cases = f_cases+1
            # print(t_f)
        if (t_f[0] != False):
            ttl_count = ttl_count+1

    # print(f_cases,'false cases')
    # print(ttl_count,'ttl_count')
    false_percent = 0
    succeed_percent = 0
    if ttl_count>0:
        false_percent = (f_cases/ttl_count)*100
    succeed_percent = 100 - false_percent
    succeed_percent = math.ceil(succeed_percent)
    print(succeed_percent)
    # merge pipeline check
    pipline_status_check =0
    tday_date = datetime.now().date()
    if(run_date == tday_date):
        pipline_status_check = 'succeed' 
    else:
        pipline_status_check = 'failed'
    print(pipline_status_check)
    query_complete_time = datetime.now() + ist_offset
    dif_tm = query_complete_time - query_start_time
    qry_durations = dif_tm - timedelta(microseconds=dif_tm.microseconds)

    # Format table data into HTML
    contents = '''<p>Hi team,</p><p>Today\'s monitoring results for the following lab are as follows:</p><style>
  table {
    border-collapse: collapse;
    width: 100%;
  }
  #header-row th {
    background-color: #ffb84d;
  }
  th, td {
    padding: 8px;
    text-align: left;
    border:1px solid black;
  }

  th {
    background-color: #5A61FF;
  }
</style>'''
    contents += ''' <table>
        <tr id="header-row">
            <th>Task</th>
            <th>Started_at</th>
            <th>Completed_at</th>
            <th>Time taken</th>
            <th>Merge pipeline status</th>
            <th>Total succeed %</th>
        </tr>
        <tr>
            <td>Validations</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
            <td>{}</td>
        </tr>
    </table> 
    <br>
    <br>
    '''.format(query_start_time,query_complete_time, qry_durations,pipline_status_check,succeed_percent)
    # if pipline_status_check == 'failed':
    #     contents += '<p style="text-align:right">Merge pipeline status:<b style="color:red"> {} </b></p>'.format(
    #         pipline_status_check)
    # else:
    #     contents += '<p style="text-align:right">Merge pipeline status:<b> {} </b></p>'.format(
    #         pipline_status_check)

    # contents += '<p style="text-align:right">Total succeed precent is: <b>{}% </b></p>'.format(
    #     false_percent)
    contents += '<table style="border: 1px solid black;">'
    contents += ''' <tr style = "padding:200px;"> <th colspan= "4" style = "text-align:center;border: 1px solid black; ">1<sup>st</sup> Priority Test cases</th></tr>
    <tr style="font-weight: bold;">
                        <th style ="width:10%">Query ID</th> 
                        <th style = "width:60%">Testname</th>
                        <th style = "width:20%;">Merge</th>
                       <th style = "width:5%">Success</th>

                        </tr>'''
    for row in tl1:
        if (row[3] == False):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'

    for row in tl1:
        if (row[3] == True):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                if '**' in str(cell):
                    contents += '<td  style="background-color: #ffff4d;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'
    contents += '</table>'
    contents += '<br>'
    contents += '<br>'
    contents += '<table style="border: 1px solid black;">'
    contents += '''<tr> <th colspan= "4" style = "text-align: center ;border: 1px solid black;">Billing and HASH Test cases</th></tr>
    <tr style="font-weight: bold; ">
                        <th style ="width:10%">Query ID</th> 
                        <th style = "width:60%">Testname</th>
                        <th style = "width:20%;">Merge</th>
                        <th style = "width:5%">Success</th>

                        </tr>'''
    for row in tl2:
        if (row[3] == False):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue

                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'

    for row in tl2:
        if (row[3] == True):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                if '**' in str(cell):
                    print('*******')
                    contents += '<td  style="background-color: #ffff4d;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'
    contents += '</table>'
    contents += '<br>'
    contents += '<br>'
    contents += '<table style="border: 1px solid black;">'
    contents += ''' <tr> <th colspan= "4" style = "text-align: center ;border: 1px solid black;">Daily count Test cases </th></tr>
    <tr style="font-weight: bold; ">
                        <th style ="width:10%">Query ID</th> 
                        <th style = "width:60%">Testname</th>
                        <th style = "width:20%;">Raw || Merge</th>
                        <th style = "width:5%">Success</th>

                        </tr>'''
    for row in tl3:
        if (row[3] == False):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'

    for row in tl3:
        if (row[3] == True):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                if '**' in str(cell):
                    contents += '<td  style="background-color: #ffff4d;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'

    contents += '</table>'
    contents += '<br>'
    contents += '<br>'
    contents += '<table style="border: 1px solid black;">'
    contents += ''' <tr style = "padding:200px;"> <th colspan= "4" style = "text-align:center;border: 1px solid black; ">Merge Test cases</th></tr>
    <tr style="font-weight: bold;">
                        <th style ="width:10%">Query ID</th> 
                        <th style = "width:60%">Testname</th>
                        <th style = "width:20%;">Merge</th>
                       <th style = "width:5%">Success</th>

                        </tr>'''
    for row in tl4:
        if (row[3] == False):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'

    for row in tl4:
        if (row[3] == True):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                if '**' in str(cell):
                    contents += '<td  style="background-color: #ffff4d;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'
    contents += '</table>'
    contents += '<br>'
    contents += '<br>'

    contents += '<table style="border: 1px solid black;">'
    contents += ''' <tr style = "padding:200px;"> <th colspan= "4" style = "text-align:center;border: 1px solid black; ">ID Stamping between multiple tables</th></tr>
    <tr style="font-weight: bold;">
                        <th style ="width:10%">Query ID</th> 
                        <th style = "width:60%">Testname</th>
                        <th style = "width:20%;">Merge</th>
                       <th style = "width:5%">Success</th>

                        </tr>'''
    for row in tl5:
        if (row[3] == False):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'

    for row in tl5:
        if (row[3] == True):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                if '**' in str(cell):
                    contents += '<td  style="background-color: #ffff4d;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'
    contents += '</table>'
    contents += '<br>'
    contents += '<br>'

    contents += '<table style="border: 1px solid black;">'
    contents += ''' <tr style = "padding:200px;"> <th colspan= "4" style = "text-align:center;border: 1px solid black; ">Known issue in testclient data</th></tr>
    <tr style="font-weight: bold;">
                        <th style ="width:10%">Query ID</th> 
                        <th style = "width:60%">Testname</th>
                        <th style = "width:20%;">Raw</th>
                       <th style = "width:5%">Success</th>

                        </tr>'''
    for row in tl6:
        if (row[3] == False):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'

    for row in tl6:
        if (row[3] == True):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                if '**' in str(cell):
                    contents += '<td  style="background-color: #ffff4d;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'
    contents += '</table>'
    contents += '<br>'
    contents += '<br>'
    contents += '<table style="border: 1px solid black;">'
    contents += ''' <tr style = "padding:200px;"> <th colspan= "4" style = "text-align:center;border: 1px solid black; ">Source validation</th></tr>
    <tr style="font-weight: bold;">
                        <th style ="width:10%">Query ID</th> 
                        <th style = "width:60%">Testname</th>
                        <th style = "width:20%;">Raw</th>
                       <th style = "width:5%">Success</th>

                        </tr>'''
    for row in tl7:
        if (row[3] == False):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'

    for row in tl7:
        if (row[3] == True):
            contents += '<tr>'
            row = row[0:4]
            # print(row)
            # exit()
            for cell in row:
                if cell is False:
                    contents += '<td  style="background-color: red;">{}</td>'.format(
                        cell)
                    continue
                if '**' in str(cell):
                    contents += '<td  style="background-color: #ffff4d;">{}</td>'.format(
                        cell)
                    continue
                contents += '<td>{}</td>'.format(cell)
            contents += '</tr>'
    contents += '</table>'
    contents += '<br>'
    contents += '<br>'
    for lst in reason_note:
        contents += '''
    <p><b>**{}</b></p>'''.format(lst)

    req = {
        "emailList": ['nitesh.xortix@gmail.com'],
        "labname": 'Testclient',
        "content": contents
    }

    try:
        response = requests.post(
            url='https://mddr7aigo9.execute-api.ap-south-1.amazonaws.com/test/sync/email',
            data=json.dumps(req),
            headers={"content-type": "application/json"}
        )

        if response.status_code == 200:
            print(response.text)
        else:
            print("Error: Request failed with status code", response.status_code)

    except Exception as e:
        print("Error:", str(e))
    
   
