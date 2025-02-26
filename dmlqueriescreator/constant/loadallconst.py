


def querygenerator():
    querylist=[]

    querylist.append(loadconstants( 'nationality', allsrc))
    querylist.append(loadconstants( 'salutation', allsrc))

    allsrc = ['patients_json', 'labreports_json', 'billings_json']
    querylist.append(loadconstants( 'channel',  allsrc))
    
    querylist.append(loadconstants( 'patienttype', allsrc))
    
    querylist.append(loadconstants( 'department', allsrc))

    billingpatsrc= ['billings_json', 'patients_json']
    querylist.append(loadconstants( 'servicetype', billingpatsrc))

    billinglabsrc= ['billings_json', 'labreports_json']
    querylist.append(loadconstants( 'itemtype', billinglabsrc))

    querylist.append(loadconstants( 'indicator', ['labreports_json']))

    return querylist



def loadconstants( columnName, srcList):
        print('now loading {}'.format(columnName))
        query = readQuery(columnName, srcList)
        return query


def readQuery(columnName, srcList):
    sample = """ 
   
    insert into `pepbigqueryexp.sync_constant.channel`(id,value,insertedon)
    with mid as (
	select max(id) as id from `sync_constant.channel`
    )
    select distinct (select IFNULL(id, 0) from mid)+(ROW_NUMBER() OVER(
        ORDER BY 
              channel
          ) ) as id, channel,CURRENT_TIMESTAMP()
    FROM
    (
        %s
    ) a
    where (channel is not null and channel != '')
    and channel not in
    (
        select distinct value
        from `sync_constant.channel`
    );
    """
    sample = sample % (getsrc(srcList))
    return sample.replace('channel', columnName)
    
def getsrc(srclist):
    srcsample = 'select distinct channel from sync_raw.samplesrc'
    srcqueries = []
    for src in srclist:
        srcqueries.append(srcsample.replace('samplesrc', src))
    return ' UNION distinct '.join(srcqueries)

    