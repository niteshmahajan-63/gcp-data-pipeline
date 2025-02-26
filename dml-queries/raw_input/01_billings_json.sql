insert into `sync_rawinput.billings_json`(patientid,sourcepatientid,encounterid,bookingid,bookingdate,billstatusid,billstatus
,bookinghash,bookingmetahash,centreid,centrename,centrehash,centrecode,testid,sourcetestname,testname,itemtype
,partyid,partyname,billtopartyid,billtoparty,channelid,channel,packageid,packagename,customertype,remarks,itemrow
,referalid ,referalname ,entityreferalhash,referal1,departmentid,department,grossamount,discount,tax,netamount,totalnetamount,parentproductcode,productcode,customerno,ort01,recipient,labname, ignoreitemamount,baddata,baddataremarks,insertedon )
with new_rows as
(select *, row_number() OVER (PARTITION BY bookingid, packagename ORDER BY bookingdate desc, netamount desc ) as rnum 
from `sync_dump.billings_dump` where packagename is not null and srcbaddata <> true and srcbaddata1 <> true and srcbaddata2 <> true
),
new_input as (
select patientid,sourcepatientid,encounterid,bookingid,bookingdate,billstatusid,billstatus,centreid,centrename,centrecode
,packagename as testid,'amount_on_package' as testname, 'amount_on_package' as sourcetestname, partyid
,partyname,billtopartyid,billtoparty,channelid,channel,packageid,packagename,customertype,remarks,itemrow
,referalid,referalname,referal1
,'amount_on_package' as departmentid
,'amount_on_package' as department
,cast(grossamount as float64) as grossamount
,cast(discount as float64) as discount
,cast(tax as float64) as tax
,netamount
,cast(totalnetamount as float64) as totalnetamount
,parentproductcode
,productcode
,customerno
,ort01
,recipient
,labname
,rnum,srcbaddata,srcbaddata1,srcbaddata2,srcbaddataremarks,srcbaddataremarks1,srcbaddataremarks2 from new_rows where rnum = 1

union distinct

select patientid,sourcepatientid,encounterid,bookingid,bookingdate,billstatusid,billstatus,centreid,centrename,centrecode
,testid,testname,testname as sourcetestname,partyid,partyname,billtopartyid,billtoparty,channelid,channel
,packageid,packagename,customertype,remarks,itemrow,referalid,referalname,referal1,departmentid,department
,CASE WHEN packagename is not null THEN cast(0 as float64) ELSE cast(grossamount as float64) END as grossamount
,CASE WHEN packagename is not null THEN cast(0.0 as float64) ELSE cast(discount as float64) END as discount
,CASE WHEN packagename is not null THEN cast(0.0 as float64) ELSE cast(tax as float64) END as tax
,CASE WHEN packagename is not null THEN cast(0.0 as float64) ELSE cast(netamount as float64) END as netamount
,CASE WHEN packagename is not null THEN cast(0 as float64) ELSE cast(totalnetamount as float64) END as totalnetamount
,parentproductcode
,productcode
,customerno
,ort01
,recipient
,labname
,-1 as rnum,srcbaddata,srcbaddata1,srcbaddata2,srcbaddataremarks,srcbaddataremarks1,srcbaddataremarks2  from `sync_dump.billings_dump`
)
select
patientid,sourcepatientid,encounterid,bookingid
,bookingdate
,billstatusid
,billstatus
,TO_BASE64( CAST( CONCAT( COALESCE(labname,''), COALESCE(bookingid,''), COALESCE(encounterid,''), COALESCE(patientid,''), COALESCE(cast(DATE(bookingdate) as string),''), COALESCE(testid,''), COALESCE(cast(netamount as string),'') ) AS BYTES) ) AS bookinghash  -- new created
,TO_BASE64( CAST( CONCAT( COALESCE(labname,''), COALESCE(testid,''), COALESCE(testname,''), COALESCE(REGEXP_REPLACE(packagename, r'[^A-Za-z0-9\-]', ''),'') ) AS BYTES) ) AS bookingmetahash  -- new created
,centreid
,centrename
,CASE WHEN centreid IS NOT NULL THEN TO_BASE64( CAST( CONCAT( COALESCE(labname,''), COALESCE(centreid,''), COALESCE(centrename,''), COALESCE(centrecode,'') ) AS BYTES) ) ELSE NULL END AS centrehash -- new created
,centrecode
,testid
,sourcetestname -- new created, extra column created to store source testname
,REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(testname, r'\s{2,}', ' '),",", ";"),"{", "["),"}", "]") as testname
,CASE WHEN packagename IS NOT NULL THEN 'package' ELSE 'test' END AS itemtype  -- new created
,partyid
,partyname
,billtopartyid
,billtoparty
,channelid
,channel
,packageid
,packagename 
,customertype
,remarks
,itemrow  -- row id crossponding to billid
,referalid   -- required, currently not present
,COALESCE(referalname, referal1) as referalname
,CASE WHEN referalid IS NOT NULL OR referalname IS NOT NULL OR referal1 IS NOT NULL THEN 
	TO_BASE64(cast(
	concat(COALESCE(labname,''),COALESCE(referalid,''),COALESCE(referalname, referal1, ''),COALESCE(centreid,''),'referal') as bytes
	)) ELSE NULL END as entityreferalhash  -- new created
,referal1    -- unknown field
,departmentid
,department
,grossamount
,discount
,tax
,netamount
,totalnetamount
,parentproductcode
,productcode
,customerno
,ort01
,recipient
,labname
,CASE WHEN rnum = 1 THEN false WHEN rnum = -1 and packagename is not null THEN true ELSE false end as ignoreitemamount  -- new created
,case WHEN bookingid IS NOT NULL AND
  patientid IS NOT NULL AND
	bookingdate IS NOT NULL AND
	testid IS NOT NULL AND
	srcbaddata <> true AND
	srcbaddata1 <> true AND
	srcbaddata2 <> true
  THEN false
  ELSE true
  END AS baddata  -- new created
,
concat(
	CASE WHEN srcbaddata is true THEN concat(srcbaddataremarks,' || ') ELSE '' END,
	CASE WHEN srcbaddata1 is true THEN concat(srcbaddataremarks1,' || ') ELSE '' END,
	CASE WHEN srcbaddata2 is true THEN concat(srcbaddataremarks2,' || ') ELSE '' END,
  	CASE WHEN bookingid IS NULL THEN concat('bookingid does not exist',' || ') ELSE '' END,
  	CASE WHEN patientid IS NULL THEN concat('patientid does not exist',' || ') ELSE '' END,
    CASE WHEN bookingdate IS NULL THEN concat('bookingdate does not exist',' || ') ELSE '' END,
	CASE WHEN testid IS NULL THEN 'testid does not exist' ELSE '' END 
) AS baddataremarks -- new created
,TIMESTAMP(CURRENT_TIMESTAMP()) as insertedon 
from new_input