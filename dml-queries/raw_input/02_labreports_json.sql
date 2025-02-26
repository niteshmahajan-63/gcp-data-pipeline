insert into `sync_rawinput.labreports_json`(patientid,cancelledflag,approvedstatus,authenticateddate,biomarkerid,biomarkername,biomarkerstampid,normalizedbiomarkername,sourcebiomarkername,biomarkercode,encounterid,bookingid,biomarkerhash,biomarkermetahash,centreid,centrename,centrecode,centrehash, packageid,packagename,departmentid,department,type,itemtype,normalrange,rangestart,rangeend,indicator,comment,resulttype,result,resultnum,resultdate,testid,testname,sourcetestname,unit,remarks,itemrow,sourcepatientid,productcode,parentproductcode,method,processingunitid,processingunit,processingunitcode,rslt_crtcl_txt,rslt_stg,demographic,lab_ctgry_cd,range_val,baddata,baddataremarks,labname,insertedon)
WITH parsed_data AS (
  SELECT
	patientid,
	cancelledflag,
	approvedstatus,
	authenticateddate,
	biomarkerid,
	REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(biomarkername, r'\s{2,}', ' '),",", ";"),"{", "["),"}", "]") as biomarkername,
	REGEXP_REPLACE(biomarkername, r'[^A-Za-z0-9\-]', '') as normalizedbiomarkername,
	biomarkername as sourcebiomarkername,
	biomarkercode,
	encounterid,
	bookingid,
	centreid,
	centrename,
	centrecode,
	packageid,
	packagename,
	departmentid,
	department,
	CASE WHEN packagename IS NOT NULL THEN 'package' ELSE 'test' END AS itemtype,
	CASE
		WHEN result IS NULL THEN null
		WHEN SAFE_CAST(result AS FLOAT64) IS NOT NULL THEN 'n'
		WHEN LENGTH(result) > 75 AND
			 REGEXP_CONTAINS(result, r'<[^>]+>') THEN 'h'
		ELSE 't'
	END AS type,
	normalrange,
	rangestart,
	rangeend,
	indicator,
	comment,
	resulttype,
	REGEXP_REPLACE(REGEXP_REPLACE(result, r'<[^>]*>', ' '), r'\s+', ' ') AS result,
	resultdate,
	testid,
	REPLACE(REPLACE(REPLACE(REGEXP_REPLACE(testname, r'\s{2,}', ' '),",", ";"),"{", "["),"}", "]") as testname,
	testname as sourcetestname,
	unit,
	CONCAT(COALESCE(remarks, ''),COALESCE(remarks1, '')) as remarks,
	itemrow,
	sourcepatientid,
	productcode,
	parentproductcode,
	method,
	processingunitid,
	processingunit,
	processingunitcode,
	rslt_crtcl_txt,
	rslt_stg,
	demographic,
	lab_ctgry_cd,
	range_val,
	labname,
	srcbaddata,
	srcbaddata1,
	srcbaddata2,
	srcbaddataremarks,
	srcbaddataremarks1,
	srcbaddataremarks2
	from `sync_dump.labreports_dump`
)
select
patientid,
cancelledflag,
approvedstatus,
authenticateddate,
biomarkerid,
biomarkername,
CASE 
	WHEN testname IS NOT NULL OR biomarkername IS NOT NULL OR unit IS NOT NULL THEN
		CONCAT(
			IFNULL(REGEXP_REPLACE(LOWER(testname), r'[^a-z0-9\-&]', ''), ''),
			'__',
			IFNULL(REGEXP_REPLACE(LOWER(biomarkername), r'[^a-z0-9\-&]', ''), ''),
			'__',
			IFNULL(REGEXP_REPLACE(unit, r'\s', ''), '')
		)
	ELSE NULL
END AS biomarkerstampid,
normalizedbiomarkername,
sourcebiomarkername,
biomarkercode,
encounterid,
bookingid,
TO_BASE64(CAST (CONCAT (COALESCE(labname,''), COALESCE(bookingid,''), COALESCE(patientid,''), COALESCE(cast(resultdate as string),''), COALESCE(testid,''), COALESCE(testname,''), COALESCE(biomarkerid,''), COALESCE(biomarkername,''), COALESCE(result,''), COALESCE(unit,'') ) AS BYTES) ) AS biomarkerhash,
TO_BASE64(CAST(CONCAT(COALESCE(labname,''), COALESCE(testid,''),COALESCE(testname,''),COALESCE(biomarkername,''),COALESCE(unit,''),COALESCE(type,'')) AS BYTES)) AS biomarkermetahash,
centreid,
centrename,
centrecode,
CASE WHEN centreid IS NOT NULL THEN TO_BASE64( CAST( CONCAT( COALESCE(labname,''), COALESCE(centreid,''), COALESCE(centrename,''), COALESCE(centrecode,'') ) AS BYTES) ) ELSE NULL END AS centrehash, -- new created
packageid,
packagename,
departmentid,
department,
type,
itemtype,
normalrange,
rangestart,
rangeend,
indicator,
comment,
resulttype,
result,
SAFE.PARSE_NUMERIC(result) AS resultnum,
resultdate,
testid,
testname,
sourcetestname,
unit,
remarks,
itemrow,
sourcepatientid,
productcode,
parentproductcode,
method,
processingunitid,
processingunit,
processingunitcode,
rslt_crtcl_txt,
rslt_stg,
demographic,
lab_ctgry_cd,
range_val,
CASE WHEN resultdate IS NOT NULL AND
	bookingid IS NOT NULL AND
	patientid IS NOT NULL AND
	result IS NOT NULL AND
	biomarkername IS NOT NULL AND
	testname IS NOT NULL AND
	srcbaddata <> true AND
	srcbaddata1 <> true AND
	srcbaddata2 <> true
	THEN false
	ELSE true
END AS baddata,
concat(
	CASE WHEN srcbaddata is true THEN concat(srcbaddataremarks, ' || ') ELSE '' END,
	CASE WHEN srcbaddata1 is true THEN concat(srcbaddataremarks1,' || ') ELSE '' END,
	CASE WHEN srcbaddata2 is true THEN concat(srcbaddataremarks2,' || ') ELSE '' END,
	CASE WHEN bookingid IS NULL THEN concat('bookingid does not exist',' || ') ELSE '' END,
	CASE WHEN patientid IS NULL THEN concat('patientid does not exist',' || ') ELSE '' END,
	CASE WHEN result IS NULL THEN concat('result does not exist',' || ') ELSE '' END,
	CASE WHEN resultdate IS NULL THEN concat('resultdate does not exist',' || ') ELSE '' END,
	CASE WHEN biomarkername IS NULL THEN concat('biomarkername does not exist',' || ') ELSE '' END,
	CASE WHEN testname IS NULL THEN 'testname does not exist' ELSE '' END
) AS baddataremarks
,labname
,TIMESTAMP(CURRENT_TIMESTAMP()) as insertedon
from parsed_data