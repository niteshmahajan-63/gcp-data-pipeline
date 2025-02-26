insert into `sync_rawinput.patients_json`(patientid,encounterid,salutation,sourcename,name,firstname,lastname,gender,age,dob,sourceemail,email,sourcealternatemobile,alternatemobile,sourcemobile,mobile,joiningdate,updateddate,patienthash,upd_dt,sourcepatientid,centreid,centrecode,centrename,centrehash,address,address1,address2,city,state,country,pin,remarks,nationality,vipflag,maritalstatus,ptnt_actual_dob_flg,labname, baddata,baddataremarks, insertedon)
select
patientid
,encounterid
,salutation
,sourcename
,trim(NULLIF(name,'')) as name
,firstname
,lastname
,gender
,age
,dob
,sourceemail
,email
,sourcealternatemobile
,case when regexp_contains(alternatemobile, "^[6-9][0-9]{9}$") is true then alternatemobile else null end as alternatemobile
,sourcemobile
,COALESCE(case when regexp_contains(mobile, "^[6-9][0-9]{9}$") is true then mobile else null end,case when regexp_contains(alternatemobile, "^[6-9][0-9]{9}$") is true then alternatemobile else null end) as mobile
,joiningdate
,updateddate
,TO_BASE64( CAST( CONCAT( COALESCE(labname,''), COALESCE(encounterid,''), COALESCE(patientid,''),COALESCE(case when regexp_contains(mobile, "^[6-9][0-9]{9}$") is true then mobile else null end,''), COALESCE(name,''), COALESCE(cast(age as string),''), COALESCE(cast(gender as string),'') ) AS BYTES) ) AS patienthash  -- new created
,upd_dt
,sourcepatientid
,centreid
,centrecode
,centrename
,CASE WHEN centreid IS NOT NULL THEN TO_BASE64( CAST( CONCAT( COALESCE(labname,''), COALESCE(centreid,''), COALESCE(centrename,''), COALESCE(centrecode,'') ) AS BYTES) ) ELSE NULL END AS centrehash  -- new created
,address
,address1
,address2
,city
,state
,country
,pin
,remarks
,nationality
,vipflag
,marital_status as maritalstatus
,ptnt_actual_dob_flg
,labname
,case WHEN joiningdate IS NOT NULL AND
         patientid IS NOT NULL AND
		 srcbaddata <> true
    THEN false
    ELSE true
  END AS baddata  -- new created

,
concat(
    CASE WHEN srcbaddata is true THEN concat(srcbaddataremarks,' || ') ELSE '' END,
    CASE WHEN joiningdate IS NULL THEN concat('joiningdate does not exist',' || ') ELSE '' END,
    CASE WHEN patientid IS NULL THEN 'patientid does not exist' ELSE '' END
  ) AS baddataremarks -- new created
,TIMESTAMP(CURRENT_TIMESTAMP()) as insertedon
from (
  select
  patientid
  ,encounterid
  ,salutation
  ,sourcename
  ,REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(name, r'\b(?:mrs|miss|doctor|mohd|mr|ms|dr|er|prof|mst|md|sri|shri|smt|mast|master)[\.|\s]{1,}', ''),r'[.]',' '), r'(\([^)]*\)|\[[^\]]*\]|\{[^}]*\})', ''), r'[^a-zA-Z\s]', ''), r'\s{2,}', ' '), r'\b(?:mrs|miss|doctor|mohd|mr|ms|dr|er|prof|mst|md|sri|shri|smt|mast|master)[\s]{1,}', '') as name
  ,firstname
  ,lastname
  ,case when gender='f' then 'female' when gender='m' then 'male' when gender is not null then 'others' else null end as gender
  ,cast(extract(year from dob)  as int) as age
  ,date(dob) as dob
  ,email as sourceemail
  ,case when REGEXP_CONTAINS(REPLACE(REPLACE(REGEXP_REPLACE(email, r"\s+", ""), 'mailto:', ''), ':', ''), r'^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$')
  is true then REPLACE(REPLACE(REGEXP_REPLACE(email, r"\s+", ""), 'mailto:', ''), ':', '') else null end as email
  ,alternatemobile as sourcealternatemobile
  ,CASE WHEN LENGTH(alternatemobile) = 12 and SUBSTR(alternatemobile, 0, 2) = '91' THEN SUBSTR(alternatemobile, 3) 
  WHEN LENGTH(REGEXP_REPLACE(alternatemobile,r'^\+91[\s-]*|^91[\s-]{1,}|^0{1,}','')) = 10 THEN REGEXP_REPLACE(alternatemobile,r'^\+91[\s-]*|^91[\s-]{1,}|^0{1,}','') 
  WHEN LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(alternatemobile, r'[^0-9]',''),r'^\+91[\s-]*|^91[\s-]{1,}|^0{1,}','')) = 10 THEN REGEXP_REPLACE(REGEXP_REPLACE(alternatemobile, r'[^0-9]',''),r'^\+91[\s-]*|^91[\s-]{1,}|^0{1,}','') 
  when length(alternatemobile) >= 12 THEN 
  REGEXP_REPLACE(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(alternatemobile,r'^\+91[\s-]*|^91[\s-]{1,}|^0{1,}',''),r'(?:-|\s|/|,|;|.){1,}', ' '), r'[ ]', '|'), '|')[SAFE_OFFSET(0)], r'[^0-9]','') ELSE NULL END AS alternatemobile
  ,mobile as sourcemobile
  ,CASE WHEN LENGTH(mobile) = 12 and SUBSTR(mobile, 0, 2) = '91' THEN SUBSTR(mobile, 3) 
  WHEN LENGTH(REGEXP_REPLACE(mobile,r'^\+91[\s-]*|^91[\s-]{1,}|^0{1,}','')) = 10 THEN REGEXP_REPLACE(mobile,r'^\+91[\s-]*|^91[\s-]{1,}|^0{1,}','') 
  WHEN LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(mobile, r'[^0-9]',''),r'^\+91[\s-]*|^91[\s-]{1,}|^0{1,}','')) = 10 THEN REGEXP_REPLACE(REGEXP_REPLACE(mobile, r'[^0-9]',''),r'^\+91[\s-]*|^91[\s-]{1,}|^0{1,}','') 
  when length(mobile) >= 12 THEN 
  REGEXP_REPLACE(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(mobile,r'^\+91[\s-]*|^91[\s-]{1,}|^0{1,}',''),r'(?:-|\s|/|,|;|.){1,}', ' '), r'[ ]', '|'), '|')[SAFE_OFFSET(0)], r'[^0-9]','') ELSE NULL END AS mobile
  ,joiningdate
  ,updateddate
  ,upd_dt
  ,sourcepatientid
  ,centreid
  ,centrecode
  ,centrename
  ,address
  ,address1
  ,address2
  ,city
  ,state
  ,country
  ,pin
  ,remarks
  ,nationality
  ,vipflag
  ,marital_status
  ,ptnt_actual_dob_flg
  ,labname
  ,srcbaddata
  ,srcbaddataremarks
  from
  (
    select
    patientid
    ,encounterid
    ,salutation
    ,name as sourcename
    ,case when name like 'baby %' or name like 'b/o%' then name when name not like 'baby %' and name not like 'b/o %' and (salutation like 'baby%' or  salutation like 'b/o%') then NULLIF(trim(concat(ifnull(salutation,''), ' ',ifnull(name,''))), '') else name end as name
    ,firstname
    ,lastname
    ,gender
    ,dob
    ,email
    ,alternatemobile
    ,mobile
    ,joiningdate
    ,updateddate
    ,upd_dt
    ,sourcepatientid
    ,centreid
    ,centrecode
    ,centrename
    ,case when NULLIF(trim(address1),'') is not null and NULLIF(trim(address2),'') is not null then CONCAT(LOWER(NULLIF(trim(address1),'')), ' || ', LOWER(NULLIF(trim(address2),''))) when NULLIF(trim(address1),'') is not null then LOWER(NULLIF(trim(address1),'')) when NULLIF(trim(address2),'') is not null then LOWER(NULLIF(trim(address2),'')) end as address
    ,address1
    ,address2
    ,city
    ,state
    ,country
    ,pin
    ,remarks
    ,nationality
    ,vipflag
    ,marital_status
    ,ptnt_actual_dob_flg
    ,labname
    ,srcbaddata
    ,srcbaddataremarks
    from `sync_dump.patients_dump`
  )
)