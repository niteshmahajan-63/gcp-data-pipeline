truncate table `sync_pre_merge.patients_new`;
insert into `sync_pre_merge.patients_new` (patientid, encounterid, salutation, name, age, gender, dob, maritalstatus, uid, mobile, alternatemobile, email, remarks, address, centrehash, city, state, country, nationality, pin, labname, patienthash, deceased, joiningdate, updateddate, insertedon) 
select patientid, encounterid, salutation, NULLIF(name,'') as name, age, NULLIF(gender,'') as gender, dob, maritalstatus, null as uid, NULLIF(mobile,'') as mobile, NULLIF(alternatemobile,'') as alternatemobile, NULLIF(email,'') as email, remarks, address, centrehash, city, state, country, nationality, pin, labname, patienthash, null as deceased, joiningdate, updateddate, insertedon
from (
SELECT *,
  	ROW_NUMBER() OVER(PARTITION BY 
        patienthash, name, mobile, email, alternatemobile, labname
        ORDER BY 
        	DATETIME(updateddate) desc, DATETIME(joiningdate) desc, DATETIME(insertedon) desc
        ) AS duplicate 
	from `sync_rawinput.patients_json` where baddata <> true

) where duplicate = 1;