drop table if exists `sync_pre_merge.patient_merge_input`;
create table `sync_pre_merge.patient_merge_input` cluster by patientid as
select distinct patientid, encounterid, age, gender, dob, maritalstatus,
uid, mobile, alternatemobile, email, remarks, address, centreid,
city, state, country, nationalityid, pin, insertedon, joiningdate, updateddate, salutation, name, patienthash, deceased,
labname, pid, eid, emailid, mobileid, alternatemobileid, remoteid
FROM
`sync_pre_merge.patients_new_with_ids`;