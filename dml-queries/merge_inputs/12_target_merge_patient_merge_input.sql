drop table if exists `sync_target_merge.patientnew_pid`;
create table `sync_target_merge.patientnew_pid` cluster by pid
AS
select distinct pid, labname from `sync_pre_merge.patients_new_with_ids`
where pid > 0;

drop table if exists `sync_target_merge.patientnew_mobileid`;
create table `sync_target_merge.patientnew_mobileid` cluster by mobileid
AS
select distinct mobileid, labname from `sync_pre_merge.patients_new_with_ids`
where mobileid > 0
union distinct
select distinct alternatemobileid as mobileid, labname from `sync_pre_merge.patients_new_with_ids`
where alternatemobileid > 0;

drop table if exists `sync_target_merge.patientnew_emailid`;
create table `sync_target_merge.patientnew_emailid` cluster by emailid
AS
select distinct emailid, labname from `sync_pre_merge.patients_new_with_ids`
where emailid > 0;


drop table if exists `sync_target_merge.patientnew_eid`;
create table `sync_target_merge.patientnew_eid` cluster by eid
AS
select distinct eid, labname from `sync_pre_merge.patients_new_with_ids`
where eid > 0;

--------------------------------------------------------------------------------

drop table if exists `sync_target_merge.patienttarget_mobileid`;
create table `sync_target_merge.patienttarget_mobileid` cluster by mobileid
AS
select distinct pm.patientid, pm.encounterid, pm.age, pm.gender, pm.dob, pm.maritalstatus, pm.uid, pm.mobile, pm.alternatemobile, pm.email, pm.remarks, pm.address, pm.centreid, pm.city, pm.state, pm.country, pm.nationalityid, pm.pin, pm.insertedon, pm.joiningdate, pm.updateddate, pm.salutation, pm.name, pm.patienthash, pm.deceased, pm.labname, pm.pid, pm.eid, pm.emailid, pm.mobileid, pm.alternatemobileid, pm.remoteid
from `sync_merged.patientwithsrc` pm
inner join `sync_target_merge.patientnew_mobileid` pnm on pm.mobileid = pnm.mobileid and pm.labname = pnm.labname
union distinct
select distinct pma.patientid, pma.encounterid, pma.age, pma.gender, pma.dob, pma.maritalstatus, pma.uid, pma.mobile, pma.alternatemobile, pma.email, pma.remarks, pma.address, pma.centreid, pma.city, pma.state, pma.country, pma.nationalityid, pma.pin, pma.insertedon, pma.joiningdate, pma.updateddate, pma.salutation, pma.name, pma.patienthash, pma.deceased, pma.labname, pma.pid, pma.eid, pma.emailid, pma.mobileid, pma.alternatemobileid, pma.remoteid
from `sync_merged.patientwithsrc` pma
inner join `sync_target_merge.patientnew_mobileid` pnma on pma.alternatemobileid = pnma.mobileid and pma.labname = pnma.labname
;


drop table if exists `sync_target_merge.patienttarget_emailid`;
create table `sync_target_merge.patienttarget_emailid` cluster by emailid
AS
select distinct pm.patientid, pm.encounterid, pm.age, pm.gender, pm.dob, pm.maritalstatus, pm.uid, pm.mobile, pm.alternatemobile, pm.email, pm.remarks, pm.address, pm.centreid, pm.city, pm.state, pm.country, pm.nationalityid, pm.pin, pm.insertedon, pm.joiningdate, pm.updateddate, pm.salutation, pm.name, pm.patienthash, pm.deceased, pm.labname, pm.pid, pm.eid, pm.emailid, pm.mobileid, pm.alternatemobileid, pm.remoteid
from `sync_merged.patientwithsrc` pm
inner join `sync_target_merge.patientnew_emailid` pne on pm.emailid = pne.emailid and pm.labname = pne.labname
;


drop table if exists `sync_target_merge.patienttarget_eid`;
create table `sync_target_merge.patienttarget_eid` cluster by eid
AS
select distinct pm.patientid, pm.encounterid, pm.age, pm.gender, pm.dob, pm.maritalstatus, pm.uid, pm.mobile, pm.alternatemobile, pm.email, pm.remarks, pm.address, pm.centreid, pm.city, pm.state, pm.country, pm.nationalityid, pm.pin, pm.insertedon, pm.joiningdate, pm.updateddate, pm.salutation, pm.name, pm.patienthash, pm.deceased, pm.labname, pm.pid, pm.eid, pm.emailid, pm.mobileid, pm.alternatemobileid, pm.remoteid
from `sync_merged.patientwithsrc` pm
inner join `sync_target_merge.patientnew_eid` pnu on pm.eid = pnu.eid and pm.labname = pnu.labname
;

drop table if exists `sync_target_merge.patienttarget_pid`;
create table `sync_target_merge.patienttarget_pid` cluster by pid
AS
select distinct pm.patientid, pm.encounterid, pm.age, pm.gender, pm.dob, pm.maritalstatus, pm.uid, pm.mobile, pm.alternatemobile, pm.email, pm.remarks, pm.address, pm.centreid, pm.city, pm.state, pm.country, pm.nationalityid, pm.pin, pm.insertedon, pm.joiningdate, pm.updateddate, pm.salutation, pm.name, pm.patienthash, pm.deceased, pm.labname, pm.pid, pm.eid, pm.emailid, pm.mobileid, pm.alternatemobileid, pm.remoteid
from `sync_merged.patientwithsrc` pm
inner join `sync_target_merge.patientnew_pid` pnp on pm.pid = pnp.pid and pm.labname = pnp.labname
;
-------------------------------------------------------------------------

drop table if exists `sync_target_merge.combined_input`;
create table `sync_target_merge.combined_input` as
select distinct patientid, encounterid, age, gender, dob, maritalstatus, uid, mobile, alternatemobile, email, remarks, address, centreid, city, state, country, nationalityid, pin, insertedon, joiningdate, updateddate, salutation, name, patienthash, deceased, labname, pid, eid, emailid, mobileid, alternatemobileid, remoteid
from `sync_pre_merge.patient_merge_input`
union distinct
select distinct patientid, encounterid, age, gender, dob, maritalstatus, uid, mobile, alternatemobile, email, remarks, address, centreid, city, state, country, nationalityid, pin, insertedon, joiningdate, updateddate, salutation, name, patienthash, deceased, labname, pid, eid, emailid, mobileid, alternatemobileid, remoteid
from `sync_target_merge.patienttarget_pid`
union distinct
select distinct patientid, encounterid, age, gender, dob, maritalstatus, uid, mobile, alternatemobile, email, remarks, address, centreid, city, state, country, nationalityid, pin, insertedon, joiningdate, updateddate, salutation, name, patienthash, deceased, labname, pid, eid, emailid, mobileid, alternatemobileid, remoteid
from `sync_target_merge.patienttarget_mobileid`
union distinct
select distinct patientid, encounterid, age, gender, dob, maritalstatus, uid, mobile, alternatemobile, email, remarks, address, centreid, city, state, country, nationalityid, pin, insertedon, joiningdate, updateddate, salutation, name, patienthash, deceased, labname, pid, eid, emailid, mobileid, alternatemobileid, remoteid
from `sync_target_merge.patienttarget_emailid`
union distinct
select distinct patientid, encounterid, age, gender, dob, maritalstatus, uid, mobile, alternatemobile, email, remarks, address, centreid, city, state, country, nationalityid, pin, insertedon, joiningdate, updateddate, salutation, name, patienthash, deceased, labname, pid, eid, emailid, mobileid, alternatemobileid, remoteid
from `sync_target_merge.patienttarget_eid`;


drop table if exists `sync_target_merge.patient_merge_input`;
create table `sync_target_merge.patient_merge_input` as
select distinct patientid, encounterid, age, gender, dob, maritalstatus, uid, mobile, alternatemobile, email, remarks, address, centreid, city, state, country, nationalityid, pin, insertedon, joiningdate, updateddate, salutation, name, patienthash, deceased, labname, pid, eid, emailid, mobileid, alternatemobileid, remoteid
from `sync_target_merge.combined_input`;
