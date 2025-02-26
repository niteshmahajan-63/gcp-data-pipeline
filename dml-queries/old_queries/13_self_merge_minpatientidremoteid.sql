drop table if exists `sync_target_merge.minpidremoteid`;
create table `sync_target_merge.minpidremoteid` cluster by pid as
select pid, name, labname, patientremoteid, alid as patientlocalid from
(select pid, name, labname,
min(patientremoteid) as patientremoteid,
array_agg(patientremoteid) as agglist from
(
select distinct pid, name, labname, patientremoteid as patientlid, patientremoteid
from `sync_target_merge.patient_merge_input`
where name is not null
)
group by pid, name, labname
having count(distinct patientlid) > 1
) x cross join unnest(agglist) alid where patientremoteid != alid;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.merge_input_apid`;
create table `sync_target_merge.merge_input_apid` cluster by patientremoteid as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.centrecode, inp.centrename, inp.city, inp.state, inp.country, inp.pin, inp.insertedon, inp.joiningdate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(mpr.patientremoteid,inp.patientremoteid) as patientremoteid
from `sync_target_merge.patient_merge_input` inp
left join `sync_target_merge.minpidremoteid` mpr
on inp.patientremoteid = mpr.patientlocalid and inp.pid = mpr.pid and inp.name = mpr.name and inp.labname = mpr.labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minmobileremoteid`;
create table `sync_target_merge.minmobileremoteid` cluster by mobileid as
select mobileid, name, labname, patientremoteid, alid as patientlocalid from
(select mobileid, name, labname,
min(patientremoteid) as patientremoteid,
array_agg(patientremoteid) as agglist
from
(
  select distinct mobileid, name, labname, patientremoteid, patientremoteid as patientlid
  from `sync_target_merge.merge_input_apid`
    where name is not null
    and mobileid > 0
)
group by mobileid, name, labname
having count(distinct patientlid) > 1
) x cross join unnest(agglist) alid where patientremoteid != alid;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.merge_input_amid`;
create table `sync_target_merge.merge_input_amid` cluster by patientremoteid as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.centrecode, inp.centrename, inp.city, inp.state, inp.country, inp.pin, inp.insertedon, inp.joiningdate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(mmr.patientremoteid,inp.patientremoteid) as patientremoteid
from `sync_target_merge.merge_input_apid` inp
left join `sync_target_merge.minmobileremoteid` mmr
on inp.patientremoteid = mmr.patientlocalid and inp.mobileid = mmr.mobileid and inp.name = mmr.name and inp.labname = mmr.labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minaltmobileremoteid`;
create table `sync_target_merge.minaltmobileremoteid` cluster by alternatemobileid as
select alternatemobileid, name, labname, patientremoteid, alid as patientlocalid from
(select alternatemobileid, name, labname,
min(patientremoteid) as patientremoteid,
array_agg(patientremoteid) as agglist
from
(
  select distinct alternatemobileid, name, labname, patientremoteid, patientremoteid as patientlid
  from `sync_target_merge.merge_input_amid`
    where name is not null
    and alternatemobileid > 0
)
group by alternatemobileid, name, labname
having count(distinct patientlid) > 1
) x cross join unnest(agglist) alid where patientremoteid != alid;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.merge_input_aamid`;
create table `sync_target_merge.merge_input_aamid` cluster by patientremoteid as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.centrecode, inp.centrename, inp.city, inp.state, inp.country, inp.pin, inp.insertedon, inp.joiningdate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(mmr.patientremoteid,inp.patientremoteid) as patientremoteid
from `sync_target_merge.merge_input_amid` inp
left join `sync_target_merge.minaltmobileremoteid` mmr
on inp.patientremoteid = mmr.patientlocalid and inp.alternatemobileid = mmr.alternatemobileid and inp.name = mmr.name and inp.labname = mmr.labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minemailremoteid`;
create table `sync_target_merge.minemailremoteid` cluster by emailid as
select emailid, name, labname, patientremoteid, alid as patientlocalid from
(select emailid, name, labname,
min(patientremoteid) as patientremoteid,
array_agg(patientremoteid) as agglist
from (
select distinct emailid, name, labname, patientremoteid, patientremoteid as patientlid
from `sync_target_merge.merge_input_aamid`
where name is not null
and emailid > 0
)
group by emailid, name, labname
having count(distinct patientlid) > 1
) x cross join unnest(agglist) alid where patientremoteid != alid;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.merge_input_aeid`;
create table `sync_target_merge.merge_input_aeid` cluster by patientremoteid as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.centrecode, inp.centrename, inp.city, inp.state, inp.country, inp.pin, inp.insertedon, inp.joiningdate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(mer.patientremoteid,inp.patientremoteid) as patientremoteid
from `sync_target_merge.merge_input_aamid` inp
left join `sync_target_merge.minemailremoteid` mer
on inp.patientremoteid = mer.patientlocalid and inp.emailid = mer.emailid and inp.name = mer.name and inp.labname = mer.labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minuidremoteid`;
create table `sync_target_merge.minuidremoteid` cluster by eid as
select eid, name, labname, patientremoteid, alid as patientlocalid from
(select eid, name, labname,
min(patientremoteid) as patientremoteid,
array_agg(patientremoteid) as agglist
from (
select distinct eid, name, labname, patientremoteid, patientremoteid as patientlid
from `sync_target_merge.merge_input_aeid`
where name is not null
and eid > 0
)
group by eid, name, labname
having count(distinct patientlid) > 1
) x cross join unnest(agglist) alid where patientremoteid != alid;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.merge_input_auid`;
create table `sync_target_merge.merge_input_auid` cluster by patientremoteid as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.centrecode, inp.centrename, inp.city, inp.state, inp.country, inp.pin, inp.insertedon, inp.joiningdate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(mur.patientremoteid,inp.patientremoteid) as patientremoteid
from `sync_target_merge.merge_input_aeid` inp
left join `sync_target_merge.minuidremoteid` mur
on inp.patientremoteid = mur.patientlocalid and inp.eid = mur.eid and inp.name = mur.name and inp.labname = mur.labname;
