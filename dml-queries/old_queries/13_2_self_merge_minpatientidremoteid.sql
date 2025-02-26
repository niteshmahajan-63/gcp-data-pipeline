drop table if exists `sync_target_merge.minpidremoteid`;
create table `sync_target_merge.minpidremoteid` cluster by pid as
select pid, name, labname, patientremoteid, alid as patientlocalid
(select pid, name, labname,
min(patientremoteid) as patientremoteid,
array_agg(patientremoteid) as agglist
from `sync_target_merge.patient_merge_input`
group by pid, name, labname
having count(distinct a.patientremoteid) > 1
) x cross join unnest(agglist) alid where patientremoteid != alid;

drop table if exists `sync_target_merge.minremoteidafterpid`;
create table `sync_target_merge.minremoteidafterpid`
select distinct inp.patientid, inp.encounterid, inp.patienthash, COALESCE(mpr.patientremoteid, inp.patientremoteid), inp.name, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.age, inp.gender, inp.dob, inp.deceased, inp.joiningdate ,inp.insertedon ,inp.labname, inp.pid, inp.mobileid, inp.alternatemobileid, inp.emailid, inp.eid 
from `sync_target_merge.patient_merge_input` inp
left join `sync_target_merge.minpidremoteid` mpr
on 


drop table if exists `sync_target_merge.minmobileremoteid`;
create table `sync_target_merge.minmobileremoteid` cluster by mobileid as
select mobileid, name, labname,
  min(patientremoteid) as patientremoteid
from
(
  select mobileid, name, labname,
    count(distinct patientremoteid) as cnt,
    min(patientremoteid) as patientremoteid
  from `sync_target_merge.patient_merge_input`
    where name is not null and length(name) > 2
    and mobileid > 0
    group by mobileid, name, labname
  union distinct
  select alternatemobileid as mobileid, name, labname,
    count(distinct patientremoteid) as cnt,
    min(patientremoteid) as patientremoteid
  from `sync_target_merge.patient_merge_input`
    where name is not null and length(name) > 2
    and alternatemobileid > 0
    group by alternatemobileid, name, labname
) as a
group by mobileid, name, labname
having count(distinct a.patientremoteid) > 1
or max(cnt) > 1;





drop table if exists `sync_target_merge.minemailremoteid`;
create table `sync_target_merge.minemailremoteid` cluster by emailid as
select emailid, name, labname,
min(patientremoteid) as patientremoteid
from 
(
  select emailid, name, labname,
    count(distinct patientremoteid) as cnt,
    min(patientremoteid) as patientremoteid
  from `sync_target_merge.patient_merge_input`
  where 
    name is not null and length(name) > 2
    and emailid > 0
    group by emailid, name, labname
) as a
group by emailid, name, labname
having count(distinct a.patientremoteid) > 1
or max(cnt) > 1;



drop table if exists `sync_target_merge.minuidremoteid`;
create table `sync_target_merge.minuidremoteid` cluster by eid as
select eid, name, labname,
min(patientremoteid) as patientremoteid
from 
(
  select eid, name, labname,
    count(distinct patientremoteid) as cnt,
    min(patientremoteid) as patientremoteid
  from `sync_target_merge.patient_merge_input`
  where 
    name is not null and length(name) > 2
    and eid > 0
    group by eid, name, labname
) as a
group by eid, name, labname
having count(distinct a.patientremoteid) > 1
or max(cnt) > 1;






drop table if exists `sync_self_merge.minpatientidremoteid`;
create table `sync_self_merge.minpatientidremoteid` cluster by patientid as
select patientid, labname,
min(patientremoteid) as patientremoteid
from 
(
  select patientid, labname,
  min(patientremoteid) as patientremoteid
  from `sync_pre_merge.patient_merge_input`
  group by patientid, labname
  union distinct
  select distinct a.patientid, a.labname, b.patientremoteid
  from `sync_pre_merge.patient_merge_input` as a
  inner join `sync_self_merge.minuidremoteid` as b
  on a.eid = b.eid
  and a.name is not null and length(a.name) > 2 and 
   a.name = b.name
  and a.labname = b.labname
  union distinct
  select distinct a.patientid, a.labname, b.patientremoteid
  from `sync_pre_merge.patient_merge_input` as a
  inner join `sync_self_merge.minemailremoteid` as b
  on a.emailid = b.emailid
  and a.name is not null and length(a.name) > 2 and 
   a.name = b.name
  and a.labname = b.labname
  UNION distinct
  select distinct a.patientid, a.labname, b.patientremoteid
  from `sync_pre_merge.patient_merge_input` as a
  inner join `sync_self_merge.minmobileremoteid` as b
  on (
  (a.mobileid > 0 and a.mobileid = b.mobileid) or
  (a.alternatemobileid > 0 and a.alternatemobileid = b.mobileid)
  )
  and a.name is not null and length(a.name) > 2 
  and a.name = b.name and a.labname = b.labname
  ) as a
group by patientid, labname
;