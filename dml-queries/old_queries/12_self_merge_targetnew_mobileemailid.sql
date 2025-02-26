drop table if exists `sync_self_merge.target_mobileemailid`;
create table `sync_self_merge.target_mobileemailid`
AS
select 
  patientid,
  name,
  'mobile' as idtype, mobileid as idvalue,
  patientremoteid,
  labname
from `sync_merged.patientwithsrc` as idTbl
where
  name is not null and length(name) > 2
  and mobileid > 0 
  and not EXISTS (
    SELECT 1 FROM 
    `sync_merged.mobileblacklist` as mb 
    where 
    idTbl.mobileid = mb.mobileid
    and (idTbl.labname = mb.labname)
  )
union distinct
select 
  patientid,
  name,
  'mobile' as idtype, alternatemobileid as idvalue,
  patientremoteid,
  labname
from `sync_merged.patientwithsrc` as idTbl
where 
  name is not null and length(name) > 2
  and alternatemobileid > 0 
  and not EXISTS (
    SELECT 1 FROM 
    `sync_merged.mobileblacklist` as mb 
    where 
    idTbl.alternatemobileid = mb.mobileid
    and (idTbl.labname = mb.labname)
  )
union distinct
select  
  patientid,
  name,
  'email' as idtype, emailid as idvalue,
  patientremoteid,
  labname
from `sync_merged.patientwithsrc` as idTbl
where 
  emailid > 0 
  and name is not null and length(name) > 2
  and not EXISTS (
    SELECT 1 FROM 
    `sync_merged.emailblacklist` as eb 
    where 
    idTbl.emailid = eb.emailid
    and (idTbl.labname = eb.labname)
  )
union distinct
select  
  patientid,
  name,
  'uid' as idtype, eid as idvalue,
  patientremoteid,
  labname
from `sync_merged.patientwithsrc` as idTbl
where 
  eid > 0 
  and name is not null and length(name) > 2
  and not EXISTS (
    SELECT 1 FROM 
    `sync_merged.uidblacklist` as ub 
    where 
    idTbl.eid = ub.eid
    and (idTbl.labname = ub.labname)
  )
union distinct
select  
  patientid,
  name,
  'pid' as idtype, pid as idvalue,
  patientremoteid,
  labname
from `sync_merged.patientwithsrc` as idTbl
where 
  pid > 0 
  and name is not null and length(name) > 2
  ;
  
  
drop table if exists `sync_self_merge.targetnew_mobileemailid`;
create table `sync_self_merge.targetnew_mobileemailid` cluster by idvalue as
select a.idvalue, a.idtype, a.name, a.labname,
count(distinct patientremoteid) as cnt,
min(patientremoteid) as patientremoteid
from 
`sync_self_merge.target_mobileemailid` as a
INNER JOIN
`sync_self_merge.patientnew_mobileemailid` as b
on a.idvalue = b.idvalue and a.idtype = b.idtype and a.labname = b.labname
GROUP by a.idvalue, a.idtype, a.name, a.labname;