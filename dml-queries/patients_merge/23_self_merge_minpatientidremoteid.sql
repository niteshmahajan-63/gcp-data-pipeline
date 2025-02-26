drop table if exists `sync_target_merge.minpidanmremoteid`;
create table `sync_target_merge.minpidanmremoteid` cluster by pid as
select pid, name, labname, remoteid, alid as patientlocalid from
(select pid, name, labname,
min(remoteid) as remoteid,
array_agg(remoteid) as agglist from
(
select distinct pid, name, labname, remoteid as patientlid, remoteid
from `sync_target_merge.patients_merged_anm`
where name is not null
and pid > 0
)
group by pid, name, labname
having count(distinct patientlid) > 1
) x cross join unnest(agglist) alid where remoteid != alid;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minpanmremoteid`;
create table `sync_target_merge.minpanmremoteid` cluster by patientlocalid as
with mergeRemoteIds AS (
  select
    distinct
    remoteid,
    patientlocalid,
    labname
  from
    `sync_target_merge.minpidanmremoteid`
),
minRemoteId AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  mergeRemoteIds
group by
  patientlocalid,
  labname
),
cascadeMatch AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join minRemoteId as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch2 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch3 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch4 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch5 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
)
select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch5
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch4
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch3
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch2
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch
    UNION distinct
    SELECT 
      patientlocalid,
      remoteid,
      labname
    FROM 
      minRemoteId
  ) as a
group by
  patientlocalid,
  labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.patients_merged_apid`;
create table `sync_target_merge.patients_merged_apid` cluster by remoteid as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.city, inp.state, inp.country, inp.nationalityid, inp.pin, inp.insertedon, inp.joiningdate, inp.updateddate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(mpr.remoteid,inp.remoteid) as remoteid
from `sync_target_merge.patients_merged_anm` inp
left join `sync_target_merge.minpanmremoteid` mpr
on inp.remoteid = mpr.patientlocalid and inp.labname = mpr.labname;

-----------------------------------------------------------------------------


drop table if exists `sync_target_merge.minmobileanmremoteid`;
create table `sync_target_merge.minmobileanmremoteid` cluster by mobileid as
select mobileid, name, labname, remoteid, alid as patientlocalid from
(select mobileid, name, labname,
min(remoteid) as remoteid,
array_agg(remoteid) as agglist
from
(
  select distinct mobileid, name, labname, remoteid, remoteid as patientlid
  from `sync_target_merge.patients_merged_apid`
    where name is not null
    and mobileid > 0
)
group by mobileid, name, labname
having count(distinct patientlid) > 1
) x cross join unnest(agglist) alid where remoteid != alid;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minmanmremoteid`;
create table `sync_target_merge.minmanmremoteid` cluster by patientlocalid as
with mergeRemoteIds AS (
  select
    distinct
    remoteid,
    patientlocalid,
    labname
  from
    `sync_target_merge.minmobileanmremoteid`
),
minRemoteId AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  mergeRemoteIds
group by
  patientlocalid,
  labname
),
cascadeMatch AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join minRemoteId as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch2 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch3 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch4 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch5 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
)
select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch5
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch4
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch3
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch2
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch
    UNION distinct
    SELECT 
      patientlocalid,
      remoteid,
      labname
    FROM 
      minRemoteId
  ) as a
group by
  patientlocalid,
  labname;
-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.patients_merged_amid`;
create table `sync_target_merge.patients_merged_amid` cluster by remoteid as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.city, inp.state, inp.country, inp.nationalityid, inp.pin, inp.insertedon, inp.joiningdate, inp.updateddate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(mmr.remoteid,inp.remoteid) as remoteid
from `sync_target_merge.patients_merged_apid` inp
left join `sync_target_merge.minmanmremoteid` mmr
on inp.remoteid = mmr.patientlocalid and inp.labname = mmr.labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minaltmobileanmremoteid`;
create table `sync_target_merge.minaltmobileanmremoteid` cluster by alternatemobileid as
select alternatemobileid, name, labname, remoteid, alid as patientlocalid from
(select alternatemobileid, name, labname,
min(remoteid) as remoteid,
array_agg(remoteid) as agglist
from
(
  select distinct alternatemobileid, name, labname, remoteid, remoteid as patientlid
  from `sync_target_merge.patients_merged_amid`
    where name is not null
    and alternatemobileid > 0
)
group by alternatemobileid, name, labname
having count(distinct patientlid) > 1
) x cross join unnest(agglist) alid where remoteid != alid;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minamanmremoteid`;
create table `sync_target_merge.minamanmremoteid` cluster by patientlocalid as
with mergeRemoteIds AS (
  select
    distinct
    remoteid,
    patientlocalid,
    labname
  from
    `sync_target_merge.minaltmobileanmremoteid`
),
minRemoteId AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  mergeRemoteIds
group by
  patientlocalid,
  labname
),
cascadeMatch AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join minRemoteId as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch2 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch3 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch4 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch5 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
)
select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch5
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch4
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch3
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch2
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch
    UNION distinct
    SELECT 
      patientlocalid,
      remoteid,
      labname
    FROM 
      minRemoteId
  ) as a
group by
  patientlocalid,
  labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.patients_merged_aamid`;
create table `sync_target_merge.patients_merged_aamid` cluster by remoteid as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.city, inp.state, inp.country, inp.nationalityid, inp.pin, inp.insertedon, inp.joiningdate, inp.updateddate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(mmr.remoteid,inp.remoteid) as remoteid
from `sync_target_merge.patients_merged_amid` inp
left join `sync_target_merge.minamanmremoteid` mmr
on inp.remoteid = mmr.patientlocalid and inp.labname = mmr.labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minemailanmremoteid`;
create table `sync_target_merge.minemailanmremoteid` cluster by emailid as
select emailid, name, labname, remoteid, alid as patientlocalid from
(select emailid, name, labname,
min(remoteid) as remoteid,
array_agg(remoteid) as agglist
from (
select distinct emailid, name, labname, remoteid, remoteid as patientlid
from `sync_target_merge.patients_merged_aamid`
where name is not null
and emailid > 0
)
group by emailid, name, labname
having count(distinct patientlid) > 1
) x cross join unnest(agglist) alid where remoteid != alid;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.mineanmremoteid`;
create table `sync_target_merge.mineanmremoteid` cluster by patientlocalid as
with mergeRemoteIds AS (
  select
    distinct
    remoteid,
    patientlocalid,
    labname
  from
    `sync_target_merge.minemailanmremoteid`
),
minRemoteId AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  mergeRemoteIds
group by
  patientlocalid,
  labname
),
cascadeMatch AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join minRemoteId as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch2 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch3 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch4 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch5 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
)
select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch5
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch4
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch3
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch2
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch
    UNION distinct
    SELECT 
      patientlocalid,
      remoteid,
      labname
    FROM 
      minRemoteId
  ) as a
group by
  patientlocalid,
  labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.patients_merged_aeid`;
create table `sync_target_merge.patients_merged_aeid` cluster by remoteid as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.city, inp.state, inp.country, inp.nationalityid, inp.pin, inp.insertedon, inp.joiningdate, inp.updateddate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(mer.remoteid,inp.remoteid) as remoteid
from `sync_target_merge.patients_merged_aamid` inp
left join `sync_target_merge.mineanmremoteid` mer
on inp.remoteid = mer.patientlocalid and inp.labname = mer.labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minuidanmremoteid`;
create table `sync_target_merge.minuidanmremoteid` cluster by eid as
select eid, name, labname, remoteid, alid as patientlocalid from
(select eid, name, labname,
min(remoteid) as remoteid,
array_agg(remoteid) as agglist
from (
select distinct eid, name, labname, remoteid, remoteid as patientlid
from `sync_target_merge.patients_merged_aeid`
where name is not null
and eid > 0
)
group by eid, name, labname
having count(distinct patientlid) > 1
) x cross join unnest(agglist) alid where remoteid != alid;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.minuanmremoteid`;
create table `sync_target_merge.minuanmremoteid` cluster by patientlocalid as
with mergeRemoteIds AS (
  select
    distinct
    remoteid,
    patientlocalid,
    labname
  from
    `sync_target_merge.minuidanmremoteid`
),
minRemoteId AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  mergeRemoteIds
group by
  patientlocalid,
  labname
),
cascadeMatch AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join minRemoteId as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch2 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch3 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch4 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
),
cascadeMatch5 AS(
  select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select
      a.patientlocalid,
      COALESCE(b.remoteid, a.remoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.remoteid = b.patientlocalid
      and a.remoteid > b.remoteid
      and a.labname = b.labname
  ) as a
  group by
    a.patientlocalid,
    a.labname
)
select
  patientlocalid,
  min(remoteid) as remoteid,
  labname 
from
  (
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch5
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch4
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch3
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch2
    UNION distinct
    select 
      patientlocalid,
      remoteid,
      labname
    FROM cascadeMatch
    UNION distinct
    SELECT 
      patientlocalid,
      remoteid,
      labname
    FROM 
      minRemoteId
  ) as a
group by
  patientlocalid,
  labname;

-----------------------------------------------------------------------------
drop table if exists `sync_target_merge.patients_merged_auid`;
create table `sync_target_merge.patients_merged_auid` cluster by remoteid as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.city, inp.state, inp.country, inp.nationalityid, inp.pin, inp.insertedon, inp.joiningdate, inp.updateddate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(mur.remoteid,inp.remoteid) as remoteid
from `sync_target_merge.patients_merged_aeid` inp
left join `sync_target_merge.minuanmremoteid` mur
on inp.remoteid = mur.patientlocalid and inp.labname = mur.labname;

