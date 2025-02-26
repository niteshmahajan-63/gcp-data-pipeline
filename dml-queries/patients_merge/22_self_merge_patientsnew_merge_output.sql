drop table if exists `sync_target_merge.patients_merge_output` ;
create table `sync_target_merge.patients_merge_output` as
with mergeRemoteIds AS (
  select
    distinct
    remoteid,
    matchremoteid,
    labname
  from
    `sync_target_merge.patients_matched`
),
minRemoteId AS(
  select
  patientlocalid,
  min(matchremoteid) as remoteid,
  labname 
from
  (
    select remoteid as patientlocalid, matchremoteid, labname
    from mergeRemoteIds
  )
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
      a.remoteid as patientlocalid,
      COALESCE(b.remoteid, a.matchremoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join minRemoteId as b 
      on a.matchremoteid = b.patientlocalid
      and a.matchremoteid > b.remoteid
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
      a.remoteid as patientlocalid,
      COALESCE(b.remoteid, a.matchremoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch as b 
      on a.matchremoteid = b.patientlocalid
      and a.matchremoteid > b.remoteid
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
      a.remoteid as patientlocalid,
      COALESCE(b.remoteid, a.matchremoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch2 as b 
      on a.matchremoteid = b.patientlocalid
      and a.matchremoteid > b.remoteid
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
      a.remoteid as patientlocalid,
      COALESCE(b.remoteid, a.matchremoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch3 as b 
      on a.matchremoteid = b.patientlocalid
      and a.matchremoteid > b.remoteid
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
      a.remoteid as patientlocalid,
      COALESCE(b.remoteid, a.matchremoteid) as remoteid,
      a.labname
    from
      mergeRemoteIds as a
      inner join cascadeMatch4 as b 
      on a.matchremoteid = b.patientlocalid
      and a.matchremoteid > b.remoteid
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





drop table if exists `sync_target_merge.patients_merged_anm`;
create table `sync_target_merge.patients_merged_anm` as
select distinct inp.patientid, inp.encounterid, inp.age, inp.gender, inp.dob, inp.maritalstatus, inp.uid, inp.mobile, inp.alternatemobile, inp.email, inp.remarks, inp.address, inp.centreid, inp.city, inp.state, inp.country, inp.nationalityid, inp.pin, inp.insertedon, inp.joiningdate, inp.updateddate, inp.salutation, inp.name, inp.patienthash, inp.deceased, inp.labname, inp.pid, inp.eid, inp.emailid, inp.mobileid, inp.alternatemobileid, COALESCE(pmo.remoteid,inp.remoteid) as remoteid
from `sync_target_merge.merge_input_auid` inp
left join `sync_target_merge.patients_merge_output` pmo
on inp.remoteid = pmo.patientlocalid and inp.labname = pmo.labname;
