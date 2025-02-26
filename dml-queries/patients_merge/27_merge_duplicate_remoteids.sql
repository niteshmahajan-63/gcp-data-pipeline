drop table if exists `sync_target_merge.merged_remoteids`;
create table `sync_target_merge.merged_remoteids` cluster by patientlocalid as
with mergeRemoteIds AS (
    select patientlocalid, remoteid, labname from
    (
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.minpremoteid`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.minmremoteid`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.minamremoteid`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.mineremoteid`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.minuremoteid`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.patients_merge_output`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.minpanmremoteid`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.minmanmremoteid`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.minamanmremoteid`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.mineanmremoteid`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.minuanmremoteid`
        union distinct
        select distinct patientlocalid, remoteid, labname from `sync_target_merge.patients_merge_output2`
    )
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


insert into `sync_merged.merged_duplicate_remoteids` (patientlocalid,  labname,remoteid)
with fk as (
  select distinct patientlocalid, remoteid, labname from `sync_target_merge.merged_remoteids`
	where patientlocalid != remoteid
)
select a.remoteid as patientlocalid, a.labname, min(b.remoteid) as remoteid
from `sync_merged.patientmerged` as a
inner join fk as b on a.remoteid = b.patientlocalid and a.labname = b.labname
group by a.remoteid, a.labname;

update `sync_merged.patientwithsrc` as a 
set remoteid = b.remoteid , updatedon = current_timestamp()
from  (
  select * from `sync_merged.merged_duplicate_remoteids` where updated = false 
) as b
where a.remoteid = b.patientlocalid and a.labname = b.labname;

update `sync_merged.patientvisit` as a 
set remoteid = b.remoteid , updatedon = current_timestamp()
from  (
  select * from `sync_merged.merged_duplicate_remoteids` where updated = false 
) as b
where a.remoteid = b.patientlocalid and a.labname = b.labname;