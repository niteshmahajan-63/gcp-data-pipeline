truncate table `sync_target_merge.patients_merge_noname_output`;
insert into `sync_target_merge.patients_merge_noname_output` (
    patientid,
    patienthash,
    patientlocalid,
    patientremoteid,
    labname
  )
with allNewPatients AS(
  select
    a.patienthash,
    a.patientremoteid as patientlocalid,
    COALESCE(b.patientremoteid, a.patientremoteid) as patientremoteid,
    name,
    a.patientid,
    a.labname
from
    `sync_target_merge.patient_merge_input` as a
    left join `sync_target_merge.patients_merge_output` as b 
    on a.patientremoteid = b.patientlocalid
    and b.labname = a.labname
),
pidwithNames as (
  select
    patientid,
    labname,
    count(distinct patientremoteid) as ridcount,
    min(patientremoteid) as patientremoteid
  from
    allNewPatients as anp
  where
    name is not null
  group by
    patientid,
    labname
  having
    count(distinct anp.patientremoteid) = 1
),
pidwithNoName as (
  select
    a.patientid,
    a.labname,
    min(b.patientremoteid) as patientremoteid
  FROM
    (
      select a.patientid, a.labname from
      (select
        patientid,
        labname
      from
        allNewPatients
      where
        name is null) as a
      left join
      (
        select distinct
        patientid,
        labname
      from
        allNewPatients
      where
        name is not null
      ) as b
      on a.patientid = b.patientid and a.labname = b.labname
      where b.patientid is null
      
    ) as a
    inner join allNewPatients as b on a.patientid = b.patientid
    and (
      a.labname = b.labname
    )
  group by
    a.patientid,
    a.labname
)
select
  a.patientid,
  a.patienthash,
  a.patientlocalid,
  COALESCE(
    pidwithNoName.patientremoteid,
    pidwithNames.patientremoteid,
    a.patientremoteid
  ) as patientremoteid,
  a.labname
from
  allNewPatients as a
  left join pidwithNoName on pidwithNoName.patientid = a.patientid
  and (
    pidwithNoName.labname is null
    or pidwithNoName.labname = a.labname
  )
  left join pidwithNames on pidwithNames.patientid = a.patientid
  and ( 
    pidwithNames.labname = a.labname
  )