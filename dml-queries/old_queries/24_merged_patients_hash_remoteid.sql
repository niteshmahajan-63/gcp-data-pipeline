truncate table `sync_merged.patients_hash_remoteid_old`;

insert into `sync_merged.patients_hash_remoteid_old` (patientid,patienthash,patientlocalid,patientremoteid,labname)
select patientid,patienthash,patientlocalid,patientremoteid,labname from `sync_merged.patients_hash_remoteid`;

truncate table `sync_merged.patients_hash_remoteid`;

insert into `sync_merged.patients_hash_remoteid` 
(patientid,patienthash,patientlocalid,patientremoteid,labname)
select
  patientid,
  patienthash,
  max(patientlocalid) as patientlocalid,
  min(patientremoteid) as patientremoteid,
  labname
FROM
(
  select patientid, patienthash, patientlocalid,
  patientremoteid,labname 
  from `sync_merged.patients_hash_remoteid_old` 
  union distinct
  select
    patientid,
    patienthash,
    patientlocalid,
    patientremoteid,
    labname
  from
    `sync_target_merge.patients_merge_output`
  union distinct
  select
    patientid,
    patienthash,
    patientlocalid,
    patientremoteid,
    labname
  from
    `sync_target_merge.patients_merge_noname_output`
  union distinct
  select
    patientid,
    patienthash,
    patientremoteid as patientlocalid,
    patientremoteid,
    labname
  from
    `sync_pre_merge.patientsnew_combine_premerge`
) as a
group by
  patientid,
  patienthash,
  labname;