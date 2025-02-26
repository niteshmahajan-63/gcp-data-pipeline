create table if not exists `sync_target_merge.patients_merge_output`
(
  patientid string,
  patienthash string,
  patientlocalid int64,
  patientremoteid int64,
  labname string
)
cluster by patientremoteid;