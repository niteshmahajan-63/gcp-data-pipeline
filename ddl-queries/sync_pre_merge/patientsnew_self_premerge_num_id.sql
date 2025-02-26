create table if not exists `sync_target_merge.patientsnew_target_premerge_num_id`
(
  patientid string,
  patienthash string,
  patientremoteid int64,
  name string,
  age string,
  gender string,
  dob date,
  idtype string,
  idvalueid int64,
  joiningdate timestamp,
  labname string
)
cluster by patientremoteid;