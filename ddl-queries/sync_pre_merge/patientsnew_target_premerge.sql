create table if not exists `sync_pre_merge.patientsnew_target_premerge`
(
  patientremoteid int64,
  patientid string,
  age string,
  gender string,
  uid string,
  mobile string,
  alternatemobile string,
  email string,
  insertedon timestamp,
  joiningdate timestamp,
  name string,
  patienthash string,
  deceased bool,
  labname string
)
cluster by patientremoteid;