create table if not exists `sync_pre_merge.patientsnew_combine_premerge`
(
  patientid string,
  patienthash string,
  patientremoteid int64,
  name string,
  uid string,
  mobile string,
  alternatemobile string,
  email string,
  age string,
  gender string,
  deceased bool,
  joiningdate timestamp,
  insertedon timestamp,
  labname string
)
cluster by patientremoteid;