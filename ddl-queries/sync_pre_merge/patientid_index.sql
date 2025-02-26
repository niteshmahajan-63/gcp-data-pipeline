create table if not exists `sync_pre_merge.patientidindex`
(
  id int64 not null,
  patientid string not null,
  labname string,
  insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
cluster by patientid;