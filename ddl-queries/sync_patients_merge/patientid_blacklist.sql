create table if not exists `sync_merged.patientidblacklist`
(
  patientid string not null,
  pid int64 not null,
  names_count int64,
  deceased bool,
  labname string,
  insertedon timestamp default CURRENT_TIMESTAMP()
)
cluster by pid;