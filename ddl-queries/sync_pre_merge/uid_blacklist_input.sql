create table if not exists `sync_pre_merge.uid_blacklist_input`
(
  patientid string,
  name string,
  patienthash string,
  uid string,
  deceased bool,
  labname string,
  insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
cluster by uid;