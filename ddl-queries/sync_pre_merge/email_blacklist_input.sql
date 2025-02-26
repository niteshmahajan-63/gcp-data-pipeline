create table if not exists `sync_pre_merge.email_blacklist_input`
(
  patientid string,
  name string,
  patienthash string,
  email string,
  deceased bool,
  labname string,
  insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
cluster by email;