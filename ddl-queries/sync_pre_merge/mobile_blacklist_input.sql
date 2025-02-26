create table if not exists `sync_pre_merge.mobile_blacklist_input`
(
  patientid string not null,
  name string,
  patienthash string not null,
  mobile string not null,
  deceased bool,
  labname string,
  insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);