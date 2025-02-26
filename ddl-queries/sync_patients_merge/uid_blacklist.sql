create table if not exists `sync_merged.uidblacklist`
(
  uid string not null,
  eid int64 not null,
  names_count int64,
  deceased bool,
  labname string,
  insertedon timestamp default CURRENT_TIMESTAMP()
)
cluster by eid;