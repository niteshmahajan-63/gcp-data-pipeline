create table if not exists `sync_merged.emailblacklist`
(
  email string not null,
  emailid int64 not null,
  names_count int64,
  deceased bool,
  labname string,
  insertedon timestamp default CURRENT_TIMESTAMP()
)
cluster by emailid;