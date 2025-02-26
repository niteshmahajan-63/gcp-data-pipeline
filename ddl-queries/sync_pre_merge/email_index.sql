create table if not exists `sync_pre_merge.emailindex`
(
  id int64,
  email string,
  labname string,
  insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
cluster by email;
