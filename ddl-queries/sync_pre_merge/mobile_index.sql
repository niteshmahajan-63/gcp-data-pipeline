create table if not exists `sync_pre_merge.mobileindex`
(
  id int64,
  mobile string,
  labname string,
  insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
cluster by mobile;