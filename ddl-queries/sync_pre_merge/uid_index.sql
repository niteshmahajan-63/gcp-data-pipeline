create table if not exists `sync_pre_merge.uidindex`
(
  id int64,
  uid string,
  labname string,
  insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
