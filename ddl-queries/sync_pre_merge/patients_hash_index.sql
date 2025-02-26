create table if not exists `sync_pre_merge.patients_hash_index`
(
  id int64 not null,
  patienthash string not null,
  labname string,
  insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
cluster by patienthash;