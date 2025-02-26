create table if not exists `sync_target_merge.patients_match_merge`
(
  patientid string not null,
  patienthash string not null,
  patientremoteid int64 not null,
  name string,
  age int64,
  gender string,
  dob date,
  matchtype string not null,
  matchvalue string not null,
  matchpatientid string not null,
  matchpatienthash string not null,
  matchremoteid int64 not null,
  matchname string,
  matchage int64,
  matchgender string,
  matchdob date,
  match bool not null,
  matchcandidate bool not null,
  nmscore int64,
  daydiff int64,
  labname string
)
cluster by patientremoteid;