create table if not exists `sync_candidates.emailcandidates` (
  patientid string,
  emailid int64,
  remoteid int64,
  mergeby string,
  name string,
  age int64,
  gender string,
  labname string
) cluster by emailid