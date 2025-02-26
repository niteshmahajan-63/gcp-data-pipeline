create table `sync_candidates.mobilecandidates` (
  patientid string,
  mobileid int64,
  remoteid int64,
  mergeby string,
  name string,
  age int64,
  gender string,
  labname string
) cluster by mobileid