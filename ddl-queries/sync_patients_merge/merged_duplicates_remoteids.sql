CREATE TABLE `sync_merged.merged_duplicate_remoteids`
(
  patientlocalid INT64,
  labname STRING,
  remoteid INT64 NOT NULL,
  updated boolean default false,
  insertedon timestamp default CURRENT_TIMESTAMP()
) cluster by patientlocalid;