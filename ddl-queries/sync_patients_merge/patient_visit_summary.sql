CREATE TABLE `sync_merged.patientvisitsummary`
(
  remoteid INT64 NOT NULL,
  labname STRING,
  patientid STRING,
  encounterid STRING,
  bookingid STRING,
  entitydoctorid INT64,
  entityreferalid INT64,
  centreid INT64,
  channelid INT64,
  patienttypeid INT64,
  lastbilldate TIMESTAMP,
  joiningdate TIMESTAMP,
  lastvisitdate TIMESTAMP,
  visitgap INT64,
  visitcount INT64,
  deceased boolean,
  bookingids ARRAY<STRING>
);