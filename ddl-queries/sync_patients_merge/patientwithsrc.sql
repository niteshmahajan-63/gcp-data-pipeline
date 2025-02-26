CREATE TABLE `sync_merged.patientwithsrc` (
  patientid STRING,
  encounterid STRING,
  age INT64,
  gender STRING,
  dob DATE,
  maritalstatus STRING,
  uid STRING,
  mobile STRING,
  alternatemobile STRING,
  email STRING,
  remarks STRING,
  address STRING,
  centreid INT64,
  nationalityid INT64,
  city STRING,
  state STRING,
  country STRING,
  pin STRING,
  joiningdate TIMESTAMP,
  updateddate TIMESTAMP,
  salutation STRING,
  name STRING,
  patienthash STRING,
  deceased BOOL,
  labname STRING,
  pid INT64,
  eid INT64,
  emailid INT64,
  mobileid INT64,
  alternatemobileid INT64,
  remoteid INT64 NOT NULL,
  insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updatedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY remoteid;