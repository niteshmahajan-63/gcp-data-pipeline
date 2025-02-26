CREATE TABLE `sync_merged.items`
(
  patientid STRING,
  encounterid STRING,
  bookingid STRING,
  bookingdate TIMESTAMP,
  billstatus STRING,
  bookinghash STRING,
  patientremoteid INT64 NOT NULL,
  centreid INT64,
  entitydoctorid INT64,
  entityreferalid INT64,
  channelid INT64,
  billingmetaid INT64,
  departmentid INT64,
  grossamount FLOAT64,
  discount FLOAT64,
  netamount FLOAT64,
  tax FLOAT64,
  remarks STRING,
  subdepartment STRING,
  visittype STRING,
  qty INT64,
  processing_unit STRING,
  processing_branch STRING,
  patienttypeid INT64,
  specialityid INT64,
  ignoreitemamount BOOL,
  labname STRING,
  insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updatedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
cluster by bookingdate;