CREATE TABLE `sync_merged.patientvisit`
(
    patientid STRING,
    encounterid STRING,
    bookingid STRING,
    entitydoctorid INT64,
    entityreferalid INT64,
    centreid INT64,
    channelid INT64,
    patienttypeid INT64,
    src INT64,
    labname STRING,
    joiningdate TIMESTAMP,
    billdate TIMESTAMP,
    remoteid INT64 NOT NULL,
    insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updatedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);