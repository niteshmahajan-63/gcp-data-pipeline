create table `sync_merged.billingsummary`
(
    patientid string,
    encounterid string,
    patientremoteid int64 NOT NULL,
    bookingid string,
    bookingdate timestamp,
    patienttypeid INT64,
    centreid int64,
    entityreferalid int64,
    channelid int64,
    partyname STRING,
    billtoparty STRING,
    customertype STRING,
    sourcetotalnetamount FLOAT64,
    customerno STRING,
    labname string,
    insertedon timestamp default current_timestamp(),
    updatedon timestamp default current_timestamp()
) cluster by bookingdate;
