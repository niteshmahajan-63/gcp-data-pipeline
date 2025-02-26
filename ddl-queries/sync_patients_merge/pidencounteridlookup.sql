create table `sync_merged.pidencounteridlookup`
(
    patientid STRING,
    encounterid STRING,
    labname STRING,
    remoteid INT64 NOT NULL,
    insertedon timestamp default current_timestamp()
)