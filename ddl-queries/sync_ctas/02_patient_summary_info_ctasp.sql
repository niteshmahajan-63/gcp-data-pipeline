CREATE OR REPLACE TABLE `sync_ctas.patient_summary_info_ctasp`
(
    patientid INTEGER,
    labpatientid STRING,
    labbillid STRING,
    name STRING,
    arab_name STRING,
    yearofbirth INTEGER,
    age INTEGER,
    gender STRING,
    mobile STRING,
    email STRING,
    mobileid INTEGER,
    emailid INTEGER,
    nationalityid INTEGER,
    country STRING,
    hasvalidmobile BOOLEAN,
    hasvalidemail BOOLEAN,
    joiningdate DATE,
    channelid INTEGER,
    channel STRING,
    lastcenterid INTEGER,
    lastcentrename STRING,
    servicetype STRING,
    patienttypeid INTEGER,
    patienttype STRING,
    visittype STRING,
    visit_count INTEGER,
    lastvisitdate DATE,
    labname STRING,
    encounterid STRING
)

