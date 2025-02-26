CREATE OR REPLACE TABLE `sync_ctas.billing_items_ctasp_temp`
(
    patientid INTEGER,
    labbillid STRING,
    billdate TIMESTAMP,
    doctorname STRING,
    centreid STRING,
    centrename STRING,
    itemid INTEGER,
    itemname STRING,
    itemtypeid INTEGER,
    itemtype STRING,
    patienttypeid INTEGER,
    patienttype STRING,
    channelid INTEGER,
    channel STRING,
    packagename STRING,
    packageid STRING,
    serviceid INTEGER,
    servicetype STRING,
    departmentid INTEGER,
    department STRING,
    entity_doctorname STRING,
    doctorid INTEGER,
    grossamount FLOAT64,
    netamount FLOAT64,
    discount FLOAT64,
    referalname STRING,
    remarks STRING,
    visittype STRING,
    insurerparty STRING,
    labname STRING,
    ignoreitemamount BOOLEAN,
    encounterid STRING
)
