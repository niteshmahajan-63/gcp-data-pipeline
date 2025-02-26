CREATE OR REPLACE TABLE `sync_ctas.biomarkers_ctasp_temp`
(
    patientid INTEGER,
    biomarkermetaid INTEGER,
    bookingid STRING,
    bookingdate TIMESTAMP,
    creationdate TIMESTAMP,
    valued FLOAT64,
    testid STRING,
    testname STRING,
    biomarkercode STRING,
    labname STRING,
    encounterid STRING
)