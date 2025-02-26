CREATE OR REPLACE TABLE `sync_ctas.biomarkers_ctasp_new_temp`
(
    patientid INTEGER,
    biomarkermetaid INTEGER,
    bookingid STRING,
    bookingdate TIMEZONE,
    creationdate TIMEZONE,
    valued FLOAT64,
    testid STRING,
    testname STRING,
    biomarkercode STRING,
    labname STRING,
    encounterid STRING
)