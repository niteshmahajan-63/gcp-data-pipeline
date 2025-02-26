CREATE OR REPLACE TABLE `sync_ctas.biomarkers_ctasp_new`
(
    patientid INTEGER,
    biomarkercode STRING,
    testid STRING,
    testname STRING,
    bookingid STRING,
    bookingdate TIMESTAMP,
    valued FLOAT64,
    latbookingdate TIMESTAMP,
    latvalued FLOAT64,
    prebookingdate TIMESTAMP,
    prevalued FLOAT64,
    labname STRING,
    encounterid STRING
)