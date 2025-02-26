CREATE OR REPLACE TABLE `sync_ctas.biomarkers_ctasp`
(
    patientid INTEGER,
    biomarkermetaid INTEGER,
    bookingid STRING,
    bookingdate TIMESTAMP,
    insertedon TIMESTAMP,
    valued FLOAT64,
    testid STRING,
    testname STRING,
    biomarkercode STRING,
    labname STRING,
    biomarkerhash STRING,
    encounterid STRING
)
