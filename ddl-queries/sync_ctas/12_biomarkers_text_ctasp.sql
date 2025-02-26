CREATE OR REPLACE TABLE `sync_ctas.biomarkers_text_ctasp`
(
    biomarkermetaid INTEGER,
    code STRING,
    testname STRING,
    unit STRING,
    biomarkername STRING,
    patientremoteid INTEGER,
    resultdate TIMESTAMP,
    result STRING,
    patientid STRING,
    bookingid STRING,
    centreid INTEGER,
    labname STRING,
    biomarkerhash STRING,
    encounterid STRING
)
