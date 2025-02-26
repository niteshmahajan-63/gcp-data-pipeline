
create table `sync_post_merge.biorawsummarytemp` as
SELECT
  DISTINCT
  patientid,
  bookingid,
  name,
  '' as arab_name,
  uid,
  country,
  mobile,
  alternatemobile,
  email,
  department,
  patienttype,
  entitydoctorhash,
  entityreferalhash,
  centrehash,
  authenticateddate,
  specimendate,
  resultdate,
  CONCAT(
    REGEXP_REPLACE(patientid, r'[^A-Za-z0-9]', ''),
    '_',
    REGEXP_REPLACE(name, r'[^A-Za-z0-9]', ''),
    '_',
    CAST(age AS STRING),
    '_',
    REGEXP_REPLACE(CAST(DATE(bookingdate) AS STRING), r'[^A-Za-z0-9]', '')
  )
 AS patientkey,
 bookingdate,
  CASE
    WHEN gender IS NULL OR LENGTH(gender) < 1 THEN NULL
    WHEN TRIM(LOWER(gender)) LIKE 'f%' THEN 1
    WHEN TRIM(LOWER(gender)) LIKE 'm%' THEN 2
    ELSE 3
  END AS gender,
  NULLIF(TRIM(age), '') AS age,
  CASE WHEN TRIM(type) = 't' THEN TRUE ELSE FALSE END AS hastext,
  CASE WHEN TRIM(type) = 'n' THEN TRUE ELSE FALSE END AS hasnum,
  CASE WHEN TRIM(type) = 'h' THEN TRUE ELSE FALSE END AS hashtml,
  CASE WHEN TRIM(type) = 'd' THEN TRUE ELSE FALSE END AS hasdesc
FROM
  `sync_raw.labreports_json`
WHERE
  patientid IS NOT NULL;
