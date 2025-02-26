CREATE OR REPLACE TABLE `sync_rawinput.labreports_json`
(
  patientid		STRING,
  approvedstatus		STRING,
  cancelledflag STRING,
  authenticateddate		TIMESTAMP,
  biomarkerid		STRING,
  biomarkername		STRING,
  biomarkerstampid		STRING,
  normalizedbiomarkername		STRING,
  sourcebiomarkername		STRING,
  biomarkercode		STRING,
  encounterid		STRING,
  bookingid		STRING,
  biomarkerhash		STRING,
  biomarkermetahash		STRING,
  centreid		STRING,
  centrename		STRING,
  centrecode		STRING,
  centrehash		STRING,
  patienttype		STRING,
  packageid		STRING,
  packagename		STRING,
  departmentid		STRING,
  department		STRING,
  type		STRING,
  itemtype		STRING,
  normalrange		STRING,
  rangestart		STRING,
  rangeend		STRING,
  indicator		STRING,
  comment		STRING,
  resulttype		STRING,
  result		STRING,
  resultnum		NUMERIC,
  resultdate		TIMESTAMP,
  testid		STRING,
  testname		STRING,
  sourcetestname		STRING,
  unit		STRING,
  remarks		STRING,
  itemrow		STRING,
  sourcepatientid		STRING,
  productcode		STRING,
  parentproductcode		STRING,
  method		STRING,
  processingunitid		STRING,
  processingunit		STRING,
  processingunitcode		STRING,
  rslt_crtcl_txt		STRING,
  rslt_stg		STRING,
  demographic		STRING,
  lab_ctgry_cd		STRING,
  range_val		STRING,
  baddata		BOOLEAN,
  baddataremarks		STRING,
  insertedon		TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
