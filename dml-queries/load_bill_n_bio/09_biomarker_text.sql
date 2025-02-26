drop table if exists `sync_target_merge.biomarkertextrawids`;
create table `sync_target_merge.biomarkertextrawids` cluster by biomarkerhash as
SELECT 
patientid, encounterid, patientremoteid, bookingid, resultdate, authenticateddate, biomarkerhash, result,
comment, remarks, approvedstatus, cancelledflag, processingunit,
indicator, centreid, biomarkermetaid, departmentid, labname
FROM
(
    SELECT 
    input.patientid, input.encounterid, input.bookingid, input.resultdate, input.authenticateddate, input.biomarkerhash, input.result,
    input.comment, input.remarks, input.approvedstatus, input.cancelledflag, input.processingunit,
    pat.remoteid AS patientremoteid,
    indic.id AS indicator,
    c.id AS centreid,
    bmh.id AS biomarkermetaid,
    dpt.id AS departmentid,
    input.labname,
    ROW_NUMBER() OVER (PARTITION BY input.biomarkerhash, input.patientid ORDER BY resultdate DESC) AS rnum
    FROM
    (
        SELECT DISTINCT
        patientid, encounterid, bookingid, resultdate, authenticateddate, biomarkerhash, result,
        comment, remarks, approvedstatus, cancelledflag, processingunit,
        indicator, centrehash, biomarkermetahash, department, labname
        from `sync_rawinput.labreports_json` where baddata <> true and type = 't'
    ) input
    LEFT JOIN `sync_merged.pidencounteridlookup` pat
    ON input.patientid = pat.patientid and input.encounterid = pat.encounterid and input.labname = pat.labname
    LEFT JOIN `sync_constant.centres` c ON input.centrehash = c.centrehash
    LEFT JOIN `sync_constant.indicator` indic ON input.indicator = indic.value
    LEFT JOIN `sync_constant.department` dpt ON input.department = dpt.value
    LEFT JOIN `sync_merged.biomarkermeta` bmh ON input.biomarkermetahash = bmh.biomarkermetahash AND bmh.type = 't'
) AS temp
WHERE rnum = 1;

DELETE FROM `sync_merged.text` a
WHERE EXISTS (
    SELECT 1
    FROM `sync_target_merge.biomarkertextrawids` f
    WHERE a.biomarkerhash = f.biomarkerhash
    AND a.patientid = f.patientid
    AND a.labname = f.labname
);

INSERT INTO `sync_merged.text`
(
    patientid, encounterid, patientremoteid, bookingid, resultdate, authenticateddate, biomarkerhash, result,
    comment, remarks, approvedstatus, cancelledflag, processing_unit,
    indicator, centreid, biomarkermetaid, departmentid, labname
)
SELECT 
patientid, encounterid, patientremoteid, bookingid, resultdate, authenticateddate, biomarkerhash, result,
comment, remarks, approvedstatus, cancelledflag, processingunit as processing_unit,
indicator, centreid, biomarkermetaid, departmentid, labname
FROM `sync_target_merge.biomarkertextrawids`;