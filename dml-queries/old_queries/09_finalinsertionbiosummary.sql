DELETE FROM `sync_merged.biomarker_summary`
WHERE EXISTS (
    SELECT 1
    FROM `sync_post_merge.biorawsummaryfinaltemp` f
    WHERE `sync_merged.biomarker_summary`.bookingid = f.bookingid
    AND `sync_merged.biomarker_summary`.patientid = f.patientid
);


INSERT INTO  `sync_merged.biomarker_summary` (patientid, bookingid, patientremoteid, patienttypeid, entitydoctorid, entityreferalid, centreid, authenticateddate, specimendate, resultdate, gender, age, hastext, hasnum, hashtml, hasdesc)
SELECT DISTINCT
    a.patientid as patientid,
    a.bookingid as bookingid,
    b.patientremoteid as patientremoteid,
    a.patienttypeid as patienttypeid,
    a.entitydoctorid as entitydoctorid,
    a.entityreferalid as entityreferalid,
    a.centreid as centreid,
    a.authenticateddate as authenticateddate,
    a.specimendate as specimendate,
    a.resultdate as resultdate,
    a.gender as gender,
    a.age as age,
    a.hastext as hastext,
    a.hasnum as hasnum,
    a.hashtml as hashtml,
    a.hasdesc as hasdesc,
    current_timestamp() as creationdate
FROM `sync_post_merge.biorawsummaryfinaltemp` a
INNER JOIN `sync_merged.patientwithsrc` b ON a.patientid = b.patientid;
