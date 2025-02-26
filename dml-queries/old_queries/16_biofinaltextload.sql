DELETE FROM `sync_merged.biomarker_text`
WHERE EXISTS (
    SELECT 1
    FROM `sync_post_merge.bionumrawfinaltemp` f
    WHERE `sync_merged.biomarker_text`.biomarkerhash = f.biomarkerhash
    AND `sync_merged.biomarker_text`.encounterid = f.encounterid
    AND `sync_merged.biomarker_text`.bookingid = f.bookingid
);

INSERT INTO     `sync_merged.biomarker_text` (bookingid, patientid, biomarkerhash, result, comment, encounterid, labno, encounterno, subdepartment, fieldtype, patientremoteid, indicator, entitydoctorid, entityreferalid, centreid, biomarkermetaid, resultdate)
SELECT 
    bookingid, patientid, biomarkerhash, result, comment, encounterid, labno, encounterno, subdepartment, fieldtype, patientremoteid, indicator, entitydoctorid, entityreferalid, centreid, biomarkermetaid, resultdate
FROM `sync_post_merge.biotextrawfinaltemp`;