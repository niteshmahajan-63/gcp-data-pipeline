DELETE FROM `sync_merged.numeric`
WHERE EXISTS (
    SELECT 1
    FROM `sync_post_merge.bionumrawfinaltemp` f
    WHERE `sync_merged.numeric`.biomarkerhash = f.biomarkerhash
    AND `sync_merged.numeric`.encounterid = f.encounterid
    AND `sync_merged.numeric`.bookingid = f.bookingid
);

INSERT INTO `sync_merged.numeric` (bookingid, patientid, biomarkerhash, result, comment, encounterid, labno, encounterno, subdepartment, fieldtype, patientremoteid, indicator, entitydoctorid, entityreferalid, centreid, biomarkercode, resultnum, resultdate)
SELECT 
    bookingid, patientid, biomarkerhash, result, comment, encounterid, labno, encounterno, subdepartment, fieldtype, patientremoteid, indicator, entitydoctorid, entityreferalid, centreid, biomarkercode, resultnum, resultdate
FROM `sync_post_merge.bionumrawfinaltemp`;