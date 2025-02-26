DELETE FROM `sync_merged.billingsummary` a
WHERE EXISTS (
    SELECT 1
    FROM `sync_target_merge.billingsummaryrawids` f
    WHERE a.bookingid = f.bookingid
    AND a.encounterid = f.encounterid
    AND a.patientid = f.patientid
    AND a.labname = f.labname
);

INSERT INTO `sync_merged.billingsummary` (
    patientid, encounterid, patientremoteid, bookingid, bookingdate,
    centreid, entityreferalid, channelid, partyname, billtoparty, customertype,
    sourcetotalnetamount, customerno, labname
)
SELECT DISTINCT
    patientid, encounterid, patientremoteid, bookingid, bookingdate,
    centreid, entityreferalid, channelid, partyname, billtoparty, customertype,
    sourcetotalnetamount, customerno, labname
FROM
(
    SELECT DISTINCT
        b.patientid, b.encounterid, c.remoteid AS patientremoteid,
        b.bookingid, b.bookingdate, b.channelid, b.entityreferalid, b.centreid,
        b.partyname, b.billtoparty, b.customertype,
        b.sourcetotalnetamount, b.customerno, b.labname,
        ROW_NUMBER() OVER (
            PARTITION BY b.patientid, b.encounterid, b.bookingid, b.labname
            ORDER BY b.bookingdate DESC, b.sourcetotalnetamount DESC, b.channelid DESC
        ) AS rnum
    FROM `sync_target_merge.billingsummaryrawids` b
    INNER JOIN `sync_merged.pidencounteridlookup` c 
    ON b.patientid = c.patientid and b.encounterid = c.encounterid and b.labname = c.labname
) a
WHERE a.rnum = 1;