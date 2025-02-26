DELETE FROM `sync_merged.billingsummary`
WHERE EXISTS (
    SELECT 1
    FROM `sync_post_merge.billingrawsummaryfinaltemp` f
    WHERE `sync_merged.billingsummary`.bookingid = f.bookingid
    AND `sync_merged.billingsummary`.patientid = f.patientid
);


INSERT INTO sync_merged.billingsummary (
    patientid,
    bookingid,
    bookinghash,
    encounterid,
    bedtype,
    subdepartment,
    policyname,
    invoicestatus,
    billstatus,
    patientremoteid,
    patienttypeid,
    channelid,
    entitydoctorid,
    entityreferalid,
    entityreferaldocid,
    centreid,
    departmentid,
    admitdate,
    bookingdate,
    dischargedate,
    gender,
    age
)
WITH c AS (
    SELECT patientid, bookingid, SUM(netamount) AS netamount
    FROM  `sync_merged.items`
    WHERE ignoreitemamount = false
    GROUP BY 1, 2
)
SELECT DISTINCT
    a.patientid AS patientid,
    a.bookingid AS bookingid,
    a.bookinghash AS bookinghash,
    a.encounterid AS encounterid,
    a.bedtype AS bedtype,
    a.subdepartment AS subdepartment,
    a.policyname AS policyname,
    a.invoicestatus AS invoicestatus,
    a.billstatus AS billstatus,
    a.patientremoteid AS patientremoteid,
    a.patienttypeid AS patienttypeid,
    a.channelid AS channelid,
    a.entitydoctorid AS entitydoctorid,
    a.entityreferalid AS entityreferalid,
    a.entityreferaldocid AS entityreferaldocid,
    a.centreid AS centreid,
    a.departmentid AS departmentid,
    a.admitdate AS admitdate,
    a.bookingdate AS bookingdate,
    a.dischargedate AS dischargedate,
    a.gender AS gender,
    a.age AS age
FROM (
    SELECT DISTINCT
        b.patientid AS patientid,
        b.bookingid AS bookingid,
        b.bookinghash AS bookinghash,
        b.encounterid AS encounterid,
        b.bedtype AS bedtype,
        b.subdepartment AS subdepartment,
        b.policyname AS policyname,
        b.invoicestatus AS invoicestatus,
        b.billstatus AS billstatus,
c.patientremoteid AS patientremoteid,
        b.patienttypeid AS patienttypeid,
        b.channelid AS channelid,
        b.entitydoctorid AS entitydoctorid,
        '' AS entityreferalid,
        '' AS entityreferaldocid,
        b.centreid AS centreid,
        b.departmentid AS departmentid,
        b.admitdate AS admitdate,
        b.bookingdate AS bookingdate,
        b.dischargedate AS dischargedate,
        b.gender AS gender,
        b.age AS age,
        RANK() OVER (
            PARTITION BY b.bookinghash
            ORDER BY b.bookingdate, b.age, b.gender, b.channelid DESC
        ) AS rnum
    FROM `sync_post_merge.billingrawsummaryfinaltemp` b
    INNER JOIN `sync_merged.patientwithsrc` c ON b.patientid = c.patientid
) a
WHERE a.rnum = 1;

