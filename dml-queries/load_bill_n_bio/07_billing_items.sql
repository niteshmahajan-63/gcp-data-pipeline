drop table if exists `sync_target_merge.billingrawids`;
create table `sync_target_merge.billingrawids` cluster by bookinghash as
SELECT DISTINCT patientid,encounterid,bookingid,bookingdate,billstatus,bookinghash,patientremoteid,
centreid,entityreferalid,channelid,billingmetaid,departmentid,grossamount,discount,netamount,tax,
remarks,ignoreitemamount,labname
FROM (
    SELECT input.patientid, input.encounterid, input.bookingid, input.bookingdate, input.billstatus, input.bookinghash,
    pat.remoteid AS patientremoteid,
    c.id AS centreid, enr.id AS entityreferalid, ch.id AS channelid, met.id AS billingmetaid,
    dpt.id AS departmentid, input.grossamount, input.discount, input.netamount, input.tax,
    input.remarks, input.ignoreitemamount, input.labname,
    ROW_NUMBER () OVER (
        PARTITION BY input.bookinghash, input.patientid
        ORDER BY input.bookingdate DESC, input.centrename DESC, input.channel DESC, input.department DESC
    ) AS rnum
    FROM `sync_rawinput.billings_json` input 
    LEFT JOIN `sync_merged.billingmeta` met ON input.bookingmetahash = met.bookingmetahash
    LEFT JOIN `sync_merged.pidencounteridlookup` pat
    ON input.patientid = pat.patientid and input.encounterid = pat.encounterid and input.labname = pat.labname
    LEFT JOIN `sync_constant.centres` c ON input.centrehash = c.centrehash
    LEFT JOIN `sync_constant.entities` enr ON input.entityreferalhash = enr.entityhash
    LEFT JOIN `sync_constant.channel` ch ON input.channel = ch.value
    LEFT JOIN `sync_constant.department` dpt ON input.department = dpt.value
    where input.baddata <> true
) temp
WHERE rnum = 1;


DELETE FROM `sync_merged.items` a
WHERE EXISTS (
    SELECT 1
    FROM `sync_target_merge.billingrawids` f
    WHERE a.bookinghash = f.bookinghash
    AND a.patientid = f.patientid
    AND a.labname = f.labname
);

INSERT INTO `sync_merged.items`
(
    patientid,encounterid,bookingid,bookingdate,billstatus,bookinghash,patientremoteid,
    centreid,entityreferalid,channelid,billingmetaid,departmentid,grossamount,discount,netamount,tax,
    remarks,ignoreitemamount,labname
)
SELECT DISTINCT
patientid,encounterid,bookingid,bookingdate,billstatus,bookinghash,patientremoteid,
centreid,entityreferalid,channelid,billingmetaid,departmentid,grossamount,discount,netamount,tax,
remarks,ignoreitemamount,labname
FROM
`sync_target_merge.billingrawids`;