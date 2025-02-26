DELETE FROM `sync_merged.items`
WHERE EXISTS (
    SELECT 1
    FROM `sync_post_merge.billingrawtemp` f
    WHERE `sync_merged.items`.bookinghash = f.bookinghash
    AND `sync_merged.items`.patientid = f.patientid
);

INSERT INTO sync_merged.billingitem (patientid, bookingid, bookinghash, remarks, visittype, insurerparty, subpartyname, doctorname, sponsor, payer, encounterid, bedtype, subdepartment, policyname, invoicestatus, billstatus, bedno, beddesc, creditnote, policyno, encounterdate, orderdate, patientremoteid, channelid, entitydoctorid, entityreferalid, centreid, billingmetaid, patienttypeid, specialityid, departmentid, grossamount, discount, patientamount, netamount, tax, insuranceamount, qty, ignoreitemamount)
SELECT DISTINCT patientid, bookingid, bookinghash, remarks, visittype, insurerparty, subpartyname, doctorname, sponsor, payer, encounterid, bedtype, subdepartment, policyname, invoicestatus, billstatus, bedno, beddesc, creditnote, policyno, encounterdate, orderdate, pat.remoteid AS patientremoteid, c.id AS channelid, endoc.id AS entitydoctorid, enr.id AS entityreferalid, ch.id AS centreid, bmh.id AS billingmetaid, pt.id AS patienttypeid, spec.id AS specialityid, dpt.id AS departmentid, CAST(grossamount AS FLOAT64), CAST(discount AS FLOAT64), CAST(patientamount AS FLOAT64), CAST(netamount AS FLOAT64), CAST(tax AS FLOAT64), CAST(insuranceamount AS FLOAT64), CAST(CAST(qty AS FLOAT64) AS INT64), CAST(ignoreitemamount AS BOOLEAN)
FROM
sync_post_merge.billingrawtemp;