INSERT INTO sync_post_merge.billingrawtemp (patientid, bookingid, bookinghash, remarks, visittype, insurerparty, subpartyname, doctorname, sponsor, payer, encounterid, bedtype, subdepartment, policyname, invoicestatus, billstatus, bedno, beddesc, creditnote, policyno, encounterdate, orderdate, patientremoteid, channelid, entitydoctorid, entityreferalid, centreid, billingmetaid, patienttypeid, specialityid, departmentid, grossamount, discount, patientamount, netamount, tax, insuranceamount, qty, ignoreitemamount)
SELECT DISTINCT patientid, bookingid, bookinghash, remarks, visittype, insurerparty, subpartyname, doctorname, sponsor, payer, encounterid, bedtype, subdepartment, policyname, invoicestatus, billstatus, bedno, beddesc, creditnote, policyno, encounterdate, orderdate, pat.remoteid AS patientremoteid, c.id AS channelid, endoc.id AS entitydoctorid, enr.id AS entityreferalid, ch.id AS centreid, bmh.id AS billingmetaid, pt.id AS patienttypeid, spec.id AS specialityid, dpt.id AS departmentid, CAST(grossamount AS FLOAT64), CAST(discount AS FLOAT64), CAST(patientamount AS FLOAT64), CAST(netamount AS FLOAT64), CAST(tax AS FLOAT64), CAST(insuranceamount AS FLOAT64), CAST(CAST(qty AS FLOAT64) AS INT64), CAST(ignoreitemamount AS BOOLEAN)
FROM (
    SELECT *,
        RANK() OVER (
            PARTITION BY patientid, bookinghash
            ORDER BY bookingdate, rnum DESC
        ) AS rnk 
    FROM (
        SELECT input.patientid, input.bookingid, input.bookinghash, input.remarks, input.visittype, input.insurerparty, input.subpartyname, input.doctorname, input.sponsor, input.payer, input.encounterid, input.bedtype, input.subdepartment, input.policyname, input.invoicestatus, input.billstatus, input.bedno, input.beddesc, input.creditnote, input.policyno, input.encounterdate, input.orderdate,
            pat.remoteid AS patientremoteid,
            c.id AS centreid,
            endoc.id AS entitydoctorid,
            enr.id AS entityreferalid,
            ch.id AS channelid,
            spec.id AS specialityid,
            pt.id AS patienttypeid,
            bmh.id AS billingmetaid,
            dpt.id AS departmentid,
            CAST(input.grossamount AS FLOAT64),
            CAST(input.discount AS FLOAT64),
            CAST(input.patientamount AS FLOAT64),
            CAST(input.netamount AS FLOAT64),
            CAST(input.tax AS FLOAT64),
            CAST(input.insuranceamount AS FLOAT64),
            CAST(CAST(input.qty AS FLOAT64) AS INT64),
            CAST(input.ignoreitemamount AS BOOLEAN),
            ROW_NUMBER () OVER (
                PARTITION BY input.patientid, input.bookinghash
                ORDER BY input.bookingdate DESC
            ) AS rnum
        FROM sync_raw.billings_json input
        LEFT JOIN minkeyparttable pat ON input.patientid = pat.patientid
        LEFT JOIN `pepbigqueryexp.sync_constant.centres` c ON input.centrehash = c.centrehash
        LEFT JOIN `pepbigqueryexp.sync_constant.entities` endoc ON input.entitydoctorhash = endoc.entityhash
        LEFT JOIN `pepbigqueryexp.sync_constant.entities` enr ON input.entityreferalhash = enr.entityhash
        LEFT JOIN `pepbigqueryexp.sync_constant.channel` ch ON input.channel = ch.value
        LEFT JOIN `pepbigqueryexp.sync_constant.patienttype` pt ON input.patienttype = pt.value
        LEFT JOIN `pepbigqueryexp.sync_constant.salutation` salut ON input.salutation = salut.value
        LEFT JOIN `pepbigqueryexp.sync_constant.nationality` nat ON input.nationality = nat.value
        LEFT JOIN `pepbigqueryexp.sync_constant.department` dpt ON input.department = dpt.value
    ) temp
) temp2
WHERE rnk = 1;
