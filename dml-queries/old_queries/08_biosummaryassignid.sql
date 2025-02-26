-- INSERT INTO `your_output_table`(
--     patientid, bookingid, name, arab_name, uid, country,
--     mobile, alternatemobile, email, department, patienttype, entitydoctorhash, entityreferalhash, centrehash,
--     authenticateddate, specimendate, resultdate,
--     hastext, hasnum, hashtml, hasdesc,
--     patientkey, gender, age,
--     mobileid, alternatemobileid, emailid, departmentid, patienttypeid, entitydoctorid, entityreferalid, centreid
-- )
create or replace table `sync_post_merge.biorawsummaryfinaltemp` as
SELECT
    patientid, bookingid, input.name, arab_name, uid, country,
     department, patienttype, entitydoctorhash, entityreferalhash, 
    authenticateddate, specimendate, resultdate,
    hastext, hasnum, hashtml, hasdesc,
    input.patientkey AS patientkey,
    gender, age,
    mb.id AS mobileid,
    amb.id AS alternatemobileid,
    em.id AS emailid,
    c.id AS centreid,
    endoc.id AS entitydoctorid,
    enr.id AS entityreferalid,
    pt.id AS patienttypeid,
    dpt.id as departmentid
FROM
    `sync_post_merge.biorawsummarytemp` input
LEFT JOIN `sync_pre_merge.mobileindex` mb ON input.mobile = mb.mobile
    LEFT JOIN `sync_pre_merge.mobileindex` amb ON input.alternatemobile = amb.mobile
    LEFT JOIN `sync_pre_merge.emailindex` em ON input.email = em.email
    LEFT JOIN `pepbigqueryexp.sync_constant.centres` c ON input.centrehash = c.centrehash
    LEFT JOIN `pepbigqueryexp.sync_constant.entities` endoc ON input.entitydoctorhash = endoc.entityhash
    LEFT JOIN `pepbigqueryexp.sync_constant.entities` enr ON input.entityreferalhash = enr.entityhash
    LEFT JOIN `pepbigqueryexp.sync_constant.patienttype` pt ON input.patienttype = pt.value


    LEFT JOIN `pepbigqueryexp.sync_constant.department` dpt ON input.department = dpt.value