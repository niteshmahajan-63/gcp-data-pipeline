create or replace table `sync_post_merge.billingrawsummaryfinaltemp` as
select patientid, bookingid,bookinghash, name, arab_name, address, uid, country, dischargetype, encounterid,
    mobileid, alternatemobileid, emailid, patienttypeid, channelid, entitydoctorid,  entityreferaldochash, centreid, salutationid, nationalityid, departmentid,
    admitdate, bookingdate, dischargedate,
    dob, patientkey, gender, age,  deceased,deceaseddate,deceasedcause
from
(
    select input.patientid as patientid, input.bookingid as bookingid,input.bookinghash as bookinghash,
        input.name as name, null as arab_name, input.address as address,
        input.uid as uid, input.country as country, input.dischargetype as dischargetype,
        input.encounterid as encounterid, mb.id as mobileid, amb.id as alternatemobileid,
        em.id as emailid, pt.id as patienttypeid, ch.id as channelid, endoc.id as entitydoctorid,
      enr.id as entityreferaldochash, c.id as centreid,
        salut.id as salutationid, nat.id as nationalityid, dpt.id as departmentid,
        TIMESTAMP(input.admitdate) as admitdate, TIMESTAMP(input.bookingdate) as bookingdate,
        TIMESTAMP(input.dischargedate) as dischargedate, DATE(input.dob) as dob,
        patientkey,
       gender,
       age, 
        CASE
            WHEN input.deceased = true THEN TRUE
            ELSE FALSE
        END AS deceased,deceaseddate,deceasedcause,
    FROM `sync_post_merge.billingrawsummarytemp` input
    LEFT JOIN `sync_pre_merge.mobileindex` mb ON input.mobile = mb.mobile
    LEFT JOIN `sync_pre_merge.mobileindex` amb ON input.alternatemobile = amb.mobile
    LEFT JOIN `sync_pre_merge.emailindex` em ON input.email = em.email
    LEFT JOIN `pepbigqueryexp.sync_constant.centres` c ON input.centrehash = c.centrehash
    LEFT JOIN `pepbigqueryexp.sync_constant.entities` endoc ON input.entitydoctorhash = endoc.entityhash
    LEFT JOIN `pepbigqueryexp.sync_constant.entities` enr ON input.entityreferalhash = enr.entityhash
    LEFT JOIN `pepbigqueryexp.sync_constant.channel` ch ON input.channel = ch.value
    LEFT JOIN `pepbigqueryexp.sync_constant.patienttype` pt ON input.patienttype = pt.value
    LEFT JOIN `pepbigqueryexp.sync_constant.salutation` salut ON input.salutation = salut.value
    LEFT JOIN `pepbigqueryexp.sync_constant.nationality` nat ON input.nationality = nat.value
    LEFT JOIN `pepbigqueryexp.sync_constant.department` dpt ON input.department = dpt.value
) a
