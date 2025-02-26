drop table if exists `sync_target_merge.existing_patients`;
create table `sync_target_merge.existing_patients` as
select distinct m.patientid, m.encounterid, m.age, m.gender, m.dob, m.maritalstatus, m.uid, m.mobile, m.alternatemobile, m.email, m.remarks, m.address, m.centreid, m.city, m.state, m.country, m.nationalityid, m.pin, m.insertedon, m.joiningdate, m.updateddate, m.salutation, m.name, m.patienthash, m.deceased, m.labname, m.pid, m.eid, m.emailid, m.mobileid, m.alternatemobileid, COALESCE(b.remoteid,m.remoteid) as remoteid
from `sync_merged.patientwithsrc` m
inner join `sync_target_merge.patients_merged_aemailidstamping` b
on m.patientid = b.patientid and m.patienthash = b.patienthash and m.labname = b.labname;


DELETE FROM `sync_merged.patientwithsrc` a
WHERE EXISTS (
    SELECT 1
    FROM `sync_target_merge.existing_patients` b
    WHERE a.patientid = b.patientid
    AND a.patienthash = b.patienthash
    AND a.labname = b.labname
);

insert into `sync_merged.patientwithsrc` (patientid, encounterid, age, gender, dob, maritalstatus, uid, mobile, alternatemobile, email, remarks, address, centreid, city, state, country, nationalityid, pin, insertedon, joiningdate, updateddate, salutation, name, patienthash, deceased, labname, pid, eid, emailid, mobileid, alternatemobileid, remoteid)

select distinct patientid, encounterid, age, gender, dob, maritalstatus, case when ub.eid is null and a.eid is not null then a.uid else null end as uid , case when mb.mobileid is null and a.mobileid is not null then a.mobile else null end as mobile, case when amb.mobileid is null and a.alternatemobileid is not null then alternatemobile else null end as alternatemobile, case when eb.emailid is null and a.emailid is not null then a.email else null end as email, remarks, address, centreid, city, state, country, nationalityid, pin, a.insertedon, joiningdate, updateddate, salutation, name, patienthash, a.deceased, a.labname, pid, case when ub.eid is null then a.eid else null end as eid, case when eb.emailid is null and a.emailid is not null then a.emailid else null end as emailid,case when  mb.mobileid is null and a.mobileid is not null then a.mobileid else null end as mobileid,case when  amb.mobileid is null and a.alternatemobileid is not null then alternatemobileid else null end as alternatemobileid, remoteid
from
(
    select patientid, encounterid, age, gender, dob, maritalstatus, uid, mobile, alternatemobile, email, remarks, address, centreid, city, state, country, nationalityid, pin, insertedon, joiningdate, updateddate, salutation, name, patienthash, deceased, labname, pid, eid, emailid, mobileid, alternatemobileid, remoteid
    from `sync_target_merge.patients_merged_aemailidstamping`
    union distinct
    select patientid, encounterid, age, gender, dob, maritalstatus, uid, mobile, alternatemobile, email, remarks, address, centreid, city, state, country, nationalityid, pin, insertedon, joiningdate, updateddate, salutation, name, patienthash, deceased, labname, pid, eid, emailid, mobileid, alternatemobileid, remoteid
    from `sync_target_merge.existing_patients`
)a left join sync_merged.mobileblacklist mb on a.mobileid = mb.mobileid and a.labname = mb.labname
left join sync_merged.emailblacklist eb on a.emailid = eb.emailid and a.labname = eb.labname
left join sync_merged.uidblacklist ub on a.eid = ub.eid and a.labname = ub.labname
left join sync_merged.mobileblacklist amb on a.alternatemobileid = amb.mobileid and a.labname = amb.labname;


truncate table `sync_merged.pidencounteridlookup`;
insert into `sync_merged.pidencounteridlookup` (patientid, encounterid, labname, remoteid)
select distinct patientid, encounterid, labname, min(remoteid) as remoteid
from `sync_merged.patientwithsrc` group by patientid, encounterid, labname;


