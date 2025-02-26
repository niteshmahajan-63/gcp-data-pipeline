truncate table `sync_pre_merge.patientsnew_self_premerge`;
insert into `sync_pre_merge.patientsnew_self_premerge`
(patientremoteid, patientid, age, gender, uid, mobile, alternatemobile, email, 
 insertedon, joiningdate, name,
 patienthash, deceased, labname)
select 
COALESCE(pidMin.patientremoteid, pf.patientremoteid) as patientremoteid, 
pf.patientid,
COALESCE(pf.age, stamp.age) as age,
  COALESCE(pf.gender, stamp.gender) as gender,
   pf.uid, pf.mobile, pf.alternatemobile, pf.email,
 pf.insertedon, pf.joiningdate,
 pf.name,
 pf.patienthash, pf.deceased, 
 pf.labname  
from 
  `sync_pre_merge.patients_new_with_ids` as pf 

  left join 
  `sync_self_merge.minpatientidremoteid` as pidMin
  on pidMin.patientid = pf.patientid
  and (
     pidMin.labname = pf.labname
  )
  left join 
  (select patienthash, labname, rnk, age, gender
  from `sync_self_merge.patienthash_stamp_demo` 
  where rnk=1
  ) 
   stamp on stamp.patienthash = pf.patienthash 
  and (
    stamp.labname is null 
    or stamp.labname = pf.labname
  );