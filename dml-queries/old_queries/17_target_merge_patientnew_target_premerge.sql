truncate table `sync_pre_merge.patientsnew_target_premerge`;
insert into `sync_pre_merge.patientsnew_target_premerge`
  (
    patientremoteid,patientid,age,gender,uid,mobile,
    alternatemobile,email,insertedon,joiningdate,
    name,patienthash,deceased,labname
  )
SELECT
  patientremoteid,patientid,age,gender,uid,mobile,
  alternatemobile,email,insertedon,joiningdate,
  name,patienthash,deceased,labname
from
  (
    select
      pf.patientremoteid,pf.patientid,
      COALESCE(pf.age, stamp.age) as age,
      COALESCE(pf.gender, stamp.gender) as gender,
      pf.uid,pf.mobile,pf.alternatemobile,pf.email,pf.insertedon,pf.joiningdate,
      pf.name,pf.patienthash,pf.deceased,pf.labname
    from
      `sync_target_merge.target_patient_for_merge` as pf
      left join `sync_target_merge.patienthash_stamp_demo` as stamp 
      on stamp.patienthash = pf.patienthash and (stamp.labname = pf.labname)
  ) as a;