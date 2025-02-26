insert into `sync_pre_merge.mobile_blacklist_input`
(patientid, name, patienthash, mobile, deceased, labname)

select a.patientid, a.name, a.patienthash, a.mobile, a.deceased, a.labname
FROM
  (
    select distinct 
    patientid, name, patienthash, mobile, deceased, labname
    from `sync_pre_merge.patients_new` as a
    WHERE mobile is not null and ( name is not null or deceased = true)
    union distinct
    select distinct 
    patientid, name, patienthash,  alternatemobile as mobile, deceased, labname
    from `sync_pre_merge.patients_new` as a
    WHERE alternatemobile is not null and ( name is not null or deceased = true)
  ) as a
  LEFT OUTER JOIN `sync_pre_merge.mobile_blacklist_input` b 
  ON  a.patienthash = b.patienthash AND a.mobile = b.mobile AND  a.labname = b.labname
WHERE b.patienthash is null or b.mobile is null or b.labname is null or a.labname is null;
