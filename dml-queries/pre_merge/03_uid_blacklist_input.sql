insert into `sync_pre_merge.uid_blacklist_input`
(patientid, name, patienthash, uid, deceased, labname)
select a.patientid, a.name, a.patienthash, a.uid, a.deceased, a.labname
FROM
  (
    select distinct 
    patientid, name, patienthash, uid, deceased, labname
    from `sync_pre_merge.patients_new` as a
    WHERE uid is not null and ( name is not null or deceased = true)
  ) as a
  LEFT OUTER JOIN `sync_pre_merge.uid_blacklist_input` b 
  ON  a.patienthash = b.patienthash AND a.uid = b.uid AND  a.labname = b.labname
WHERE b.patienthash is null or b.uid is null or b.labname is null or a.labname is null;
