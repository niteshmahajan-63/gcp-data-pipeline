insert into `sync_pre_merge.patientid_blacklist_input`
(patientid, name, patienthash, deceased, labname)
select a.patientid, a.name, a.patienthash, a.deceased, a.labname
FROM
  (
    select distinct 
    patientid, name, patienthash, deceased, labname
    from `sync_pre_merge.patients_new` as a
    WHERE patientid is not null and ( name is not null or deceased = true)
  ) as a
  LEFT OUTER JOIN `sync_pre_merge.patientid_blacklist_input` b 
  ON  a.patienthash = b.patienthash AND a.patientid = b.patientid AND  a.labname = b.labname
WHERE b.patienthash is null or b.patientid is null or b.labname is null or a.labname is null;
