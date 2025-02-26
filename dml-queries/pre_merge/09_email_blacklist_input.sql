insert into `sync_pre_merge.email_blacklist_input`
(patientid, name, patienthash, email, deceased, labname)
select a.patientid, a.name, a.patienthash, a.email, a.deceased, a.labname
FROM
  (
    select distinct 
    patientid, name, patienthash, email, deceased, labname
    from `sync_pre_merge.patients_new` as a
    WHERE email is not null and ( name is not null or deceased = true)
  ) as a
  LEFT OUTER JOIN `sync_pre_merge.email_blacklist_input` b 
  ON  a.patienthash = b.patienthash AND a.email = b.email AND  a.labname = b.labname
WHERE b.patienthash is null or b.email is null or b.labname is null or a.labname is null;
