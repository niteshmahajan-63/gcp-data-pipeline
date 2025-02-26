drop table if exists `sync_self_merge.patientnew_mobileemailid`;
create table `sync_self_merge.patientnew_mobileemailid`
AS 
  SELECT DISTINCT 'mobile' as idtype, mobileid as idvalue, 
       labname
  FROM `sync_pre_merge.patients_new_with_ids`
  WHERE mobileid > 0

  UNION distinct
   SELECT DISTINCT 'mobile' as idtype,
      alternatemobileid as idvalue, labname
  FROM
  `sync_pre_merge.patients_new_with_ids`

  WHERE alternatemobileid > 0
  UNION distinct

  SELECT DISTINCT 'email' as idtype, emailid as idvalue, labname
  FROM
  `sync_pre_merge.patients_new_with_ids` 
  WHERE emailid > 0
  UNION distinct

  SELECT DISTINCT 'uid' as idtype, eid as idvalue, labname
  FROM
  `sync_pre_merge.patients_new_with_ids` 
  WHERE eid > 0
  UNION distinct

  SELECT DISTINCT 'pid' as idtype, pid as idvalue, labname
  FROM
  `sync_pre_merge.patients_new_with_ids` 
  WHERE pid > 0
  ;
