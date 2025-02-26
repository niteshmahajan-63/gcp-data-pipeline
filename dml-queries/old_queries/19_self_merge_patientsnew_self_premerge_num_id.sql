truncate table `sync_target_merge.patientsnew_target_premerge_num_id`;

insert into `sync_target_merge.patientsnew_target_premerge_num_id`
(patientid, patienthash, patientremoteid, name, age, gender,
    idtype, idvalueid, joiningdate, labname)
select pn.patientid, pn.patienthash,patientremoteid, name, age, gender,
    'mobile' as idtype, idTbl.id as idvalueid,
    pn.joiningdate as joiningdate,
    pn.labname
from `sync_pre_merge.patientsnew_combine_premerge` as pn
inner join
  `sync_pre_merge.mobileindex` as idTbl
  on pn.mobile = idTbl.mobile
  and (pn.labname is null or pn.labname = idTbl.labname)
  where pn.name is not null and pn.mobile is not null
  and not EXISTS (
      SELECT 
        1 
      FROM 
        `sync_merged.mobileblacklist` as mb 
      where 
        idTbl.mobile = mb.mobile 
        and (
          idTbl.labname is null 
          or idTbl.labname = mb.labname
        )
    )

  union distinct

  select pn.patientid, pn.patienthash, patientremoteid, name, age, gender,
    'mobile', idTbl.id, 
    pn.joiningdate as joiningdate, pn.labname
from `sync_pre_merge.patientsnew_combine_premerge` as pn
inner join
  `sync_pre_merge.mobileindex` as idTbl
  on pn.alternatemobile = idTbl.mobile
  and (pn.labname is null or pn.labname = idTbl.labname)
  where pn.name is not null and pn.alternatemobile is not null
  and not EXISTS (
      SELECT 
        1 
      FROM 
        `sync_merged.mobileblacklist` as mb 
      where 
        idTbl.mobile = mb.mobile 
        and (
          idTbl.labname is null 
          or idTbl.labname = mb.labname
        )
    )
  union distinct

  select pn.patientid, pn.patienthash, patientremoteid, name, age, gender,
    'email', idTbl.id,pn.joiningdate as joiningdate,
    pn.labname 
from `sync_pre_merge.patientsnew_combine_premerge` as pn
inner join
  `sync_pre_merge.emailindex` as idTbl
  on pn.email = idTbl.email
  and (pn.labname is null or pn.labname = idTbl.labname)
  where pn.name is not null and pn.email is not null
  and not EXISTS (
      SELECT 
        1 
      FROM 
        `sync_merged.emailblacklist` as eb 
      where 
        idTbl.email = eb.email 
        and (
          idTbl.labname is null 
          or idTbl.labname = eb.labname
        )
    )

-- merge src and merge target
-- src -> 

-- var columns = "name,patientremoteid,age,gender,namelen,patientid,bookingid,joiningdate,nameinitials,cardinitials,mobileid,alternatemobileid,emailid,labname"


-- patientid, patienthash, gender, age, joiningdate, mobileid, emailid, labname

--  group by hash, patientid, name same -> use least patientremoteid
--  group by mobileid, name same -> use data.
--  group by patientid, no name match ,and date < 90 days -> use data nearest match.

-- name match function 

-- name, idtype, idvalue group by. -> find age, gender neareast -> stamp it.

-- match by pid, name match , joining date match 
-- 