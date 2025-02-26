truncate table `sync_pre_merge.patients_new_with_ids`;
insert into `sync_pre_merge.patients_new_with_ids` (patientid,encounterid,age,gender,dob,maritalstatus,uid,mobile,alternatemobile,email,remarks,address,centreid,city,state,country,nationalityid,pin,insertedon,joiningdate,updateddate,salutation,name,patienthash,deceased,labname,pid,mobileid,alternatemobileid,remoteid)
with pmi as (
  select mitemp.mobile, mitemp.id, mitemp.labname
  from `sync_pre_merge.mobileindex` as mitemp
  left join `sync_merged.mobileblacklist` as mb
  on mitemp.mobile = mb.mobile and mitemp.labname = mb.labname
  where mb.mobile is null
), ppi as (
  select pitemp.patientid, pitemp.id, pitemp.labname
  from `sync_pre_merge.patientidindex` as pitemp
  left join `sync_merged.patientidblacklist` as pb
  on pitemp.patientid = pb.patientid and pitemp.labname = pb.labname
  where pb.patientid is null
)
select 
pn.patientid, pn.encounterid, pn.age, pn.gender, pn.dob, pn.maritalstatus, pn.uid, pn.mobile, pn.alternatemobile, pn.email,
    pn.remarks, pn.address, cm.id as centreid, pn.city, pn.state, pn.country, nlty.id as nationalityid, pn.pin,
    pn.insertedon, 
    pn.joiningdate,
    pn.updateddate,
    pn.salutation,
    pn.name,
    pn.patienthash, pn.deceased, 
    pn.labname,
    ppi.id as pid,
    pmi.id as mobileid,
    pmia.id as alternatemobileid,
    phi.id as remoteid
FROM 
    `sync_pre_merge.patients_new` as pn
    inner join `sync_pre_merge.patients_hash_index` as phi
    on pn.patienthash = phi.patienthash and pn.labname = phi.labname
    left join ppi on ppi.patientid = pn.patientid and pn.labname = ppi.labname
    left join pmi on pmi.mobile = pn.mobile and pn.labname = pmi.labname
    left join pmi as pmia 
    on pmia.mobile = pn.alternatemobile and pn.labname = pmia.labname
    left join `sync_constant.centres` cm on cm.centrehash = pn.centrehash
    left join `sync_constant.nationality` nlty on nlty.value = pn.nationality;
