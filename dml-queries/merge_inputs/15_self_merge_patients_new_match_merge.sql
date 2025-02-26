drop table if exists `sync_target_merge.patients_match_merge`;
create table `sync_target_merge.patients_match_merge` as
select
  a.patientid as patientid,
  a.patienthash as patienthash,
  a.remoteid as remoteid,
  a.name as name,
  a.age as age,
  a.gender as gender,
  a.dob as dob,
  a.idtype as matchtype,
  CAST(a.idvalueid AS string) as matchvalue,
  b.patientid as matchpatientid,
  b.patienthash as matchpatienthash,
  b.remoteid as matchremoteid,
  b.name as matchname,
  b.age as matchage,
  b.gender as matchgender,
  b.dob as matchdob,
  Abs(DATE_DIFF(a.joiningdate, b.joiningdate,day)) as daydiff,
  (
    (
      a.name = b.name
    )
    or
    (
      length(a.name) > 5 and length(b.name) > 5
      and
      sync_metadata.levenshteinDistance(a.name, b.name) <= 1
      and
      SUBSTR(a.name, 1, 3) = SUBSTR(b.name, 1, 3)
      and
      (
        a.age is null or b.age is null or abs(a.age-b.age) <= 5
      )
      and 
      (
        a.gender is null or b.gender is null or a.gender = b.gender
      )
    )
    or
    (
      length(a.name) > 8 and length(b.name) > 8
      and
      sync_metadata.levenshteinDistance(a.name, b.name) <= 2
      and
      SUBSTR(a.name, 1, 4) = SUBSTR(b.name, 1, 4)
      and
      (
        a.age is null or b.age is null or abs(a.age-b.age) <= 5
      )
      and 
      (
        a.gender is null or b.gender is null or a.gender = b.gender
      )
    )
  ) as match, 
  (
    a.age is null
    or b.age is null
    or Abs(a.age - b.age) < 15
  ) as matchcandidate,
  a.labname
from
  `sync_target_merge.patientsnew_target_premerge_num_id` as a
  inner join `sync_target_merge.patientsnew_target_premerge_num_id` as b 
  on a.idvalueid = b.idvalueid and a.idtype = b.idtype
  and a.remoteid > b.remoteid
  and a.name is not null
  and b.name is not null
  and a.labname = b.labname;

drop table if exists `sync_target_merge.patients_namematchscore`;
create table `sync_target_merge.patients_namematchscore` as
select patientid,patienthash,remoteid,name,age,gender,dob,
matchtype,matchvalue,matchpatientid,matchpatienthash,matchremoteid,
matchname,matchage,matchgender,matchdob,daydiff,match,matchcandidate,labname,
sync_metadata.namematchscore(name,matchname,age,matchage,gender,matchgender,cast(dob as string),cast(matchdob as string),extract(YEAR from CURRENT_DATE)) as nmscore
from  `sync_target_merge.patients_match_merge` where match is false and matchcandidate is true and name is not null and length(name) > 2 and matchname is not null and length(matchname) > 2;


drop table if exists `sync_target_merge.patients_matched`;
create table `sync_target_merge.patients_matched` as
select patientid,patienthash,remoteid,name,age,gender,dob,matchtype,matchvalue,matchpatientid,matchpatienthash,matchremoteid,matchname,matchage,matchgender,matchdob,daydiff,match,matchcandidate,labname from `sync_target_merge.patients_match_merge` where match is true
union distinct
select patientid,patienthash,remoteid,name,age,gender,dob,matchtype,matchvalue,matchpatientid,matchpatienthash,matchremoteid,matchname,matchage,matchgender,matchdob,daydiff,match,matchcandidate,labname from `sync_target_merge.patients_namematchscore` where nmscore between 1 and 6;
