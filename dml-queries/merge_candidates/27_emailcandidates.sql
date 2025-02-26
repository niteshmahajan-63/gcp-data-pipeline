insert into `sync_candidates.emailcandidates` (patientid, emailid, remoteid, mergeby, name, age, gender, labname)
with remoteids as (
  select distinct patientremoteid from `sync_merged.patientwithsrc`
)
select patientid, cast(emailid as int64) as mobileid, remoteid, mergeby, name, age, gender, labname from (
select patientid, matchvalue as emailid, patientremoteid as remoteid, matchtype as mergeby, name, age, gender, labname  from `sync_target_merge.patients_match_merge` where match is false and nmscore > 6 and matchtype = 'email' and patientremoteid in (select patientremoteid from remoteids) and matchremoteid in (select patientremoteid from remoteids)
union distinct
select matchpatientid as patientid, matchvalue as emailid, matchremoteid as remoteid, matchtype as mergeby, matchname as name, matchage as age, matchgender as gender, labname  from `sync_target_merge.patients_match_merge` where match is false and nmscore > 6 and matchtype = 'email' and patientremoteid in (select patientremoteid from remoteids) and matchremoteid in (select patientremoteid from remoteids)
)