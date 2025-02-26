drop table if exists `sync_target_merge.patientsnew_target_merge_num_id`;
create table `sync_target_merge.patientsnew_target_merge_num_id` as
select distinct patientid, patienthash, remoteid, name, age, gender, dob,
    'pid' as idtype, pid as idvalueid, joiningdate, labname
from `sync_target_merge.patients_merged_auid` where pid is not null
union distinct
select distinct patientid, patienthash, remoteid, name, age, gender, dob,
    'uid' as idtype, eid as idvalueid, joiningdate, labname
from `sync_target_merge.patients_merged_auid` where eid is not null
union distinct
select distinct patientid, patienthash, remoteid, name, age, gender, dob,
    'mobile' as idtype, mobileid as idvalueid, joiningdate, labname
from `sync_target_merge.patients_merged_auid` where mobileid is not null
union distinct
select distinct patientid, patienthash, remoteid, name, age, gender, dob,
    'mobile' as idtype, alternatemobileid as idvalueid, joiningdate, labname
from `sync_target_merge.patients_merged_auid` where alternatemobileid is not null
union distinct
select distinct patientid, patienthash, remoteid, name, age, gender, dob,
    'email' as idtype, emailid as idvalueid, joiningdate, labname
from `sync_target_merge.patients_merged_auid` where emailid is not null;