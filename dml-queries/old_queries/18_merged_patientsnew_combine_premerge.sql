truncate table `sync_pre_merge.patientsnew_combine_premerge`;
insert into `sync_pre_merge.patientsnew_combine_premerge`
(patientid, patienthash, patientremoteid, name, uid, mobile, alternatemobile, email, age, gender, deceased, joiningdate ,insertedon ,labname)
select patientid, patienthash, patientremoteid, name, uid, mobile, alternatemobile, email, age, gender, deceased, joiningdate ,insertedon ,labname from `sync_pre_merge.patientsnew_self_premerge`
union distinct 
select patientid, patienthash, patientremoteid, name, uid, mobile, alternatemobile, email, age, gender, deceased, joiningdate ,insertedon ,labname from `sync_pre_merge.patientsnew_target_premerge`;
