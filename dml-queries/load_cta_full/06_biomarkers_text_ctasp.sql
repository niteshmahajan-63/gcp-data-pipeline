truncate table `sync_ctas.biomarkers_text_ctasp`;

insert into `sync_ctas.biomarkers_text_ctasp` (biomarkermetaid, code, testname, unit, biomarkername, patientremoteid,resultdate, result, patientid, bookingid, centreid,labname, biomarkerhash, encounterid)
select a.biomarkermetaid,b.code,b.testname,b.unit,b.biomarkername,a.patientremoteid,a.resultdate,a.result, a.patientid, a.bookingid, a.centreid, 'testclient' as labname,a.biomarkerhash,a.encounterid from( (SELECT patientremoteid, centreid, biomarkermetaid, resultdate, result, bookingid, patientid,biomarkerhash,encounterid FROM `sync_merged.text`) as a inner join (select id,testname,biomarkername,unit,code from `sync_merged.biomarkermeta` Where type = 't') as b on a.biomarkermetaid = b.id );
