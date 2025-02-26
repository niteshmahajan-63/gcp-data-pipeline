insert into `sync_pre_merge.patientidindex`
(id, patientid, labname)
with mid as (
	select max(id) as id from `sync_pre_merge.patientidindex`
)
select distinct (select IFNULL(id, 0) from mid)+(ROW_NUMBER() OVER(
        ORDER BY 
              patientid
          ) ) as id, patientid, labname
from (
SELECT DISTINCT pn.patientid, pn.labname 
from `sync_pre_merge.patients_new` as pn
left outer join `sync_pre_merge.patientidindex` pii
on pn.patientid = pii.patientid AND pn.labname = pii.labname
WHERE pii.patientid is null or pii.labname is null
)
;