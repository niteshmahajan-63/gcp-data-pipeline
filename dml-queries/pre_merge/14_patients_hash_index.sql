insert into `sync_pre_merge.patients_hash_index`
(id, patienthash, labname)
with mid as (
	select max(id) as id from `sync_pre_merge.patients_hash_index`
)
select distinct (select IFNULL(id, 0) from mid)+(ROW_NUMBER() OVER(
        ORDER BY 
              patienthash
          ) ) as id, patienthash, labname
from (
SELECT DISTINCT pn.patienthash, pn.labname 
from `sync_pre_merge.patients_new` as pn
left outer join `sync_pre_merge.patients_hash_index`phi
on pn.patienthash = phi.patienthash AND pn.labname = phi.labname
WHERE phi.patienthash is null or phi.labname is null
)
;