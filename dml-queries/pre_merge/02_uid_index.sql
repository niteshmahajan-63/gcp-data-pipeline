insert into `sync_pre_merge.uidindex` (id, uid, labname)
with mid as (
	select max(id) as id from `sync_pre_merge.uidindex`
)
select distinct (select IFNULL(id, 0) from mid)+(ROW_NUMBER() OVER(
        ORDER BY 
              a.uid
          ) ) as id,  a.uid, a.labname
from
(
select distinct uid, labname from `sync_pre_merge.patients_new`
where uid is not null
) as a LEFT OUTER JOIN `sync_pre_merge.uidindex` b 
  ON  a.uid = b.uid AND a.labname = b.labname
WHERE b.uid IS NULL AND b.labname IS NULL