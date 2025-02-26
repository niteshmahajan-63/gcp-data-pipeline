insert into `sync_pre_merge.mobileindex` (id, mobile, labname)
with mid as (
	select max(id) as id from `sync_pre_merge.mobileindex`
)
select distinct (select IFNULL(id, 0) from mid)+(ROW_NUMBER() OVER(
        ORDER BY 
              a.mobile
          ) ) as id,  a.mobile, a.labname
from
(
select distinct mobile, labname from `sync_pre_merge.patients_new`
where mobile is not null
union distinct
select distinct alternatemobile as mobile, labname from `sync_pre_merge.patients_new`
where alternatemobile is not null
) as a LEFT OUTER JOIN `sync_pre_merge.mobileindex` b 
  ON  a.mobile = b.mobile AND a.labname = b.labname
WHERE b.mobile IS NULL AND b.labname IS NULL