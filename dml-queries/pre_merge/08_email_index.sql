insert into `sync_pre_merge.emailindex` (id, email, labname)
with mid as (
	select max(id) as id from `sync_pre_merge.emailindex`
)
select distinct (select IFNULL(id, 0) from mid)+(ROW_NUMBER() OVER(
        ORDER BY 
              a.email
          ) ) as id,  a.email, a.labname
from
(
select distinct email, labname from `sync_pre_merge.patients_new`
where email is not null
) as a LEFT OUTER JOIN `sync_pre_merge.emailindex` b 
  ON  a.email = b.email AND a.labname = b.labname
WHERE b.email IS NULL AND b.labname IS NULL