truncate table `sync_merged.uidblacklist`;

insert into `sync_merged.uidblacklist`
(uid, eid, names_count, deceased, labname)

SELECT a.uid, b.id as eid, names_count, a.mdeceased as deceased, a.labname
from (
select 
uid, labname, count(distinct name) as names_count,
MAX(deceased) as mdeceased

from `sync_pre_merge.uid_blacklist_input`
 group by uid, labname
 having count(distinct name) > 4 
  or max(deceased) = true
) a inner join `sync_pre_merge.uidindex` b
on a.uid=b.uid
and (a.labname is null or a.labname = b.labname);