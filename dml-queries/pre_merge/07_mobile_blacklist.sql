truncate table `sync_merged.mobileblacklist`;

insert into `sync_merged.mobileblacklist`
(mobile, mobileid, names_count, deceased, labname)

SELECT a.mobile, b.id as mobileid, names_count, a.mdeceased as deceased, a.labname
from (
select 
mobile, labname, count(distinct name) as names_count,
MAX(deceased) as mdeceased

from `sync_pre_merge.mobile_blacklist_input`
 group by mobile, labname
 having count(distinct name) > 10 
  or max(deceased) = true
) a inner join `sync_pre_merge.mobileindex` b
on a.mobile=b.mobile
and (a.labname is null or a.labname = b.labname);