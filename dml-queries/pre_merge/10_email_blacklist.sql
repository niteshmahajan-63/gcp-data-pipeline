truncate table `sync_merged.emailblacklist`;

insert into `sync_merged.emailblacklist`
(email, emailid, names_count, deceased, labname)

SELECT a.email, b.id as emailid, names_count, a.mdeceased as deceased, a.labname
from (
select 
email, labname, count(distinct name) as names_count,
MAX(deceased) as mdeceased

from `sync_pre_merge.email_blacklist_input`
 group by email, labname
 having count(distinct name) > 10 
  or max(deceased) = true
) a inner join `sync_pre_merge.emailindex` b
on a.email=b.email
and (a.labname is null or a.labname = b.labname);