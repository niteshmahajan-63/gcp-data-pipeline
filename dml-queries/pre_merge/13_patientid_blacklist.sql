truncate table `sync_merged.patientidblacklist`;

insert into `sync_merged.patientidblacklist`
(patientid, pid, names_count, deceased, labname)

SELECT a.patientid, b.id as pid, names_count, a.mdeceased as deceased, a.labname
from (
select 
patientid, labname, count(distinct name) as names_count,
MAX(deceased) as mdeceased

from `sync_pre_merge.patientid_blacklist_input`
 group by patientid, labname
 having count(distinct name) > 4
  or max(deceased) = true
) a inner join `sync_pre_merge.patientidindex` b
on a.patientid=b.patientid
and (a.labname is null or a.labname = b.labname);