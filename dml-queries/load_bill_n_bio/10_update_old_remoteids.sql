update `sync_merged.billingsummary` as a 
set patientremoteid = b.remoteid , updatedon = current_timestamp()
from  (
  select * from `sync_merged.merged_duplicate_remoteids` where updated = false 
) as b
where a.patientremoteid = b.patientlocalid and a.labname = b.labname;


update `sync_merged.items` as a 
set patientremoteid = b.remoteid , updatedon = current_timestamp()
from  (
  select * from `sync_merged.merged_duplicate_remoteids` where updated = false 
) as b
where a.patientremoteid = b.patientlocalid and a.labname = b.labname;


update `sync_merged.numeric` as a 
set patientremoteid = b.remoteid , updatedon = current_timestamp()
from  (
  select * from `sync_merged.merged_duplicate_remoteids` where updated = false 
) as b
where a.patientremoteid = b.patientlocalid and a.labname = b.labname;

update `sync_merged.text` as a 
set patientremoteid = b.remoteid , updatedon = current_timestamp()
from  (
  select * from `sync_merged.merged_duplicate_remoteids` where updated = false 
) as b
where a.patientremoteid = b.patientlocalid and a.labname = b.labname;



update `sync_merged.merged_duplicate_remoteids` set updated = true where updated = false;