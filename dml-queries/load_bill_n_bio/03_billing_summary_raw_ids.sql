drop table if exists `sync_target_merge.billingsummaryrawids`;
create table `sync_target_merge.billingsummaryrawids` as
SELECT
input.patientid, input.encounterid, input.bookingid,
input.bookingdate,
c.id as centreid,
enr.id as entityreferalid,
ch.id as channelid,
input.partyname, input.billtoparty, input.customertype,
input.sourcetotalnetamount, input.customerno, input.labname
from
(
    SELECT DISTINCT 
        patientid, encounterid, bookingid, bookingdate, channel, entityreferalhash, centrehash,
        partyname, billtoparty, customertype, totalnetamount as sourcetotalnetamount, customerno, labname
    FROM `sync_rawinput.billings_json` where baddata <> true
) input
left join  `sync_constant.centres` c
on input.centrehash=c.centrehash
left join `sync_constant.entities` enr
on input.entityreferalhash=enr.entityhash
left join `sync_constant.channel` ch
on input.channel=ch.value
;
