drop table if exists `sync_target_merge.visits_input`;
create table `sync_target_merge.visits_input` as
select patientid,encounterid,bookingid,entityreferalid,centreid,channelid,
bookingdate as joiningdate,
2 as src,
bookingdate as billdate,
labname
from `sync_target_merge.billingsummaryrawids`
;


drop table if exists `sync_target_merge.existing_visits`;
create table `sync_target_merge.existing_visits` as
select a.patientid, a.encounterid, a.bookingid, a.entityreferalid, a.centreid, a.channelid, a.src,
a.labname, a.joiningdate, a.billdate
from `sync_merged.patientvisit` a
inner join
`sync_target_merge.visits_input` b
on a.patientid = b.patientid and a.encounterid = b.encounterid and a.labname = b.labname
;

DELETE FROM `sync_merged.patientvisit` a
WHERE EXISTS (
    SELECT 1
    FROM `sync_target_merge.existing_visits` f
    WHERE a.patientid = f.patientid
    AND a.encounterid = f.encounterid
    AND a.labname = f.labname
);

insert into `sync_merged.patientvisit`
(
    patientid, encounterid, bookingid, entityreferalid, centreid, channelid,
    src, labname, joiningdate, billdate, remoteid
)
select a.patientid, a.encounterid, a.bookingid, a.entityreferalid, a.centreid, a.channelid,
a.src, a.labname, a.joiningdate, a.billdate, b.remoteid as remoteid
from
(
    select patientid, encounterid, bookingid, entityreferalid, centreid, channelid,
    src, labname,
    joiningdate,
    billdate
    from
    (
        select distinct patientid, bookingid, entityreferalid, centreid, encounterid, channelid,
        src, labname, joiningdate, billdate from `sync_target_merge.visits_input`
        union distinct
        select distinct patientid, bookingid, entityreferalid, centreid, encounterid, channelid,
        src, labname, joiningdate, billdate from `sync_target_merge.existing_visits`
    ) a
) a
left join `sync_merged.pidencounteridlookup` b
on a.patientid = b.patientid and a.encounterid = b.encounterid and a.labname = b.labname;