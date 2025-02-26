truncate table `sync_merged.patientvisitsummary`;
insert into `sync_merged.patientvisitsummary`
(
  remoteid,labname,patientid,encounterid,bookingid,entityreferalid,centreid,channelid,lastbilldate,joiningdate,
  lastvisitdate,visitgap,visitcount,bookingids
)
select
remoteid,
labname,
patientids[0] as patientid,
encounterids[0] as encounterid,
bookings[0] as bookingid,
entityreferalids[0] as entityreferalid,
centreids[0] as centreid,
channelids[0] as channelid,
lastbilldate,
joiningdate,
lastvisitdate,
visitgap,
visitcount,
bookingids
from
(
    select
    remoteid,
    labname,
    array_agg(patientid order by maxjoiningdate desc, insertedon desc) as patientids,
    array_agg(encounterid order by maxjoiningdate desc, insertedon desc) as encounterids,
    array_agg(bookingid IGNORE NULLS ORDER BY maxjoiningdate DESC, insertedon DESC) as bookings,
    array_agg(entityreferalid order by maxjoiningdate desc, insertedon desc) as entityreferalids,
    array_agg(centreid order by maxjoiningdate desc, insertedon desc) as centreids,
    array_agg(channelid order by maxjoiningdate desc, insertedon desc) as channelids,
    max(maxbilldate) as lastbilldate,
    min(minjoiningdate) as joiningdate,
    max(maxjoiningdate) as lastvisitdate,
    -- max(maxjoiningdate) - min(minjoiningdate) as visitgap,
    DATE_DIFF(DATE(max(maxjoiningdate)), DATE(min(minjoiningdate)), DAY) as visitgap,
    -- ( max(dt_rank) - array_length(array_agg(distinct case when a.bill_rank > 1 then date_trunc(maxjoiningdate, DAY) end IGNORE NULLS))
    -- ) as visitcount,
    ( max(dt_rank) - IFNULL(array_length(array_agg(distinct case when a.bill_rank > 1 then date_trunc(maxjoiningdate, DAY) else null end IGNORE NULLS)),0)
    ) as visitcount,
    array_agg(distinct bookingid IGNORE NULLS) as bookingids
    from
    (
        select patientid,encounterid,bookingid,entityreferalid,centreid,channelid,insertedon,
        remoteid,src,labname,minjoiningdate, maxjoiningdate, maxbilldate,
        dense_rank() over (partition by remoteid, bookingid order by maxjoiningdate) as bill_rank,
        dense_rank() over (partition by remoteid order by date_trunc(maxjoiningdate, DAY)) as dt_rank
        from
        (
            select patientid,encounterid,bookingid,entityreferalid,centreid,channelid,current_timestamp() as insertedon, remoteid,src,labname,
            (
                select min(x)
                from unnest(joiningdates) x
            ) as minjoiningdate,
            (
                select max(x)
                from unnest(joiningdates) x
            ) as maxjoiningdate,
            (
                select max(x)
                from unnest(billdates) x
            ) as maxbilldate
            from
            (
                select patientid, encounterid, bookingid, entityreferalid, centreid, channelid,
                src, labname,remoteid,
                array_agg(distinct joiningdate IGNORE NULLS) as joiningdates,
                array_agg(distinct billdate IGNORE NULLS) as billdates
                from `sync_merged.patientvisit`
                group by 1,2,3,4,5,6,7,8,9
            )
        ) temp
        where minjoiningdate is not null and maxjoiningdate is not null
    ) a
    group by remoteid,labname
);