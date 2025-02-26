
insert into `sync_constant.centres`(id,centreid,centrecode,centrename,centrehash)
with  mid as (
	select max(id) as id from `sync_constant.centres`
)
select
distinct (select IFNULL(id, 0) from mid) +(row_number() over(order by centrehash)) as id,*
from 
(
    select centreid,centrecode,centrename,centrehash from
    (
        select *,
        row_number() over (partition by centrehash) as row_num
        FROM
        (
            select distinct centreid, centrecode, centrename, centrehash from `sync_rawinput.patients_json` where baddata <> true
            union distinct
            select distinct centreid, centrecode, centrename, centrehash from `sync_rawinput.billings_json` where  baddata <> true
            union distinct
            select distinct centreid, centrecode, centrename, centrehash from `sync_rawinput.labreports_json` where baddata <> true
        )  a
        where (centrehash is not null and centrehash != '')
        and centrehash not in
        (
            select distinct centrehash
            from `sync_constant.centres`
        )
    ) as tmp
    where row_num = 1
);


insert into `sync_constant.entities`(id,name,type,entityhash,centreid,doctorid)
with mid as (
	select max(id) as id from `sync_constant.entities`
)
SELECT
distinct (select IFNULL(id, 0) from mid) +(ROW_NUMBER() OVER(ORDER BY entityhash)) AS id, 
name,
type,
entityhash,
centreid,
doctorid
from
(
    select
    name,type,entityhash,centreid,doctorid,
    ROW_NUMBER() OVER (PARTITION BY entityhash) AS row_num
    FROM 
    (
        SELECT
        distinct
        a.name,
        a.type,
        a.entityhash,
        b.centreid AS centreid,
        a.doctorid
        FROM
        (
            -- select distinct doctorname as name,'doc' as type, centrehash, entitydoctorhash as entityhash, doctorid from `sync_rawinput.billings_json`
            -- union distinct
            select distinct referalname as name, 'ref' as type, centrehash, entityreferalhash as entityhash, referalid as doctorid from `sync_rawinput.billings_json`  where baddata <> true
            -- union distinct
            -- select distinct referaldocname as name, 'refdoc' as type, centrehash, entityreferaldochash as entityhash, referaldocid as doctorid from `sync_rawinput.billings_json`
        ) a
        left JOIN `sync_constant.centres` b ON a.centrehash = b.centrehash
    )
    where entityhash is not null and entityhash != '' and entityhash not in (select entityhash from `sync_constant.entities`)
)
WHERE row_num = 1;