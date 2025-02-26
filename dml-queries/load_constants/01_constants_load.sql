insert into `sync_constant.channel`(id, value, channelid)
with  mid as (
	select max(id) as id from `sync_constant.channel`
)
select
distinct (select IFNULL(id, 0) from mid) +(row_number() over(order by value)) as id,*
from
(
    select distinct channel as value, max(channelid) as channelid from `sync_rawinput.billings_json` 
    where baddata <> true and (channel is not null and channel != '')
    and channel not in
    (
        select distinct value
        from `sync_constant.channel`
    ) group by channel
);


insert into `sync_constant.department`(id, value, departmentid)
with  mid as (
	select max(id) as id from `sync_constant.department`
)
select
distinct (select IFNULL(id, 0) from mid) +(row_number() over(order by value)) as id,*
from
(
    select distinct department as value, max(departmentid) as departmentid
    from 
    (
        select distinct department, departmentid from `sync_rawinput.billings_json` where baddata <> true 
        union distinct
        select distinct department, departmentid from `sync_rawinput.labreports_json` where baddata <> true 
    )
    where (department is not null and department != '')
    and department not in
    (
        select distinct value
        from `sync_constant.department`
    ) group by department
);


insert into `sync_constant.itemtype`(id,value)
with  mid as (
	select max(id) as id from `sync_constant.itemtype`
)
select
distinct (select IFNULL(id, 0) from mid) +(row_number() over(order by value)) as id,*
from
(
    select distinct itemtype as value
    from
    (
        select distinct itemtype from `sync_rawinput.billings_json` where baddata <> true 
        union distinct
        select distinct itemtype from `sync_rawinput.labreports_json` where baddata <> true  
    )
    where (itemtype is not null and itemtype != '')
    and itemtype not in
    (
        select distinct value
        from `sync_constant.itemtype`
    )
);


insert into `sync_constant.indicator`(id,value)
with  mid as (
	select max(id) as id from `sync_constant.indicator`
)
select
distinct (select IFNULL(id, 0) from mid) +(row_number() over(order by value)) as id,*
from
(
    select distinct indicator as value from `sync_rawinput.labreports_json` where baddata <> true 
    and (indicator is not null and indicator != '')
    and indicator not in
    (
        select distinct value
        from `sync_constant.indicator`
    )
);

insert into `sync_constant.nationality`(id,value)
with  mid as (
	select max(id) as id from `sync_constant.nationality`
)
select
distinct (select IFNULL(id, 0) from mid) +(row_number() over(order by value)) as id,*
from
(
    select distinct nationality as value from `sync_rawinput.patients_json` where baddata <> true 
    and (nationality is not null and nationality != '')
    and nationality not in
    (
        select distinct value
        from `sync_constant.nationality`
    )
);
