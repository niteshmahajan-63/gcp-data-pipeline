drop table if exists `sync_target_merge.patientnew_patientid`;
create table `sync_target_merge.patientnew_patientid` cluster by patientid
AS
select distinct patientid, labname from `sync_pre_merge.patients_new_with_ids`;


drop table if exists `sync_target_merge.patientnew_mobileid`;
create table `sync_target_merge.patientnew_mobileid` cluster by mobileid
AS
select distinct mobileid, labname from `sync_pre_merge.patients_new_with_ids`
where mobileid > 0
union distinct
select distinct alternatemobileid as mobileid, labname from `sync_pre_merge.patients_new_with_ids`
where alternatemobileid > 0;


drop table if exists `sync_target_merge.patientnew_emailid`;
create table `sync_target_merge.patientnew_emailid` cluster by emailid
AS
select distinct emailid, labname from `sync_pre_merge.patients_new_with_ids`
where emailid > 0;


drop table if exists `sync_target_merge.patientnew_eid`;
create table `sync_target_merge.patientnew_eid` cluster by eid
AS
select distinct eid, labname from `sync_pre_merge.patients_new_with_ids`
where eid > 0;

---------------------------------------------------------

drop table if exists `sync_target_merge.patienttarget_patientid`;
create table `sync_target_merge.patienttarget_patientid` cluster by patientid
AS
select distinct patientid, labname from `sync_merged.patientwithsrc`;


drop table if exists `sync_target_merge.patienttarget_mobileid`;
create table `sync_target_merge.patienttarget_mobileid` cluster by mobileid
AS
select distinct patientid, mobileid, labname from `sync_merged.patientwithsrc`
where mobileid > 0
union distinct
select distinct patientid, alternatemobileid as mobileid, labname from `sync_merged.patientwithsrc`
where alternatemobileid > 0;


drop table if exists `sync_target_merge.patienttarget_emailid`;
create table `sync_target_merge.patienttarget_emailid` cluster by emailid
AS
select distinct patientid, emailid, labname from `sync_merged.patientwithsrc`
where emailid > 0;


drop table if exists `sync_target_merge.patienttarget_eid`;
create table `sync_target_merge.patienttarget_eid` cluster by eid
AS
select distinct patientid, eid, labname from `sync_merged.patientwithsrc`
where eid > 0;

--------------------------------------

drop table if exists `sync_target_merge.target_patientid_for_merge`;
create table `sync_target_merge.target_patientid_for_merge` cluster by patientid
AS
select distinct a.patientid, a.labname from `sync_target_merge.patienttarget_patientid` as a
inner join 
`sync_target_merge.patientnew_patientid` as b
on a.patientid = b.patientid and a.labname = b.labname;


drop table if exists `sync_target_merge.target_mobileid_patientid_for_merge`;
create table `sync_target_merge.target_mobileid_patientid_for_merge` cluster by patientid
AS
select distinct a.patientid, a.labname from `sync_target_merge.patienttarget_mobileid` as a
inner join 
`sync_target_merge.patientnew_mobileid` as b
on a.mobileid = b.mobileid and a.labname = b.labname;


drop table if exists `sync_target_merge.target_emailid_patientid_for_merge`;
create table `sync_target_merge.target_emailid_patientid_for_merge` cluster by patientid
AS
select distinct a.patientid, a.labname from `sync_target_merge.patienttarget_emailid` as a
inner join 
`sync_target_merge.patientnew_emailid` as b
on a.emailid = b.emailid and a.labname = b.labname;

drop table if exists `sync_target_merge.target_eid_patientid_for_merge`;
create table `sync_target_merge.target_eid_patientid_for_merge` cluster by patientid
AS
select distinct a.patientid, a.labname from `sync_target_merge.patienttarget_eid` as a
inner join 
`sync_target_merge.patientnew_eid` as b
on a.eid = b.eid and a.labname = b.labname;


--------------------------------------------------------
drop table if exists `sync_target_merge.target_patient_for_merge`;
create table `sync_target_merge.target_patient_for_merge` cluster by patientid
AS
select a.* from `sync_merged.patientwithsrc` as a
inner join 
(
  select patientid, labname from `sync_target_merge.target_patientid_for_merge`
  union distinct
  select patientid, labname from `sync_target_merge.target_eid_patientid_for_merge`
  union distinct
  select patientid, labname from `sync_target_merge.target_mobileid_patientid_for_merge`
  union distinct 
  select patientid, labname from `sync_target_merge.target_emailid_patientid_for_merge`
) b
on a.patientid = b.patientid and a.labname = b.labname;



----------------------------------

drop table if exists `sync_target_merge.patientid_name_demo`;
create table `sync_target_merge.patientid_name_demo` cluster by patientid
AS
select
  patientid,name,age,gender,labname,
  cast(min(joiningdate) as datetime) as minjoiningdate,
  cast(max(joiningdate) as datetime) as maxjoiningdate
from
  `sync_target_merge.target_patient_for_merge`
where
  age is not null and gender is not null
group by
  patientid, name, labname, age, gender;
  
  
  
drop table if exists `sync_target_merge.mobileid_name_demo`;
create table `sync_target_merge.mobileid_name_demo` cluster by mobileid
AS
select
  mobileid,name,age,gender,labname,
  cast(min(joiningdate) as datetime) as minjoiningdate,
  cast(max(joiningdate) as datetime) as maxjoiningdate
from
  `sync_target_merge.target_patient_for_merge`
where
  age is not null and gender is not null
  and mobileid is not null and mobileid > 0
group by
  mobileid,name,labname,age,gender
union distinct
select
  alternatemobileid as mobileid,
  name,age,gender,labname,
  cast(min(joiningdate) as datetime) as minjoiningdate,
  cast(max(joiningdate) as datetime) as maxjoiningdate
from
  `sync_target_merge.target_patient_for_merge`
where
  age is not null and gender is not null
  and alternatemobileid is not null and alternatemobileid > 0
group by
  alternatemobileid,name,labname,age,gender;
  
  
  
drop table if exists `sync_target_merge.patientid_demo`;
create table `sync_target_merge.patientid_demo` cluster by patientid
AS
select
  patientid,age,gender,labname,
  cast(min(joiningdate) as datetime) as minjoiningdate,
  cast(max(joiningdate) as datetime) as maxjoiningdate
from
  `sync_target_merge.target_patient_for_merge`
where
  age is not null and gender is not null
group by
  patientid, labname, age, gender;


---------------------------------------


drop table if exists `sync_target_merge.patienthash_stamp_demo`;
create table `sync_target_merge.patienthash_stamp_demo` cluster by patienthash as
select
  *
from
  (
    select
      patienthash,
      labname,
      age,
      gender,
      RANK() OVER(
      PARTITION BY patienthash, 
      labname 
      ORDER BY 
        daydiff desc
	  ) AS rnk 
    FROM
      (
        select
          patientid,
          name,
          joiningdate,
          patienthash,
          labname,
          CASE
            WHEN daydiff1 < daydiff2 then coalesce(age1, age2, age3)
            ELSE coalesce(age2, age1, age3)
          END as age,
          CASE
            WHEN daydiff1 < daydiff2 then coalesce(gender1, gender2, gender3)
            ELSE coalesce(gender2, gender1, gender3)
          END as gender,
          -- if (
          --   daydiff1 < daydiff2, 
          --   coalesce(gender1, gender2, gender3), 
          --   coalesce(gender2, gender1, gender3)
          -- ) as gender, 
          CASE
            WHEN age1 is null
            and age2 is null
            and age3 is not null THEN daydiff3
            ELSE CASE
              WHEN daydiff1 < daydiff2 THEN daydiff1
              ELSE daydiff2
            END
          END as daydiff
        FROM
          (
            select
              pf.patientid,
              pf.name,
              pf.joiningdate,
              pf.patienthash,
              pf.labname,
              pidnwithdemo.age as age1,
              pidnwithdemo.gender as gender1,
              coalesce(
            CASE WHEN Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), pidnwithdemo.minjoiningdate, year
                  )
                ) > Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), pidnwithdemo.maxjoiningdate, year
                  )
                ) THEN 
                   Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), pidnwithdemo.maxjoiningdate, year
                  )
                )
                ELSE 
                    Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), pidnwithdemo.minjoiningdate, year
                  )
                )
            END 
            , 999999
            ) as daydiff1,
              -- SELECT IIF(first>second, second, first) the_minimal FROM table
              midnwithdemo.age as age2,
              midnwithdemo.gender as gender2,
              coalesce(
              CASE WHEN 
                Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), midnwithdemo.minjoiningdate, year
                  )
                ) > Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), midnwithdemo.maxjoiningdate, year
                  )
                ) 
                THEN 
                    Abs(
                    DATETIME_DIFF(
                      CAST(pf.joiningdate AS DATETIME), midnwithdemo.maxjoiningdate, year
                    )
                    ) 
                ELSE 
                    Abs(
                    DATETIME_DIFF(
                      CAST(pf.joiningdate AS DATETIME), midnwithdemo.minjoiningdate, year
                    )
                    )
              END, 
              999999
            ) as daydiff2,
              piddemo.age as age3,
              piddemo.gender as gender3,
              coalesce(
              CASE WHEN
                Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), piddemo.minjoiningdate, year
                  )
                ) > Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), piddemo.maxjoiningdate, year
                  )
                )
                THEN
                    Abs(
                    DATETIME_DIFF(
                      CAST(pf.joiningdate AS DATETIME), piddemo.maxjoiningdate, year
                    )
                    )
                ELSE
                    Abs(
                    DATETIME_DIFF(
                      CAST(pf.joiningdate AS DATETIME), piddemo.minjoiningdate, year
                    )
                    )
                END, 
              999999
            ) as daydiff3
            from
              -- [s_etl_merged].[patients_merged_target] as pf
              `sync_target_merge.target_patient_for_merge` as pf
              left join `sync_target_merge.patientid_name_demo` as pidnwithdemo on pidnwithdemo.name = pf.name
              and pidnwithdemo.patientid = pf.patientid
              and (
                pf.labname is null
                or pf.labname = pidnwithdemo.labname
              )
              left join `sync_target_merge.mobileid_name_demo` as midnwithdemo on midnwithdemo.name = pf.name
              and midnwithdemo.mobileid = pf.mobileid
              and (
                pf.labname is null
                or pf.labname = midnwithdemo.labname
              )
              left join `sync_target_merge.patientid_demo` as piddemo on piddemo.patientid = pf.patientid
              and coalesce(
              CASE WHEN
                Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), piddemo.minjoiningdate, year
                  )
                ) > Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), piddemo.maxjoiningdate, year
                  )
                ) 
            THEN
                Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), piddemo.maxjoiningdate, year
                  )
                )
            ELSE
                Abs(
                  DATETIME_DIFF(
                    CAST(pf.joiningdate AS DATETIME), piddemo.minjoiningdate, year
                  )
                )
            END, 
              999999
            ) < 180 
              and (
                pf.labname is null
                or pf.labname = piddemo.labname
              )
            where
              pf.name is not null
              and pf.age is null
              and pf.gender is null
          ) as a
      ) AS a
    where
      age is not null
      and gender is not null
  ) as a
where
  rnk = 1;