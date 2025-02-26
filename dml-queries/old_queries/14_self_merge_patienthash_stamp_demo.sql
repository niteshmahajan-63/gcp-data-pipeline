drop table if exists `sync_self_merge.patientid_name_demo`;
create table `sync_self_merge.patientid_name_demo` cluster by patientid as
select 
  patientid, 
  name, 
  age, 
  gender, 
  labname, 
  cast(min(joiningdate) as datetime) as minjoiningdate, 
  cast(max(joiningdate) as datetime) as maxjoiningdate 
from `sync_pre_merge.patient_merge_input`
where age is not null and gender is not null 
group by patientid, name, labname, age, gender;



drop table if exists `sync_self_merge.mobileid_name_demo`;
create table `sync_self_merge.mobileid_name_demo` cluster by mobileid as
select 
  distinct mobileid, name, age, gender, labname, cast(minjoiningdate as datetime) as minjoiningdate,
  cast(maxjoiningdate as datetime) as maxjoiningdate
  from (
	select 
	  mobileid, name, age, gender, labname, 
	  min(joiningdate) as minjoiningdate, 
	  max(joiningdate) as maxjoiningdate
	from `sync_pre_merge.patient_merge_input` 
	where 
	  age is not null and gender is not null 
	  and mobileid is not null and mobileid > 0
	  group by mobileid, name, labname, age, gender
	union distinct
	select 
	  alternatemobileid as mobileid, 
	  name, age, gender, labname,
	  min(joiningdate) as minjoiningdate, 
	  max(joiningdate) as maxjoiningdate
	from `sync_pre_merge.patient_merge_input` 
	where 
	  age is not null and gender is not null 
	  and alternatemobileid is not null and alternatemobileid > 0
	group by alternatemobileid, name, labname, age, gender);





drop table if exists `sync_self_merge.patientid_demo`;
create table `sync_self_merge.patientid_demo` cluster by patientid as
select 
  patientid, age, gender, labname, 
  cast(min(joiningdate) as datetime) as minjoiningdate, 
  cast(max(joiningdate) as datetime) as maxjoiningdate 
from `sync_pre_merge.patient_merge_input`
where age is not null and gender is not null 
group by patientid, labname, age, gender;



drop table if exists `sync_self_merge.patienthash_stamp_demo`;
create table `sync_self_merge.patienthash_stamp_demo` cluster by patienthash as
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
      WHEN  daydiff1 < daydiff2 then coalesce(age1, age2, age3)
      ELSE coalesce(age2, age1, age3)
      END as age,

      CASE   
      WHEN  daydiff1 < daydiff2 then coalesce(gender1, gender2, gender3)
      ELSE coalesce(gender2, gender1, gender3)
      END as gender,

        -- if (
        --   daydiff1 < daydiff2, 
        --   coalesce(gender1, gender2, gender3), 
        --   coalesce(gender2, gender1, gender3)
        -- ) as gender, 

    CASE   
      WHEN  age1 is null 
          and age2 is null 
          and age3 is not null THEN daydiff3
    ELSE
        CASE WHEN  daydiff1 < daydiff2 THEN  daydiff1 ELSE  daydiff2 END
        END as daydiff

      FROM 
        (
          select 
            pf.patientid, 
            pf.orgName as name, 
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
            -- [s_pre_merge].[patients_new_with_ids] as pf 
            `sync_pre_merge.patient_merge_input` as pf
            left join `sync_self_merge.patientid_name_demo` as pidnwithdemo 
            on pidnwithdemo.name = pf.name 
            and pidnwithdemo.patientid = pf.patientid 
            and (
              pf.labname is null 
              or pf.labname = pidnwithdemo.labname
            ) 
            left join `sync_self_merge.mobileid_name_demo` as midnwithdemo 
            on midnwithdemo.name = pf.name 
            and midnwithdemo.mobileid = pf.mobileid 
            and (
              pf.labname is null 
              or pf.labname = midnwithdemo.labname
            ) 
            left join `sync_self_merge.patientid_demo` as piddemo 
            on 
                piddemo.patientid = pf.patientid 
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
;