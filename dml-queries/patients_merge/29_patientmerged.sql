truncate table `sync_merged.patientmerged`;
insert into `sync_merged.patientmerged`
(
remoteid,labname,patientid,encounterid,salutation,name,age,gender,dob,maritalstatus,uid,mobile,
alternatemobile,email,city,state,country,pin,address,remarks,centreid,nationalityid,pid,eid,emailid,mobileid,
alternatemobileid,patientids,encounterids,names,ages,genders,dobs,uids,mobiles,alternatemobiles,
emails,centreids,pids,eids,emailids,mobileids,alternatemobileids,deceased,joiningdate,updateddate
)
select
remoteid,
labname,
patientids[0] as patientid,
encounterids[0] as encounterid,
salutations[0] as salutation,
names[0] as name,
ages[0] as age,
genders[0] as gender,
dobs[0] as dob,
maritalstatuslist[0] as maritalstatus,
uids[0] as uid,
mobiles[0] as mobile,
alternatemobiles[0] as alternatemobile,
emails[0] as email,
citys[0] as city,
states[0] as state,
countrylist[0] as country,
pins[0] as pin,
addresslist[0] as address,
remarkslist[0] as remarks,
centreids[0] as centreid,
nationalityids[0] as nationalityid,
pids[0] as pid,
eids[0] as eid,
emailids[0] as emailid,
mobileids[0] as mobileid,
alternatemobileids[0] as alternatemobileid,
patientids,
encounterids,
names,
ages,
genders,
dobs,
uids,
mobiles,
alternatemobiles,
emails,
centreids,
pids,
eids,
emailids,
mobileids,
alternatemobileids,
deceased,
joiningdate,
updateddate
from
(
    select
    remoteid,
    labname,
    array_agg(patientid IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as patientids,
    array_agg(encounterid IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as encounterids,
    array_agg(salutation IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as salutations,
    array_agg(name IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as names,
    array_agg(age IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as ages,
    array_agg(gender IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as genders,
    array_agg(dob IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as dobs,
    array_agg(maritalstatus IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as maritalstatuslist,
    array_agg(uid IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as uids,
    array_agg(mobile IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as mobiles,
    array_agg(alternatemobile IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as alternatemobiles,
    array_agg(email IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as emails,
    array_agg(city IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as citys,
    array_agg(state IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as states,
    array_agg(country IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as countrylist,
    array_agg(pin IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as pins,
    array_agg(address IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as addresslist,
    array_agg(remarks IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as remarkslist,
    array_agg(centreid IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as centreids,
    array_agg(nationalityid IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as nationalityids,
    array_agg(pid IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as pids,
    array_agg(eid IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as eids,
    array_agg(emailid IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as emailids,
    array_agg(mobileid IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as mobileids,
    array_agg(alternatemobileid IGNORE NULLS ORDER BY updateddate DESC, joiningdate DESC, insertedon DESC) as alternatemobileids,
    max(deceased) as deceased,
    max(joiningdate) as joiningdate,
    max(updateddate) as updateddate
    from `sync_merged.patientwithsrc` 
    group by remoteid,labname
);