drop table if exists `sync_dump.testclient_centre_list`;
create table `sync_dump.testclient_centre_list` as
select * from `ORACLE_DATA.TESTCLIENT_CENTRE_LIST`;