truncate table `sync_ctas.meta_test_ctas`;

insert into `sync_ctas.meta_test_ctas` select testname, max(id) as id , array_agg(distinct id) as ids from `sync_merged.billingmeta` group by 1;
