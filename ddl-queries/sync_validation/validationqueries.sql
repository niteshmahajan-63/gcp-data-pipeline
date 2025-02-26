CREATE OR REPLACE TABLE `sync_validation.validationqueries`
(
    id INT64,
    testname STRING,
    queryid STRING,
    query STRING,
    comparison BOOL,
    enabled BOOL,
    priority INT64,
    ignorecase BOOL DEFAULT false,
    ignorereason STRING,
    percenttype BOOL DEFAULT false
);