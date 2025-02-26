CREATE OR REPLACE TABLE `sync_metadata.audit`
(
    pipelineid STRING,
    type STRING,
    taskname STRING,
    starttime TIMESTAMP,
    endtime TIMESTAMP,
    totalexecutiontime TIME,
    condition_start_value STRING,
    condition_end_value STRING,
    source_datacount STRING,
    copy_datacount STRING,
    error_msg STRING,
    status STRING,
    other STRING,
    currentdate DATE
)