    CREATE OR REPLACE TABLE `sync_validation.validationresult`
    (
        id INT64,
        queryid STRING,
        testname STRING,
        query STRING,
        result STRING,
        ispass BOOL,
        creationdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        start_exec TIMESTAMP,
        end_exec TIMESTAMP,
        total_exec_time STRING
    );