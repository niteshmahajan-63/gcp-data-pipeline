CREATE OR REPLACE TABLE `sync_merged.billingmeta`
(
    id INT64,
    testid STRING,
    packagename STRING,
    bookingmetahash STRING,
    testname STRING,
    itemtypeid INT64,
    servicetypeid INT64,
    productcode STRING,
    labname STRING,
    insertedon TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
