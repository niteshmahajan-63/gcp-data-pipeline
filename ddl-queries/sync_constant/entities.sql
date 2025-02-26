CREATE TABLE `sync_constant.entities` (
  id INT64 ,
  name STRING NOT NULL,
  type STRING,
  centrehash STRING,
  entityhash STRING,
  centreid STRING,
  doctorid STRING,
  insertedon TIMESTAMP default CURRENT_TIMESTAMP()
);
