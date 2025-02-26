CREATE TABLE `sync_constant.centres` (
  id INT64,
  centreid STRING,
  centrecode STRING,
  centrename STRING,
  centrehash STRING,
  insertedon TIMESTAMP default CURRENT_TIMESTAMP()
);
