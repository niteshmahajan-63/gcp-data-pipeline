INSERT INTO `sync_merged.billingmeta` (id, testid, packagename, bookingmetahash, testname, itemtypeid, productcode, labname, insertedon)
with billing_meta_temp as (
  select testid, packagename, bookingmetahash, testname, itemtype, productcode, labname
  FROM
  (
      select distinct testid, packagename, bookingmetahash, testname, itemtype,
      productcode, labname,
      ROW_NUMBER() OVER (
        PARTITION BY bookingmetahash, labname
        ORDER BY testname DESC, packagename DESC
      ) AS rnum
      from `sync_rawinput.billings_json` where baddata <> true
  ) a
  where bookingmetahash is not null
  and rnum = 1
  and bookingmetahash not in
  (
      select bookingmetahash
      from `sync_merged.billingmeta`
  )
),
mid as (
	select max(id) as id from `sync_merged.billingmeta`
)
-- Select and insert data into the billing_meta table from the temporary table
select  distinct (select IFNULL(id, 0) from mid)+(ROW_NUMBER() OVER (ORDER BY bookingmetahash)) AS id, *
from (
SELECT DISTINCT
  testid,
  packagename,
  bookingmetahash,
  testname,
  itemty.id AS itemtypeid,
  productcode, labname,
  CURRENT_TIMESTAMP() AS insertedon
FROM
  billing_meta_temp input
LEFT JOIN `sync_constant.itemtype` itemty
ON input.itemtype = itemty.value);
