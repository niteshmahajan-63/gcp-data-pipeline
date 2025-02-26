INSERT INTO `sync_merged.biomarkermeta` (
    id, testid, testname, sourcetestname, rangestart, rangeend, unit, biomarkerstampid, normalizedbiomarkername, type, biomarkermetahash, biomarkername, sourcebiomarkername, code, labname, insertedon
)
WITH mid AS (
    SELECT MAX(id) AS id FROM `sync_merged.biomarkermeta`
)
SELECT 
distinct (select IFNULL(id, 0) from mid)+(ROW_NUMBER() OVER (ORDER BY biomarkermetahash)) AS id,
input.testid, input.testname, input.sourcetestname, input.rangestart, input.rangeend, input.unit, input.biomarkerstampid, input.normalizedbiomarkername, input.type, input.biomarkermetahash, input.biomarkername, input.sourcebiomarkername, 
bmap.code AS code, input.labname, input.insertedon
FROM
(
    SELECT
        testid, testname, sourcetestname, rangestart, rangeend, unit, biomarkerstampid, normalizedbiomarkername, type, biomarkermetahash, biomarkername, sourcebiomarkername,
        ROW_NUMBER() OVER (PARTITION BY biomarkermetahash, type ORDER BY biomarkername, testid DESC) AS row_num,
        CURRENT_TIMESTAMP() AS insertedon,
        labname
    FROM `sync_rawinput.labreports_json` where baddata <> true
    AND biomarkermetahash IS NOT NULL
) AS input
LEFT JOIN (
    SELECT DISTINCT biomarkerstampid, code FROM `sync_merged.biomappings`
) bmap ON input.biomarkerstampid = bmap.biomarkerstampid
WHERE row_num = 1
AND (input.biomarkermetahash || COALESCE(input.type, 'bd')) NOT IN (
    SELECT biomarkermetahash || COALESCE(type, 'bd') FROM `sync_merged.biomarkermeta`
);
