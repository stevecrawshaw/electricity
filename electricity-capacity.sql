-- Is there disparity in the regional connection delay for grid request connections?
-- 

duckdb
LOAD SPATIAL;
CREATE OR REPLACE TABLE tec_tbl as 
FROM read_csv('data/tec-register-08-august-2025.csv', normalize_names=true);

DESCRIBE tec_tbl;

CREATE OR REPLACE TABLE dist_sub_tbl AS 
SELECT *,
ST_Point(longitude, latitude) geo_point_2d
FROM read_csv('data/distribution-substations.csv', normalize_names=true);

DESCRIBE dist_sub_tbl;

FROM tec_tbl INNER JOIN dist_sub_tbl ON connection_site = substation_name;

CREATE OR REPLACE TABLE primary_sub_tbl AS 
FROM read_csv('data/primary_substation_locations.csv', normalize_names=true);

DESCRIBE primary_sub_tbl;

FROM tec_tbl INNER JOIN primary_sub_tbl ON connection_site = substation_name;