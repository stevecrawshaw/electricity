duckdb
LOAD SPATIAL;

CREATE OR REPLACE TABLE repd_tbl AS 
FROM read_csv('data/repd-q1-apr-2025.csv',
normalize_names=true,
ignore_errors=true);

DESCRIBE repd_tbl;

CREATE OR REPLACE TABLE weca_rep_tbl AS 
SELECT 
site_name
,strptime(operational, '%d/%m/%Y')::DATE AS operational_date
,extract(YEAR FROM operational_date) AS year
,record_last_updated_ddmmyyyy
,technology_type
,storage_type
,installed_capacity_mwelec::FLOAT installed_capacity_mwelec
,share_community_scheme
,development_status
,planning_authority
,county
,post_code
,xcoordinate bng_x
,ycoordinate bng_y
, ST_Transform(ST_Point(xcoordinate, ycoordinate), 'EPSG:27700', 'EPSG:4326') geo_point_2d
FROM repd_tbl
WHERE 
    county IN('South Gloucestershire', 'North Somerset', 'Bristol, City of', 'Bath and North East Somerset', 'Avon')
    AND
    installed_capacity_mwelec IS NOT NULL
    AND 
    development_status_short = 'Operational';


SET VARIABLE start_year = (SELECT MAX("year") - 5 FROM weca_rep_tbl);

SELECT getvariable('start_year');

SELECT 
SUM(installed_capacity_mwelec) capacity_added
FROM weca_rep_tbl
WHERE year >= getvariable('start_year');
