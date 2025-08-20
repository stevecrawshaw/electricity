duckdb
LOAD SPATIAL;

CREATE OR REPLACE TABLE raw_dukes_tbl AS
SELECT * FROM read_xlsx('data/DUKES_5.11.xlsx',
                         sheet = '5.11 Full list',
                         range = 'A6:Q1375',
                         all_varchar = true,
                         normalize_names = true);

ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);

SELECT * FROM weca_postgres.information_schema.tables;

CREATE OR REPLACE TABLE lep_boundary_tbl AS
SELECT ST_GeomFromWKB(shape).ST_Transform('EPSG:27700', 'EPSG:4326') geometry
FROM weca_postgres.os.bdline_ua_lep_diss;

-- The ODS sourced LEP boundary isn't working as a polygon?
-- use the WECA GIS one
-- CREATE OR REPLACE TABLE lep_boundary_tbl AS
-- SELECT * FROM ST_Read('../opendatasoft/data/lep_boundary.geojson');

SELECT ST_GeometryType(geometry) FROM lep_boundary_tbl;


-- table of all generators within the LEP using sptial join on LEP polygon from weca_postgres
CREATE OR REPLACE TABLE lep_generators_tbl AS
WITH cte_geom AS
(SELECT
"type"
, technology
, primary_fuel
, installedcapacity_mw::FLOAT capacity
, postcode
, ST_Transform(ST_Point(xcoordinate::DOUBLE, ycoordinate::DOUBLE), 'EPSG:27700', 'EPSG:4326') geom
FROM raw_dukes_tbl)
SELECT c.*
FROM cte_geom c
JOIN lep_boundary_tbl l
ON ST_Within(c.geom, l.geometry);

SET VARIABLE total_capacity = (SELECT SUM(capacity) FROM lep_generators_tbl);

CREATE OR REPLACE TABLE energy_source_lep_summary_tbl AS 
WITH source_type AS
(SELECT if(technology = 'Fossil Fuel', 'Fossil', 'Renewable') "Fuel category", capacity
FROM lep_generators_tbl)
SELECT 
"Fuel category"
, SUM(capacity).round(1) "Installed capacity (MW)"
, ("Installed capacity (MW)" * 100/ getvariable('total_capacity')).round(1) "Proportion of total"
FROM source_type
GROUP BY "Fuel category";

DESCRIBE energy_source_lep_summary_tbl;

COPY energy_source_lep_summary_tbl TO 'data/energy_source_lep_summary_tbl.csv';












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
