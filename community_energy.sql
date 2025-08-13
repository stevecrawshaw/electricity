duckdb
LOAD HTTPFS;

CREATE OR REPLACE TABLE com_energy_weca_tbl AS
SELECT year_installed, k_wp
FROM read_csv('https://opendata.westofengland-ca.gov.uk/api/explore/v2.1/catalog/datasets/local-energy-sites/exports/csv?lang=en&timezone=Europe%2FLondon&use_labels=false&delimiter=%2C');

FROM com_energy_weca_tbl;

SET VARIABLE start_year = (SELECT MAX(year_installed) - 5 FROM com_energy_weca_tbl);
SELECT getvariable('start_year');

SELECT SUM(k_wp) FROM com_energy_weca_tbl
WHERE year_installed >= getvariable('start_year');
