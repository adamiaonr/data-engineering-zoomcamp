-- create yellow trip data external table for HW4
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.yellow_trip_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_psychic-medley-376114/data/yellow/yellow_tripdata_*.csv.gz']
);

-- create green trip data external table for HW4
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.green_trip_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_psychic-medley-376114/data/green/green_tripdata_*.csv.gz']
);

-- create fhv trip data external table for HW4
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_trip_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_psychic-medley-376114/data/fhv/fhv_tripdata_*.csv.gz']
);

-- HW4 Q1
SELECT COUNT(1) FROM `psychic-medley-376114.dbt_de.fact_trips`
WHERE DATE(pickup_datetime) BETWEEN DATE('2019-01-01') AND DATE('2020-12-31');

-- HW4 Q3
SELECT COUNT(1) FROM `psychic-medley-376114.dbt_de.stg_fhv_tripdata`
WHERE DATE(pickup_datetime) BETWEEN DATE('2019-01-01') AND DATE('2019-12-31');

-- HW4 Q4
SELECT COUNT(1) FROM `psychic-medley-376114.dbt_de.fact_fhv_trips`
WHERE DATE(pickup_datetime) BETWEEN DATE('2019-01-01') AND DATE('2019-12-31');

-- HW4 Q5
SELECT COUNT(1), FORMAT_DATETIME("%B", DATETIME(pickup_datetime)) as month FROM `psychic-medley-376114.dbt_de.fact_fhv_trips`
WHERE DATE(pickup_datetime) BETWEEN DATE('2019-01-01') AND DATE('2019-12-31')
GROUP BY month;
