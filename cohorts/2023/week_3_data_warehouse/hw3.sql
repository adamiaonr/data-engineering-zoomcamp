-- create external table
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_fhv_2019_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_psychic-medley-376114/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- create a non partitioned table from external table
CREATE OR REPLACE TABLE `trips_data_all.fhv_2019_data` AS
SELECT * FROM `trips_data_all.external_fhv_2019_data`;

-- count fhv 2019 records
SELECT COUNT(1) FROM `psychic-medley-376114.trips_data_all.external_fhv_2019_data`;

-- count the distinct number of affiliated_base_number (external) : estimated 0 MB
SELECT COUNT(DISTINCT affiliated_base_number) FROM `trips_data_all.external_fhv_2019_data`;

-- count the distinct number of affiliated_base_number (BQ table) : estimated 317.94 MB : 3165
SELECT COUNT(DISTINCT affiliated_base_number) FROM `trips_data_all.fhv_2019_data`;

-- count both a blank (null) PUlocationID and DOlocationID
SELECT COUNT(1) FROM `trips_data_all.fhv_2019_data`
WHERE (PUlocationID IS NULL) AND (DOlocationID IS NULL);

-- count distinct pickup_datetime : 14742692
SELECT COUNT(DISTINCT pickup_datetime) FROM `trips_data_all.fhv_2019_data`;

-- create a partition and cluster table
CREATE OR REPLACE TABLE `trips_data_all.fhv_2019_data_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `trips_data_all.external_fhv_2019_data`;

-- distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive) : estimated 23.05 MB (partitioned and clustered case)
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `trips_data_all.fhv_2019_data_partitioned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive) : estimated 647.87 MB (non-partitioned case)
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `trips_data_all.fhv_2019_data`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
