/*
-- Q01
*/
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_zoomcamp-375723/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `trips_data_all.fhv_tripdata_non_partitoned` AS
SELECT *
  FROM `trips_data_all.external_fhv_tripdata`;


/*
-- Q02
*/
SELECT DISTINCT COUNT(affiliated_base_number)
FROM `trips_data_all.external_fhv_tripdata`
;

SELECT DISTINCT COUNT(affiliated_base_number)
FROM `trips_data_all.fhv_tripdata_non_partitoned`
;

/*
-- Q03
*/
SELECT COUNT(1)
  FROM `trips_data_all.fhv_tripdata_non_partitoned`
 WHERE PUlocationID IS NULL
   AND DOlocationID IS NULL
;

/*
-- Q04
*/
--> Partition by `pickup_datetime` because is used a WHERE condition 
-- and Cluster on `affiliated_base_number` because it has a very high cardinality and Cluster store data in a ordered form

/*
-- Q05
*/
-- Create partioned and clustered table
CREATE OR REPLACE TABLE `trips_data_all.fhv_tripdata_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM `trips_data_all.fhv_tripdata_non_partitoned`
);

-- 647.87 MB
SELECT DISTINCT affiliated_base_number
  FROM `trips_data_all.fhv_tripdata_non_partitoned`
 WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'
;

-- 23.05 MB
SELECT DISTINCT affiliated_base_number
  FROM `trips_data_all.fhv_tripdata_partitoned_clustered`
 WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'


/*
-- Q06
*/
--> GCP Bucket


/*
-- Q07
*/
--> False
