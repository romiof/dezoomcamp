{{ config(materialized='table') }}

  with tempquery AS 
  (
    SELECT *
      FROM {{ ref('stg_fhv_tripdata') }}
  )

, dim_zones as (
    SELECT *
      FROM {{ ref('dim_zones') }}
     WHERE borough != 'Unknown'
    )

, joined AS (
   SELECT Affiliated_base_number
        , dispatching_base_num
        , pickup_datetime
        , puzone.borough AS pickup_borough
        , puzone.zone AS pickup_zone
        , dozone.borough AS dropoff_borough
        , dozone.zone AS dropoff_zone
        , dropOff_datetime
        , COALESCE(SR_Flag, 'Empty') AS SR_Flag
     FROM tempquery t 
     JOIN dim_zones dozone ON t.DOlocationID = dozone.locationid
     JOIN dim_zones puzone ON t.PUlocationID = puzone.locationid
    )

 SELECT *
   FROM joined
