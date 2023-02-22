SELECT * 
   FROM {{ source('staging','fhv_tripdata_non_partitoned') }}

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}