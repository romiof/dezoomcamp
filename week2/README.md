# First steps with Prefect

This folder contains PY files for first touch with Prefect.

1- Hands on with Prefect for processing a CSV to local Postgres
- download a CSV file from DataTalks.Club's GitHub
- processing it with Pandas
- save it to a Postgres Database
- all task very similar to what was done in week1, but this time using Prefect and all functions being packaged in a **@task** decorator

2- Processing and upload the file to a GCS Bucket
- a one more time .csv download and now we convert it to a .parquet
- the .parquet file was uploaded to a GCS Bucket

3- Uploading the content of a GCS Bucket to a Big Query Dataset
- with the .parquet file in GCS, we create a table at BQ
- first time, all rows from .parquet file was inserted in the table
- as we would to make the "INSERT INTO" using Pandas, the BQ table was TRUNCATED and after it, we use another function to Pandas save its dataframe into a table in BG
