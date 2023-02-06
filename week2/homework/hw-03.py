#etl_gcs_to_bq.py
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    my_pc_path = Path(f"./hw_data/")
    my_pc_path.mkdir(parents=True, exist_ok=True)
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=my_pc_path)
    return my_pc_path / Path(gcs_path)


@task()
def load_df(path: Path) -> pd.DataFrame:
    """Data load in df"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""
    gcp_credentials_block = GcpCredentials.load("gpc-auth")
    df.to_gbq(
        destination_table="trips_data_all.yellow_taxi_rides",
        project_id="zoomcamp-375723",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(color: str, year: int, months: list[int]):
    """Main ETL flow to load data into Big Query"""
    tot_rows = 0
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = load_df(path)
        tot_rows += len(df)
        write_bq(df)
    print (f"Total Rows Writted into Big Query:{tot_rows}")


if __name__ == "__main__":
    color = "yellow"
    year = 2021
    months = [1, 2]
    etl_gcs_to_bq(color, year, months)
