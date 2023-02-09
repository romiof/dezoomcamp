from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os


# @task(retries=3)
# def fetch(dataset_url: str) -> pd.DataFrame:
#     """Read taxi data from web into pandas DataFrame"""
#     # if randint(0, 1) > 0:
#     #     raise Exception
#     df = pd.read_csv(dataset_url)
#     return df


# @task(log_prints=True)
# def clean(df: pd.DataFrame) -> pd.DataFrame:
#     """Fix dtype issues"""
#     df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
#     df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
#     print(df.head(2))
#     print(f"columns: {df.dtypes}")
#     print(f"rows: {len(df)}")
#     return df


@task()
def download_local(color: str, dataset_url: str, dataset_file: str) -> Path:
    """Write CSV.GZ locally"""
    path_dir = Path(f"data/{color}")
    path_dir.mkdir(parents=True, exist_ok=True)
    os.system(f"wget {dataset_url} -O data/{color}/{dataset_file}")
    path_file = f"{dataset_file}"
    return path_dir / path_file


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "fhv"
    year = 2019
    for month in range(1,13):
        print(month)
        dataset_file = f"{color}_tripdata_{year}-{month:02}.csv.gz"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}"
        path = download_local(color, dataset_url, dataset_file)
        print("\n\n   *******  DEBUG ******** \n\n")
        write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
