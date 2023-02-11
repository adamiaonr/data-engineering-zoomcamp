from pathlib import Path
from datetime import timedelta
import pandas as pd

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

PREFECT_GCS_BUCKET_BLOCK="zoom-gcs"


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str, datetime_columns: list[str]) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(
        dataset_url, 
        parse_dates=datetime_columns
    )
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def create_local_dir(color: str) -> Path:
    """Creates local directory to save files"""
    path = Path(f"data/{color}/")
    path.mkdir(parents=True, exist_ok=True)
    return path


@task()
def write_local(df: pd.DataFrame, local_dir: Path, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = local_dir / f"{dataset_file}.parquet"
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load(PREFECT_GCS_BUCKET_BLOCK)
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@task()
def get_datetime_columns(color:str):
    """
    Returns the names of datetime columns for the NYC taxi dataset,
    given the color of the dataset.
    """
    datetime_columns = []

    if color == "green":
        datetime_columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    elif color == "yellow":
        datetime_columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    elif color == "fhv":
        datetime_columns = ['pickup_datetime', 'dropOff_datetime']
    else:
        raise ValueError(f"unknown taxi dataset color : {color}")

    return datetime_columns


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url, datetime_columns=get_datetime_columns(color))
    df_clean = clean(df)
    local_dir = create_local_dir(color)
    path = write_local(df_clean, local_dir, dataset_file)
    write_gcs(path)


@flow()
def etl_web_to_gcs_parent(
    year: int = 2020,
    months: list[int] = [1],
    color: str = "green"
) -> None:
    """Main ETL flow (web to GCS)"""
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    year=2019
    months=[2, 3] # Feb and March
    color="yellow"

    etl_web_to_gcs_parent(year, months, color)
