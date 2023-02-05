from pathlib import Path
import pandas as pd

from datetime import timedelta

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

GCP_PROJECT_ID="psychic-medley-376114"
GCP_BQ_TABLE_NAME="trips_data_all.rides"

PREFECT_GCS_BUCKET_BLOCK="zoom-gcs"
PREFECT_GCP_CREDENTIALS_BLOCK="zoom-gcp-credentials"


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load(PREFECT_GCS_BUCKET_BLOCK)
    gcs_block.get_directory(from_path=gcs_path, local_path=f".")
    return Path(gcs_path)


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load(PREFECT_GCP_CREDENTIALS_BLOCK)

    df.to_gbq(
        destination_table=GCP_BQ_TABLE_NAME,
        project_id=GCP_PROJECT_ID,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> int:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)

    return len(df)


@flow(log_prints=True)
def etl_gcs_to_bq_parent(year: int, months: list[int], color : str):
    """Main ETL flow"""
    num_processed_rows=0
    for month in months:
        num_processed_rows += etl_gcs_to_bq(year, month, color)

    print(f"num processed rows : {num_processed_rows}")


if __name__ == "__main__":
    etl_parent(
        2019, 
        [2, 3], # Feb and March
        "yellow"
    )