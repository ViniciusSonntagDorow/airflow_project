from airflow.decorators import dag, task
from datetime import datetime
from include.extractors.sidra_extractor import SidraExtractor
from include.loaders.minio_loader import MinioLoader
import pandas as pd
from include.helpers.constants import YEARS, AGRICULTURE_PRODUCTS


agriculture_extractor = SidraExtractor(
    table_code="5457",
    variable="8331,216,214,215",
    classifications_key="782"
)

agriculture_loader = MinioLoader(
    bucket_name="agricultura"
)

@dag(
    start_date=datetime(2026, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["agricultura", "sidra"],
)
def agricultura():
    @task(retries=2, retry_delay=10)
    def extract_load_data(product: str, year: str) -> str:
        df = agriculture_extractor.fetch_data(product, year)
        object_name = f"{product}_{year}"
        return agriculture_loader.upload_parquet(df, object_name)

    extract_load_data.expand(product=AGRICULTURE_PRODUCTS, year=YEARS)


agricultura()
