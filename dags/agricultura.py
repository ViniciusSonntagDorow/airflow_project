from airflow.decorators import dag, task
from datetime import datetime
from include.extractors.sidra_extractor import SidraExtractor
from include.helpers.minio_manager import MinioManager
from include.helpers.postgres_manager import PostgresManager
import pandas as pd
from include.helpers.constants import YEARS, AGRICULTURE_PRODUCTS

@dag(
    start_date=datetime(2026, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["agricultura", "sidra"],
)
def agricultura():
    
    @task(retries=2, retry_delay=10)
    def raw_layer(product: str, year: str) -> str:
        agriculture_extractor = SidraExtractor(
            table_code="5457",
            variable="8331,216,214,215",
            classifications_key="782"
        )
        agriculture_loader = MinioManager(bucket_name="agricultura")
        
        df = agriculture_extractor.fetch_data(product, year)
        object_name = f"{product}_{year}"
        path = agriculture_loader.upload_parquet(df, object_name)
        return path

    @task(retries=2, retry_delay=10)
    def bronze_aggregate(paths: list[str]) -> str:
        agriculture_loader = MinioManager(bucket_name="agricultura")
        agriculture_postgres = PostgresManager(
            table_name="bronze_agricultura",
            conn_id="postgres",
            schema="public",
            if_exists="append",
        )
        
        dfs = [agriculture_loader.get_parquet(path) for path in paths]
        agriculture_postgres.save_dataframes(dfs)
        return agriculture_postgres.table_name

    paths = raw_layer.expand(product=AGRICULTURE_PRODUCTS, year=YEARS)
    bronze_aggregate(paths)

agricultura()