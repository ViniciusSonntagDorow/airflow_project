from airflow.decorators import dag, task
from datetime import datetime
from include.extractors.sidra_extractor import SidraExtractor
from include.helpers.minio_manager import MinioManager
from include.helpers.postgres_manager import PostgresManager
import pandas as pd
from include.helpers.constants import YEARS, AQUICULTURE_PRODUCTS

@dag(
    start_date=datetime(2026, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["aquiculture", "sidra"],
)
def aquiculture():
    
    @task(retries=1, retry_delay=10)
    def raw_layer(product: str, year: str) -> str:
        aquiculture_extractor = SidraExtractor(
            table_code="5457",
            variable="8331,216,214,215",
            classifications_key="782"
        )
        aquiculture_loader = MinioManager(bucket_name="aquiculture")
        
        df = aquiculture_extractor.fetch_data(product, year)
        object_name = f"{product}_{year}"
        path = aquiculture_loader.upload_parquet(df, object_name)
        return path

    @task(retries=1, retry_delay=10)
    def bronze_aggregate(paths: list[str]) -> str:
        aquiculture_loader = MinioManager(bucket_name="aquiculture")
        aquiculture_postgres = PostgresManager(
            table_name="bronze_aquiculture",
            conn_id="postgres",
            schema="bronze",
            if_exists="append",
            chunksize=1000,
        )
        
        dfs = [aquiculture_loader.get_parquet(path) for path in paths]
        aquiculture_postgres.save_dataframes(dfs)
        return aquiculture_postgres.table_name

    paths = raw_layer.expand(product=AQUICULTURE_PRODUCTS, year=YEARS)
    bronze_aggregate(paths)

aquiculture()