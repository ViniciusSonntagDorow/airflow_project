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
    tags=["agriculture", "sidra"],
)
def agriculture():
    
    @task(retries=1, retry_delay=10)
    def raw_layer(product: str, year: str) -> str:
        agriculture_extractor = SidraExtractor(
            table_code="5457",
            variable="8331,216,214,215",
            classifications_key="782"
        )
        agriculture_loader = MinioManager(bucket_name="agriculture")
        
        df = agriculture_extractor.fetch_data(product, year)
        object_name = f"{product}_{year}"
        path = agriculture_loader.upload_parquet(df, object_name)
        return path

    @task(retries=1, retry_delay=10)
    def bronze_layer(paths: list[str]) -> str:
        agriculture_loader = MinioManager(bucket_name="agriculture")
        agriculture_postgres = PostgresManager(
            table_name="agriculture",
            conn_id="postgres",
            schema="bronze",
            if_exists="append",
            chunksize=1000,
        )
        
        dfs = [agriculture_loader.get_parquet(path) for path in paths]
        agriculture_postgres.save_dataframes(dfs)
        return agriculture_postgres.table_name

    # @task(retries=1, retry_delay=10)
    # def silver_layer(table_name: str) -> str:
    #     agriculture_postgres = PostgresManager(
    #         table_name=table_name,
    #         conn_id="postgres",
    #         schema="public",
    #         if_exists="replace",
    #         chunksize=1000,
    #     )
        
    #     df = pd.read_sql(f"SELECT * FROM {table_name}", agriculture_postgres.engine)
    #     df["year"] = pd.to_datetime(df["year"], format="%Y")
    #     df["product"] = df["product"].astype(str)
        
    #     silver_table_name = "silver_agriculture"
    #     agriculture_postgres.table_name = silver_table_name
    #     agriculture_postgres.save_dataframe(df)
        
    #     return silver_table_name

    paths = raw_layer.expand(product=AGRICULTURE_PRODUCTS, year=YEARS)
    bronze_layer(paths)

agriculture()