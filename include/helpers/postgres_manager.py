from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
import pandas as pd


class PostgresManager:
    def __init__(
        self,
        table_name: str,
        conn_id: str = "postgres",
        schema: str | None = None,
        if_exists: str = "append",
        chunksize: int = 1000,
    ):
        self.table_name = table_name
        self.conn_id = conn_id
        self.schema = schema
        self.if_exists = if_exists
        self.chunksize = chunksize
        self.pg_conn = BaseHook.get_connection(self.conn_id)
        self.engine = self._create_engine()

    def _create_engine(self):
        username = self.pg_conn.login or "postgres"
        password = self.pg_conn.password or "postgres"
        host = self.pg_conn.host or "postgres"
        port = self.pg_conn.port or 5432
        database = self.pg_conn.schema or self.pg_conn.extra_dejson.get("database") or "postgres"

        return create_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{database}",
            pool_pre_ping=True,
        )

    def save_dataframe(self, df: pd.DataFrame) -> str:
        if df.empty:
            raise ValueError("Cannot save an empty DataFrame to Postgres.")

        df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema=self.schema,
            if_exists=self.if_exists,
            index=False,
            method="multi",
            chunksize=self.chunksize,
        )

        return self.table_name

    def save_dataframes(self, dfs: list[pd.DataFrame]) -> str:
        if not dfs:
            raise ValueError("No DataFrames were provided to save.")

        combined = pd.concat(dfs, ignore_index=True)
        return self.save_dataframe(combined)
