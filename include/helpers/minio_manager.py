from airflow.hooks.base import BaseHook
from minio import Minio
import pandas as pd
from io import BytesIO


class MinioManager:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.minio_conn = BaseHook.get_connection("minio")
        self.minio_client = Minio(
            endpoint=self.minio_conn.extra_dejson["endpoint_url"].split("//")[1],
            access_key=self.minio_conn.login,
            secret_key=self.minio_conn.password,
            secure=False
        )

    def upload_parquet(self, df: pd.DataFrame, object_name: str) -> str:
        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)

        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        self.minio_client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=buffer,
            length=buffer.getbuffer().nbytes,
        )
        return f"{self.bucket_name}/{object_name}"

    def get_parquet(self, path: str) -> pd.DataFrame:
        bucket_name, object_name = path.split("/", 1)
        response = self.minio_client.get_object(bucket_name, object_name)
        df = pd.read_parquet(BytesIO(response.read()), engine="pyarrow")
        return df
