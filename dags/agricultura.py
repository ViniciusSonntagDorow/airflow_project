from airflow.decorators import dag
from datetime import datetime


@dag(
    start_date=datetime(2026, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["agricultura", "sidra"],
)
def agricultura():
    pass


agricultura()
