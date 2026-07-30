from airflow.decorators import dag, task
from datetime import datetime


@dag(
    start_date=datetime(2026, 1, 1),
    schedule_interval="@monthly",
    catchup=False,
    tags=["agricultura", "sidra"],
)
def agricultura():
    @task
    def extract_data():
        pass

    @task
    def transform_data():
        pass

    @task
    def load_data():
        pass

    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)


agricultura()
