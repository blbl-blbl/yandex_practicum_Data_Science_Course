from datetime import datetime
from pathlib import Path
from typing import Any

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from botocore.exceptions import ClientError

from calculate_batch_features import calculate_features, table_preprocessing


# Имя DAG, которое будет отображаться в Airflow UI
DAG_ID = "batch_features"
# Connection ID для чтения необработанных таблиц из PostgreSQL
# TODO: укажите свой Connection ID для БД
POSTGRES_CONN_ID = "postgres_raw"
# Connection ID для загрузки результата в S3
# TODO: укажите свой Connection ID для S3
S3_CONN_ID = "s3_features"
# Схема в PostgreSQL, где лежат исходные таблицы
DEFAULT_SOURCE_SCHEMA = "public"
# Локальная папка для временного сохранения файла перед загрузкой в S3
DEFAULT_OUTPUT_DIR = "/tmp/batch_features"

# V.2 Исправил имя функции
def get_postgres_hook():
    """Возвращает URI подключения к PostgreSQL из Airflow Connection."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook


def get_s3_client_and_bucket() -> tuple[Any, str]:
    """Создаёт S3-клиент и возвращает bucket из Airflow Connection `extra`."""
    connection = BaseHook.get_connection(S3_CONN_ID)
    extras = connection.extra_dejson

    bucket = extras.get("bucket")
    if not bucket:
        raise ValueError(f"Missing required `bucket` in extras for connection `{S3_CONN_ID}`")
    client = boto3.client(
        "s3",
        aws_access_key_id=extras.get("aws_access_key_id"),
        aws_secret_access_key=extras.get("aws_secret_access_key"),
        endpoint_url=extras.get("endpoint_url")
    )
    return client, bucket

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch-features"],
) as dag:
    @task(task_id="build_and_upload_features")
    def build_and_upload_features() -> dict[str, str | int]:
        """Считывает run_date, считает batch-признаки и загружает результат в S3."""
        run_date = Variable.get("batch_features_run_date")

        # 1. Загрузите данные из исходных таблиц до момента `run_date`
        hook = get_postgres_hook()
        df_customers = hook.get_pandas_df("SELECT * FROM customers")[['customer_id', 'signup_date']]
        df_sessions = hook.get_pandas_df("SELECT * FROM sessions")[['session_id', 'customer_id', 'start_time']]
        df_events   = hook.get_pandas_df("SELECT * FROM events")[['event_id', 'session_id', 'timestamp', 'event_type', 'product_id']]
        df_orders   = hook.get_pandas_df("SELECT * FROM orders")[['order_id', 'customer_id', 'order_time', 'total_usd']]

        # 2. Сформируйте итоговую таблицу batch-признаков
        df = table_preprocessing(
            df_customers=df_customers,
            df_sessions=df_sessions,
            df_events=df_events,
            df_orders=df_orders
        )
        features = calculate_features(df=df, run_date=run_date)
        # 3. При необходимости провалидируйте результат
        if features.empty:
            raise ValueError("Получен пустой DataFrame")
        if features["customer_id"].duplicated().any():
            raise ValueError("Найдены дубликататы в customer_id")

        # 4. Сохраните итоговую таблицу перед загрузкой в S3
        local_path = Path(DEFAULT_OUTPUT_DIR) / f"run_date={run_date}" / "batch_features.csv"
        local_path.parent.mkdir(parents=True, exist_ok=True)
        features.to_csv(local_path, index=False)

        s3_client, s3_bucket = get_s3_client_and_bucket()
        s3_key = f"run_date={run_date}/batch_features.csv"
        s3_client.upload_file(str(local_path), s3_bucket, s3_key)

        return {
            "run_date": run_date,
            "rows": len(features),
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
        }

    # Важно, чтобы после сохранения была хотя бы минимальная проверка результата:
    # файл существует в S3 и не пустой
    @task(task_id="validate_saved_result")
    def validate_saved_result(result_info: dict[str, str | int]) -> None:
        """Проверяет, что файл с batch-признаками существует в S3 и не пустой."""
        if int(result_info["rows"]) <= 0:
            raise ValueError("Saved feature file is empty")

        s3_client, _ = get_s3_client_and_bucket()
        s3_bucket = str(result_info["s3_bucket"])
        s3_key = str(result_info["s3_key"])

        try:
            metadata = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(f"S3 object was not found: s3://{s3_bucket}/{s3_key}") from error
            raise

        if int(metadata.get("ContentLength", 0)) <= 0:
            raise ValueError(f"S3 object is empty: s3://{s3_bucket}/{s3_key}")

    validate_saved_result(build_and_upload_features())
