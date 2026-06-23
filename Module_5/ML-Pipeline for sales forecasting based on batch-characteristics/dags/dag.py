"""
DAG для batch-инференса продаж "Прилавок"
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import numpy as np
import logging
import io

# ========== Функция предобработки данных ==========
from preprocessing import preprocess_data

# ========== Конфигурация ==========

S3_BUCKET = Variable.get("s3_bucket_name", default_var="your-bucket-name")
S3_ACCESS_KEY = Variable.get("s3_access_key", default_var=None)
S3_SECRET_KEY = Variable.get("s3_secret_key", default_var=None)
S3_MODEL_KEY = Variable.get("s3_model_key", default_var="catboost_sales_model.pkl")

POSTGRES_CONN_ID = "postgres_sales_db"  # ID подключения в Airflow Connections

logger = logging.getLogger(__name__)


# ========== Задачи DAG ==========
def load_data_from_postgres(**context):
    """
    Загрузка данных для инференса из Postgres.
    Загружаем таблицы plan, stores, features и исторические продажи для расчета лаговых признаков.

    Передавать большие данные через xcom_push нельзя!
    Поэтому в данной функции создайте таблицу, которую будем использовать для предобработки данных (назовем ее inference_data_temp).
    В этой таблице соберите исторические данные (до первой даты плана), а также плановые данные (столбец weekly_sales заполните null).
    Здесь же соберите признаки из всех необходимых таблиц воедино.

    После создания выведите информацию (можно через print(), можно через logging.info):
    - количество строк в полученной таблице
    - минимальная дата
    - максимальная дата

    В XCom передайте первую дату плана (по ней будем разделять все данные на обучающие и инференс).

    """

    from psycopg2.extras import execute_values

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    drop_table_sql = f"""
        DROP TABLE IF EXISTS inference_data_temp;
    """

    hook.run(drop_table_sql)
    logger.info(f"Удаление временной таблицы inference_data_temp (если существует)")


    create_table_sql = """
    CREATE TABLE inference_data_temp AS
    WITH combined_data AS (
        SELECT s.store, s.dept, s.date, s.weekly_sales,
            CAST(NULL AS boolean) as is_holiday
        FROM sales s
        WHERE s.date < (SELECT MIN(date) FROM plan)

        UNION ALL

        SELECT p.store, p.dept, p.date,
            CAST(NULL AS double precision) as weekly_sales,
            p.is_holiday
        FROM plan p
    )
    SELECT cd.*, st.type, st.size,
        f.temperature, f.fuel_price,
        f.factor1, f.factor2, f.factor3, f.factor4, f.factor5,
        f.cpi, f.unemployment
    FROM combined_data cd
    LEFT JOIN stores st ON cd.store = st.store
    LEFT JOIN features f ON cd.store = f.store
                        AND cd.dept = f.dept
                        AND cd.date = f.date
    ORDER BY cd.store, cd.dept, cd.date;
    """

    hook.run(create_table_sql)
    logger.info(f"Создана таблица inference_data_temp")

    table_len = hook.get_pandas_df("SELECT COUNT(*) FROM inference_data_temp").iloc[0, 0]
    min_date = hook.get_pandas_df("SELECT MIN(date) FROM inference_data_temp").iloc[0, 0]
    max_date = hook.get_pandas_df("SELECT MAX(date) FROM inference_data_temp").iloc[0, 0]
    first_plan_date = hook.get_pandas_df("SELECT MIN(date) FROM plan").iloc[0, 0]

    logger.info(f'Length: {table_len}')
    logger.info(f"Min date: {min_date}")
    logger.info(f"Max date: {max_date}")
    

    # передача в xcom первой даты plan
    context['ti'].xcom_push(key='first_plan_date', value=first_plan_date)


def preprocess_features(**context):
    """
    Предобработка признаков для инференса.

    1. Читаем первую дату плана из XCom
    2. Читаем данные из таблицы inference_data_temp
    3. Выполняем предобработку признаков (функция preprocess_data)
    4. Оставляем только строки, относящиеся к плану (дату берем из п.1)
    5. Не забудьте удалить колонку weekly_sales (здесь она всегда будет заполнена null)
    6. Лаговые переменные для первых недель стоит заполнить нулями.
    7. Удаляем строки с оставшимися NaN
    8. Передаем датафрейм в XCom (теперь он уже небольшой)
    9. Удалим нашу временную таблицу inference_data_temp из БД
    """

    # первая дата из плана
    first_plan_date = context['ti'].xcom_pull(key='first_plan_date', task_ids='load_data')
    first_plan_date = pd.to_datetime(first_plan_date)

    # чтение данных из таблицы inference_data_temp
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    df = hook.get_pandas_df("SELECT * FROM inference_data_temp")

    df['date'] = pd.to_datetime(df['date'])
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype('float32')
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = df[col].astype('int32')

    logger.info(f" Успешно выгружено {len(df)} записей")

    # преобработка данных
    df = preprocess_data(df)

    # оставляем только даты plan first_plan_date
    df_inference = df[df['date']>=first_plan_date]

    # удаляем целевую переменную
    df_inference = df_inference.drop(columns=['weekly_sales'])

    # заполнение нулями лаговых переменных
    lag_cols = ['avg_sales_before', 'sales_1week_ago', 'sales_2week_ago',
                'sales_4week_ago', 'mean_sales_2week', 'mean_sales_4week']

    df_inference[lag_cols] = df_inference[lag_cols].fillna(0)

    # удаляем строки с оставшимися nan
    df_inference = df_inference.dropna()

    logger.info(f"Выполнена предобработка данных")

    # передаем датафрейм в xcom как json
    context['ti'].xcom_push(key='inference_data', value=df_inference.to_json(orient='records',
                                                                             date_format='iso'))

    # удаление временной таблицы
    drop_table_sql = f"""
        DROP TABLE IF EXISTS inference_data_temp;
    """

    hook.run(drop_table_sql)
    logger.info(f"Удаление временной таблицы inference_data_temp (если существует)")


def load_model_from_s3(**context):
    """
    Загрузка обученной модели CatBoost из S3 (Yandex Cloud) через pickle.

    Клиент уже создан для вас. Необходимо загрузить модель используя io.BytesIO().
    Сохраните модель во временный файл для передачи через XCom.
    В XCom передайте путь до модели.
    """

    import boto3


    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )

    # скачивание модели в bytesio
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_MODEL_KEY)
    model_data = obj['Body'].read()

    # сохранение во временный файл
    import tempfile
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pkl')
    temp_file.write(model_data)
    temp_file.close()

    logger.info(f"Модель CatBoostRegressor сохранена во временный файл")

    context['ti'].xcom_push(key='model_file_path', value=temp_file.name)


def run_batch_inference(**context):
    """
    Batch-инференс: применение модели к подготовленным данным.

    1. Из XCom загрузите подготовленный датафрейм для инференса и путь к обученной модели.
    2. Прочитайте датафрейм используя pandas
    3. Загрузите модель из pickle файла
    4. Выполните predict
    5. Сохраните предсказания в качестве нового столбца - predicted_weekly_sales
    6. Обработайте аномальные предсказания (замените на 0)
    7. Передайте в XCom датафрейм с предсказаниями (используйте .to_json())
    """

    from catboost import CatBoostRegressor
    import pickle


    inference_json = context['ti'].xcom_pull(key='inference_data', task_ids='preprocess_features')
    model_path = context['ti'].xcom_pull(key='model_file_path', task_ids='load_model')

    # восстановление датафрейма
    df = pd.read_json(io.StringIO(inference_json), orient='records')
    df['date'] = pd.to_datetime(df['date'])

    logger.info(f"Из XCom получен датафрейм размером {len(df)} записей")

    with open(model_path, 'rb') as f:
        model = pickle.load(f)

    # убираем лишний для предсказаний столбец
    X = df[['store', 'dept', 'is_holiday', 'type', 'size', 'factor3', 'factor5',
       'month', 'quarter', 'year', 'avg_sales_before', 'sales_1week_ago',
       'sales_2week_ago', 'sales_4week_ago', 'mean_sales_2week',
       'mean_sales_4week']]

    logger.info(f"Столбцы датафрейма: {X.columns}")

    # предсказания
    preds = model.predict(X)
    df['predicted_weekly_sales'] = preds

    # обработка аномальных предсказаний
    df['predicted_weekly_sales'] = df['predicted_weekly_sales'].clip(lower=0)

    logger.info(f"Получены и обработаны предсказания модели. Количество предсказанных записей {len(df)}")

    # удаление временного файла модели
    import os
    os.unlink(model_path)

    logger.info(f"Удален временный файл модели")

    result_json = df[['date', 'store', 'dept', 'predicted_weekly_sales']].to_json(orient='records',date_format='iso')
    context['ti'].xcom_push(key='predictions', value=result_json)

    logger.info(f"В XCom передан датафрейм с предсказаниями")


def save_predictions_to_postgres(**context):
    """
    Запись результатов предсказаний в таблицу predictions в Postgres.

    1. Загрузите датафрейм с предсказаниями из XCom
    2. Подключение к БД уже создано
    3. Удалите таблицу predictions, если она уже существует
    4. Создайте таблицу predictions в БД со столбцами:
    (
        store INT,
        dept INT,
        date DATE,
        predicted_weekly_sales FLOAT,
        prediction_timestamp TIMESTAMP
    )
    5. Вставьте данные из датафрейма в данную таблицу
    6. Выведите через print или logging.info количество строк итоговой таблицы и первые 5 строк.
    """

    from psycopg2.extras import execute_values


    # чтение данных из s3
    pred_json = context['ti'].xcom_pull(key='predictions', task_ids='run_inference')
    df = pd.read_json(io.StringIO(pred_json), orient='records')
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

    logger.info(f"Датафрейм выгружен из XCom")

    # подключение к postgres
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


    # удаление таблицы
    drop_table_sql = f"""
        DROP TABLE IF EXISTS predictions;
    """
    hook.run(drop_table_sql)
    logger.info(f"Удаление таблицы predictions (если существует)")

    create_table_sql = f"""
        CREATE TABLE predictions (
            date DATE,
            store INT,
            dept INT,
            predicted_weekly_sales FLOAT,
            prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (store, dept, date)
        )
    """

    hook.run(create_table_sql)
    logger.info(f"Создана таблица predictions")

    values = df[['store', 'dept', 'date', 'predicted_weekly_sales']].to_records(index=False).tolist()
    conn = hook.get_conn()
    cursor = conn.cursor()

    execute_values(
        cursor,
        """INSERT INTO predictions (store, dept, date, predicted_weekly_sales)
        VALUES %s""",
        values
    )
    conn.commit() 

    # логирование
    logger.info(f"Вставлен {len(df)} записей в таблицу predictions")
    logger.info(f"Первые 5 записей таблицы predictions:\n{df.head().to_string()}")


# ========== Определение DAG ==========

default_args = {
    'owner': 'owner',
    'depends_on_past': False,
    'email': ['email'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'sales_prediction_batch_inference',
    default_args=default_args,
    description='Batch-инференс прогнозирования продаж для Прилавка',
    schedule_interval='0 20 * * 0',  # Каждое воскресенье в 20:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['sales', 'ml', 'batch-inference', 'production'],
)

task_load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data_from_postgres,
    provide_context=True,
    dag=dag,
)

task_preprocess = PythonOperator(
    task_id='preprocess_features',
    python_callable=preprocess_features,
    provide_context=True,
    dag=dag,
)

task_load_model = PythonOperator(
    task_id='load_model',
    python_callable=load_model_from_s3,
    provide_context=True,
    dag=dag,
)

task_inference = PythonOperator(
    task_id='run_inference',
    python_callable=run_batch_inference,
    provide_context=True,
    dag=dag,
)

task_save_predictions = PythonOperator(
    task_id='save_predictions',
    python_callable=save_predictions_to_postgres,
    provide_context=True,
    dag=dag,
)

task_load_data >> task_preprocess
task_load_model >> task_inference
task_preprocess >> task_inference >> task_save_predictions
