"""ETL-пайплайн для построения витрин"""

from __future__ import annotations
import argparse
import os
from datetime import date
from typing import Dict, Iterable, List, Tuple
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

AUDITION_COLUMNS = [
    "audition_id",
    "puid",
    "usage_platform_ru",
    "msk_business_dt_str",
    "app_version",
    "adult_content_flg",
    "hours",
    "hours_sessions_long",
    "kids_content_flg",
    "main_content_id",
    "usage_geo_id",
]

CONTENT_COLUMNS = [
    "main_content_id",
    "main_author_id",
    "main_content_type",
    "main_content_name",
    "main_content_duration_hours",
    "published_topic_title_list",
]


def create_spark_session(app_name="ETL Pipeline") -> SparkSession:
    """Создать Spark-сессию с настройками ресурсов и подключения к S3."""
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    spark = (
        SparkSession.builder
        # .master("yarn")
        .appName(app_name)

        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "2")
        .config("spark.driver.cores", "4")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.network.timeout", "300s")
        .config("spark.hadoop.fs.s3a.endpoint.region", "ru-central1")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "https://storage.yandexcloud.net",
        )
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")

        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "2")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def print_dataset_overview(df: DataFrame, name: str) -> None:
    """Вывести количество строк, колонок, схему и первые пять записей."""
    print(f"\n===== Общая информация: {name} =====")
    print(f"Количество строк: {df.count()}")
    print(f"Количество колонок: {len(df.columns)}")
    df.printSchema()
    df.show(5, truncate=False)


def print_missing_values(df: DataFrame, name: str) -> None:
    expressions = [
        F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
        for column in df.columns
    ]
    print(f"\n===== Пропуски: {name} =====")
    df.agg(*expressions).show(vertical=True, truncate=False)


def parse_genres(column: F.Column) -> F.Column:
    """Преобразовать строку вида `'Жанр 1', 'Жанр 2'` в массив."""
    cleaned = F.trim(F.regexp_replace(column, r"['\[\]\"]", ""))
    return F.when(
        column.isNull() | (cleaned == ""),
        F.array(F.lit("unknown")),
    ).otherwise(
        F.array_distinct(F.transform(F.split(cleaned, r"\s*,\s*"), lambda x: F.trim(x)))
    )


def preprocess_data(
    audition_raw: DataFrame,
    content_raw: DataFrame,
    start_date: str,
    end_date: str,
    geo_id: str,
) -> Tuple[DataFrame, DataFrame]:
    """Очистить, отфильтровать и объединить исходные данные."""
    audition = (
        audition_raw.select(*AUDITION_COLUMNS)
        .withColumn(
            "session_ts",
            F.to_timestamp("msk_business_dt_str", "yyyy-MM-dd"),
        )
        .withColumn("session_date", F.to_date("session_ts"))
    )

    invalid_dates = audition.filter(F.col("session_ts").isNull()).count()
    invalid_hours = audition.filter(
        F.col("hours").isNull() | ~F.col("hours").between(0.0, 24.0)
    ).count()
    invalid_long_hours = audition.filter(
        F.col("hours_sessions_long").isNull()
        | ~F.col("hours_sessions_long").between(0.0, 24.0)
    ).count()
    duplicate_sessions = audition.count() - audition.select("audition_id").distinct().count()
    print(
        "Проверка audition: "
        f"некорректных дат={invalid_dates}, "
        f"аномальных hours={invalid_hours}, "
        f"аномальных hours_sessions_long={invalid_long_hours}, "
        f"повторов audition_id={duplicate_sessions}"
    )

    audition = (
        audition
        .dropna(
            subset=[
                "audition_id",
                "puid",
                "main_content_id",
                "session_ts",
                "usage_geo_id",
            ]
        )
        .filter(F.col("hours").between(0.0, 24.0))
        .filter(F.col("hours_sessions_long").between(0.0, 24.0))
        .fillna(
            {
                "usage_platform_ru": "unknown",
                "app_version": "unknown",
                "adult_content_flg": False,
                "kids_content_flg": False,
            }
        )
        .dropDuplicates(["audition_id"])
        .filter(
            F.col("session_date").between(
                F.lit(start_date),
                F.lit(end_date),
            )
        )
    )

    if geo_id is not None:
        audition = audition.filter(
            F.col("usage_geo_id") == F.lit(geo_id)
        )

    content = content_raw.select(*CONTENT_COLUMNS)

    # Считаем проблемы с длительностью до очистки и медианной замены.
    duration_checks = content.agg(
        F.count(
            F.when(
                F.col("main_content_duration_hours").isNull(),
                1,
            )
        ).alias("null_duration_count"),
        F.count(
            F.when(
                F.col("main_content_duration_hours").isNotNull()
                & (F.col("main_content_duration_hours") <= 0),
                1,
            )
        ).alias("non_positive_duration_count"),
    ).first()

    null_duration_count = duration_checks["null_duration_count"]
    non_positive_duration_count = duration_checks[
        "non_positive_duration_count"
    ]
    replacement_count = (
            null_duration_count + non_positive_duration_count
    )

    duplicate_content_ids = (
            content.count()
            - content.select("main_content_id").distinct().count()
    )

    print(
        "Проверка content: "
        f"NULL main_content_duration_hours={null_duration_count}, "
        f"main_content_duration_hours <= 0={non_positive_duration_count}, "
        f"всего замен медианой={replacement_count}, "
        f"повторов main_content_id={duplicate_content_ids}"
    )

    content = content.fillna(
        {
            "main_author_id": "unknown",
            "main_content_type": "unknown",
            "main_content_name": "unknown",
            "published_topic_title_list": "unknown",
        }
    )

    # Пропущенную или неположительную длительность заменяем медианой
    # соответствующего типа контента, чтобы можно было рассчитать завершённость.
    content = content.withColumn(
        "_valid_duration",
        F.when(F.col("main_content_duration_hours") > 0, F.col("main_content_duration_hours")),
    )
    duration_medians = content.groupBy("main_content_type").agg(
        F.percentile_approx("_valid_duration", 0.5).alias("_type_median_duration")
    )
    content = (
        content.join(duration_medians, on="main_content_type", how="left")
        .withColumn(
            "main_content_duration_hours",
            F.when(
                F.col("main_content_duration_hours").isNull()
                | (F.col("main_content_duration_hours") <= 0),
                F.col("_type_median_duration"),
            ).otherwise(F.col("main_content_duration_hours")),
        )
        .drop("_valid_duration", "_type_median_duration")
        .withColumn(
            "published_topic_title_list",
            parse_genres(F.col("published_topic_title_list"))
        )
    )

    filtered_sessions = audition.count()
    unmatched_sessions = audition.join(
        content.select("main_content_id"), on="main_content_id", how="left_anti"
    ).count()
    joined = audition.join(content, on="main_content_id", how="inner")
    joined_rows = joined.count()
    duplicated_after_join = joined_rows - joined.select("audition_id").distinct().count()
    print(
        "Проверка соединения: "
        f"сессий после фильтрации={filtered_sessions}, "
        f"сессий без данных о контенте={unmatched_sessions}, "
        f"строк после соединения={joined_rows}, "
        f"повторов audition_id={duplicated_after_join}"
    )
    if duplicated_after_join:
        raise RuntimeError("После соединения появились повторы сессий")

    return joined, content


def with_exploded_genre(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "genre",
            F.explode("published_topic_title_list")
        )
        .filter(F.col("genre") != "")
    )


def run_exploratory_analysis(joined: DataFrame, distribution_columns: Iterable[str]) -> None:
    """Вывести распределения и зависимости из третьей задачи."""
    exploded = with_exploded_genre(joined)
    columns = list(dict.fromkeys(distribution_columns))

    for column in columns:
        source = exploded if column == "genre" else joined
        if column not in source.columns:
            raise ValueError(
                f"Неизвестная колонка {column!r}. Доступные колонки: {source.columns}"
            )
        print(f"\n===== Распределение по колонке {column} =====")
        (
            source.groupBy(column)
            .agg(
                F.countDistinct("puid").alias("unique_users"),
                F.countDistinct("audition_id").alias("sessions_count"),
                F.sum("hours").alias("total_hours"),
            )
            .orderBy(F.desc("unique_users"), F.desc("total_hours"))
            .show(50, truncate=False)
        )

    print("\n===== Популярность типов контента =====")
    (
        joined.groupBy("main_content_type")
        .agg(
            F.countDistinct("puid").alias("unique_users"),
            F.countDistinct("audition_id").alias("sessions_count"),
            F.sum("hours").alias("total_hours"),
        )
        .orderBy(F.desc("total_hours"))
        .show(50, truncate=False)
    )

    print("\n===== Популярность жанров =====")
    (
        exploded.groupBy("genre")
        .agg(
            F.countDistinct("puid").alias("unique_users"),
            F.countDistinct("audition_id").alias("sessions_count"),
            F.sum("hours").alias("total_hours"),
        )
        .orderBy(F.desc("total_hours"))
        .show(50, truncate=False)
    )

    print("\n===== Длительность сессий по дням недели и географии =====")
    (
        joined.withColumn("day_of_week", F.date_format("session_date", "EEEE"))
        .groupBy("day_of_week", "usage_geo_id")
        .agg(
            F.countDistinct("audition_id").alias("sessions_count"),
            F.sum("hours").alias("total_hours"),
            F.avg("hours").alias("avg_session_hours"),
        )
        .orderBy("usage_geo_id", F.desc("total_hours"))
        .show(100, truncate=False)
    )


def build_all_marts(joined: DataFrame) -> Dict[str, DataFrame]:
    """Построить пять витрин из очищенного объединённого датафрейма."""
    user_content_features = (
        joined.groupBy("puid", "main_content_id")
        .agg(
            F.sum("hours").alias("total_hours"),
            F.countDistinct("audition_id").alias("sessions_count"),
            F.first("main_content_duration_hours", ignorenulls=True).alias(
                "main_content_duration_hours"
            ),
            F.max("session_date").alias("last_listening_date"),
            F.min("session_date").alias("first_listening_date"),
            F.avg("hours").alias("avg_session_hours"),
            F.max(F.col("adult_content_flg").cast("int")).alias("adult_content_flag"),
            F.max(F.col("kids_content_flg").cast("int")).alias("kids_content_flag"),
        )
        .withColumn(
            "finished_percent",
            F.least(
                F.col("total_hours") / F.col("main_content_duration_hours"),
                F.lit(1.0),
            ),
        )
        .withColumn(
            "is_completed",
            (F.col("finished_percent") >= F.lit(1.0)).cast("int"),
        )
        .withColumn(
            "days_spent",
            F.datediff("last_listening_date", "first_listening_date") + F.lit(1),
        )
    )

    user_content_mart = user_content_features.select(
        "puid",
        "main_content_id",
        "total_hours",
        "sessions_count",
        "finished_percent",
        "is_completed",
        "last_listening_date",
        "days_spent",
        "avg_session_hours",
    )

    content_genres = joined.select(
        "main_content_id",
        "published_topic_title_list",
    ).dropDuplicates(["main_content_id"])

    user_content_by_genre = with_exploded_genre(
        user_content_features.join(content_genres, on="main_content_id", how="inner")
    )
    genre_rank_window = Window.partitionBy("puid").orderBy(
        F.desc("total_hours"), F.desc("sessions_count"), F.asc("genre")
    )
    user_genre_mart = (
        user_content_by_genre.groupBy("puid", "genre")
        .agg(
            F.sum("total_hours").alias("total_hours"),
            F.sum("sessions_count").alias("sessions_count"),
            F.sum("is_completed").cast("long").alias("completed_count"),
            F.sum("adult_content_flag").cast("long").alias("adult_count"),
            F.sum("kids_content_flag").cast("long").alias("kids_count"),
        )
        .withColumn("genre_rank", F.row_number().over(genre_rank_window))
        .select(
            "puid",
            "genre",
            "total_hours",
            "sessions_count",
            "completed_count",
            "genre_rank",
            "adult_count",
            "kids_count",
        )
    )

    user_base = user_content_features.groupBy("puid").agg(
        F.sum("total_hours").alias("total_hours"),
        F.countDistinct("main_content_id").alias("unique_content_count"),
        F.avg("finished_percent").alias("completion_rate"),
        F.max("last_listening_date").alias("last_date"),
        F.sum("adult_content_flag").cast("long").alias("adult_content_count"),
        F.sum("kids_content_flag").cast("long").alias("kids_content_count"),
    )
    active_days = joined.groupBy("puid").agg(
        F.countDistinct("session_date").alias("active_days")
    )
    genre_profile = with_exploded_genre(joined).groupBy("puid").agg(
        F.countDistinct("genre").alias("unique_genres_count")
    )
    favorite_genre = (
        user_genre_mart.filter(F.col("genre_rank") == 1)
        .select("puid", F.col("genre").alias("favorite_genre"))
    )

    def favorite_dimension(column: str, output_column: str) -> DataFrame:
        ranking = Window.partitionBy("puid").orderBy(
            F.desc("dimension_hours"),
            F.desc("dimension_sessions"),
            F.asc(column),
        )
        return (
            joined.groupBy("puid", column)
            .agg(
                F.sum("hours").alias("dimension_hours"),
                F.countDistinct("audition_id").alias("dimension_sessions"),
            )
            .withColumn("_rank", F.row_number().over(ranking))
            .filter(F.col("_rank") == 1)
            .select("puid", F.col(column).alias(output_column))
        )

    favorite_content_type = favorite_dimension(
        "main_content_type", "favorite_content_type"
    )
    favourite_platform = favorite_dimension(
        "usage_platform_ru", "favourite_platform"
    )

    user_mart = (
        user_base.join(active_days, on="puid", how="left")
        .join(genre_profile, on="puid", how="left")
        .join(favorite_genre, on="puid", how="left")
        .join(favorite_content_type, on="puid", how="left")
        .join(favourite_platform, on="puid", how="left")
        .select(
            "puid",
            "total_hours",
            "active_days",
            "unique_content_count",
            "unique_genres_count",
            "completion_rate",
            "favorite_genre",
            "favorite_content_type",
            "favourite_platform",
            "last_date",
            "adult_content_count",
            "kids_content_count",
        )
    )

    user_content_completion = (
        joined.groupBy("main_content_id", "puid", "main_content_duration_hours")
        .agg(F.sum("hours").alias("user_total_hours"))
        .withColumn(
            "user_finished_percent",
            F.col("user_total_hours") / F.col("main_content_duration_hours"),
        )
    )
    completion_by_content = user_content_completion.groupBy("main_content_id").agg(
        (F.avg("user_finished_percent") * F.lit(100.0)).alias("avg_finished_percent")
    )
    total_users = joined.select("puid").distinct().count()
    content_mart = (
        joined.groupBy("main_content_id")
        .agg(
            F.countDistinct("puid").alias("unique_users"),
            F.sum("hours").alias("total_hours"),
            F.countDistinct("usage_platform_ru").alias("platforms_count"),
        )
        .join(completion_by_content, on="main_content_id", how="left")
        .withColumn(
            "popularity",
            F.when(
                F.lit(total_users) > 0,
                F.col("unique_users") / F.lit(float(total_users)),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            "main_content_id",
            "unique_users",
            "total_hours",
            "avg_finished_percent",
            "platforms_count",
            "popularity",
        )
    )

    daily_user_mart = (
        joined.groupBy("puid", "session_date")
        .agg(
            F.countDistinct("main_content_id").alias("content_count"),
            F.sum("hours").alias("hours_spent"),
            F.countDistinct("audition_id").alias("sessions_count"),
        )
        .withColumn("day_of_week", F.date_format("session_date", "EEEE"))
        .select(
            "puid",
            "session_date",
            "day_of_week",
            "content_count",
            "hours_spent",
            "sessions_count",
        )
    )

    return {
        "user_content_mart": user_content_mart,
        "user_genre_mart": user_genre_mart,
        "user_mart": user_mart,
        "content_mart": content_mart,
        "daily_user_mart": daily_user_mart,
    }


def validate_unique_keys(df: DataFrame, mart_name: str, keys: List[str]) -> None:
    has_duplicates = (
        df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count() > 0
    )
    if has_duplicates:
        raise RuntimeError(f"В витрине {mart_name} найдены повторяющиеся ключи: {keys}")


def save_marts(marts: Dict[str, DataFrame], output_path: str) -> None:
    keys = {
        "user_content_mart": ["puid", "main_content_id"],
        "user_genre_mart": ["puid", "genre"],
        "user_mart": ["puid"],
        "content_mart": ["main_content_id"],
        "daily_user_mart": ["puid", "session_date"],
    }
    output_root = output_path.rstrip("/")
    for mart_name, mart in marts.items():
        validate_unique_keys(mart, mart_name, keys[mart_name])
        target = f"{output_root}/{mart_name}.parquet"
        print(f"Сохранение витрины {mart_name}: {target}")
        mart.write.mode("overwrite").parquet(target)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start_date", "--start-date", required=True)
    parser.add_argument("--end_date", "--end-date", required=True)
    parser.add_argument("--geo_id", "--geo-id", default=None)
    parser.add_argument(
        "--columns",
        nargs="+",
        default=["usage_platform_ru"],
        help="Колонки для распределения, перечисленные через пробел или запятую",
    )
    parser.add_argument("--audition_path", "--audition-path", required=True)
    parser.add_argument("--content_path", "--content-path", required=True)
    parser.add_argument("--output_path", "--output-path", required=True)
    args = parser.parse_args()

    try:
        start = date.fromisoformat(args.start_date)
        end = date.fromisoformat(args.end_date)
    except ValueError as error:
        parser.error(f"Даты должны иметь формат YYYY-MM-DD: {error}")
    if start > end:
        parser.error("start_date не может быть позже end_date")

    args.columns = [
        item.strip()
        for token in args.columns
        for item in token.split(",")
        if item.strip()
    ]
    return args


def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    try:
        audition_raw = spark.read.parquet(args.audition_path)
        content_raw = spark.read.parquet(args.content_path)

        print_dataset_overview(audition_raw, "audition")
        print_dataset_overview(content_raw, "content")
        print_missing_values(audition_raw, "audition")
        print_missing_values(content_raw, "content")

        joined, _ = preprocess_data(
            audition_raw,
            content_raw,
            args.start_date,
            args.end_date,
            args.geo_id,
        )
        joined = joined.cache()
        joined.count()

        run_exploratory_analysis(joined, args.columns)
        marts = build_all_marts(joined)

        print("\n===== Полный план выполнения user_mart =====")
        marts["user_mart"].explain(mode="extended")

        save_marts(marts, args.output_path)
        print("\nВсе витрины сохранены.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
