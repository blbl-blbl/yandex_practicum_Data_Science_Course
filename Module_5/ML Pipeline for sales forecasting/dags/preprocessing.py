import pandas as pd

import logging
logger = logging.getLogger(__name__)

def create_temporal_features(df):
    df_copy  = df.copy()
    df_copy['date'] = pd.to_datetime(df_copy['date'])
    df_copy['month'] = df_copy['date'].dt.month
    df_copy['quarter'] = df_copy['date'].dt.quarter
    df_copy['year'] = df_copy['date'].dt.year

    logger.info(f"Созданы временные признаки")

    return df_copy

def create_avg_sales_feature(df):
    df_copy = df.copy()
    df_copy = df_copy.sort_values(['store', 'dept', 'date'])

    expanding_mean = df_copy.groupby(['store', 'dept'])['weekly_sales'].expanding().mean()

    df_copy['avg_sales_before'] = (
        expanding_mean
        .groupby(level=['store', 'dept'])
        .shift(1)
        .values
    )

    logger.info(f"Созданы агрегированные признаки средних продаж")

    return df_copy

def create_lag_features(df):
    df_copy = df.copy()
    df_copy = df_copy.sort_values(['store', 'dept', 'date'])

    df_copy['sales_1week_ago'] = df_copy.groupby(['store', 'dept'])['weekly_sales'].shift(1)
    df_copy['sales_2week_ago'] = df_copy.groupby(['store', 'dept'])['weekly_sales'].shift(2)
    df_copy['sales_4week_ago'] = df_copy.groupby(['store', 'dept'])['weekly_sales'].shift(4)

    logger.info(f"Созданы лаговые признаки")

    return df_copy

def create_rolling_features(df):
    df_copy = df.copy()
    df_copy = df_copy.sort_values(['store', 'dept', 'date'])

    df_copy['mean_sales_2week'] = (
        df_copy.groupby(['store', 'dept'])['weekly_sales']
        .rolling(window=2, min_periods=2)
        .mean()
        .groupby(level=[0, 1])
        .shift(1)
        .reset_index(level=[0, 1], drop=True)
    )

    df_copy['mean_sales_4week'] = (
        df_copy.groupby(['store', 'dept'])['weekly_sales']
        .rolling(window=4, min_periods=4)
        .mean()
        .groupby(level=[0, 1])
        .shift(1)
        .reset_index(level=[0, 1], drop=True)
    )

    logger.info(f"Созданы скользящие признаки средних продаж")

    return df_copy


def preprocess_data(df):
    """
    Полная предобработка данных (идентична логике обучения).
    Необходимо обработать аномальные продажи. Пропуски заполняем средним.
    После вызываем функции для вычисления признаков:
        create_temporal_features
        create_avg_sales_feature
        create_lag_features
        create_rolling_features
    """

    # заполнение пустых (встречалось в factor2-factor5) непрерывных значений средним
    for col in df.select_dtypes(include='number').columns:
        df[col] = df[col].fillna(df[col].mean())

    # Удаление отрицательных продаж
    df = df[df['weekly_sales'] >= 0]

    # feature engineering
    df = create_temporal_features(df)
    df = create_avg_sales_feature(df)
    df = create_lag_features(df)
    df = create_rolling_features(df)

    # удаление дрейфующих признаков
    df = df.drop(columns=['factor1', 'fuel_price', 'factor4',
                          'unemployment', 'temperature', 'cpi',
                          'factor2'])

    return df

