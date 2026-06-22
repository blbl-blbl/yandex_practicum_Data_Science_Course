import pandas as pd
from datetime import timedelta

def table_preprocessing(
    df_customers,
    df_events,
    df_sessions,
    df_orders
):

    # Объединение df_events, df_sessions и df_customers
    df_ev_ses_cust = df_events.merge(df_sessions, on='session_id', how='right').merge(df_customers, on='customer_id', how='right')
    # Объединение df_orders с df_ev_ses_cust
    df = df_orders.merge(df_ev_ses_cust, on='customer_id', how='right')

    # приведение столбцов к datetime
    datetime_cols = df.select_dtypes(include=['datetime64']).columns
    for col in datetime_cols:
        df[col] = df[col].dt.tz_localize('Europe/Moscow')

    # удаление дубликатов
    df = df.drop_duplicates(subset='event_id')

    return df

def events_features(df, days):
    grouped = df.groupby('customer_id').agg(
        page_view_counter=('event_type', lambda x: (x == 'page_view').sum()),
        add_to_cart_counter=('event_type', lambda x: (x == 'add_to_cart').sum()),
        purchase_counter=('event_type', lambda x: (x == 'purchase').sum()),
        product_id_counter=('product_id', 'nunique')
    )

    grouped[f'conversion_add_to_cart_div_page_view_{days}'] = (
        grouped['add_to_cart_counter']
        .div(grouped['page_view_counter'].replace(0, pd.NA)) # добавил replace
        .fillna(0)
    )

    grouped[f'conversion_purchase_div_add_to_cart_{days}'] = (
        grouped['purchase_counter']
        .div(grouped['add_to_cart_counter'].replace(0, pd.NA)) # добавил replace
        .fillna(0)
    )

    col_names = {
        'page_view_counter' : f"page_view_counter_{days}",
        'add_to_cart_counter' : f"add_to_cart_counter_{days}",
        'product_id_counter' : f"unique_product_in_{days}",
        'purchase_counter' : f"purchase_counter_{days}"
    }

    grouped = grouped.rename(columns=col_names)

    return grouped


def orders_features(df, days):
    grouped_by_orders = df.groupby('customer_id').agg(
        mean_amount=('total_usd', 'mean'),
        total_amount=('total_usd', 'sum')
    )

    col_names = {
        'mean_amount': f"mean_amount_{days}",
        'total_amount': f"total_amount_{days}"
    }

    grouped_by_orders = grouped_by_orders.rename(columns=col_names)

    return grouped_by_orders


def session_features(df, days):
    session_counter = df.groupby('customer_id')['session_id'].count().reset_index()

    session_duration = df.groupby(['customer_id', 'session_id'])['timestamp'].agg(
        session_start='min',
        session_end='max'
    )
    session_duration['duration_sec'] = (
            session_duration['session_end'] - session_duration['session_start']
    ).dt.total_seconds()

    avg_duration = session_duration.groupby('customer_id')['duration_sec'].mean().reset_index()

    session_features = session_counter.merge(avg_duration, on='customer_id', how='left')

    col_names = {
        "duration_sec": f"session_duration_in_{days}",
        "session_id": f"session_counter_{days}"
    }

    session_features = session_features.rename(columns=col_names)

    return session_features

def days_since_last_purchase(df, run_date):
    grouped = df.groupby('customer_id')['order_time'].max().reset_index()
    grouped['days_since_last_purchase'] = (run_date - grouped['order_time']).dt.days
    return grouped


def calculate_features(df: pd.DataFrame, run_date):
    df_copy = df.copy()

    try:
        run_date = pd.to_datetime(run_date).tz_localize('Europe/Moscow')
    except ValueError:
        print("Передан неверный формат даты!")

    features = pd.DataFrame()
    features['customer_id'] = df_copy['customer_id'].unique()
    features['run_date'] = run_date

    events_7d = df_copy[
        (df_copy['timestamp'] >= run_date - timedelta(days=7)) &
        (df_copy['timestamp'] < run_date)
        ]
    events_features_7d = events_features(events_7d, 7)
    features = features.merge(events_features_7d, on='customer_id', how='left').fillna(0)

    events_30d = df_copy[
        (df_copy['timestamp'] >= run_date - timedelta(days=30)) &
        (df_copy['timestamp'] < run_date)
        ]
    events_features_30d = events_features(events_30d, 30)
    features = features.merge(events_features_30d, on='customer_id', how='left').fillna(0)

    orders_30d = df_copy[
        (df_copy['order_time'] >= run_date - timedelta(days=30)) &
        (df_copy['order_time'] < run_date)
        ]
    orders_features_30d = orders_features(orders_30d, 30)
    features = features.merge(orders_features_30d, on='customer_id', how='left').fillna(0)

    sessions_7d = df_copy[
        (df_copy['start_time'] >= run_date - timedelta(days=7)) &
        (df_copy['start_time'] < run_date)
        ]
    sessions_features_7d = session_features(sessions_7d, 7)
    features = features.merge(sessions_features_7d, on='customer_id', how='left').fillna(0)

    sessions_30d = df_copy[
        (df_copy['start_time'] >= run_date - timedelta(days=30)) &
        (df_copy['start_time'] < run_date)
        ]
    sessions_features_30d = session_features(sessions_30d, 30)
    features = features.merge(sessions_features_30d, on='customer_id', how='left').fillna(0)

    orders_before_run_date = df_copy[df_copy['order_time'] < run_date]
    last_purchase = days_since_last_purchase(orders_before_run_date, run_date)
    features = features.merge(last_purchase, on='customer_id', how='left').fillna(-1)

    return features


