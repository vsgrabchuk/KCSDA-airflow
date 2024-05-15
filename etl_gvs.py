import pandas as pd
import numpy as np

import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from datetime import timedelta, datetime


# Подключение к БД
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 
    'user': 'student',
    'database': 'simulator'
}
db='simulator_20240320'
# Для создания и пополнения таблицы
connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'test',
    'user':'student-rw', 
    'password':
}
default_args = {
    'owner': 'v.grabchuk',
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 2, 3),
}

# Интервал запуска DAG
schedule_interval = '1 0 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=True)
def etl_gvs():
    @task
    def users_feed_stat():
        # Статистика юзеров в feed_actions
        context = get_current_context()
        today = context['ds']
        today = f"'{today}'"
        q = f"""
        SELECT
        user_id,
        countIf(action='view') AS views,
        countIf(action='like') AS likes
        FROM {db}.feed_actions
        WHERE toDate(time)+1 == {today}
        GROUP BY user_id
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task
    def users_message_stat():
        # Статистика юзеров в message_actions
        context = get_current_context()
        today = context['ds']
        today = f"'{today}'"
        q = f"""
        WITH
            sent_t AS (
                SELECT
                    user_id,
                    COUNT(*) AS messages_sent,
                    COUNT(DISTINCT receiver_id) AS users_sent
                FROM {db}.message_actions
                WHERE toDate(time)+1 == {today}
                GROUP BY user_id
            ),
            received_t_1 AS (
                SELECT
                    receiver_id,
                    COUNT(*) AS messages_received,
                    COUNT(DISTINCT user_id) AS users_received
                FROM {db}.message_actions
                WHERE toDate(time)+1 == {today}
                GROUP BY receiver_id
            ),
            received_t AS (
                SELECT
                    receiver_id AS user_id,
                    messages_received,
                    users_received
                FROM received_t_1
            )

        SELECT *
        FROM received_t
            FULL JOIN sent_t
                USING user_id
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task
    def full_merge(df_1, df_2, on):
        # Объединение таблиц (FULL JOIN)
        df = df_1.merge(df_2, how='outer', on=on)
        df = df.fillna(0)
        return df
        
    @task
    def cols_astype(df, cols, new_type):
        # Преобразование колонок к новому типу данных
        df[cols] = df[cols].astype(new_type)
        return df
    
    @task
    def get_info(agg_col, cols):
        # Инфа по agg_col, которая содержится в cols
        any_cols = ', '.join(f'any({col}) AS {col}' for col in cols)
        cols = ', '.join(cols)
        context = get_current_context()
        today = context['ds']
        today = f"'{today}'"
        q = f"""
        WITH
            feed_info_t AS (
                SELECT 
                    {agg_col},
                    {any_cols}
                FROM {db}.feed_actions
                WHERE toDate(time)+1 == {today}
                GROUP BY {agg_col}
            ),
            message_info_t AS (
                SELECT 
                    {agg_col},
                    {any_cols}
                FROM {db}.message_actions
                WHERE toDate(time)+1 == {today}
                GROUP BY {agg_col}
            ),
            union_t AS
            (
                SELECT *
                FROM feed_info_t
                UNION ALL
                SELECT *
                FROM message_info_t
            )


        SELECT DISTINCT {agg_col}, {cols}
        FROM union_t
        """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task
    def left_merge(df_1, df_2, on):
        df = df_1.merge(df_2, how='left', on=on)
        return df
    
    @task
    def get_slice(df, agg_col, metric_cols, dropna=False):
        # Срез куба
        df = (
            df.groupby(agg_col, as_index=False, dropna=dropna)[metric_cols].sum()
            .rename(columns={agg_col: 'dimension_value'})
        )
        context = get_current_context()
        today = pd.Series(context['ds'])
        info_df = pd.DataFrame({
            'event_date': today, 
            'dimension': pd.Series(agg_col)
        })
        df = info_df.merge(df, how='cross')
        return df

    @task
    def concat(*args):
        df = pd.concat(args, ignore_index=True)
        return df
    
    @task
    def create_table():
        q = '''
        CREATE TABLE IF NOT EXISTS test.gvs_etl (
            event_date Date,
            dimension String,
            dimension_value String,
            views UInt64,
            likes UInt64,
            messages_received UInt64,
            messages_sent UInt64,
            users_received UInt64,
            users_sent UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (event_date, dimension, dimension_value)
        '''
        ph.execute(q, connection=connection_test)

    @task
    def fill_table(df):
        # Присоединение df к таблице в clickhouse
        ph.to_clickhouse(
            df=df, table="gvs_etl", 
            index=False, connection=connection_test
        )
        
    
    # Статистика
    df_1 = users_feed_stat()
    df_2 = users_message_stat()
    # join
    users_activity_df = full_merge(df_1, df_2, on='user_id')
    # Приведение типов
    columns = ['views', 'likes', 'messages_received', 'users_received',
       'messages_sent', 'users_sent']
    users_activity_df = cols_astype(users_activity_df, columns, 'int')
    # Доп инфа
    users_info_df = get_info(agg_col='user_id', cols=['gender', 'age', 'os'])
    users_info_df = cols_astype(users_info_df, ['gender', 'age'], 'str')
    # Кубик
    giga_cube_df = left_merge(users_activity_df, users_info_df, on='user_id')
    # Срезы
    metric_cols = [
        'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'
    ]
    gender_slice_df = get_slice(giga_cube_df, agg_col='gender', metric_cols=metric_cols)
    age_slice_df = get_slice(giga_cube_df, agg_col='age', metric_cols=metric_cols)
    os_slice_df = get_slice(giga_cube_df, agg_col='os', metric_cols=metric_cols)
    df = concat(gender_slice_df, age_slice_df, os_slice_df)
    # Пополнение таблицы
    create_table()
    fill_table(df)
    
etl_gvs = etl_gvs()

