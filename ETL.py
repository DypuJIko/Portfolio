# Импорт библиотек
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Список необходимых столбцов
metrics_list = ['event_date', 'dimension', 'dimension_value', 'views', 'likes',
                'messages_received', 'messages_sent', 'users_received', 'users_sent']

# Подключение к ClickHouse
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20230420'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'test',
    'user':'student-rw', 
    'password':'656e2b0c9c'
}

# Запросы
query_feed = """SELECT user_id,
                    toDate(time) as event_date,
                    countIf(action='like') as likes,
                    countIf(action='view') as views,
                    gender,
                    age,
                    os
                FROM {db}.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY user_id, event_date, gender, age, os
"""

query_message = """SELECT DISTINCT user_id,
                          event_date,
                          messages_sent,
                          users_sent,
                          gender,
                          age,
                          os,
                          messages_received,
                          users_received
                   FROM
                        (SELECT user_id,
                            toDate(time) as event_date,
                            count(reciever_id) as messages_sent,
                            count(DISTINCT reciever_id) as users_sent,
                            gender,
                            age,
                            os
                        FROM {db}.message_actions
                        WHERE toDate(time) = yesterday()
                        GROUP BY user_id, event_date, gender, age, os) as t1 

                        FULL OUTER JOIN

                        (SELECT reciever_id,
                            toDate(time) as event_date,
                            count(user_id) as messages_received,
                            count(DISTINCT user_id) as users_received,
                            gender,
                            age,
                            os
                        FROM {db}.message_actions
                        WHERE toDate(time) = yesterday()
                        GROUP BY reciever_id, event_date, gender, age, os) as t2 
                        ON t1.user_id = t2.reciever_id
"""

query_test = '''CREATE TABLE IF NOT EXISTS test.akomissarov_etl
                    (event_date Date,
                    dimension String,
                    dimension_value String,
                    views UInt64,
                    likes UInt64,
                    messages_sent UInt64,
                    users_sent UInt64,
                    messages_received UInt64,
                    users_received UInt64
                    )
                    ENGINE = MergeTree()
                    ORDER BY event_date
'''

# Дефолтные параметры, которые прокидываются в таски
default_args= {
    'owner': 'a-komissarov-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 7),
}

# Интервал запуска DAG
schedule_interval = '0 9 * * *'

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['a-komissarov-18'])
def akomissarov_etl():
    
    # Функция получения данных из базы feed_actions
    @task
    def extract_feed(query_feed, connection):
        feed = ph.read_clickhouse(query_feed, connection=connection)
        return feed
    
    # Функция получения данных из базы message_actions
    @task
    def extract_message(query_message, connection):
        message = ph.read_clickhouse(query_message, connection=connection)
        return message
    
    # Функция объединения двух запросов
    @task
    def transform_concat(feed_cube, message_cube):
        table = feed_cube.merge(message_cube, how='inner', suffixes=('_m', '_r'))
        return table                       
    
    # Функция среза по полу                            
    @task
    def gender_dimension(df_cube):
        df_cube_gender = df_cube.groupby(['event_date', 'gender'])\
        ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_cube_gender.rename(columns={'gender': 'dimension_value'}, inplace=True)
        df_cube_gender['dimension'] = 'gender'
        df_cube_gender['dimension_value'] = df_cube_gender['dimension_value'].replace({0:'Female', 1:'Male'})
        return df_cube_gender[metrics_list]
    
    # Функция среза по возрасту                            
    @task
    def age_dimension(df_cube):
        df_cube_age = df_cube.groupby(['event_date', 'age'])\
        ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_cube_age.rename(columns={'age': 'dimension_value'}, inplace=True)
        df_cube_age['dimension'] = 'age'
        return df_cube_age[metrics_list]

    # Функция среза по ОС                            
    @task
    def os_dimension(df_cube):
        df_cube_os = df_cube.groupby(['event_date', 'os'])\
        ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'].sum().reset_index()
        df_cube_os.rename(columns={'os': 'dimension_value'}, inplace=True)
        df_cube_os['dimension'] = 'os'
        return df_cube_os[metrics_list]
    
    # Функция объединения срезов
    @task
    def concat_final_tables(gender_cube, age_cube, os_cube):
        return pd.concat([gender_cube, age_cube, os_cube]).reset_index(drop=True)
    
    # Функция записи данных в новую таблицу в ClickHouse
    @task
    def load(concat_cube):        
        ph.execute(query=query_test, connection=connection_test)
        ph.to_clickhouse(concat_cube, 'akomissarov_etl', index = False, connection=connection_test)
     
    feed_cube = extract_feed(query_feed, connection)
    message_cube = extract_message(query_message, connection)
    df_cube = transform_concat(feed_cube, message_cube)
    gender_cube = gender_dimension(df_cube)
    age_cube = age_dimension(df_cube)
    os_cube = os_dimension(df_cube)
    concat_cube = concat_final_tables(gender_cube, age_cube, os_cube)
    upload = load(concat_cube)
    
akomissarov_etl = akomissarov_etl()