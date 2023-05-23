from datetime import datetime, timedelta
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import os
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Настройки бота
my_token = '6201765217:AAFZrpQWdf3TYmxB_DAxBpAjSuTD6lWe7Vw' # токен вашего бота
bot = telegram.Bot(token=my_token) # получаем доступ
chat_id = -943174102

# Подключение к ClickHouse
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20230420'
}    

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'a-komissarov-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 11)
}

# Запрос на выборку данных
query = '''
SELECT 
    lenta.ts,
    lenta.date,
    lenta.hm,
    users_lenta,
    likes,
    views,
    CTR,
    users_message,
    count_message
FROM
(SELECT
    toStartOfFifteenMinutes(time) as ts,
    toDate(ts) as date,
    formatDateTime(ts, '%R') as hm,
    uniqExact(user_id) as users_lenta,
    countIf(action='like') as likes, 
    countIf(action='view') as views, 
    100 * likes / views as CTR 
FROM {db}.feed_actions
WHERE ts >=  yesterday() and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm
ORDER BY ts) as lenta

FULL OUTER JOIN

(SELECT
    toStartOfFifteenMinutes(time) as ts,
    toDate(ts) as date,
    formatDateTime(ts, '%R') as hm,
    uniqExact(user_id) as users_message,
    count(reciever_id) as count_message
FROM {db}.message_actions
WHERE ts >=  yesterday() and ts < toStartOfFifteenMinutes(now())
GROUP BY ts, date, hm
ORDER BY ts) as message
ON lenta.ts = message.ts
'''

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['a-komissarov-18'])
def komissarov_alert():

    # Функция получения данных из базы feed_actions
    @task()
    def extract_feed(query, connection):
        feed = ph.read_clickhouse(query, connection=connection)
        return feed
    
    # Функция проверки данных на аномалии
    @task()
    def check_anomaly(data, threshold=25):
        group = 'дня'
        for column in data.columns[3:]:        
            metric = column
            
            current_ts = data['ts'].max() # достаем максимальную 15-минутку из датафрейма  
            yesterday_ts = current_ts - pd.DateOffset(days=1) # достаем такую же 15-минутку сутки назад
        
            current_value = data[data['ts'] == current_ts][metric].iloc[0] # достаем из датафрейма значение метрики в максимальную 15-минутку
            yesterday_value = data[data['ts'] == yesterday_ts][metric].iloc[0] # достаем из датафрейма значение метрики в такую же 15-минутку сутки назад
        
            # вычисляем отклонение
            if current_value <= yesterday_value:
                diff = 100 * abs(current_value / yesterday_value - 1)
            else:
                diff = 100 * abs(yesterday_value / current_value - 1)                    
                      
            if diff > threshold:
                message = f'Метрика {metric} в срезе {group}. Текущее значение {current_value:0.2f}. Вчерашнее значение {yesterday_value:0.2f}. Отклонение {diff:0.2f}%. https://superset.lab.karpov.courses/superset/dashboard/3364'                   

                # Задаем размер графика
                plt.figure(figsize=(16, 10))   
                sns.set_style("darkgrid")                        

                # Строим график  
                plt.title(metric)
                sns.lineplot(x=data['ts'], y=data[metric], color='red').set(xlabel='time', ylabel=metric)
                plt.xticks(data['ts'].iloc[::20])

                # Сохраняем график
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = 'DAU.png'
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=message)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)    
    
    data = extract_feed(query, connection) 
    check_anomaly(data, threshold=25)
    
komissarov_alert = komissarov_alert()