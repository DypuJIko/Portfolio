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
#bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN")) 
chat_id = -992300980

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
query_dau = '''
SELECT toDate(time) as day,
    count(DISTINCT user_id) as users
FROM {db}.feed_actions
WHERE toDate(time) = yesterday()
GROUP BY day
'''

query_daily_session = '''
SELECT user_id as users,
    count(toDateTime(time)) as session_count
FROM {db}.feed_actions
WHERE toDate(time) = yesterday()
GROUP BY users
'''

query_ctr = """
SELECT avg(user_id) as users,
    (toDate(time)) as day,
    countIf(action='like') as likes,
    countIf(action='view') as views,
    100 * likes / views as ctr
FROM {db}.feed_actions
WHERE day between today() -30 and yesterday()
GROUP BY day
ORDER BY day
"""

query_retantion = """
SELECT date as day,            
    count(user_id) as users
FROM
    (SELECT DISTINCT user_id,
        toDate(time) as date                      
    FROM {db}.feed_actions
    WHERE user_id in
        (SELECT DISTINCT user_id
        FROM
            (SELECT user_id,
                min(toDate(time)) as start_day
            FROM {db}.feed_actions
            GROUP BY user_id
            HAVING start_day = today() - 30)))
GROUP BY day
HAVING day between today() - 30 and yesterday()
"""

query_stickiness = """
SELECT 
    (dau.unique_users / mau.unique_users:: float)*100 as stickiness,
    day
FROM
    (SELECT toStartOfDay(toDateTime(time)) as day,
        toStartOfMonth(toDateTime(time)) as month,
        count(distinct user_id) as unique_users
    FROM {db}.feed_actions
    GROUP BY toStartOfDay(toDateTime(time)),
        toStartOfMonth(toDateTime(time)) as month
      ) as dau
   JOIN
     (
    SELECT toStartOfMonth(toDateTime(time)) as month,
        count(distinct user_id) as unique_users
    FROM {db}.feed_actions
    GROUP BY toStartOfMonth(toDateTime(time))
      ) as mau 
      using month
WHERE toDate(day) between today() -30 and yesterday()
ORDER BY day
"""

query_retained_users = '''
SELECT  event_date as day,
    sum(retained) as retained_user
FROM
    (SELECT toDate(this_day) AS event_date,       
           AVG(num_users) AS retained
    FROM
      (SELECT this_day,
              previous_day, -uniq(user_id) as num_users,
                              status
       FROM
         (SELECT user_id,
                 groupUniqArray(toDate(time)) as day_visited,
                 addDays(arrayJoin(day_visited), +1) this_day,
                 if(has(day_visited, this_day) = 1, 'retained', 'gone') as status,
                 addDays(this_day, -1) as previous_day
          FROM {db}.feed_actions
          group by user_id)
       where status = 'gone'
       group by this_day,
                previous_day,
                status
       HAVING this_day != addDays(today(), +1)
       union all SELECT this_day,
                        previous_day,
                        toInt64(uniq(user_id)) as num_users,
                        status
       FROM
         (SELECT user_id,
                 groupUniqArray(toDate(time)) as day_visited,
                 arrayJoin(day_visited) this_day,
                 if(has(day_visited, addDays(this_day, -1)) = 1, 'retained', 'new') as status,
                 addDays(this_day, -1) as previous_day
          FROM {db}.feed_actions
          group by user_id)
       group by this_day,
                previous_day,
                status) AS virtual_table
    GROUP BY status,
             event_date
    ORDER BY event_date)   
WHERE toDate(day) between today() -30 and yesterday()
GROUP BY day
'''

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['a-komissarov-18'])
def and_komissarov_full_report():

    # Функция получения коэффициента оттока
    @task()
    def extract_churn_rate(query_retantion, connection):
        feed = ph.read_clickhouse(query_retantion, connection=connection)
        retantion = (feed['users'].min() / feed['users'].max())
        churn_rate = (1 - retantion) * 100
        return churn_rate

    # Функция получения показателя возврата
    @task()
    def extract_retantion(query_retantion, connection):
        retantion = ph.read_clickhouse(query_retantion, connection=connection)           
        return retantion
    
    # Функцию получения показателя CTR
    @task()
    def extract_ctr(query_ctr, connection):
        ctr = ph.read_clickhouse(query_ctr, connection=connection)
        return ctr
    
    # Функция получения показателя «липкость» или «привязываемость»
    @task()
    def extract_stickiness(query_stickiness, connection):
        stickiness = ph.read_clickhouse(query_stickiness, connection=connection)
        return stickiness
    
    # Функция получения показателя количества сессий на пользователя
    @task()
    def extract_daily_per_dau(query_dau, query_daily_session, connection):
        dau = ph.read_clickhouse(query_dau, connection=connection)
        daily_session = ph.read_clickhouse(query_daily_session, connection=connection)
        daily_per_dau = daily_session['session_count'].sum() / dau['users']
        return daily_per_dau
    
    # Функция получения показателя количества уникальных пользователей
    @task()
    def extract_dau(query_dau, connection):
        dau = ph.read_clickhouse(query_dau, connection=connection)
        return dau
    
    # Функция получения показателя «сохраненных пользователей»
    @task()
    def extract_retained_users(query_retained_users, connection):
        retained_users = ph.read_clickhouse(query_retained_users, connection=connection)
        return retained_users
    
    # Функция отправки отчета
    @task()
    def sent_report(chat_id, churn_rate, daily_per_dau, dau, ctr, retantion, stickiness, retained_users):
        DAU = dau['users'].iloc[-1]
        CTR = ctr['ctr'].iloc[-1]
        daily_per_dau = daily_per_dau.iloc[-1]
        
        message = (f'Показатели основных метрик:\n\
        CTR за вчерашний день: {CTR:0.3f}%\n\
        DAU за вчерашний день: {DAU}\n\
        Koэффициент оттока за месяц: {churn_rate:0.2f}%\n\
        Количество сессий на пользователя за день: {daily_per_dau:0.0f}')                     
        
        # Создание фигуры    
        fig = plt.figure(figsize=(16, 10))        
        plt.suptitle('Ключевые метрики приложения')
        sns.set_style("darkgrid")
        
        # Настройка каждого графика
        ax1 = plt.subplot(221)
        ax1.set_title('CTR')
        sns.lineplot(x=ctr['day'], y=ctr['ctr'], ax=ax1, color='red').set(xlabel=None, ylabel=None)
        plt.xticks(ctr['day'].iloc[::7])
        
        ax2 = plt.subplot(222)
        ax2.set_title('Retantion')
        sns.lineplot(x=retantion['day'], y=retantion['users'], ax=ax2, color='red').set(xlabel=None, ylabel=None)
        plt.xticks(retantion['day'].iloc[::7])
        
        ax3 = plt.subplot(223)
        ax3.set_title('Stickiness')
        sns.lineplot(x=stickiness['day'], y=stickiness['stickiness'], ax=ax3, color='red').set(xlabel=None, ylabel=None) 
        plt.xticks(stickiness['day'].iloc[::7])
        
        ax4 = plt.subplot(224)
        ax4.set_title('Retained_users')
        sns.lineplot( x=retained_users['day'], y=retained_users['retained_user'], ax=ax4, color='red').set(xlabel=None, ylabel=None) 
        plt.xticks(retained_users['day'].iloc[::7])
        
        # Сохраняем график
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'DAU.png'
        plt.close()                         
        
        bot.sendMessage(chat_id=chat_id, text=message)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
    dau = extract_dau(query_dau, connection)
    churn_rate = extract_churn_rate(query_retantion, connection)
    retantion = extract_retantion(query_retantion, connection)
    ctr = extract_ctr(query_ctr, connection)
    stickiness = extract_stickiness(query_stickiness, connection)
    daily_per_dau = extract_daily_per_dau(query_dau, query_daily_session, connection) 
    retained_users = extract_retained_users(query_retained_users, connection)
    sent_report(chat_id, churn_rate, daily_per_dau, dau, ctr, retantion, stickiness, retained_users)
    
and_komissarov_full_report = and_komissarov_full_report()