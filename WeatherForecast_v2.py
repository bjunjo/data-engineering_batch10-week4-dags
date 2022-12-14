"""
Incremental update for weather_forecast table
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
# from plugins import slack

import requests
import logging
import psycopg2
import json

def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def extract(**context):
    api_key = Variable.get("open_weather_api_key")
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]
    url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={api_key}&units=metric&exclude=current,minutely,hourly,alerts"
    response = requests.get(url)
    data = json.loads(response.text)
    logging.info("Extract done")
    return data

def transform(**context):
    data = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    result = []
    for d in data["daily"]:
        day = datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d')
        result.append("('{}',{},{},{})".format(day, d["temp"]["day"], d["temp"]["min"], d["temp"]["max"]))
    return result

def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    
    cur = get_Redshift_connection()

    # Make a temproary table
    create_temp_table = f"DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS);INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table};"
    logging.info(create_temp_table)
    try:
        cur.execute(create_temp_table)
        cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    
    # Insert data into the temprorary table
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
  
    insert_sql = f"INSERT INTO {schema}.temp_{table} VALUES" + ",".join(lines)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("Commit;")
    except Exception as error:
        print(error)
        cur.execute("Rollback;")
        raise
    
    # Swap the weather_forecast table with the temprorary table
    alter_table = f"DELETE FROM {schema}.{table};INSERT INTO {schema}.{table} SELECT date, temp, min_temp, max_temp, created_date FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq FROM {schema}.temp_{table}) WHERE seq = 1;"
    logging.info(alter_table)
    try:
        cur.execute(alter_table)
        cur.execute("Commit;")
    except Exception as error:
        print(error)
        cur.execute("Rollback;")

dag_second_assignment = DAG(
    dag_id = 'weather_forecast_v2',
    start_date = datetime(2022,10,6), # ????????? ????????? ?????? ????????? ??????
    schedule_interval = '0 2 * * *',  # ????????? ??????
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        "lat": 37.5665,
        "lon": 126.9780
    },
    dag = dag_second_assignment)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    params = { 
    },  
    dag = dag_second_assignment)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'schema': 'contact',   ## ????????? ???????????? ??????
        'table': 'weather_forecast'
    },
    dag = dag_second_assignment)

extract >> transform >> load
