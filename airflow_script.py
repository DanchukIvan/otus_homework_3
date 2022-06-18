
import requests as req, datetime as dt, json, pathlib
import pathlib
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Connection

conn = Connection(conn_id='airflow_db', host='rc1a-agt6jfik899akldm.mdb.yandexcloud.net', login='dis', password="otuseducation", schema='airflow_db',\
                    port=9440, extra={'secure':True, 'compression':'zstd'})

dag = DAG(dag_id='get_bitcoin_quot', start_date=dt.datetime(2022, 6, 18, 11, 30), schedule_interval=dt.timedelta(minute=15), \
                                                                                        template_searchpath='/tmp/raw_quote')

def _get_btc_quoting(**context):
    bitcoin_quot = req.get('https://api.coincap.io/v2/rates/bitcoin').json()
    pathlib.Path('/tmp/raw_quote').mkdir(parents=True, exist_ok=True)
    with open('/home/dsivan/otus_homework_3/tmp/raw_quote/bitcoin.json', 'w') as f:
        json.dump(bitcoin_quot['data'], f)

def _write_to_db(connection, conn_id, database, **context):
    session = settings.Session()
    session.add(connection)
    session.commit()
    ch_hook = ClickHouseHook(conn_id, database)
    with open('/home/dsivan/otus_homework_3/tmp/raw_quote/bitcoin.json', 'r') as f:
        for raw in f:
            d = json.loads(raw)
            d_list = [{key:d[key]} for key in d]
    ch_hook.run(f'INSERT INTO {{database}}.bitcoin_quot_buffer VALUES', d_list)
    

start_dag = EmptyOperator(task_id='starting_dag', dag=dag)

get_quote = PythonOperator(task_id='get_bitcoin_quote', python_callable=_get_btc_quoting, dag=dag)

write_quote = PythonOperator(task_id='write_to_ch_bitcoin_quote', python_callable=_write_to_db, \
                op_kwargs={'connection':conn, 'conn_id':conn.conn_id, 'database':conn.schema}, dag=dag)

#send_email = EmailOperator(to='isdanchu@yandex.ru', subject='DAG done', html_content='Привет! Все задачи выполнены, проверяй')

detect_failed_dag = EmptyOperator(task_id='detect_failed_dag', trigger_rule='one_failed', dag=dag)

start_dag >> get_quote >> write_quote >> detect_failed_dag

    




