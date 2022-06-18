import requests as req, pathlib, json


bitcoin_quot = req.get('https://api.coincap.io/v2/rates/bitcoin').json()
pathlib.Path('/home/dsivan/otus_homework_3/tmp/raw_quote').mkdir(parents=True, exist_ok=True)
with open('/home/dsivan/otus_homework_3/tmp/raw_quote/bitcoin.json', 'w') as f:
    json_data = json.dump(bitcoin_quot['data'], f)
    f.write('\n')
    
with open('/home/dsivan/otus_homework_3/tmp/raw_quote/bitcoin.json', 'r') as f:
    for raw in f:
        d = json.loads(raw)
        d_list = [{key:d[key]} for key in d]
        print(d_list)

from airflow.models import Connection

conn = Connection(conn_id='airflow_db', host='rc1a-agt6jfik899akldm.mdb.yandexcloud.net', login='dis', password="otuseducation", schema='airflow_db',\
                    port=9440, extra={'secure':True, 'compression':'zstd'})

print(conn.extra)