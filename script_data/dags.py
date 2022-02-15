import airflow
from datetime import datetime, timedelta
from subprocess import Popen, PIPE
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
from airflow import DAG, macros
from airflow.decorators import task
import os

YEAR = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}'
MONTH = '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}'
YESTERDAY = '{{ macros.ds_format(macros.ds_add(ds, 1), "%Y-%m-%d", "%d") }}'

dag = DAG(
    dag_id='DAG_GP10',
    schedule_interval='0 0 * * *',
    max_active_runs=1,
    start_date=datetime(2022, 2, 14),
)

@task(task_id='download_raw_data', dag=dag)
def download_raw_data(year, month, day):
    raw_data_path = f"/data/gp10/raw"

    url = f'https://opendata.paris.fr/api/v2/catalog/datasets/comptages-routiers-permanents/exports/csv?refine=t_1h%3A{year}%2F{month}%2F{day}&timezone=UTC'
    r = requests.get(url, allow_redirects=True)
    open(f'/tmp/circulation-{year}-{month}-{day}.csv', 'wb').write(r.content)
    put = Popen(["hadoop", "fs", "-put", f"/tmp/circulation-{year}-{month}-{day}.csv", raw_data_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    os.remove(f'/tmp/circulation-{year}-{month}-{day}.csv')
    print("ok")


download_raw_data = download_raw_data(YEAR, MONTH, YESTERDAY)

clean_data = BashOperator(
    task_id="spark_job_clean",
    bash_command="spark-submit --name Groupe10-SCRIPT-SPARK --master yarn --deploy-mode cluster --class esgi.circulation.Clean hdfs:///gp10/circulation-gp10.jar /data/gp10/raw/circulation-2022-2-14.csv /data/gp10/clean/circulation",
    dag=dag
)

transform_data = BashOperator(
    task_id="spark_job_transform",
    bash_command=f"spark-submit --name Groupe10-SCRIPT-SPARK --master yarn --deploy-mode cluster --class esgi.circulation.Jointure hdfs:///gp10/circulation-gp10.jar /data/gp10/clean/circulation/year={YEAR}/month={MONTH}/day={YESTERDAY}/ /data/gp10/meteo/arome-0025-sp1_sp2_paris.csv /data/gp10/final/",
    dag=dag
)

download_raw_data >> clean_data >> transform_data