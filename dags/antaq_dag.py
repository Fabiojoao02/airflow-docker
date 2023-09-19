from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import requests
import zipfile
import os
import shutil
from pathlib import Path
from zipfile import ZipFile
from airflow.utils.email import send_email


CAMINHO_RAIZ = Path(__file__).parent.parent
currentDateTime = datetime.now()
date = currentDateTime.date()
ano = date.strftime("%Y")

arquivo = str(ano)+'.zip'
caminho = CAMINHO_RAIZ / 'data' / arquivo

expurgos = CAMINHO_RAIZ / 'expurgos' / arquivo

default_args = {
    'depends_on_past': False,
    'email': ['fbianastacio@gmail.com'],
    'email_on_failure': False,  # True
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}


def captura_conta_dados():

    url = 'https://web3.antaq.gov.br/ea/txt/2023.zip'
    # caminho = r'g:\airflow-docker\dags\data\2023.zip'
    response = requests.get(url)
    if response.status_code == 200:

        # shutil.rmtree(caminho, ignore_errors=True)
        with open(caminho, 'wb') as f:
            f.write(response.content)

        try:
            # Path.unlink(caminho, missing_ok=True)
            with ZipFile(caminho, 'r') as zip:
                zip.extractall()
            print(f"Arquivo ZIP descompactado com sucesso.{caminho}")
        except Exception as e:
            print(f"Erro ao descompactar o arquivo ZIP: {e}")

        # move arquivos para expurgos
        # shutil.move(caminho, expurgos)

        print(f'Download do arquivo ZIP concluído com sucesso.{caminho}')
        resultado = f'Download do arquivo ZIP concluído com sucesso.{caminho}'
        qtd = 1
    else:
        print("Erro ao fazer o download do arquivo ZIP.")
        resultado = 'Erro ao fazer o download do arquivo ZIP.'
        qtd = 0
    return qtd


# schedule_interval="*/3 * * * * "
dag = DAG('antaq_dag', description='Dados da ANTAQ',
          schedule_interval=None, start_date=datetime(2023, 9, 10),
          catchup=False, default_args=default_args, default_view='graph',
          doc_md="## Dag pegar os dados da ANTAQ no site antaq.gov")

group_check_temp = TaskGroup("group_check_temp", dag=dag)
# group_database = TaskGroup('group_database', dag=dag)

file_sensor_task = FileSensor(
    task_id='file_sensor_task',
    filepath=Variable.get('path_file'),
    fs_conn_id='fs_default',
    poke_interval=10,
    dag=dag)

captura_conta_dados = PythonOperator(
    task_id='captura_conta_dados',
    python_callable=captura_conta_dados
)

send_email_alert = EmailOperator(
    task_id='send_email_alert',
    to='fbianastacio@gmail.com',
    subject='Airlfow alert',
    html_content='''<h3>Erro ao fazer o download do arquivo ZIP. </h3>
                                <p> Dag: antaq_dag </p>
                                ''',
    task_group=group_check_temp,
    dag=dag)

send_email_normal = EmailOperator(
    task_id='send_email_normal',
    to='fbianastacio@gmail.com',
    subject='Airlfow advise',
    html_content='''<h3>Download do arquivo ZIP concluído com sucesso. </h3>
                                <p> Dag: antaq_dag </p>
                                ''',
    task_group=group_check_temp,
    dag=dag)


def avalia_temp(ti):
    number = ti.xcom_pull(task_ids='captura_conta_dados', key="qde")
    if number == 0:
        return 'group_check_temp.send_email_alert'
    else:
        return 'group_check_temp.send_email_normal'


check_temp_branc = BranchPythonOperator(
    task_id='check_temp_branc',
    python_callable=avalia_temp,
    provide_context=True,
    dag=dag,
    task_group=group_check_temp)

with group_check_temp:
    check_temp_branc >> [send_email_alert, send_email_normal]

captura_conta_dados >> file_sensor_task >> group_check_temp
