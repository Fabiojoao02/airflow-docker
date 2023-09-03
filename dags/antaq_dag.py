from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
# import pandas as pd
import requests
import zipfile
import os
import shutil
from pathlib import Path
from zipfile import ZipFile


def captura_conta_dados():
    CAMINHO_ARQUIVO = Path(__file__).parent
    arquivo = '2023.zip'

    # caminho = CAMINHO_ARQUIVO / 'tmp' / arquivo
    caminho = CAMINHO_ARQUIVO / 'tmp' / arquivo
    # caminho.touch #criar
    # caminho.unlink  # apagar
    # caminho.mkdir(exist_ok=True)

    print(CAMINHO_ARQUIVO)
    print(caminho)

    url = 'https://web3.antaq.gov.br/ea/txt/2023.zip'
    response = requests.get(url)
    print(response)
    if response.status_code == 200:
        with open('/tmp/2023.zip', 'wb') as f:
            f.write(response.content)
        print("Download do arquivo ZIP concluído com sucesso.")
        resultado = 'Download do arquivo ZIP concluído com sucesso.'
        qtd = 1
    else:
        print("Erro ao fazer o download do arquivo ZIP.")
        resultado = 'Erro ao fazer o download do arquivo ZIP.'
        qtd = 0

    return qtd


def email_carga(ti):
    qtd = ti.xcom_pull(task_ids='captura_conta_dados')
    print('ipoipoipoipoipoipoipoipoipoipoipoipoipoipoipoipoiop')
    print(qtd)
    if (qtd == 1):
        return 'carga_delta'
    return 'nvalido'


def carga_delta(ti):
    qtd = ti.xcom_pull(task_ids='email_carga')
    if (qtd == 1):
        return 'carga_delta'
    return 'nvalido'


def email_concluido(ti):
    qtd = ti.xcom_pull(task_ids='carga_delta')
    if (qtd == 1):
        return 'email_concluido'
    return 'nvalido'


with DAG('antaq_dag', start_date=datetime(2023, 9, 1),
         schedule_interval='@daily', catchup=False
         ) as dag:

    captura_conta_dados = PythonOperator(
        task_id='captura_conta_dados',
        python_callable=captura_conta_dados

    )

    email_carga = BranchPythonOperator(
        task_id='email_carga',
        python_callable=email_carga
    )

    carga_delta = PythonOperator(
        task_id='carga_delta',
        python_callable=carga_delta
    )

    email_concluido = BashOperator(
        task_id='email_concluido',
        bash_command="echo 'Quantidade não OK'"
    )


captura_conta_dados >> email_carga >> carga_delta >> email_concluido
