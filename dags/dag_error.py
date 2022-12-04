import logging
from datetime import datetime, timedelta
from time import strftime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger("Test")

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'email': [Variable.get("default_email")],
    'email_on_failure': True
}


def falla():
    raise Exception("Se simuló un error")


with DAG(
    "dag_error",
    default_args=default_args,
    description="Simular un error para mail fallo",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 11)
) as dag:
    tarea_falla = PythonOperator(
                    task_id='falla_1',
                    python_callable=falla
                    )

    tarea_falla
