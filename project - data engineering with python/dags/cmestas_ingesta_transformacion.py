from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

args = {"owner": "cmestas", "start_date":days_ago(1)}
dag = DAG(
    dag_id="cmestas_ingesta_transformacion",
    default_args=args,
    schedule_interval='@once', # * * * * * *
)

with dag:
    run_script_ingest = BashOperator(
        task_id='run_script_ingest',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/Ingest.py"'
    )

    run_script_transform = BashOperator(
        task_id='run_script_transform',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/Transform.py"'
    )


    run_script_ingest >> run_script_transform