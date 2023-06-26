from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup

args = {"owner": "cmestas", "start_date":days_ago(1)}
dag = DAG(
    dag_id="cmestas_parallel_v1",
    default_args=args,
    schedule_interval='@once', # * * * * * *
    catchup=False
)

with dag:
    run_script_ingest_categories = BashOperator(
        task_id='run_script_ingest_categories',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/ingest_categories.py"'
    )

    run_script_ingest_customers = BashOperator(
        task_id='run_script_ingest_customers',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/ingest_customers.py"'
    )

    run_script_ingest_deparments = BashOperator(
        task_id='run_script_ingest_deparments',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/ingest_deparments.py"'
    )

    run_script_ingest_order_items = BashOperator(
        task_id='run_script_ingest_order_items',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/ingest_order_items.py"'
    )

    run_script_ingest_orders = BashOperator(
        task_id='run_script_ingest_orders',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/ingest_orders.py"'
    )

    run_script_ingest_products = BashOperator(
        task_id='run_script_ingest_products',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/ingest_products.py"'
    )

    with TaskGroup("transform" , tooltip="task group #1") as section:
        run_script_transform_enunciado1 = BashOperator(
            task_id='run_script_transform_enunciado1',
            bash_command='python "/user/app/ProyectoEndToEndPython/project/transform_enunciado1.py"'
        )
        run_script_transform_enunciado2 = BashOperator(
            task_id='run_script_transform_enunciado2',
            bash_command='python "/user/app/ProyectoEndToEndPython/project/transform_enunciado2.py"'
        )
    
    run_script_load_enunciado1 = BashOperator(
        task_id='run_script_load_enunciado1',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/load_enunciado1.py"'
    )

    run_script_load_enunciado2 = BashOperator(
        task_id='run_script_load_enunciado2',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/load_enunciado2.py"'
    )

    run_script_ingest_categories  >> section 
    run_script_ingest_customers   >> section 
    run_script_ingest_deparments  >> section
    run_script_ingest_order_items >> section
    run_script_ingest_orders      >> section 
    run_script_ingest_products    >> section 
    section >> run_script_load_enunciado1
    section >> run_script_load_enunciado2