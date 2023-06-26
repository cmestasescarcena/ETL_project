from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.utils.task_group import TaskGroup

args = {"owner": "cmestas", "start_date":days_ago(1)}
dag = DAG(
    dag_id="cmestas_parallel_v2",
    default_args=args,
    schedule_interval='@once', # * * * * * *
)

with dag:

    run_script_ingest_categories = BashOperator(
        task_id='run_script_ingest_categories',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/Ingest2.py" "mysql" "retail_db" "data-engineering-python" "categories" "landing/categories"'
    )
        
    run_script_ingest_customers = BashOperator(
        task_id='run_script_ingest_customers',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/Ingest2.py" "cloud_storage" "data-engineering-python" "data-engineering-python" "retail/customers" "landing/customers"'
    )

    run_script_ingest_departments = BashOperator(
        task_id='run_script_ingest_departments',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/Ingest2.py" "cloud_storage" "data-engineering-python" "data-engineering-python" "retail/departments" "landing/departments"'
    )

    run_script_ingest_order_items = BashOperator(
        task_id='run_script_ingest_order_items',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/Ingest2.py" "cloud_storage" "data-engineering-python" "data-engineering-python" "retail/order_items" "landing/order_items"'
    )

    run_script_ingest_orders = BashOperator(
        task_id='run_script_ingest_orders',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/Ingest2.py" "cloud_storage" "data-engineering-python" "data-engineering-python" "retail/orders" "landing/orders"'
    )

    run_script_ingest_products = BashOperator(
        task_id='run_script_ingest_products',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/Ingest2.py" "mongo_db" "retail_db" "data-engineering-python" "products" "landing/products"'
    )

    with TaskGroup("transform" , tooltip="task group #1") as section:
        run_script_transform_enunciado1 = BashOperator(
            task_id='run_script_transform_enunciado1',
            bash_command='python "/user/app/ProyectoEndToEndPython/project/Transform2.py" "statment01"'
        )
        run_script_transform_enunciado2 = BashOperator(
            task_id='run_script_transform_enunciado2',
            bash_command='python "/user/app/ProyectoEndToEndPython/project/Transform2.py" "statment02"'
        )

    run_script_load_enunciado1 = BashOperator(
        task_id='run_script_load_enunciado1',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/Load2.py" "statment01"'
    )

    run_script_load_enunciado2= BashOperator(
        task_id='run_script_load_enunciado2',
        bash_command='python "/user/app/ProyectoEndToEndPython/project/Load2.py" "statment02"'
    )
    run_script_ingest_categories    >> section 
    run_script_ingest_customers     >> section 
    run_script_ingest_departments   >> section      
    run_script_ingest_order_items   >> section 
    run_script_ingest_orders        >> section 
    run_script_ingest_products      >> section 
    section >> run_script_load_enunciado1
    section >> run_script_load_enunciado2
    