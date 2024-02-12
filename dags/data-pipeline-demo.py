from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
 
with DAG(
    dag_id='data-pipeline-demo',
    start_date=datetime(2022, 5, 28),
    schedule_interval=None
) as dag:
 
    start_task = EmptyOperator(
        task_id='start'
    )

    get_rates_to_bronze = BashOperator(
        task_id='get_rates_to_bronze',
        bash_command='python /opt/airflow/dags/elt/get_rates_to_bronze.py'
    )
 
    transform_rates_to_silver = BashOperator(
        task_id='transform_rates_to_silver',
        bash_command='python /opt/airflow/dags/elt/transform_rates_to_silver.py'
    )
 
    present_rates_in_gold = BashOperator(
        task_id='present_rates_in_gold',
        bash_command='python /opt/airflow/dags/elt/present_rates_in_gold.py'
    )
 
    end_task = EmptyOperator(
        task_id='end'
    )
 
start_task >> get_rates_to_bronze
get_rates_to_bronze >> transform_rates_to_silver
transform_rates_to_silver >> present_rates_in_gold
present_rates_in_gold >> end_task