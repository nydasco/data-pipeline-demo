from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from elt.get_rates_to_bronze import main as get_rates_to_bronze
from elt.transform_rates_to_silver import main as transform_rates_to_silver
from elt.present_rates_in_gold import main as present_rates_in_gold
from elt.produce_chart import generate_image
 
with DAG(
    dag_id='data-pipeline-demo',
    start_date=datetime(2022, 5, 28),
    schedule_interval=None
) as dag:
 
    start_task = EmptyOperator(
        task_id='start'
    )

    get_rates_to_bronze = PythonOperator(
        task_id='get_rates_to_bronze',
        python_callable=get_rates_to_bronze
    )
    
    transform_rates_to_silver = PythonOperator(
        task_id='transform_rates_to_silver',
        python_callable=transform_rates_to_silver
    )
 
    present_rates_in_gold = PythonOperator(
        task_id='present_rates_in_gold',
        python_callable=present_rates_in_gold
    )
 
    generate_image = PythonOperator(
        task_id='genrate_image',
        python_callable=generate_image
    )
 
    end_task = EmptyOperator(
        task_id='end'
    )
 
start_task >> get_rates_to_bronze
get_rates_to_bronze >> transform_rates_to_silver
transform_rates_to_silver >> present_rates_in_gold
present_rates_in_gold >> generate_image
generate_image >> end_task