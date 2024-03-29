from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

# bronze data
from pipelines.rates_pipeline.get_rates_to_bronze import main as get_rates_to_bronze
from pipelines.currency_pipeline.get_currencies_to_bronze import main as get_currencies_to_bronze

# silver data
from pipelines.rates_pipeline.transform_rates_to_silver import main as transform_rates_to_silver
from pipelines.currency_pipeline.transform_currencies_to_silver import main as transform_currencies_to_silver

# gold data
from pipelines.rates_pipeline.present_rates_in_gold import main as present_rates_in_gold
from pipelines.currency_pipeline.present_currencies_in_gold import main as present_currencies_in_gold
from pipelines.generic_pipeline.present_dates_in_gold import main as present_dates_in_gold
 
with DAG(
    dag_id='data-pipeline-demo',
    start_date=datetime(2024, 2, 18),
    schedule_interval=None
) as dag:
 
    start_task = EmptyOperator(
        task_id='start'
    )

    get_rates_to_bronze = PythonOperator(
        task_id='get_rates_to_bronze',
        python_callable=get_rates_to_bronze
    )

    get_currencies_to_bronze = PythonOperator(
        task_id='get_currencies_to_bronze',
        python_callable=get_currencies_to_bronze
    )
    
    transform_rates_to_silver = PythonOperator(
        task_id='transform_rates_to_silver',
        python_callable=transform_rates_to_silver
    )
    
    transform_currencies_to_silver = PythonOperator(
        task_id='transform_currencies_to_silver',
        python_callable=transform_currencies_to_silver
    )
 
    present_rates_in_gold = PythonOperator(
        task_id='present_rates_in_gold',
        python_callable=present_rates_in_gold
    )
 
    present_currencies_in_gold = PythonOperator(
        task_id='present_currencies_in_gold',
        python_callable=present_currencies_in_gold
    )
 
    present_dates_in_gold = PythonOperator(
        task_id='present_dates_in_gold',
        python_callable=present_dates_in_gold
    )
 
    end_task = EmptyOperator(
        task_id='end'
    )
 
start_task >> [get_rates_to_bronze >> transform_rates_to_silver >> present_rates_in_gold,
               get_currencies_to_bronze >> transform_currencies_to_silver >> present_currencies_in_gold] >> present_dates_in_gold >> end_task