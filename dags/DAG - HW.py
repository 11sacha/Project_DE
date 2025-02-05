from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Simple function to print a message
def hello_airflow():
    print("Hello, Airflow!")

# Define the DAG
with DAG(
    dag_id="test_hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello_airflow,
        owner = 'SG'
    )

    task_hello  # This ensures the task runs

