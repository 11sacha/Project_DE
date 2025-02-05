from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.TableCreator.Scripts import ExtractTransform, CreateTable, InsertIntoTable


# Define DAG
with DAG(
            "VehiclesData",
            start_date = datetime(2021, 1,1),
            schedule_interval='0 11 * * *',
            catchup=False,
            ) as dag:

            task_1 = PythonOperator(
                task_id="extraction_and_transformation",
                python_callable=ExtractTransform.run_process,
                owner = 'SG'
            )

            task_2 = CreateTable.create_tables_task(dag)

            task_3 = PythonOperator(
                task_id="loading_into_db",
                python_callable=InsertIntoTable.run_process,
                owner = 'SG'
            )

task_1 >> task_2 >> task_3

