from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.OpenWeather.Scripts import ApiExtraction, Create_table, LoadIntoTable

with DAG(
        "OpenWeather",
        start_date = datetime(2021, 1,1),
        schedule_interval='0 10 * * *',
        catchup=False,
        ) as dag:

        task_1 = PythonOperator(
            task_id='get_weather_data_task',
            python_callable=ApiExtraction.get_weather_data,  
            dag=dag,
            owner = 'SG'
        )

        task_2 = Create_table.create_tables_task(dag)

        task_3 = PythonOperator(
                task_id="load_weather_data",
                python_callable=LoadIntoTable.run_process,
                owner = 'SG'
            )



task_1 >> task_2 >> task_3
