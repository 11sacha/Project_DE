import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
import traceback
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


#Defino rutas relativas
FilePath = os.path.dirname(__file__)
os.chdir(os.path.join(FilePath, '../'))
ProjectPath = os.getcwd()
InputFolder = os.path.join(ProjectPath, r'Input')
OutputFolder = os.path.join(ProjectPath, r'Output')

postgres_conn_id = 'my_local_db'
schema = 'dags'

def insert_data(table_name: str, file_path: str, postgres_conn_id: str):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    print(f'Ruta del archivo a procesar: {file_path}')
    df = pd.read_csv(file_path, delimiter=',')

    print(f"Columnas encontradas en el archivo: {df.columns.tolist()}")
    
    count = 0
    # Assuming the CSV has a single column of data for simplicity
    for index, row in df.iterrows():
        insert_sql = f"INSERT INTO {schema}.{table_name} (zona, localidad, latitud, longitud, timezone) VALUES (%s, %s, %s, %s, %s);"
        pg_hook.run(insert_sql, parameters=[row['Zona'], row['Localidad'], row['Latitud'], row['Longitud'], row['Timezone']])
        count += 1
    
    print('Numero de registros insertados:', count)

def run_process():
    try:
        for filename in os.listdir(OutputFolder):
            if filename.endswith(".csv"): 
                table_name = os.path.splitext(filename)[0]  
                print(f'Se insertara data en la tabla {table_name}')
                file_path = os.path.join(OutputFolder, filename)

                insert_data(table_name, file_path, postgres_conn_id)

    except Exception as e:
        print(f'Error: {e}')
        traceback.print_exc()
