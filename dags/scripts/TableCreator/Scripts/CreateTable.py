import os
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Define paths
FilePath = os.path.dirname(__file__)
os.chdir(os.path.join(FilePath, '../'))
ProjectPath = os.getcwd()
OutputFolder = os.path.join(ProjectPath, r'Output')

postgres_conn_id = 'my_local_db'
schema = 'dags'

def generate_create_table_sql(table_name: str) -> str:
    return f'''
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        id SERIAL PRIMARY KEY,
        brand TEXT,
        model TEXT,
        year TEXT,
        mileage TEXT
    );
    '''

def create_tables_task(dag):
    sql_statements = ""
    
    print('Buscando archivos..')
    for filename in os.listdir(OutputFolder):
        if filename.endswith(".csv"): 
            table_name = os.path.splitext(filename)[0]  
            print(f'Se creara la tabla {table_name}')
            sql_statements += generate_create_table_sql(table_name) + "\n"  

    # Create a single task to execute all SQL statements
    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id=postgres_conn_id, 
        sql=sql_statements,
        dag=dag
    )

    return create_tables_task

