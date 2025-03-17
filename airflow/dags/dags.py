import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.dags.transformation import extract_movies_to_df, extract_users_to_df, load_df_to_db, transform_avg_ratings # type: ignore
from airflow.dags.transformation import *  # type: ignore # Assuming you have the transformation module

# Define the ETL function
def etl():
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    transformed_df = transform_avg_ratings(movies_df, users_df)
    load_df_to_db(transformed_df)

# Define the arguments for the DAG
default_args = {
    'owner': 'harikav',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': True,
    'email': ['info@example.com'],  # Corrected email format
    'email_on_failure': True,
    'email_on_retry': False,
    'retries':3,
    'retry_delay': timedelta(minutes=30),
}

# Define the DAG
dag = DAG(dag_id = 'etl_pipeline',  # Name of the DAG
    default_args=default_args,
    description='ETL pipeline for movie ratings',
    schedule_interval= "0 0 * * *")  # Adjust based on your needs


# Define the task to run the ETL function
etl_task = PythonOperator(
    task_id='etl_task',  # Task ID
    python_callable=etl,  # Call the ETL function
    dag=dag,
)

etl()