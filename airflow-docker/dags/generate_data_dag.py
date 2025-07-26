from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import subprocess
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'generate_synthetic_data',
    default_args=default_args,
    description='DAG to run the data_generator.py script daily',
    schedule_interval='0 10 * * 1-5',  # Monday-Friday at 10:00 AM
    start_date=datetime(2023, 7, 25),
    catchup=False,
    tags=['data-generation', 'daily', 'source'],
) as dag:
    
    def run_data_generator():
        print(f"Starting data generation at {datetime.now()}")
        print(f"Current working directory: {os.getcwd()}")

        try:
            # Run the data_generator.py script via subprocess
            result = subprocess.run(
                ["python", "data_generator.py"],  # Relative path to your script
                check=True,
                capture_output=True,
                text=True,
                cwd="/opt/airflow/dags"
            )
            # Output from the script execution
            print("Subprocess STDOUT:", result.stdout)
        except subprocess.CalledProcessError as e:
            # Log the error output
            print("Subprocess failed with error:")
            print(e.stderr)
            raise e

    # Task 1: Generate the data
    generate_data_task = PythonOperator(
        task_id='generate_data',
        python_callable=run_data_generator,
    )

    # Task 2: Trigger data loading DAG after generation completes
    trigger_load_dag = TriggerDagRunOperator(
        task_id='trigger_data_loading',
        trigger_dag_id='load_data_to_mysql',  # Name of the second DAG
        wait_for_completion=False,  # Don't wait for loading to complete
        poke_interval=30,  # Check every 30 seconds
    )

    # Set task dependencies: Generate data THEN trigger loading
    generate_data_task >> trigger_load_dag
