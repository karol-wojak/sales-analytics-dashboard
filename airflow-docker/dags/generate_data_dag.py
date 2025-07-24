from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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
    schedule_interval='@daily',  # Run once a day
    start_date=datetime(2023, 7, 25),
    catchup=False,
) as dag:
    
    def run_data_generator():
        # Debug: Print the current working directory inside the PythonOperator
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

    # Define the PythonOperator task
    generate_data_task = PythonOperator(
        task_id='generate_data',
        python_callable=run_data_generator,
    )

    # Set the task in the DAG
    generate_data_task
