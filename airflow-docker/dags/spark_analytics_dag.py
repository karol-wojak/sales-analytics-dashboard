from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'spark_sales_analytics',
    default_args=default_args,
    description='Process sales data using Apache Spark',
    schedule_interval=None,  # Triggered by data loading DAG
    catchup=False,  # Don't run for past dates
    tags=['spark', 'analytics', 'sales'],
)

# Start task
start_task = DummyOperator(
    task_id='start_analytics',
    dag=dag,
)

# Spark Submit Task
spark_submit_task = BashOperator(
    task_id='run_spark_analytics',
    bash_command='''
    docker exec spark-worker /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --driver-class-path /opt/spark/drivers/mysql-connector-j-9.3.0.jar \
        --jars /opt/spark/drivers/mysql-connector-j-9.3.0.jar \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        /opt/spark/scripts/process_data.py
    ''',
    dag=dag,
)

# End task
end_task = DummyOperator(
    task_id='analytics_complete',
    dag=dag,
)

# Task Dependencies
start_task >> spark_submit_task >> end_task