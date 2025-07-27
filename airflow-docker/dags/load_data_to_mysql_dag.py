from airflow import DAG # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from datetime import datetime, timedelta
import pandas as pd # type: ignore
import mysql.connector # type: ignore

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
    'load_data_to_mysql',
    default_args=default_args,
    description='DAG to load sales_data.csv into MySQL transactions table',
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['data-loading', 'mysql', 'etl'],
) as dag:
    
    def load_csv_to_mysql():
        print(f"Starting data loading to MySQL at {datetime.now()}")

        # Connection Configuration
        db_config = {
            'host': 'airflow-mysql',
            'port': 3306,
            'user': 'root',
            'password': 'root',
            'database': 'sales_db'
        }

        # Read the CSV File
        csv_file = '/opt/airflow/dags/sales_data.csv'  # Path inside the DAGs folder
        data = pd.read_csv(csv_file)
        print(f"Found {len(data)} rows to process")

        # Connect to MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Clear existing data first
        print("🗑️ Clearing existing data from transactions table...")
        cursor.execute("TRUNCATE TABLE transactions")
        print("Existing data cleared successfully!")

        # Insert Rows into Transactions Table
        successful_rows = 0
        for _, row in data.iterrows():
            try:
                query = """
                    INSERT INTO transactions (transaction_id, customer_name, product_name, quantity, price, sale_date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                values = (
                    int(row['Transaction_ID']),
                    row['Customer_Name'],
                    row['Product_Name'],
                    int(row['Quantity']),
                    float(row['Price']),
                    row['Date']
                )
                cursor.execute(query, values)
                successful_rows += 1
            except (ValueError, KeyError) as e:
                # Log and continue in case of bad data
                print(f"⚠️ Skipping invalid row: {row}, Error: {e}")
                continue

        # Commit and Close
        connection.commit()
        cursor.close()
        connection.close()
        print(f"Data successfully loaded into MySQL! {successful_rows} rows inserted.")

    # Task 1: Load data to MySQL
    load_data_task = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql
    )

    # Task 2: Trigger MDM processing DAG
    trigger_mdm_dag = TriggerDagRunOperator(
        task_id='trigger_mdm_processing',
        trigger_dag_id='mdm_processing',
        dag=dag,
    )

    # Set task dependencies: Load data THEN trigger Spark processing
    load_data_task >> trigger_mdm_dag