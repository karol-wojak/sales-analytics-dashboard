from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector

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
) as dag:
    
    def load_csv_to_mysql():
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

        # Connect to MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()


        # Insert Rows into Transactions Table
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
            except (ValueError, KeyError) as e:
                # Log and continue in case of bad data
                print(f"Skipping invalid row: {row}, Error: {e}")
                continue

        # Commit and Close
        connection.commit()
        cursor.close()
        connection.close()
        print("Data successfully loaded into MySQL!")

    # Define the task
    load_data_task = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql
    )

    load_data_task