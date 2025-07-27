from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.providers.mysql.operators.mysql import MySqlOperator # type: ignore
from airflow.providers.mysql.hooks.mysql import MySqlHook # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'mdm_processing',
    default_args=default_args,
    description='Master Data Management processing pipeline',
    schedule_interval=None,  # Manual trigger for now
    catchup=False,
    tags=['mdm', 'data-quality', 'master-data'],
)

def check_transactions_data(**context):
    """Check if we have transaction data to process"""
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    # Count transactions
    sql = "SELECT COUNT(*) as count FROM sales_db.transactions"
    result = mysql_hook.get_first(sql)
    transaction_count = result[0] if result else 0
    
    print(f"📊 Found {transaction_count} transactions to process")
    
    if transaction_count == 0:
        raise ValueError("No transaction data found! Run data generation first.")
    
    return transaction_count

def validate_mdm_results(**context):
    """Validate that MDM processing completed successfully"""
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    # Check master customers
    customers_sql = "SELECT COUNT(*) FROM sales_db.master_customers"
    customers_result = mysql_hook.get_first(customers_sql)
    customers_count = customers_result[0] if customers_result else 0
    
    # Check master products
    products_sql = "SELECT COUNT(*) FROM sales_db.master_products"
    products_result = mysql_hook.get_first(products_sql)
    products_count = products_result[0] if products_result else 0
    
    # Check data quality summary
    quality_sql = "SELECT COUNT(*) FROM sales_db.data_quality_summary"
    quality_result = mysql_hook.get_first(quality_sql)
    quality_count = quality_result[0] if quality_result else 0
    
    print(f"✅ MDM Results:")
    print(f"   - Master Customers: {customers_count}")
    print(f"   - Master Products: {products_count}")
    print(f"   - Quality Records: {quality_count}")
    
    if customers_count == 0 or products_count == 0:
        raise ValueError("MDM processing failed - no master records created!")
    
    return {
        'customers': customers_count,
        'products': products_count,
        'quality_records': quality_count
    }

# Task 1: Check if we have data to process
check_data_task = PythonOperator(
    task_id='check_transactions_data',
    python_callable=check_transactions_data,
    dag=dag,
)

# Task 2: Clear existing MDM tables (optional - for reprocessing)
clear_mdm_tables = MySqlOperator(
    task_id='clear_mdm_tables',
    mysql_conn_id='mysql_default',
    sql="""
    DELETE FROM sales_db.master_customers;
    DELETE FROM sales_db.master_products;
    DELETE FROM sales_db.data_quality_summary;
    """,
    dag=dag,
)

# Task 3: Run MDM processing with Spark
mdm_processing_task = BashOperator(
    task_id='run_mdm_processing',
    bash_command="""
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --jars /opt/spark/drivers/mysql-connector-j-9.3.0.jar \
        --driver-memory 1g \
        --executor-memory 1g \
        /opt/spark/scripts/mdm_processor.py
    """,
    dag=dag,
)

# Task 4: Validate results
validate_results_task = PythonOperator(
    task_id='validate_mdm_results',
    python_callable=validate_mdm_results,
    dag=dag,
)

# Task 5: Trigger Spark analytics DAG after loading completes
trigger_spark_analytics = TriggerDagRunOperator(
    task_id='trigger_spark_analytics',
    trigger_dag_id='spark_sales_analytics',  # Name of the Spark DAG
    dag=dag,
)

# Define task dependencies
check_data_task >> clear_mdm_tables >> mdm_processing_task >> validate_results_task >> trigger_spark_analytics