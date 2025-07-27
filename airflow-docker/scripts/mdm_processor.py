from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap, md5, upper, count
from pyspark.sql.types import *

def create_spark_session():
    """Create Spark session with MySQL connector"""
    return SparkSession.builder \
        .appName("MDM_Processor") \
        .config("spark.jars", "/opt/spark/drivers/mysql-connector-j-9.3.0.jar") \
        .getOrCreate()

def create_golden_records(spark):
    """Create master data golden records"""
    
    print("🔍 Starting MDM processing...")
    
    # Database connection properties
    jdbc_url = "jdbc:mysql://airflow-mysql:3306/sales_db"
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    # Read transactions data
    print("📖 Reading transaction data...")
    transactions_df = spark.read.jdbc(
        url=jdbc_url,
        table="transactions",
        properties=connection_properties
    )
    
    print(f"📊 Found {transactions_df.count()} transactions")
    
    # Process Customer Master Data
    print("👥 Processing customer master data...")
    
    customers_df = transactions_df.select("customer_name") \
        .filter(col("customer_name").isNotNull()) \
        .distinct() \
        .withColumn("standardized_name", 
                   initcap(trim(col("customer_name")))) \
        .withColumn("customer_key", 
                   md5(upper(trim(col("customer_name"))))) \
        .withColumnRenamed("customer_name", "original_name")
    
    # Add record counts
    customer_counts = transactions_df.groupBy("customer_name") \
        .count() \
        .withColumnRenamed("customer_name", "original_name") \
        .withColumnRenamed("count", "record_count")
    
    customers_final = customers_df.join(customer_counts, "original_name")
    
    print(f"👥 Creating {customers_final.count()} customer master records")
    
    # Write customer master data
    customers_final.write.jdbc(
        url=jdbc_url,
        table="master_customers",
        mode="overwrite",
        properties=connection_properties
    )
    
    # Process Product Master Data
    print("📦 Processing product master data...")
    
    products_df = transactions_df.select("product_name") \
        .filter(col("product_name").isNotNull()) \
        .distinct() \
        .withColumn("standardized_name", 
                   initcap(trim(col("product_name")))) \
        .withColumn("product_key", 
                   md5(upper(trim(col("product_name"))))) \
        .withColumnRenamed("product_name", "original_name")
    
    # Add record counts
    product_counts = transactions_df.groupBy("product_name") \
        .count() \
        .withColumnRenamed("product_name", "original_name") \
        .withColumnRenamed("count", "record_count")
    
    products_final = products_df.join(product_counts, "original_name")
    
    print(f"📦 Creating {products_final.count()} product master records")
    
    # Write product master data
    products_final.write.jdbc(
        url=jdbc_url,
        table="master_products",
        mode="overwrite",
        properties=connection_properties
    )
    
    # Create Data Quality Summary
    print("📊 Creating data quality summary...")
    
    total_customers = transactions_df.select("customer_name").count()
    unique_customers = transactions_df.select("customer_name").distinct().count()
    
    total_products = transactions_df.select("product_name").count()
    unique_products = transactions_df.select("product_name").distinct().count()
    
    # Create summary records
    quality_data = [
        ("customers", total_customers, unique_customers, 
         total_customers - unique_customers, unique_customers),
        ("products", total_products, unique_products, 
         total_products - unique_products, unique_products)
    ]
    
    quality_df = spark.createDataFrame(quality_data, [
        "entity_type", "total_records", "unique_records", 
        "duplicate_records", "standardization_applied"
    ])
    
    # Write quality summary
    quality_df.write.jdbc(
        url=jdbc_url,
        table="data_quality_summary",
        mode="overwrite",
        properties=connection_properties
    )
    
    print("✅ MDM processing completed successfully!")
    print(f"   - {customers_final.count()} customer master records created")
    print(f"   - {products_final.count()} product master records created")
    print("   - Data quality summary updated")

if __name__ == "__main__":
    spark = create_spark_session()
    create_golden_records(spark)
    spark.stop()