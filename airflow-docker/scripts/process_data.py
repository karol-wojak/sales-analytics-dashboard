from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import sum as _sum, col, month # type: ignore
from pyspark.sql.types import DecimalType # type: ignore

# MySQL Connection Details
mysql_url = "jdbc:mysql://airflow-mysql:3306/sales_db"
mysql_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Start SparkSession
spark = SparkSession.builder \
    .appName("SalesAnalytics_with_MDM") \
    .config("spark.jars", "/opt/spark/drivers/mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

print("🔍 Loading data with Master Data Management...")

# Load raw transactions
transactions = spark.read.jdbc(
    url=mysql_url,
    table="transactions",
    properties=mysql_properties
)

# Load master customer data
master_customers = spark.read.jdbc(
    url=mysql_url,
    table="master_customers",
    properties=mysql_properties
)

# Load master product data
master_products = spark.read.jdbc(
    url=mysql_url,
    table="master_products",
    properties=mysql_properties
)

print(f"📊 Raw transactions: {transactions.count()}")
print(f"👥 Master customers: {master_customers.count()}")
print(f"📦 Master products: {master_products.count()}")

# Join transactions with master data for standardized names
enriched_data = transactions \
    .join(
        master_customers.select("original_name", "standardized_name", "customer_key")
            .withColumnRenamed("original_name", "customer_name")
            .withColumnRenamed("standardized_name", "standardized_customer_name"),
        "customer_name",
        "left"
    ) \
    .join(
        master_products.select("original_name", "standardized_name", "product_key")
            .withColumnRenamed("original_name", "product_name")
            .withColumnRenamed("standardized_name", "standardized_product_name"),
        "product_name",
        "left"
    )

print("✅ Data enriched with master data")

# Ensure "Price" column is Decimal for accurate computations
enriched_data = enriched_data.withColumn("price", col("price").cast(DecimalType(10, 2)))

# Calculate Total Revenue per Product (using standardized names)
total_revenue = enriched_data \
    .withColumn("revenue", col("price") * col("quantity")) \
    .groupBy("standardized_product_name") \
    .agg(_sum("revenue").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .withColumnRenamed("standardized_product_name", "product_name")

# Calculate Monthly Revenue
monthly_revenue = enriched_data \
    .withColumn("revenue", col("price") * col("quantity")) \
    .withColumn("month", month("sale_date")) \
    .groupBy("month") \
    .agg(_sum("revenue").alias("monthly_revenue")) \
    .orderBy("month")

# Calculate Customer Analytics (bonus with master data)
customer_revenue = enriched_data \
    .withColumn("revenue", col("price") * col("quantity")) \
    .groupBy("standardized_customer_name") \
    .agg(_sum("revenue").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc()) \
    .withColumnRenamed("standardized_customer_name", "customer_name")

print("📊 Analytics calculated with standardized data")

# Save Total Revenue per Product to MySQL
total_revenue.write.jdbc(
    url=mysql_url,
    table="revenue_per_product",
    mode="overwrite",
    properties=mysql_properties
)

# Save Monthly Revenue to MySQL
monthly_revenue.write.jdbc(
    url=mysql_url,
    table="monthly_revenue",
    mode="overwrite",
    properties=mysql_properties
)

print("💾 Results saved to MySQL tables")

# Optional: Save customer analytics to new table
print("📈 Analytics completed with Master Data:")
print(f"   - Products analyzed: {total_revenue.count()}")
print(f"   - Months analyzed: {monthly_revenue.count()}")
print(f"   - Customers analyzed: {customer_revenue.count()}")

print("✅ Processed data saved successfully with MDM!")

# Stop SparkSession
spark.stop()