from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, month
from pyspark.sql.types import DecimalType

# MySQL Connection Details
mysql_url = "jdbc:mysql://airflow-mysql:3306/sales_db"
mysql_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Start SparkSession
spark = SparkSession.builder \
    .appName("SalesAnalytics") \
    .config("spark.driver.extraClassPath", "/opt/spark/drivers/mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

# Load Transactions data from MySQL into PySpark
data = spark.read.jdbc(
    url=mysql_url,
    table="transactions",
    properties=mysql_properties
)

# Ensure "Price" column is Decimal for accurate computations
data = data.withColumn("price", col("price").cast(DecimalType(10, 2)))

# Calculate Total Revenue per Product
total_revenue = data.withColumn("revenue", col("price") * col("quantity")) \
    .groupBy("product_name") \
    .agg(_sum("revenue").alias("total_revenue")) \
    .orderBy(col("total_revenue").desc())

# Calculate Monthly Revenue
monthly_revenue = data.withColumn("revenue", col("price") * col("quantity")) \
    .withColumn("month", month("sale_date")) \
    .groupBy("month") \
    .agg(_sum("revenue").alias("monthly_revenue")) \
    .orderBy("month")

# Save Total Revenue per Product to MySQL
total_revenue.write.jdbc(
    url=mysql_url,
    table="revenue_per_product",
    mode="overwrite",  # Clear the table and write fresh data
    properties=mysql_properties
)

# Save Monthly Revenue to MySQL
monthly_revenue.write.jdbc(
    url=mysql_url,
    table="monthly_revenue",
    mode="overwrite",
    properties=mysql_properties
)

print("Processed data saved successfully!")
# Stop SparkSession
spark.stop()