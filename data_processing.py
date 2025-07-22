from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, month
from pyspark.sql.types import DecimalType

# MySQL Connection Details
mysql_url = "jdbc:mysql://localhost:3306/SalesDB"
mysql_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Start SparkSession
spark = SparkSession.builder \
    .appName("SalesAnalytics") \
    .config("spark.driver.extraClassPath", "mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

# Load Transactions data from MySQL into PySpark
data = spark.read.jdbc(
    url=mysql_url,
    table="Transactions",
    properties=mysql_properties
)

# Ensure "Price" column is Decimal for accurate computations
data = data.withColumn("Price", col("Price").cast(DecimalType(10, 2)))

# Calculate Total Revenue per Product
total_revenue = data.withColumn("Revenue", col("Price") * col("Quantity")) \
    .groupBy("Product_Name") \
    .agg(_sum("Revenue").alias("Total_Revenue")) \
    .orderBy(col("Total_Revenue").desc())

# Calculate Monthly Revenue
monthly_revenue = data.withColumn("Revenue", col("Price") * col("Quantity")) \
    .withColumn("Month", month("Date")) \
    .groupBy("Month") \
    .agg(_sum("Revenue").alias("Monthly_Revenue")) \
    .orderBy("Month")

# Save Total Revenue per Product to MySQL
total_revenue.write.jdbc(
    url=mysql_url,
    table="RevenuePerProduct",
    mode="overwrite",  # Clear the table and write fresh data
    properties=mysql_properties
)

# Save Monthly Revenue to MySQL
monthly_revenue.write.jdbc(
    url=mysql_url,
    table="MonthlyRevenue",
    mode="overwrite",
    properties=mysql_properties
)

print("Processed data saved successfully!")
# Stop SparkSession
spark.stop()