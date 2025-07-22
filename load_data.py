import mysql.connector
import pandas as pd

# Load the CSV file into a DataFrame
df = pd.read_csv("sales_data.csv")

# Connect to the MySQL database
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="salesDB"
)

# Insert data into the MySQL database
cursor = connection.cursor()
for index, row in df.iterrows():
    sql = """
    INSERT INTO Transactions (Transaction_ID, Customer_Name, Product_Name, Quantity, Price, Date)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, tuple(row))

# Commit the transaction
connection.commit()
# Close the cursor and connection
cursor.close()
connection.close()
print("Data inserted successfully into the database.")