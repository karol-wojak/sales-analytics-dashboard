import csv
import random
import os
from faker import Faker

fake = Faker()
products = ["Laptop", "Tablet", "Headphones", "TV", "Smartphone"]

# Generate synthetic data
data = []
for i in range(1000):
    transaction_id = i + 1
    customer_name = fake.name()
    product_name = random.choice(products)
    quantity = random.randint(1, 5)
    price = round(random.uniform(50, 1500), 2)
    date = fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")
    data.append([transaction_id, customer_name, product_name, quantity, price, date])

# Define the CSV file name (relative path)
output_file = "sales_data.csv"

# Print the absolute path where the file will be saved
print(f"Saving file to: {os.path.abspath(output_file)}")

# Save to CSV
with open("sales_data.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Transaction_ID", "Customer_Name", "Product_Name", "Quantity", "Price", "Date"])
    writer.writerows(data)

# Confirm successful completion
print("File saved successfully!")