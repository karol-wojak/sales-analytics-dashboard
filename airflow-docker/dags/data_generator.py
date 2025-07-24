import csv
import random
import os
from faker import Faker

# Initialize Faker library
fake = Faker()

# Products list for product selection
products = ["Laptop", "Tablet", "Headphones", "TV", "Smartphone"]

# Validation function for quantity, price, and other columns
def is_valid_row(transaction_id, customer_name, product_name, quantity, price, date):
    try:
        # Ensure transaction_id is positive integer
        if not isinstance(transaction_id, int) or transaction_id <= 0:
            raise ValueError(f"Invalid Transaction ID: {transaction_id}")

        # Ensure customer_name is non-empty
        if not customer_name or not isinstance(customer_name, str) or len(customer_name.strip()) == 0:
            raise ValueError(f"Invalid Customer Name: {customer_name}")

        # Ensure product_name is valid
        if product_name not in products:
            raise ValueError(f"Invalid Product Name: {product_name}")

        # Ensure quantity is a positive integer within 1–5 range
        if not isinstance(quantity, int) or quantity < 1 or quantity > 5:
            raise ValueError(f"Invalid Quantity: {quantity}")

        # Ensure price is a positive float within expected range
        if not isinstance(price, float) or price < 50 or price > 1500:
            raise ValueError(f"Invalid Price: {price}")

        # Ensure date is non-empty
        if not date or not isinstance(date, str):
            raise ValueError(f"Invalid Date: {date}")

        # If all checks passed, return True
        return True
    except ValueError as e:
        print(f"Validation error: {e}")
        return False


# Generate synthetic data
data = []
transaction_id = 1  # Start transaction_id from 1
for _ in range(1000):
    # Generate raw data
    customer_name = fake.name()
    product_name = random.choice(products)
    quantity = random.randint(1, 5)
    price = round(random.uniform(50, 1500), 2)
    date = fake.date_between(start_date="-1y", end_date="today").strftime("%Y-%m-%d")

    # Validate row
    if is_valid_row(transaction_id, customer_name, product_name, quantity, price, date):
        # Append only valid rows
        data.append([transaction_id, customer_name, product_name, quantity, price, date])
        transaction_id += 1
    else:
        print(f"Skipping invalid row with transaction_id {transaction_id}")

# Define the CSV file name (relative path)
output_file = "sales_data.csv"

# Print the absolute path where the file will be saved
print(f"Saving file to: {os.path.abspath(output_file)}")

# Save to CSV
try:
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Transaction_ID", "Customer_Name", "Product_Name", "Quantity", "Price", "Date"])
        writer.writerows(data)
    print("File saved successfully!")
except Exception as e:
    print(f"Error saving file: {e}")