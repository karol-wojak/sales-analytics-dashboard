import csv
import random
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

# Save to CSV
with open("sales_data.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Transaction_ID", "Customer_Name", "Product_Name", "Quantity", "Price", "Date"])
    writer.writerows(data)