from flask import Flask, render_template
import mysql.connector

app = Flask(__name__)

# MySQL Connection Details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "SalesDB"
}

@app.route('/')
def dashboard():
    # Connect to MySQL
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Fetch Top Products by Revenue
    cursor.execute("""
        SELECT Product_Name, Total_Revenue 
        FROM RevenuePerProduct 
        ORDER BY Total_Revenue DESC 
        LIMIT 5
    """)
    top_products = cursor.fetchall()

    # Fetch Monthly Revenue
    cursor.execute("""
        SELECT Month, Monthly_Revenue
        FROM MonthlyRevenue
        ORDER BY Month
    """)
    monthly_revenue = cursor.fetchall()

    # Close the database connection
    cursor.close()
    conn.close()

    # Pass data to the template
    return render_template('dashboard.html', top_products=top_products, monthly_revenue=monthly_revenue)

if __name__ == '__main__':
    app.run(debug=True)