from flask import Flask, render_template
import mysql.connector
import os

app = Flask(__name__)

# MySQL Connection Details (updated for Docker)
db_config = {
    "host": os.getenv("MYSQL_HOST", "airflow-mysql"),  # Use Docker service name
    "port": os.getenv("MYSQL_PORT", 3306),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DATABASE", "sales_db")  # Updated database name
}

@app.route('/')
def dashboard():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Fetch Top Products by Revenue (updated table names)
        cursor.execute("""
            SELECT product_name, total_revenue 
            FROM revenue_per_product 
            ORDER BY total_revenue DESC 
            LIMIT 5
        """)
        top_products = cursor.fetchall()

        # Fetch Monthly Revenue (updated table names)
        cursor.execute("""
            SELECT month, monthly_revenue
            FROM monthly_revenue
            ORDER BY month
        """)
        monthly_revenue = cursor.fetchall()

        # Close the database connection
        cursor.close()
        conn.close()

        # Pass data to the template
        return render_template('dashboard.html', 
                             top_products=top_products, 
                             monthly_revenue=monthly_revenue)
    
    except Exception as e:
        return f"Database connection error: {str(e)}"

@app.route('/health')
def health_check():
    return {"status": "healthy"}, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)