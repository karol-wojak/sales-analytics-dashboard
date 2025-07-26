from flask import Flask, render_template, jsonify
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

def get_db_connection():
    """Get database connection"""
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_top_products():
    """Get top 5 products by revenue"""
    connection = get_db_connection()
    if not connection:
        return []
    
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT product_name, total_revenue 
            FROM revenue_per_product 
            ORDER BY total_revenue DESC 
            LIMIT 5
        """)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Error fetching top products: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def get_monthly_revenue():
    """Get monthly revenue data"""
    connection = get_db_connection()
    if not connection:
        return []
    
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT month, monthly_revenue
            FROM monthly_revenue
            ORDER BY month
        """)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Error fetching monthly revenue: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

@app.route('/')
def dashboard():
    try:
        # Use the extracted functions
        top_products = get_top_products()
        monthly_revenue = get_monthly_revenue()

        # Pass data to the template
        return render_template('dashboard.html', 
                             top_products=top_products, 
                             monthly_revenue=monthly_revenue)
    
    except Exception as e:
        return f"Dashboard error: {str(e)}"

@app.route('/api/data')
def get_data():
    """API endpoint for chart data"""
    try:
        top_products = get_top_products()
        monthly_revenue = get_monthly_revenue()
        
        return jsonify({
            'top_products': top_products,
            'monthly_revenue': monthly_revenue
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    return {"status": "healthy"}, 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)