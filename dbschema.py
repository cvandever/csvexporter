import logging
import os
import psycopg2
from psycopg2 import sql

def ensure_database_schema():
    """
    Ensures that the required database tables exist.
    Creates them if they don't exist.
    """
    try:
        # Get database connection info from app settings
        db_host = os.environ["POSTGRES_HOST"]
        db_name = os.environ["POSTGRES_DB"]
        db_user = os.environ["POSTGRES_USER"]
        db_password = os.environ["POSTGRES_PASSWORD"]
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            sslmode=os.environ.get("POSTGRES_SSL_MODE", "require")
        )
        
        # Set autocommit
        conn.autocommit = True
        
        # Create cursor
        cursor = conn.cursor()
        
        # Create users table if it doesn't exist
        users_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(100),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(255),
            password_hash VARCHAR(255),
            phone_number VARCHAR(20),
            date_of_birth VARCHAR(20),
            time_on_app TEXT,
            user_type VARCHAR(50),
            is_active BOOLEAN,
            last_payment_method VARCHAR(50),
            reviews JSONB,
            last_ip VARCHAR(50),
            last_coordinates TEXT,
            last_device VARCHAR(50),
            last_browser VARCHAR(50),
            last_os VARCHAR(50),
            last_login TEXT,
            last_logout TEXT,
            in_cart JSONB,
            wishlist JSONB,
            last_search TEXT,
            created_date VARCHAR(20),
            purchase_count INT,
            total_spent DECIMAL(12, 2),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(users_table_query)
        
        # Create purchases table if it doesn't exist
        purchases_table_query = """
        CREATE TABLE IF NOT EXISTS purchases (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(20),
            user_email VARCHAR(255),
            product_name VARCHAR(100),
            product_category VARCHAR(50),
            quantity INT,
            unit_price DECIMAL(10, 2),
            discount_percent INT,
            discount_amount DECIMAL(10, 2),
            shipping_cost DECIMAL(10, 2),
            total_price DECIMAL(10, 2),
            purchase_date VARCHAR(20),
            purchase_time VARCHAR(20),
            payment_method VARCHAR(50),
            purchase_status VARCHAR(20),
            month INT,
            year INT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(purchases_table_query)
        
        # Create file_analytics table if it doesn't exist
        analytics_table_query = """
        CREATE TABLE IF NOT EXISTS file_analytics (
            id SERIAL PRIMARY KEY,
            file_id VARCHAR(50),
            file_name VARCHAR(255),
            file_type VARCHAR(20),
            analytics JSONB,
            processed_at TIMESTAMP
        );
        """
        cursor.execute(analytics_table_query)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_user_id ON users (user_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_purchases_transaction_id ON purchases (transaction_id);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_purchases_user_email ON purchases (user_email);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_purchases_date ON purchases (year, month);")
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        
        logging.info("Database schema validation complete.")
        return True
    except Exception as e:
        logging.error(f"Error ensuring database schema: {str(e)}")
        raise