import logging
import os
import psycopg2
from psycopg2 import sql

def ensure_database_schema():
    """
    Ensures that the required database tables exist.
    Creates them if they don't exist with proper constraints.
    """
    conn = None
    cursor = None
    try:
        # Get database connection info from app settings
        db_host = os.environ["POSTGRES_HOST"]
        db_name = os.environ["POSTGRES_DB"]
        db_user = os.environ["POSTGRES_USER"]
        db_password = os.environ["POSTGRES_PASSWORD"]
        ssl_mode = os.environ.get("POSTGRES_SSL_MODE", "require")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            sslmode=ssl_mode
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
            generated_at TIMESTAMP,
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
            file_id VARCHAR(100),
            file_name VARCHAR(255),
            file_type VARCHAR(20),
            analytics JSONB,
            processed_at TIMESTAMP
        );
        """
        cursor.execute(analytics_table_query)
        
        # Add unique constraints separately (safer approach)
        try:
            cursor.execute("ALTER TABLE users ADD CONSTRAINT users_user_id_unique UNIQUE (user_id);")
            logging.info("Added unique constraint to users.user_id")
        except psycopg2.Error as e:
            if "already exists" in str(e) or "duplicate key" in str(e):
                logging.debug("Unique constraint on users.user_id already exists")
            else:
                logging.warning(f"Could not add unique constraint to users.user_id: {e}")
        
        try:
            cursor.execute("ALTER TABLE purchases ADD CONSTRAINT purchases_transaction_id_unique UNIQUE (transaction_id);")
            logging.info("Added unique constraint to purchases.transaction_id")
        except psycopg2.Error as e:
            if "already exists" in str(e) or "duplicate key" in str(e):
                logging.debug("Unique constraint on purchases.transaction_id already exists")
            else:
                logging.warning(f"Could not add unique constraint to purchases.transaction_id: {e}")
        
        try:
            cursor.execute("ALTER TABLE file_analytics ADD CONSTRAINT file_analytics_file_id_unique UNIQUE (file_id);")
            logging.info("Added unique constraint to file_analytics.file_id")
        except psycopg2.Error as e:
            if "already exists" in str(e) or "duplicate key" in str(e):
                logging.debug("Unique constraint on file_analytics.file_id already exists")
            else:
                logging.warning(f"Could not add unique constraint to file_analytics.file_id: {e}")
        
        # Create indexes for better performance
        indexes = [
            ("idx_users_user_id", "users", "user_id"),
            ("idx_users_email", "users", "email"),
            ("idx_purchases_transaction_id", "purchases", "transaction_id"),
            ("idx_purchases_user_email", "purchases", "user_email"),
            ("idx_purchases_date", "purchases", "year, month"),
            ("idx_file_analytics_file_id", "file_analytics", "file_id")
        ]
        
        for index_name, table_name, columns in indexes:
            try:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns});")
                logging.debug(f"Created index {index_name} on {table_name}({columns})")
            except psycopg2.Error as e:
                logging.warning(f"Could not create index {index_name}: {e}")
        
        logging.info("Database schema validation complete.")
        return True
        
    except Exception as e:
        logging.error(f"Error ensuring database schema: {str(e)}")
        raise
    finally:
        # Close cursor and connection in finally block
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def check_table_constraints(table_name: str) -> dict:
    """Check what constraints exist on a table for debugging."""
    try:
        # Get database connection info from app settings
        db_host = os.environ["POSTGRES_HOST"]
        db_name = os.environ["POSTGRES_DB"]
        db_user = os.environ["POSTGRES_USER"]
        db_password = os.environ["POSTGRES_PASSWORD"]
        ssl_mode = os.environ.get("POSTGRES_SSL_MODE", "require")
        
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            sslmode=ssl_mode
        )
        
        cursor = conn.cursor()
        
        # Query to get table constraints
        constraint_query = """
        SELECT 
            tc.constraint_name, 
            tc.constraint_type,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu 
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_name = %s 
            AND tc.table_schema = 'public'
        ORDER BY tc.constraint_name;
        """
        
        cursor.execute(constraint_query, (table_name,))
        constraints = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        constraint_info = {}
        for constraint_name, constraint_type, column_name in constraints:
            if constraint_type not in constraint_info:
                constraint_info[constraint_type] = []
            constraint_info[constraint_type].append({
                'name': constraint_name,
                'column': column_name
            })
        
        return constraint_info
        
    except Exception as e:
        logging.error(f"Error checking table constraints for {table_name}: {str(e)}")
        return {}