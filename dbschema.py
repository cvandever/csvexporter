import logging
import os
import psycopg2
from datetime import datetime

def get_db_connection():
    """Get a database connection with proper error handling."""
    try:
        conn = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            database=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            sslmode=os.environ.get("POSTGRES_SSL_MODE", "require")
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise

def create_tables():
    """Create only the essential tables we need."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Simple users table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(100) UNIQUE NOT NULL,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(255),
                user_type VARCHAR(50),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Simple purchases table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(50) UNIQUE NOT NULL,
                user_email VARCHAR(255),
                product_name VARCHAR(255),
                quantity INT,
                total_price DECIMAL(10, 2),
                purchase_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Processing log table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processing_log (
                id SERIAL PRIMARY KEY,
                file_name VARCHAR(255),
                records_processed INT,
                status VARCHAR(50),
                error_message TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Basic indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_user_id ON users (user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_purchases_transaction_id ON purchases (transaction_id)")
        
        conn.commit()
        logging.info("Database tables created successfully")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error creating tables: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()