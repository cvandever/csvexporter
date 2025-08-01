import azure.functions as func
import logging
import csv
import io
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import psycopg2
from dbschema import get_db_connection, create_tables

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Initialize database on startup
try:
    create_tables()
    logging.info("Database initialization complete")
except Exception as e:
    logging.error(f"Database initialization failed: {str(e)}")

@app.blob_trigger(arg_name="myblob", 
                 path="csv-purchases/{name}",
                 connection="AzureWebJobsStorage")
def process_csv_simple(myblob: func.InputStream):
    """Simplified CSV processing function."""
    
    blob_name = myblob.name.split('/')[-1]
    logging.info(f"Processing blob: {blob_name}")
    
    try:
        # Read blob content
        content = myblob.read().decode('utf-8')
        
        # Determine file type
        if 'user' in blob_name.lower():
            process_users_csv(content, blob_name)
        elif 'purchase' in blob_name.lower():
            process_purchases_csv(content, blob_name)
        else:
            logging.warning(f"Unknown file type: {blob_name}")
            return
            
        # Move to processed container
        move_blob_to_processed(blob_name)
        
        # Log success
        log_processing_result(blob_name, "SUCCESS", None)
        
    except Exception as e:
        logging.error(f"Error processing {blob_name}: {str(e)}")
        log_processing_result(blob_name, "ERROR", str(e))

def process_users_csv(content: str, filename: str):
    """Process users CSV data."""
    reader = csv.DictReader(io.StringIO(content))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    processed_count = 0
    
    try:
        for row in reader:
            # Insert user data (simplified)
            cursor.execute("""
                INSERT INTO users (user_id, first_name, last_name, email, user_type, is_active)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    user_type = EXCLUDED.user_type,
                    is_active = EXCLUDED.is_active
            """, (
                row.get('user_id'),
                row.get('first_name'),
                row.get('last_name'),
                row.get('email'),
                row.get('user_type'),
                row.get('is_active', 'true').lower() == 'true'
            ))
            processed_count += 1
        
        conn.commit()
        logging.info(f"Processed {processed_count} users from {filename}")
        
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

def process_purchases_csv(content: str, filename: str):
    """Process purchases CSV data."""
    reader = csv.DictReader(io.StringIO(content))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    processed_count = 0
    
    try:
        for row in reader:
            # Convert date if needed
            purchase_date = row.get('purchase_date')
            if purchase_date:
                try:
                    purchase_date = datetime.strptime(purchase_date, '%Y-%m-%d').date()
                except:
                    purchase_date = None
            
            # Insert purchase data (simplified)
            cursor.execute("""
                INSERT INTO purchases (transaction_id, user_email, product_name, quantity, total_price, purchase_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO UPDATE SET
                    user_email = EXCLUDED.user_email,
                    product_name = EXCLUDED.product_name,
                    quantity = EXCLUDED.quantity,
                    total_price = EXCLUDED.total_price,
                    purchase_date = EXCLUDED.purchase_date
            """, (
                row.get('transaction_id'),
                row.get('user_email'),
                row.get('product_name'),
                int(row.get('quantity', 0)) if row.get('quantity') else None,
                float(row.get('total_price', 0)) if row.get('total_price') else None,
                purchase_date
            ))
            processed_count += 1
        
        conn.commit()
        logging.info(f"Processed {processed_count} purchases from {filename}")
        
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

def move_blob_to_processed(blob_name: str):
    """Simple blob move operation."""
    try:
        blob_service = BlobServiceClient.from_connection_string(
            os.environ["AzureWebJobsStorage"]
        )
        
        # Copy to processed container
        source_blob = blob_service.get_blob_client("csv-purchases", blob_name)
        dest_blob = blob_service.get_blob_client("processed-csv", blob_name)
        
        # Ensure processed container exists
        try:
            blob_service.create_container("processed-csv")
        except:
            pass  # Container might already exist
        
        # Copy blob
        blob_data = source_blob.download_blob().readall()
        dest_blob.upload_blob(blob_data, overwrite=True)
        
        # Delete source
        source_blob.delete_blob()
        
        logging.info(f"Moved {blob_name} to processed container")
        
    except Exception as e:
        logging.error(f"Error moving blob {blob_name}: {str(e)}")
        # Don't raise - this is not critical

def log_processing_result(filename: str, status: str, error_message: str):
    """Log processing results to database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO processing_log (file_name, status, error_message)
            VALUES (%s, %s, %s)
        """, (filename, status, error_message))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error logging result: {str(e)}")

# Test endpoint to verify function is working
@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS, methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Simple health check endpoint."""
    try:
        # Test database connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        
        return func.HttpResponse("Function app is healthy", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Health check failed: {str(e)}", status_code=500)