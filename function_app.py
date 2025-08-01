import azure.functions as func
import logging
import csv
import io
import json
import pandas as pd
from datetime import datetime
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict, Any
from retrying import retry
from dbschema import ensure_database_schema

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

SOURCE_CONTAINER = os.environ.get("BLOB_CONTAINER")
PROCESSED_CONTAINER = os.environ.get("BLOB_PROCESSED")

# Initialize schema on startup
try:
    ensure_database_schema()
    logging.info("Database schema validated successfully on startup")
except Exception as e:
    logging.error(f"Error validating database schema on startup: {str(e)}")


@app.blob_trigger(arg_name="myblob", 
                 path="csv-purchases/{name}",
                 connection="AzureWebJobsStorage")
def process_csv_blob(myblob: func.InputStream):
    blob_name = myblob.name.split('/')[-1]  # Get just the filename without path
    
    logging.info(f"Processing blob: {blob_name} from container: {SOURCE_CONTAINER}")
    
    try:
        # Read the blob content as text
        blob_text = myblob.read().decode('utf-8')
        
        # Determine file type based on filename
        file_type = "users" if "user_data" in blob_name else "purchases" if "purchase_data" in blob_name else "unknown"
        
        if file_type == "unknown":
            logging.warning(f"Unknown file type for {blob_name}. Skipping processing.")
            return
            
        # Parse the CSV data according to file type
        if file_type == "users":
            data_records = process_csv_data(blob_text, file_type="users")
            file_metadata = extract_file_metadata(blob_name, file_type="users")
        else:  # purchases
            data_records = process_csv_data(blob_text, file_type="purchases")
            file_metadata = extract_file_metadata(blob_name, file_type="purchases")
        
        if not data_records:
            logging.warning(f"No valid data found in {blob_name}. Skipping processing.")
            return
            
        # Generate analytics
        analytics = generate_analytics(data_records, file_metadata, file_type)
        
        # Log processing statistics
        logging.info(f"Processed {len(data_records)} records from {blob_name}")
        
        # Save data to PostgreSQL database
        save_to_database(data_records, analytics, file_metadata, file_type)
        
        # Move the processed blob to the destination container
        move_blob_to_processed(blob_name)
        
        logging.info(f"Successfully processed {blob_name} and moved to {PROCESSED_CONTAINER}")
        
    except Exception as e:
        logging.error(f"Error processing {blob_name}: {str(e)}")
        raise


@retry(stop_max_attempt_number=3, wait_fixed=1000)
def process_csv_data(csv_data: str, file_type: str = "users") -> List[Dict[str, Any]]:
    if not csv_data.strip():
        logging.warning(f"Received empty CSV data for {file_type}")
        return []
        
    try:
        # Parse CSV
        reader = csv.DictReader(io.StringIO(csv_data))
        
        # Convert to list of dictionaries and parse complex fields
        records = []
        for row in reader:
            # For user data, parse JSON fields
            if file_type == "users":
                for field in ['reviews', 'in_cart', 'wishlist']:
                    if field in row and row[field]:
                        try:
                            row[field] = json.loads(row[field])
                        except json.JSONDecodeError:
                            # If JSON parsing fails, keep as string
                            pass
                
                # Convert boolean strings to actual booleans
                if 'is_active' in row:
                    row['is_active'] = row['is_active'].lower() == 'true'
                
                # Convert numeric fields
                for field in ['purchase_count', 'total_spent']:
                    if field in row and row[field]:
                        try:
                            if field == 'purchase_count':
                                row[field] = int(row[field])
                            else:
                                row[field] = float(row[field])
                        except (ValueError, TypeError):
                            # If conversion fails, keep as string
                            pass
            
            # For purchase data, convert numeric fields
            elif file_type == "purchases":
                numeric_fields = ['quantity', 'unit_price', 'discount_percent', 
                                 'discount_amount', 'shipping_cost', 'total_price',
                                 'month', 'year']
                for field in numeric_fields:
                    if field in row and row[field]:
                        try:
                            if field in ['quantity', 'discount_percent', 'month', 'year']:
                                row[field] = int(row[field])
                            else:
                                row[field] = float(row[field])
                        except (ValueError, TypeError):
                            # If conversion fails, keep as string
                            pass
            
            records.append(row)
        
        return records
    except Exception as e:
        logging.error(f"Error processing CSV data for {file_type}: {str(e)}")
        raise

def extract_file_metadata(filename: str, file_type: str = "users") -> Dict[str, Any]:
    try:
        # Extract just the filename without path
        basename = filename.split('/')[-1]
        
        # Parse year and month from filename pattern
        parts = basename.split('_')
        if len(parts) >= 4:
            year = int(parts[2])
            month = int(parts[3].split('.')[0])
            
            return {
                "year": year,
                "month": month,
                "period": f"{year}-{month:02d}",
                "filename": basename,
                "file_type": file_type
            }
    except (IndexError, ValueError) as e:
        logging.warning(f"Could not parse metadata from filename {filename}: {str(e)}")
    
    # Default metadata if parsing fails
    return {
        "filename": filename.split('/')[-1],
        "processed_time": datetime.now().isoformat(),
        "file_type": file_type
    }

def generate_analytics(data: List[Dict[str, Any]], metadata: Dict[str, Any], file_type: str) -> Dict[str, Any]:
    analytics = {
        "record_count": len(data),
        "file_type": file_type,
        "period": metadata.get("period", "unknown"),
        "processed_at": datetime.now().isoformat()
    }
    
    # Skip empty data
    if not data:
        return analytics
    
    # Convert to pandas DataFrame for easier analysis
    df = pd.DataFrame(data)
    
    # User data analytics
    if file_type == "users":
        # User type distribution
        if 'user_type' in df.columns:
            analytics["user_type_distribution"] = df['user_type'].value_counts().to_dict()
        
        # Active users percentage
        if 'is_active' in df.columns:
            active_count = df['is_active'].sum() if isinstance(df['is_active'].iloc[0], bool) else df['is_active'].apply(lambda x: x == 'true' or x == 'True').sum()
            analytics["active_users_percent"] = round((active_count / len(df)) * 100, 2)
        
        # Device distribution
        if 'last_device' in df.columns:
            analytics["device_distribution"] = df['last_device'].value_counts().to_dict()
        
        # Browser distribution
        if 'last_browser' in df.columns:
            analytics["browser_distribution"] = df['last_browser'].value_counts().to_dict()
    
    # Purchase data analytics
    elif file_type == "purchases":
        # Total sales
        if 'total_price' in df.columns:
            analytics["total_sales"] = float(df['total_price'].sum())
        
        # Average order value
        if 'total_price' in df.columns:
            analytics["average_order_value"] = float(df['total_price'].mean())
        
        # Product category distribution
        if 'product_category' in df.columns:
            analytics["category_distribution"] = df['product_category'].value_counts().to_dict()
        
        # Payment method distribution
        if 'payment_method' in df.columns:
            analytics["payment_method_distribution"] = df['payment_method'].value_counts().to_dict()
        
        # Discount analytics
        if 'discount_percent' in df.columns and 'discount_amount' in df.columns:
            analytics["discount_usage_percent"] = round((df['discount_percent'] > 0).sum() / len(df) * 100, 2)
            analytics["total_discount_amount"] = float(df['discount_amount'].sum())
    
    return analytics

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def move_blob_to_processed(blob_name: str):
    try:
        # Get connection string from app settings
        connection_string = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get source blob client
        source_blob_client = blob_service_client.get_blob_client(
            container=SOURCE_CONTAINER, 
            blob=blob_name
        )
        
        # Ensure destination container exists
        dest_container_client = blob_service_client.get_container_client(PROCESSED_CONTAINER)
        try:
            dest_container_client.create_container()
            logging.info(f"Created destination container: {PROCESSED_CONTAINER}")
        except ResourceExistsError:
            # Container already exists, which is fine
            pass
        
        # Get destination blob client
        dest_blob_client = blob_service_client.get_blob_client(
            container=PROCESSED_CONTAINER,
            blob=blob_name
        )
        
        # Copy blob data
        source_blob_data = source_blob_client.download_blob().readall()
        dest_blob_client.upload_blob(source_blob_data, overwrite=True)
        logging.info(f"Copied {blob_name} to {PROCESSED_CONTAINER}")
        
        # Delete source blob after successful copy
        source_blob_client.delete_blob()
        logging.info(f"Deleted {blob_name} from {SOURCE_CONTAINER}")
        
    except ResourceNotFoundError as e:
        logging.error(f"Blob or container not found: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Error moving blob {blob_name}: {str(e)}")
        raise

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def save_to_database(data: List[Dict[str, Any]], analytics: Dict[str, Any], file_metadata: Dict[str, Any], file_type: str):
    if not data:
        logging.warning(f"No data to save to database for {file_type}")
        return
        
    try:
        # Get database connection info from app settings
        db_host = os.environ["POSTGRES_HOST"]
        db_name = os.environ["POSTGRES_DB"]
        db_user = os.environ["POSTGRES_USER"]
        db_password = os.environ["POSTGRES_PASSWORD"]
        ssl_mode = os.environ.get("POSTGRES_SSL_MODE", "require")
        
        # Connect to PostgreSQL with SSL mode
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            sslmode=ssl_mode
        )
        
        # Create cursor
        cursor = conn.cursor()
        
        # Prepare batch insert based on file type
        if file_type == "users":
            table_name = "users"
        else:  # purchases
            table_name = "purchases"
        
        # Get column names from the data
        columns = list(data[0].keys())
        column_str = ", ".join([f'"{col}"' for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Prepare insert query
        insert_query = f'INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})'
        
        # Prepare batch data
        batch_data = []
        for record in data:
            row_values = []
            for col in columns:
                value = record.get(col)
                # Convert JSON fields to strings for database
                if isinstance(value, (list, dict)):
                    value = json.dumps(value)
                row_values.append(value)
            batch_data.append(tuple(row_values))
        
        # Execute batch insert
        execute_batch(cursor, insert_query, batch_data, page_size=100)
        
        # Save file analytics
        file_id = f"{file_metadata.get('year', 0)}_{file_metadata.get('month', 0)}_{file_type}"
        if 'year' not in file_metadata or 'month' not in file_metadata:
            file_id = f"unknown_{datetime.now().strftime('%Y%m%d%H%M%S')}_{file_type}"
            
        analytics_json = json.dumps(analytics)
        analytics_query = """
        INSERT INTO file_analytics (file_id, file_name, file_type, analytics, processed_at) 
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(analytics_query, (
            file_id, 
            file_metadata.get('filename', 'unknown'),
            file_type,
            analytics_json, 
            datetime.now()
        ))
        
        # Commit transaction
        conn.commit()
        
        # Close connection
        cursor.close()
        conn.close()
        
        logging.info(f"Successfully saved {len(data)} {file_type} records to database")
    except Exception as e:
        logging.error(f"Error saving {file_type} data to database: {str(e)}")
        raise