import azure.functions as func
import logging
import csv
import io
import json
import pandas as pd
from datetime import datetime
import os
from azure.storage.blob import BlobServiceClient
import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict, Any
from dbschema import ensure_database_schema

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Blob trigger function - automatically processes new files
@app.blob_trigger(arg_name="myblob", 
                 path="csv-purchases/{name}",
                 connection="AzureWebJobsStorage")
def process_csv_blob(myblob: func.InputStream):
    """
    Process user data CSV files when they are added to the storage account.
    
    Args:
        myblob: The blob that triggered the function
    """
    logging.info(f"Python blob trigger function processed blob: {myblob.name}")
    
    try:
        # Read the blob content as text
        blob_text = myblob.read().decode('utf-8')
        
        # Parse the CSV data
        user_data = process_csv_data(blob_text)
        
        # Get file metadata from the filename (e.g., year and month)
        file_metadata = extract_file_metadata(myblob.name)
        
        # Generate analytics (optional)
        analytics = generate_analytics(user_data, file_metadata)
        
        # Log some basic statistics
        logging.info(f"Processed {len(user_data)} user records from {myblob.name}")
        logging.info(f"Analytics: {json.dumps(analytics)}")
        
        # Save data to PostgreSQL database
        save_to_database(user_data, analytics, file_metadata)
        
        # Move the processed blob to the processed container
        move_blob_to_processed(myblob.name)
        
    except Exception as e:
        logging.error(f"Error processing {myblob.name}: {str(e)}")
        raise

# HTTP trigger function - allows manual processing of files
@app.route(route="process_file")
def process_file(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to manually process a specific file.
    
    Args:
        req: HTTP request with filename parameter
    
    Returns:
        HTTP response with processing results
    """
    logging.info('Python HTTP trigger function processed a request.')
    
    filename = req.params.get('filename')
    if not filename:
        try:
            req_body = req.get_json()
            filename = req_body.get('filename')
        except ValueError:
            pass
    
    if not filename:
        return func.HttpResponse(
            "Please pass a filename parameter in the query string or request body",
            status_code=400
        )
    
    try:
        # Connect to blob storage
        connection_string = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client("userdata")
        blob_client = container_client.get_blob_client(filename)
        
        # Download blob content
        blob_data = blob_client.download_blob().readall().decode('utf-8')
        
        # Process CSV data
        user_data = process_csv_data(blob_data)
        file_metadata = extract_file_metadata(filename)
        analytics = generate_analytics(user_data, file_metadata)
        
        return func.HttpResponse(
            json.dumps({
                "filename": filename,
                "records_processed": len(user_data),
                "analytics": analytics
            }),
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error processing {filename}: {str(e)}")
        return func.HttpResponse(
            f"Error processing file: {str(e)}",
            status_code=500
        )

# Helper functions
def process_csv_data(csv_data: str) -> List[Dict[str, Any]]:
    """
    Process CSV data into structured user records.
    
    Args:
        csv_data: String containing CSV data
    
    Returns:
        List of dictionaries containing user data
    """
    # Parse CSV
    reader = csv.DictReader(io.StringIO(csv_data))
    
    # Convert to list of dictionaries and parse complex fields
    user_records = []
    for row in reader:
        # Parse JSON fields
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
            
        user_records.append(row)
    
    return user_records

def extract_file_metadata(filename: str) -> Dict[str, Any]:
    """
    Extract metadata from filename (e.g., user_data_2023_01.csv).
    
    Args:
        filename: The blob filename
    
    Returns:
        Dictionary with file metadata
    """
    try:
        # Extract just the filename without path
        basename = filename.split('/')[-1]
        
        # Parse year and month from filename pattern user_data_YYYY_MM.csv
        parts = basename.split('_')
        if len(parts) >= 4 and parts[0] == 'user' and parts[1] == 'data':
            year = int(parts[2])
            month = int(parts[3].split('.')[0])
            
            return {
                "year": year,
                "month": month,
                "period": f"{year}-{month:02d}",
                "filename": basename
            }
    except (IndexError, ValueError) as e:
        logging.warning(f"Could not parse metadata from filename {filename}: {str(e)}")
    
    # Default metadata if parsing fails
    return {
        "filename": filename.split('/')[-1],
        "processed_time": datetime.now().isoformat()
    }

def generate_analytics(user_data: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate basic analytics from user data.
    
    Args:
        user_data: List of user records
        metadata: File metadata
    
    Returns:
        Dictionary with analytics results
    """
    # Use pandas for easier data analysis
    if not user_data:
        return {"error": "No data to analyze"}
    
    df = pd.DataFrame(user_data)
    
    analytics = {
        "total_records": len(df),
        "period": metadata.get("period", "unknown"),
    }
    
    # User type distribution
    if 'user_type' in df.columns:
        user_type_counts = df['user_type'].value_counts().to_dict()
        analytics["user_type_distribution"] = user_type_counts
    
    # Active users percentage
    if 'is_active' in df.columns:
        active_percent = (df['is_active'].sum() / len(df)) * 100
        analytics["active_users_percent"] = round(active_percent, 2)
    
    # Device distribution
    if 'last_device' in df.columns:
        device_counts = df['last_device'].value_counts().to_dict()
        analytics["device_distribution"] = device_counts
    
    # Most common products in cart/wishlist
    if 'in_cart' in df.columns and isinstance(df['in_cart'].iloc[0], list):
        # Flatten all cart items into a single list
        all_cart_items = [item for sublist in df['in_cart'].dropna() for item in sublist]
        if all_cart_items:
            # Count occurrences of each product
            from collections import Counter
            top_products = Counter(all_cart_items).most_common(5)
            analytics["top_cart_products"] = dict(top_products)
    
    return analytics

def move_blob_to_processed(blob_name: str):
    """
    Move a blob from csv-purchases to csv-processed container.
    
    Args:
        blob_name: Name of the blob to move
    """
    try:
        # Get connection string from app settings
        connection_string = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get source blob
        source_container = "csv-purchases"
        source_blob_client = blob_service_client.get_blob_client(container=source_container, blob=blob_name)
        
        # Get destination blob
        dest_container = "csv-processed"
        dest_container_client = blob_service_client.get_container_client(dest_container)
        
        # Create destination container if it doesn't exist
        try:
            dest_container_client.create_container()
        except:
            # Container already exists
            pass
        
        # Copy source to destination
        dest_blob_client = blob_service_client.get_blob_client(container=dest_container, blob=blob_name)
        source_blob_data = source_blob_client.download_blob().readall()
        dest_blob_client.upload_blob(source_blob_data, overwrite=True)
        
        # Delete source blob
        source_blob_client.delete_blob()
        
        logging.info(f"Successfully moved {blob_name} to processed container")
    except Exception as e:
        logging.error(f"Error moving blob {blob_name}: {str(e)}")
        raise

def save_to_database(user_data: List[Dict[str, Any]], analytics: Dict[str, Any], file_metadata: Dict[str, Any]):
    """
    Save processed user data to PostgreSQL database.
    
    Args:
        user_data: List of user records
        analytics: Generated analytics
        file_metadata: File metadata
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
            password=db_password
        )
        
        # Create cursor
        cursor = conn.cursor()
        
        # Prepare batch insert for users table
        users_columns = ", ".join(user_data[0].keys())
        users_placeholders = ", ".join(["%s"] * len(user_data[0].keys()))
        users_insert_query = f"INSERT INTO users ({users_columns}) VALUES ({users_placeholders})"
        
        # Convert complex fields (JSON) to strings for database
        batch_data = []
        for user in user_data:
            row_data = []
            for key, value in user.items():
                if isinstance(value, (list, dict)):
                    row_data.append(json.dumps(value))
                else:
                    row_data.append(value)
            batch_data.append(tuple(row_data))
        
        # Execute batch insert
        execute_batch(cursor, users_insert_query, batch_data)
        
        # Save file analytics
        file_id = f"{file_metadata.get('year', 0)}_{file_metadata.get('month', 0)}"
        analytics_json = json.dumps(analytics)
        analytics_query = "INSERT INTO file_analytics (file_id, file_name, analytics, processed_at) VALUES (%s, %s, %s, %s)"
        cursor.execute(analytics_query, (file_id, file_metadata.get('filename'), analytics_json, datetime.now()))
        
        # Commit transaction
        conn.commit()
        
        # Close connection
        cursor.close()
        conn.close()
        
        logging.info(f"Successfully saved {len(user_data)} records to database")
    except Exception as e:
        logging.error(f"Error saving to database: {str(e)}")
        raise
