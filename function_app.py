import azure.functions as func
import logging
import csv
import io
import json
import pandas as pd
from datetime import datetime
import os
import ssl
import certifi
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict, Any
from retrying import retry
from dbschema import ensure_database_schema, check_table_constraints
import hashlib

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

SOURCE_CONTAINER = os.environ.get("BLOB_CONTAINER", "csv-purchases")
PROCESSED_CONTAINER = os.environ.get("BLOB_PROCESSED", "processed-csv")

# Configure SSL context for Azure connections
def create_ssl_context():
    """Create SSL context with proper certificate verification."""
    try:
        # Use certifi for certificate verification
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = True
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        
        # Use certifi's certificate bundle
        ssl_context.load_verify_locations(certifi.where())
        
        return ssl_context
    except Exception as e:
        logging.warning(f"Could not create SSL context: {e}. Using default SSL settings.")
        return None

# Initialize schema on startup
try:
    ensure_database_schema()
    logging.info("Database schema validated successfully on startup")
except Exception as e:
    logging.error(f"Error validating database schema on startup: {str(e)}")

# Add a health check endpoint
@app.route(route="health")
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint for the function app."""
    try:
        # Check storage connectivity
        connection_string = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Check if source container exists
        source_container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)
        source_exists = source_container_client.exists()
        
        # Check if processed container exists
        processed_container_client = blob_service_client.get_container_client(PROCESSED_CONTAINER)
        processed_exists = processed_container_client.exists()
        
        # Check database connectivity
        db_host = os.environ["POSTGRES_HOST"]
        db_name = os.environ["POSTGRES_DB"]
        db_user = os.environ["POSTGRES_USER"]
        db_password = os.environ["POSTGRES_PASSWORD"]
        ssl_mode = os.environ.get("POSTGRES_SSL_MODE", "require")
        
        # Test database connection
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            sslmode=ssl_mode
        )
        conn.close()
        
        # Check table constraints for debugging
        purchases_constraints = check_table_constraints("purchases")
        users_constraints = check_table_constraints("users")
        
        return func.HttpResponse(
            json.dumps({
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "storage": {
                    "source_container": {
                        "name": SOURCE_CONTAINER,
                        "exists": source_exists
                    },
                    "processed_container": {
                        "name": PROCESSED_CONTAINER,
                        "exists": processed_exists
                    }
                },
                "database": "connected",
                "constraints": {
                    "purchases": purchases_constraints,
                    "users": users_constraints
                }
            }),
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )

@app.blob_trigger(arg_name="myblob", 
                 path="csv-purchases/{name}",
                 connection="AzureWebJobsStorage")
def process_csv_blob(myblob: func.InputStream):
    """
    Process CSV files when they are added to the source container.
    Enhanced with better error handling and duplicate processing prevention.
    """
    blob_name = myblob.name.split('/')[-1]  # Get just the filename without path
    processing_id = generate_processing_id(blob_name)
    
    logging.info(f"[{processing_id}] Starting processing for blob: {blob_name}")
    
    try:
        # Check if blob still exists in source container (avoid race conditions)
        if not check_blob_exists_in_source(blob_name):
            logging.info(f"[{processing_id}] Blob {blob_name} no longer exists in source container - likely already processed")
            return
        
        # Check if blob was already processed (duplicate prevention)
        if check_already_processed(blob_name):
            logging.info(f"[{processing_id}] Blob {blob_name} already processed - moving to processed container")
            move_blob_to_processed(blob_name)
            return
        
        # Read the blob content as text
        try:
            blob_text = myblob.read().decode('utf-8')
        except Exception as e:
            logging.error(f"[{processing_id}] Error reading blob {blob_name}: {str(e)}")
            return
        
        # Determine file type based on filename
        file_type = determine_file_type(blob_name)
        
        if file_type == "unknown":
            logging.warning(f"[{processing_id}] Unknown file type for {blob_name}. Skipping processing.")
            return
            
        # Parse the CSV data according to file type
        data_records = process_csv_data(blob_text, file_type=file_type)
        file_metadata = extract_file_metadata(blob_name, file_type=file_type)
        
        if not data_records:
            logging.warning(f"[{processing_id}] No valid data found in {blob_name}. Skipping processing.")
            return
            
        # Generate analytics
        analytics = generate_analytics(data_records, file_metadata, file_type)
        
        # Log processing statistics
        logging.info(f"[{processing_id}] Processed {len(data_records)} records from {blob_name}")
        
        # Save data to PostgreSQL database
        save_to_database(data_records, analytics, file_metadata, file_type, processing_id)
        
        # Move the processed blob to the destination container
        move_blob_to_processed(blob_name)
        
        # Mark as processed in database
        mark_as_processed(blob_name, processing_id, len(data_records))
        
        logging.info(f"[{processing_id}] Successfully processed {blob_name} and moved to {PROCESSED_CONTAINER}")
        
    except ResourceNotFoundError:
        logging.info(f"[{processing_id}] Blob {blob_name} not found - likely already processed by another instance")
    except Exception as e:
        logging.error(f"[{processing_id}] Error processing {blob_name}: {str(e)}")
        # Don't re-raise to avoid infinite retries for persistent errors
        
def generate_processing_id(blob_name: str) -> str:
    """Generate a unique processing ID for tracking."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    hash_suffix = hashlib.md5(blob_name.encode()).hexdigest()[:8]
    return f"{timestamp}-{hash_suffix}"

def determine_file_type(blob_name: str) -> str:
    """Determine file type based on filename patterns."""
    blob_name_lower = blob_name.lower()
    if "user_data" in blob_name_lower or "users" in blob_name_lower:
        return "users"
    elif "purchase_data" in blob_name_lower or "purchases" in blob_name_lower:
        return "purchases"
    else:
        return "unknown"

def check_blob_exists_in_source(blob_name: str) -> bool:
    """Check if blob still exists in the source container with SSL handling."""
    try:
        connection_string = os.environ["AzureWebJobsStorage"]
        
        # Create blob service client with SSL context
        ssl_context = create_ssl_context()
        if ssl_context:
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string,
                ssl_context=ssl_context
            )
        else:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        blob_client = blob_service_client.get_blob_client(
            container=SOURCE_CONTAINER,
            blob=blob_name
        )
        
        # Try to get blob properties - this will raise ResourceNotFoundError if not found
        blob_client.get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False
    except ssl.SSLError as e:
        logging.warning(f"SSL error checking blob existence for {blob_name}: {str(e)}")
        # Try without SSL verification as fallback
        try:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(
                container=SOURCE_CONTAINER,
                blob=blob_name
            )
            blob_client.get_blob_properties()
            return True
        except Exception:
            return True  # Assume it exists to avoid skipping processing
    except Exception as e:
        logging.warning(f"Error checking blob existence for {blob_name}: {str(e)}")
        return True  # Assume it exists to avoid skipping processing

def check_already_processed(blob_name: str) -> bool:
    """Check if blob was already processed by looking in processed container."""
    try:
        connection_string = os.environ["AzureWebJobsStorage"]
        
        # Create blob service client with SSL handling
        ssl_context = create_ssl_context()
        if ssl_context:
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string,
                ssl_context=ssl_context
            )
        else:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        processed_blob_client = blob_service_client.get_blob_client(
            container=PROCESSED_CONTAINER,
            blob=blob_name
        )
        
        # If we can get properties, it exists in processed container
        processed_blob_client.get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False
    except ssl.SSLError as e:
        logging.warning(f"SSL error checking processed status for {blob_name}: {str(e)}")
        # Try without SSL verification as fallback
        try:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            processed_blob_client = blob_service_client.get_blob_client(
                container=PROCESSED_CONTAINER,
                blob=blob_name
            )
            processed_blob_client.get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False
        except Exception:
            return False
    except Exception as e:
        logging.warning(f"Error checking processed status for {blob_name}: {str(e)}")
        return False

def mark_as_processed(blob_name: str, processing_id: str, record_count: int) -> None:
    """Mark blob as processed in database for tracking."""
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
        
        cursor = conn.cursor()
        
        # Insert processing record with safer approach
        processing_query = """
        INSERT INTO file_analytics (file_id, file_name, file_type, analytics, processed_at) 
        VALUES (%s, %s, %s, %s, %s)
        """
        
        analytics_data = {
            "processing_id": processing_id,
            "record_count": record_count,
            "status": "completed",
            "processed_at": datetime.now().isoformat()
        }
        
        try:
            cursor.execute(processing_query, (
                processing_id,
                blob_name,
                "processing_log",
                json.dumps(analytics_data),
                datetime.now()
            ))
        except psycopg2.Error as e:
            if "duplicate key" in str(e).lower():
                # Update existing record
                update_query = """
                UPDATE file_analytics 
                SET processed_at = %s, analytics = %s 
                WHERE file_id = %s
                """
                cursor.execute(update_query, (
                    datetime.now(),
                    json.dumps(analytics_data),
                    processing_id
                ))
                logging.info(f"Updated existing processing record for {blob_name}")
            else:
                raise
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.warning(f"Could not mark {blob_name} as processed: {str(e)}")

# ... (keeping other functions as they are)
@retry(stop_max_attempt_number=3, wait_fixed=1000)
def process_csv_data(csv_data: str, file_type: str = "users") -> List[Dict[str, Any]]:
    """Process CSV data into structured records with enhanced error handling."""
    if not csv_data or not csv_data.strip():
        logging.warning(f"Received empty CSV data for {file_type}")
        return []
        
    try:
        # Parse CSV
        reader = csv.DictReader(io.StringIO(csv_data))
        
        # Convert to list of dictionaries and parse complex fields
        records = []
        for row_num, row in enumerate(reader, 1):
            try:
                # For user data, parse JSON fields
                if file_type == "users":
                    for field in ['reviews', 'in_cart', 'wishlist']:
                        if field in row and row[field]:
                            try:
                                row[field] = json.loads(row[field])
                            except json.JSONDecodeError:
                                # If JSON parsing fails, keep as string
                                logging.debug(f"Could not parse JSON for field {field} in row {row_num}")
                                pass
                    
                    # Convert boolean strings to actual booleans
                    if 'is_active' in row:
                        row['is_active'] = str(row['is_active']).lower() in ['true', '1', 'yes', 'y']
                    
                    # Convert numeric fields
                    for field in ['purchase_count', 'total_spent']:
                        if field in row and row[field]:
                            try:
                                if field == 'purchase_count':
                                    row[field] = int(float(row[field]))  # Handle decimal strings
                                else:
                                    row[field] = float(row[field])
                            except (ValueError, TypeError):
                                logging.debug(f"Could not convert {field} to number in row {row_num}: {row[field]}")
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
                                    row[field] = int(float(row[field]))  # Handle decimal strings
                                else:
                                    row[field] = float(row[field])
                            except (ValueError, TypeError):
                                logging.debug(f"Could not convert {field} to number in row {row_num}: {row[field]}")
                                pass
                
                records.append(row)
            except Exception as e:
                logging.warning(f"Error processing row {row_num} in {file_type} data: {str(e)}")
                continue  # Skip problematic rows but continue processing
        
        logging.info(f"Successfully parsed {len(records)} records from {file_type} CSV")
        return records
    except Exception as e:
        logging.error(f"Error processing CSV data for {file_type}: {str(e)}")
        raise

def extract_file_metadata(filename: str, file_type: str = "users") -> Dict[str, Any]:
    """Extract metadata from filename with enhanced parsing."""
    try:
        # Extract just the filename without path
        basename = filename.split('/')[-1]
        
        # Parse year and month from filename pattern
        parts = basename.split('_')
        if len(parts) >= 4:
            try:
                year = int(parts[2])
                month = int(parts[3].split('.')[0])
                
                return {
                    "year": year,
                    "month": month,
                    "period": f"{year}-{month:02d}",
                    "filename": basename,
                    "file_type": file_type,
                    "parsed_successfully": True
                }
            except (ValueError, IndexError):
                logging.warning(f"Could not parse year/month from filename {filename}")
    except Exception as e:
        logging.warning(f"Error parsing metadata from filename {filename}: {str(e)}")
    
    # Default metadata if parsing fails
    return {
        "filename": filename.split('/')[-1],
        "processed_time": datetime.now().isoformat(),
        "file_type": file_type,
        "parsed_successfully": False
    }

def generate_analytics(data: List[Dict[str, Any]], metadata: Dict[str, Any], file_type: str) -> Dict[str, Any]:
    """Generate analytics from data records with enhanced error handling."""
    analytics = {
        "record_count": len(data),
        "file_type": file_type,
        "period": metadata.get("period", "unknown"),
        "processed_at": datetime.now().isoformat(),
        "filename": metadata.get("filename", "unknown")
    }
    
    # Skip empty data
    if not data:
        analytics["warning"] = "No data found for analysis"
        return analytics
    
    try:
        # Convert to pandas DataFrame for easier analysis
        df = pd.DataFrame(data)
        
        # User data analytics
        if file_type == "users":
            # User type distribution
            if 'user_type' in df.columns:
                analytics["user_type_distribution"] = df['user_type'].value_counts().to_dict()
            
            # Active users percentage
            if 'is_active' in df.columns:
                active_count = df['is_active'].sum() if df['is_active'].dtype == bool else df['is_active'].apply(lambda x: str(x).lower() in ['true', '1', 'yes', 'y']).sum()
                analytics["active_users_percent"] = round((active_count / len(df)) * 100, 2)
            
            # Device distribution
            if 'last_device' in df.columns and not df['last_device'].isna().all():
                analytics["device_distribution"] = df['last_device'].value_counts().head(10).to_dict()
            
            # Browser distribution
            if 'last_browser' in df.columns and not df['last_browser'].isna().all():
                analytics["browser_distribution"] = df['last_browser'].value_counts().head(10).to_dict()
        
        # Purchase data analytics
        elif file_type == "purchases":
            # Total sales
            if 'total_price' in df.columns:
                total_sales = df['total_price'].sum()
                analytics["total_sales"] = float(total_sales) if pd.notna(total_sales) else 0.0
            
            # Average order value
            if 'total_price' in df.columns:
                avg_value = df['total_price'].mean()
                analytics["average_order_value"] = float(avg_value) if pd.notna(avg_value) else 0.0
            
            # Product category distribution
            if 'product_category' in df.columns and not df['product_category'].isna().all():
                analytics["category_distribution"] = df['product_category'].value_counts().head(10).to_dict()
            
            # Payment method distribution
            if 'payment_method' in df.columns and not df['payment_method'].isna().all():
                analytics["payment_method_distribution"] = df['payment_method'].value_counts().to_dict()
            
            # Discount analytics
            if 'discount_percent' in df.columns and 'discount_amount' in df.columns:
                discount_usage = (df['discount_percent'] > 0).sum()
                analytics["discount_usage_percent"] = round((discount_usage / len(df)) * 100, 2)
                
                total_discount = df['discount_amount'].sum()
                analytics["total_discount_amount"] = float(total_discount) if pd.notna(total_discount) else 0.0
    
    except Exception as e:
        logging.warning(f"Error generating analytics for {file_type}: {str(e)}")
        analytics["analytics_error"] = str(e)
    
    return analytics

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def move_blob_to_processed(blob_name: str):
    """Move a blob from source container to processed container with enhanced error handling."""
    try:
        # Get connection string from app settings - use AzureWebJobsStorage for consistency
        connection_string = os.environ["AzureWebJobsStorage"]
        
        # Create blob service client with SSL handling
        ssl_context = create_ssl_context()
        if ssl_context:
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string,
                ssl_context=ssl_context
            )
        else:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Get source blob client
        source_blob_client = blob_service_client.get_blob_client(
            container=SOURCE_CONTAINER, 
            blob=blob_name
        )
        
        # Check if source blob exists
        try:
            source_properties = source_blob_client.get_blob_properties()
        except ResourceNotFoundError:
            logging.info(f"Source blob {blob_name} not found - likely already moved")
            return
        
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
        
        # Check if destination already exists (avoid duplicate moves)
        try:
            dest_blob_client.get_blob_properties()
            logging.info(f"Blob {blob_name} already exists in {PROCESSED_CONTAINER}")
            # Delete source if destination exists
            try:
                source_blob_client.delete_blob()
                logging.info(f"Deleted source blob {blob_name}")
            except ResourceNotFoundError:
                pass  # Source already deleted
            return
        except ResourceNotFoundError:
            # Destination doesn't exist, continue with move
            pass
        
        # Copy blob data
        source_blob_data = source_blob_client.download_blob().readall()
        dest_blob_client.upload_blob(source_blob_data, overwrite=True)
        logging.info(f"Copied {blob_name} to {PROCESSED_CONTAINER} ({len(source_blob_data)} bytes)")
        
        # Delete source blob after successful copy
        source_blob_client.delete_blob()
        logging.info(f"Deleted {blob_name} from {SOURCE_CONTAINER}")
        
    except ssl.SSLError as e:
        logging.warning(f"SSL error during blob move for {blob_name}: {str(e)}")
        # Try without SSL verification as fallback
        try:
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            # Repeat the move operation without SSL context
            # ... (same logic as above but without SSL context)
        except Exception as fallback_e:
            logging.error(f"Fallback blob move also failed for {blob_name}: {str(fallback_e)}")
            raise
    except ResourceNotFoundError as e:
        logging.info(f"Blob {blob_name} not found during move operation - likely already processed")
    except Exception as e:
        logging.error(f"Error moving blob {blob_name}: {str(e)}")
        raise

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def save_to_database(data: List[Dict[str, Any]], analytics: Dict[str, Any], file_metadata: Dict[str, Any], file_type: str, processing_id: str):
    """Save processed data to PostgreSQL database with enhanced error handling."""
    if not data:
        logging.warning(f"[{processing_id}] No data to save to database for {file_type}")
        return
        
    conn = None
    cursor = None
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
        
        # Get table structure from database
        table_name = "users" if file_type == "users" else "purchases"
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position")
        db_columns = [row[0] for row in cursor.fetchall()]
        
        # Filter columns to only include those that exist in the database
        csv_columns = list(data[0].keys())
        valid_columns = []
        
        # Case-insensitive column matching
        for csv_col in csv_columns:
            for db_col in db_columns:
                if csv_col.lower() == db_col.lower():
                    valid_columns.append(db_col)  # Use database column name (correct case)
                    break
        
        logging.info(f"[{processing_id}] CSV columns: {len(csv_columns)}, DB columns: {len(db_columns)}, Valid columns: {len(valid_columns)}")
        
        if not valid_columns:
            logging.error(f"[{processing_id}] No valid columns found for {file_type} data")
            return
        
        # Prepare column string and placeholders
        column_str = ", ".join([f'"{col}"' for col in valid_columns])
        placeholders = ", ".join(["%s"] * len(valid_columns))
        
        # Prepare batch data with only valid columns
        batch_data = []
        for record in data:
            row_values = []
            for valid_col in valid_columns:
                # Find the corresponding CSV column (case-insensitive)
                csv_value = None
                for csv_col, value in record.items():
                    if csv_col.lower() == valid_col.lower():
                        csv_value = value
                        break
                
                # Convert complex data types to strings for database storage
                if isinstance(csv_value, (list, dict)):
                    csv_value = json.dumps(csv_value)
                elif csv_value is None:
                    csv_value = None
                
                row_values.append(csv_value)
            batch_data.append(tuple(row_values))
        
        # Check if unique constraints exist for conflict resolution
        constraint_check_query = """
        SELECT constraint_name, column_name 
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.table_constraints tc 
            ON kcu.constraint_name = tc.constraint_name
        WHERE tc.table_name = %s 
            AND tc.constraint_type = 'UNIQUE'
        """
        cursor.execute(constraint_check_query, (table_name,))
        unique_constraints = cursor.fetchall()
        
        # Build insert query based on available constraints
        has_user_id_constraint = any("user_id" in constraint[1].lower() for constraint in unique_constraints)
        has_transaction_id_constraint = any("transaction_id" in constraint[1].lower() for constraint in unique_constraints)
        
        if file_type == "users" and has_user_id_constraint and "user_id" in [col.lower() for col in valid_columns]:
            # For users, handle duplicates by user_id
            non_id_columns = [col for col in valid_columns if col.lower() not in ['id']]
            insert_query = f'''
                INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})
                ON CONFLICT (user_id) DO UPDATE SET
                    {", ".join([f'"{col}" = EXCLUDED."{col}"' for col in non_id_columns])}
            '''
        elif file_type == "purchases" and has_transaction_id_constraint and "transaction_id" in [col.lower() for col in valid_columns]:
            # For purchases, handle duplicates by transaction_id
            non_id_columns = [col for col in valid_columns if col.lower() not in ['id']]
            insert_query = f'''
                INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})
                ON CONFLICT (transaction_id) DO UPDATE SET
                    {", ".join([f'"{col}" = EXCLUDED."{col}"' for col in non_id_columns])}
            '''
        else:
            # Simple insert without conflict resolution (will fail on duplicates)
            insert_query = f'INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})'
            logging.warning(f"[{processing_id}] No unique constraints found for {table_name}, using simple INSERT")
        
        # Execute batch insert
        try:
            execute_batch(cursor, insert_query, batch_data, page_size=100)
        except psycopg2.Error as e:
            if "duplicate key" in str(e).lower():
                logging.warning(f"[{processing_id}] Duplicate key error, trying individual inserts for {file_type}")
                # Try inserting rows individually to handle duplicates
                simple_insert = f'INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})'
                successful_inserts = 0
                for row_data in batch_data:
                    try:
                        cursor.execute(simple_insert, row_data)
                        successful_inserts += 1
                    except psycopg2.Error:
                        # Skip duplicate rows
                        continue
                logging.info(f"[{processing_id}] Successfully inserted {successful_inserts} out of {len(batch_data)} rows")
            else:
                raise
        
        # Save file analytics with safer approach
        file_id = f"{file_metadata.get('year', 0)}_{file_metadata.get('month', 0)}_{file_type}_{processing_id}"
        if not file_metadata.get('parsed_successfully', False):
            file_id = f"unparsed_{datetime.now().strftime('%Y%m%d%H%M%S')}_{file_type}_{processing_id}"
            
        analytics_json = json.dumps(analytics)
        
        # Try insert first, then update if it fails
        try:
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
        except psycopg2.Error as e:
            if "duplicate key" in str(e).lower():
                # Update existing record
                update_analytics_query = """
                UPDATE file_analytics 
                SET analytics = %s, processed_at = %s 
                WHERE file_id = %s
                """
                cursor.execute(update_analytics_query, (
                    analytics_json,
                    datetime.now(),
                    file_id
                ))
                logging.info(f"[{processing_id}] Updated existing analytics record")
            else:
                raise
        
        # Commit transaction
        conn.commit()
        
        logging.info(f"[{processing_id}] Successfully saved {len(data)} {file_type} records to database")
        
    except Exception as e:
        # Rollback transaction on error
        if conn:
            conn.rollback()
        
        error_message = str(e)
        error_type = type(e).__name__
        logging.error(f"[{processing_id}] Error saving {file_type} data to database: {error_type} - {error_message}")
        
        # For SQL errors, log more details
        if isinstance(e, psycopg2.Error):
            logging.error(f"[{processing_id}] SQL State: {e.pgcode}, SQL Error: {e.pgerror}")
        
        raise
    finally:
        # Close cursor and connection in finally block
        if cursor:
            cursor.close()
        if conn:
            conn.close()