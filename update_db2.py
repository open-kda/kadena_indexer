import os
import json
import csv
import glob
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import sys
from datetime import datetime, timezone

# MongoDB URI - destination remains the same
destination_uri = "mongodb://root:test6test6withpas5528fjF@neo.kadenaiconnect.com:27018/admin?authSource=admin&tls=true&tlsAllowInvalidCertificates=true&directConnection=true"

# CSV files directory
csv_directory = "V2"

# List of modules to migrate
target_modules = [
    'marmalade-v2.collection-policy-v1',
    'marmalade-v2.ledger',
    'marmalade-sale.conventional-auction'
]

def read_csv_files(directory):
    """Read all CSV files from the specified directory."""
    csv_files = []
    
    # Look for CSV files matching the pattern sql1.csv, sql2.csv, etc.
    pattern = os.path.join(directory, "sql*.csv")
    files = glob.glob(pattern)
    
    if not files:
        # If no sql*.csv files found, look for any CSV files
        pattern = os.path.join(directory, "*.csv")
        files = glob.glob(pattern)
    
    return sorted(files)

def process_csv_row(row):
    """Process a single CSV row and return a MongoDB document."""
    # Expected columns: name, params, param_text, request_key, chain_id, block, idx, height, module
    name = row.get('name', '')
    params = row.get('params', '')
    param_text = row.get('param_text', '')
    request_key = row.get('request_key', '')
    chain_id = row.get('chain_id', '')
    block = row.get('block', '')
    idx = row.get('idx', '')
    height = row.get('height', '')
    module = row.get('module', '')
    
    # Parse params JSON if available, otherwise use param_text
    event_params = []
    if params:
        try:
            # Try to parse as JSON
            parsed_params = json.loads(params)
            event_params = parsed_params if isinstance(parsed_params, list) else [parsed_params]
        except (json.JSONDecodeError, TypeError):
            # If params parsing fails, try to use param_text
            if param_text:
                try:
                    parsed_param_text = json.loads(param_text)
                    event_params = parsed_param_text if isinstance(parsed_param_text, list) else [parsed_param_text]
                except (json.JSONDecodeError, TypeError):
                    pass
    elif param_text:
        try:
            parsed_param_text = json.loads(param_text)
            event_params = parsed_param_text if isinstance(parsed_param_text, list) else [parsed_param_text]
        except (json.JSONDecodeError, TypeError):
            pass
    
    # Convert numeric fields
    try:
        chain_id_int = int(chain_id) if chain_id else 0
        block_int = int(block) if block else 0
        idx_int = int(idx) if idx else 0
        height_int = int(height) if height else 0
    except ValueError:
        # Handle non-numeric values
        chain_id_int = 0
        block_int = 0
        idx_int = 0
        height_int = 0
    
    # Create MongoDB document in the expected format
    mongo_doc = {
        "name": name,
        "params": event_params,
        "reqKey": request_key,
        "chain": str(chain_id_int),  # Convert to string as specified
        "block": block_int,
        "rank": idx_int,  # Using idx as the rank
        "height": height_int,
        "ts": None  # No timestamp in CSV, set to None
    }
    
    return mongo_doc, module

def migrate_data_from_csv(csv_dir, mongo_uri):
    """Migrate data from CSV files to MongoDB with specified filters."""
    mongo_client = None
    
    try:
        # Check if CSV directory exists
        if not os.path.exists(csv_dir):
            print(f"ERROR: CSV directory '{csv_dir}' does not exist.")
            return False
        
        # Get list of CSV files
        csv_files = read_csv_files(csv_dir)
        if not csv_files:
            print(f"ERROR: No CSV files found in directory '{csv_dir}'.")
            return False
        
        print(f"Found {len(csv_files)} CSV files to process:")
        for file in csv_files:
            print(f"  - {os.path.basename(file)}")
        
        # Connect to MongoDB destination
        print("Connecting to MongoDB destination...")
        mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        
        # Test connection to destination
        mongo_client.admin.command('ping')
        print("Successfully connected to MongoDB destination")
        
        # Get the events database in MongoDB
        mongo_db = mongo_client['nft_events']  # Using the specified database name
        
        # Track migration statistics
        total_documents = 0
        total_modules = 0
        total_rows_processed = 0
        total_rows_skipped = 0
        
        # Track module counts
        module_counts = {}
        batch_by_module = {}
        batch_size = 1000
        
        # Process each CSV file
        for csv_file in csv_files:
            print(f"\nProcessing file: {os.path.basename(csv_file)}")
            
            try:
                with open(csv_file, 'r', encoding='utf-8', newline='') as file:
                    # Try to detect the CSV format
                    sample = file.read(1024)
                    file.seek(0)
                    sniffer = csv.Sniffer()
                    delimiter = sniffer.sniff(sample).delimiter
                    
                    # Read CSV with detected delimiter
                    csv_reader = csv.DictReader(file, delimiter=delimiter)
                    
                    # Process each row
                    for row_num, row in enumerate(csv_reader, 1):
                        total_rows_processed += 1
                        
                        # Strip whitespace from all values
                        row = {k.strip(): v.strip() if v else v for k, v in row.items()}
                        
                        try:
                            mongo_doc, module = process_csv_row(row)
                        except Exception as e:
                            print(f"Error processing row {row_num} in {os.path.basename(csv_file)}: {e}")
                            total_rows_skipped += 1
                            continue
                        
                        # Check if this is chain_id 8 and module is in target list
                        if mongo_doc["chain"] != "8" or module not in target_modules:
                            total_rows_skipped += 1
                            continue
                        
                        # Track module counts
                        if module not in module_counts:
                            module_counts[module] = 0
                            total_modules += 1
                            batch_by_module[module] = []
                        
                        module_counts[module] += 1
                        batch_by_module[module].append(mongo_doc)
                        
                        # Check if any module's batch has reached the batch size
                        for mod, docs in list(batch_by_module.items()):
                            if len(docs) >= batch_size:
                                # Process batch for this module
                                inserted_count = insert_batch(mongo_db[mod], docs)
                                total_documents += inserted_count
                                print(f"Inserted {inserted_count} documents into {mod} (skipped {len(docs) - inserted_count} duplicates)")
                                
                                # Clear this module's batch
                                batch_by_module[mod] = []
                        
                        # Progress indicator
                        if row_num % 10000 == 0:
                            print(f"  Processed {row_num} rows from {os.path.basename(csv_file)}")
                    
                    print(f"Completed processing {os.path.basename(csv_file)} - {row_num} rows")
                    
            except Exception as e:
                print(f"Error reading CSV file {csv_file}: {e}")
                continue
        
        # Insert remaining batches
        for mod, docs in batch_by_module.items():
            if docs:
                inserted_count = insert_batch(mongo_db[mod], docs)
                total_documents += inserted_count
                print(f"Inserted {inserted_count} documents into {mod} (skipped {len(docs) - inserted_count} duplicates)")
        
        # Print summary
        print("\nMigration Summary:")
        print(f"Total CSV files processed: {len(csv_files)}")
        print(f"Total rows processed: {total_rows_processed}")
        print(f"Total rows skipped (filters/errors): {total_rows_skipped}")
        print(f"Total modules processed: {total_modules}")
        for module, count in module_counts.items():
            print(f"  {module}: {count} documents")
        print(f"Total documents migrated: {total_documents}")
        print("Migration completed successfully!")

    except Exception as e:
        print(f"An error occurred during migration: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Close the MongoDB connection
        if mongo_client:
            mongo_client.close()
    
    return True

def insert_batch(collection, docs):
    """Insert a batch of documents, avoiding duplicates based on reqKey."""
    inserted_count = 0
    
    for doc in docs:
        # Skip documents with no reqKey
        if not doc.get("reqKey"):
            continue
            
        # Check if document with this reqKey already exists
        existing = collection.find_one({"reqKey": doc["reqKey"]})
        
        if not existing:
            try:
                # Insert new document
                collection.insert_one(doc)
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting document with reqKey {doc.get('reqKey')}: {e}")
    
    return inserted_count

if __name__ == "__main__":
    print("Starting CSV to MongoDB migration...")
    
    success = migrate_data_from_csv(csv_directory, destination_uri)
    sys.exit(0 if success else 1)