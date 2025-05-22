import os
import json
import csv
import glob
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import sys

# MongoDB URI - destination remains the same
destination_uri = "mongodb://root:test6test6withpas5528fjF@neo.kadenaiconnect.com:27018/admin?authSource=admin&tls=true&tlsAllowInvalidCertificates=true&directConnection=true"

# CSV files directory
csv_directory = "V2"

# List of CSV files to process (edit this list as needed)
files = ["sql10","sql4","sql6"]

# List of modules to migrate
target_modules = [
    'marmalade-v2.collection-policy-v1',
    'marmalade-v2.ledger',
    'marmalade-sale.conventional-auction'
]

def get_csv_file_paths(directory, filenames):
    """Get the full paths to the CSV files."""
    file_paths = []
    
    for filename in filenames:
        # If filename doesn't have .csv extension, add it
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        file_path = os.path.join(directory, filename)
        
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"WARNING: File '{file_path}' does not exist. Skipping...")
            continue
        
        file_paths.append(file_path)
    
    return file_paths

def process_csv_row(row):
    """Process a single CSV row and return a MongoDB document."""
    name = row.get('name', '')
    params = row.get('params', '')
    param_text = row.get('param_text', '')
    request_key = row.get('request_key', '')
    chain_id = row.get('chain_id', '')
    block = row.get('block', '')
    idx = row.get('idx', '')
    height = row.get('height', '')
    module = row.get('module', '')


    event_params = []
    if params:
        try:
            parsed_params = json.loads(params)
            event_params = parsed_params if isinstance(parsed_params, list) else [parsed_params]
        except (json.JSONDecodeError, TypeError):
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

    try:
        chain_id_int = int(chain_id) if chain_id else 0
        idx_int = int(idx) if idx else 0
        height_int = int(height) if height else 0
    except ValueError:
        chain_id_int = 0
        idx_int = 0
        height_int = 0

    mongo_doc = {
        "name": name,
        "params": event_params,
        "reqKey": request_key,
        "chain": str(chain_id_int),
        "block": block,
        "rank": idx_int,
        "height": height_int,
        "ts": None
    }

    collection_name = f"{module}.{name}" if module and name else module or name or "unknown"
    return mongo_doc, collection_name

def insert_batch(collection, docs, batch_num, total_batches):
    """Insert a batch of documents, avoiding duplicates based on reqKey."""
    inserted_count = 0
    skipped_count = 0
    
    print(f"  Processing batch {batch_num}/{total_batches} ({len(docs)} documents)")
    
    for i, doc in enumerate(docs, 1):
        if not doc.get("reqKey"):
            skipped_count += 1
            continue
            
        if not collection.find_one({"reqKey": doc["reqKey"]}):
            try:
                collection.insert_one(doc)
                inserted_count += 1
                if i % 100 == 0 or i == len(docs):
                    print(f"    Inserted {i}/{len(docs)} documents in current batch")
            except Exception as e:
                print(f"    Error inserting document with reqKey {doc.get('reqKey')}: {e}")
                skipped_count += 1
        else:
            skipped_count += 1
    
    print(f"  Batch {batch_num} complete: {inserted_count} inserted, {skipped_count} skipped")
    return inserted_count

def migrate_data_from_csv(csv_dir, mongo_uri, csv_filenames):
    mongo_client = None
    try:
        if not os.path.exists(csv_dir):
            print(f"ERROR: CSV directory '{csv_dir}' does not exist.")
            return False

        csv_file_paths = get_csv_file_paths(csv_dir, csv_filenames)
        if not csv_file_paths:
            print("ERROR: No valid CSV files found to process.")
            return False

        print(f"Found {len(csv_file_paths)} valid CSV files to process:")
        for file_path in csv_file_paths:
            print(f"  - {os.path.basename(file_path)}")

        mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        mongo_client.admin.command('ping')
        print("Successfully connected to MongoDB destination")

        mongo_db = mongo_client['nft_events']

        # Global counters across all files
        global_total_documents = 0
        global_total_collections = 0
        global_total_rows_processed = 0
        global_total_rows_skipped = 0
        global_collection_counts = {}

        # Process each CSV file
        for file_index, csv_file_path in enumerate(csv_file_paths, 1):
            print(f"\n{'='*80}")
            print(f"PROCESSING FILE {file_index}/{len(csv_file_paths)}: {os.path.basename(csv_file_path)}")
            print(f"{'='*80}")

            # File-specific counters
            file_total_documents = 0
            file_total_rows_processed = 0
            file_total_rows_skipped = 0
            file_collection_counts = {}
            batch_by_collection = {}
            batch_size = 1000

            try:
                with open(csv_file_path, 'r', encoding='utf-8-sig', newline='') as file:  # utf-8-sig handles BOM
                    # Try to detect delimiter more carefully
                    sample = file.read(2048)  # Read more sample data
                    file.seek(0)
                    
                    # Force comma delimiter since we can see commas in the data
                    delimiter = ','
                    print(f"Using CSV delimiter: '{delimiter}'")

                    csv_reader = csv.DictReader(file, delimiter=delimiter)
                    print(f"CSV Headers detected: {csv_reader.fieldnames}")
                    
                    # Check if headers look reasonable
                    if csv_reader.fieldnames and len(csv_reader.fieldnames) < 5:
                        print("WARNING: Very few columns detected. CSV might have parsing issues.")
                        print("First few characters of file:")
                        file.seek(0)
                        first_line = file.readline()
                        print(f"  First line: {repr(first_line[:200])}")
                        file.seek(0)
                        csv_reader = csv.DictReader(file, delimiter=delimiter)

                    for row_num, row in enumerate(csv_reader, 1):
                        file_total_rows_processed += 1
                        global_total_rows_processed += 1

                        # Safely strip keys/values
                        row = {
                            k.strip() if k else k: v.strip() if isinstance(v, str) else v
                            for k, v in row.items()
                        }

                        try:
                            mongo_doc, collection_name = process_csv_row(row)
                        except Exception as e:
                            print(f"Error processing row {row_num}: {e}")
                            file_total_rows_skipped += 1
                            global_total_rows_skipped += 1
                            continue

                        module = row.get('module', '')
                        chain_value = mongo_doc["chain"]
                        
                        if chain_value != "8" or module not in target_modules:
                            file_total_rows_skipped += 1
                            global_total_rows_skipped += 1
                            
                            # Count skip reasons for summary
                            if row_num == 1000:  # Show stats at 1000 rows
                                chain_8_count = 0
                                module_match_count = 0
                                # Re-check first 1000 rows for statistics
                                print(f"\nDEBUG: Analyzing skip reasons for first 1000 rows...")
                            continue

                        # Track collections for this file
                        if collection_name not in file_collection_counts:
                            file_collection_counts[collection_name] = 0
                            batch_by_collection[collection_name] = []
                            
                            # Track global collections
                            if collection_name not in global_collection_counts:
                                global_collection_counts[collection_name] = 0
                                global_total_collections += 1
                                print(f"New collection discovered: {collection_name}")

                        file_collection_counts[collection_name] += 1
                        global_collection_counts[collection_name] += 1
                        batch_by_collection[collection_name].append(mongo_doc)

                        # Process batches when they reach batch_size
                        for coll_name, docs in list(batch_by_collection.items()):
                            if len(docs) >= batch_size:
                                batch_num = (file_collection_counts[coll_name] - len(docs)) // batch_size + 1
                                total_batches = (file_collection_counts[coll_name] + batch_size - 1) // batch_size
                                
                                print(f"\nProcessing collection: {coll_name}")
                                inserted_count = insert_batch(mongo_db[coll_name], docs, batch_num, total_batches)
                                file_total_documents += inserted_count
                                global_total_documents += inserted_count
                                batch_by_collection[coll_name] = []

                        # Progress update every 1000 rows
                        if row_num % 1000 == 0:
                            print(f"\nProgress: {row_num} rows processed from {os.path.basename(csv_file_path)}")
                            print(f"  File rows skipped (filters/errors): {file_total_rows_skipped}")
                            print(f"  Documents ready for insertion: {sum(len(docs) for docs in batch_by_collection.values())}")

                    print(f"\nCompleted reading {os.path.basename(csv_file_path)} - {row_num} total rows")

            except Exception as e:
                print(f"Error reading CSV file {csv_file_path}: {e}")
                continue

            # Process remaining batches for this file
            print(f"\nProcessing remaining batches for {os.path.basename(csv_file_path)}...")
            for coll_name, docs in batch_by_collection.items():
                if docs:
                    batch_num = (file_collection_counts[coll_name] - len(docs)) // batch_size + 1
                    total_batches = (file_collection_counts[coll_name] + batch_size - 1) // batch_size
                    
                    print(f"\nProcessing final batch for collection: {coll_name}")
                    inserted_count = insert_batch(mongo_db[coll_name], docs, batch_num, total_batches)
                    file_total_documents += inserted_count
                    global_total_documents += inserted_count

            # File summary with more details
            print(f"\n{'-'*60}")
            print(f"FILE {file_index} SUMMARY: {os.path.basename(csv_file_path)}")
            print(f"{'-'*60}")
            print(f"Rows processed in this file: {file_total_rows_processed}")
            print(f"Rows skipped in this file: {file_total_rows_skipped}")
            print(f"Documents migrated from this file: {file_total_documents}")
            
            if file_total_rows_skipped > 0:
                print(f"\nDEBUG: All rows were skipped. Let's check the first few rows...")
                # Re-read first few rows to show their content
                try:
                    with open(csv_file_path, 'r', encoding='utf-8', newline='') as debug_file:
                        debug_reader = csv.DictReader(debug_file)
                        print("Sample row data (from debug read):")
                        print(f"Debug CSV Headers: {debug_reader.fieldnames}")
                        for i, debug_row in enumerate(debug_reader, 1):
                            if i > 3:  # Show first 3 rows
                                break
                            debug_row = {k.strip() if k else k: v.strip() if isinstance(v, str) else v for k, v in debug_row.items()}
                            print(f"  Row {i}:")
                            print(f"    chain_id: '{debug_row.get('chain_id', 'MISSING')}'")
                            print(f"    module: '{debug_row.get('module', 'MISSING')}'")
                            print(f"    name: '{debug_row.get('name', 'MISSING')}'")
                            print(f"    request_key: '{debug_row.get('request_key', 'MISSING')}'")
                except Exception as e:
                    print(f"Error reading file for debug: {e}")
            
            print("Collections in this file:")
            if file_collection_counts:
                for collection_name, count in file_collection_counts.items():
                    print(f"  {collection_name}: {count} documents")
            else:
                print("  No collections created (all rows were filtered out)")
                print(f"  Required: chain_id = '8' AND module in {target_modules}")

        # Global summary
        print(f"\n{'='*80}")
        print("GLOBAL MIGRATION SUMMARY")
        print(f"{'='*80}")
        print(f"Total CSV files processed: {len(csv_file_paths)}")
        print(f"Total rows processed: {global_total_rows_processed}")
        print(f"Total rows skipped (filters/errors): {global_total_rows_skipped}")
        print(f"Total collections created/updated: {global_total_collections}")
        print("\nGlobal collection breakdown:")
        for collection_name, count in global_collection_counts.items():
            print(f"  {collection_name}: {count} documents")
        print(f"\nTotal documents migrated: {global_total_documents}")
        print("Migration completed successfully!")

    except Exception as e:
        print(f"An error occurred during migration: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if mongo_client:
            mongo_client.close()

    return True

if __name__ == "__main__":
    print("Starting CSV to MongoDB migration...")
    print(f"Target CSV files: {', '.join(files)}")
    print(f"CSV directory: {csv_directory}")
    
    success = migrate_data_from_csv(csv_directory, destination_uri, files)
    sys.exit(0 if success else 1)