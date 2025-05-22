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

# List of modules to migrate
target_modules = [
    'marmalade-v2.collection-policy-v1',
    'marmalade-v2.ledger',
    'marmalade-sale.conventional-auction'
]

def read_csv_files(directory):
    """Read all CSV files from the specified directory."""
    pattern = os.path.join(directory, "sql*.csv")
    files = glob.glob(pattern)
    if not files:
        pattern = os.path.join(directory, "*.csv")
        files = glob.glob(pattern)
    return sorted(files)

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
        block_int = int(block) if block else 0
        idx_int = int(idx) if idx else 0
        height_int = int(height) if height else 0
    except ValueError:
        chain_id_int = 0
        block_int = 0
        idx_int = 0
        height_int = 0

    mongo_doc = {
        "name": name,
        "params": event_params,
        "reqKey": request_key,
        "chain": str(chain_id_int),
        "block": block_int,
        "rank": idx_int,
        "height": height_int,
        "ts": None
    }

    collection_name = f"{module}.{name}" if module and name else module or name or "unknown"
    return mongo_doc, collection_name

def insert_batch(collection, docs):
    """Insert a batch of documents, avoiding duplicates based on reqKey."""
    inserted_count = 0
    for doc in docs:
        if not doc.get("reqKey"):
            continue
        if not collection.find_one({"reqKey": doc["reqKey"]}):
            try:
                collection.insert_one(doc)
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting document with reqKey {doc.get('reqKey')}: {e}")
    return inserted_count

def migrate_data_from_csv(csv_dir, mongo_uri):
    mongo_client = None
    try:
        if not os.path.exists(csv_dir):
            print(f"ERROR: CSV directory '{csv_dir}' does not exist.")
            return False

        csv_files = read_csv_files(csv_dir)
        if not csv_files:
            print(f"ERROR: No CSV files found in directory '{csv_dir}'.")
            return False

        print(f"Found {len(csv_files)} CSV files to process:")
        for file in csv_files:
            print(f"  - {os.path.basename(file)}")

        mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        mongo_client.admin.command('ping')
        print("Successfully connected to MongoDB destination")

        mongo_db = mongo_client['nft_events']

        total_documents = 0
        total_collections = 0
        total_rows_processed = 0
        total_rows_skipped = 0
        collection_counts = {}
        batch_by_collection = {}
        batch_size = 1000

        for csv_file in csv_files:
            print(f"\nProcessing file: {os.path.basename(csv_file)}")

            try:
                with open(csv_file, 'r', encoding='utf-8', newline='') as file:
                    sample = file.read(1024)
                    file.seek(0)
                    sniffer = csv.Sniffer()
                    delimiter = sniffer.sniff(sample).delimiter

                    csv_reader = csv.DictReader(file, delimiter=delimiter)

                    for row_num, row in enumerate(csv_reader, 1):
                        total_rows_processed += 1

                        # Safely strip keys/values
                        row = {
                            k.strip() if k else k: v.strip() if isinstance(v, str) else v
                            for k, v in row.items()
                        }

                        try:
                            mongo_doc, collection_name = process_csv_row(row)
                        except Exception as e:
                            print(f"Error processing row {row_num} in {os.path.basename(csv_file)}: {e}")
                            total_rows_skipped += 1
                            continue

                        module = row.get('module', '')
                        if mongo_doc["chain"] != "8" or module not in target_modules:
                            total_rows_skipped += 1
                            continue

                        if collection_name not in collection_counts:
                            collection_counts[collection_name] = 0
                            total_collections += 1
                            batch_by_collection[collection_name] = []

                        collection_counts[collection_name] += 1
                        batch_by_collection[collection_name].append(mongo_doc)

                        for coll_name, docs in list(batch_by_collection.items()):
                            if len(docs) >= batch_size:
                                inserted_count = insert_batch(mongo_db[coll_name], docs)
                                total_documents += inserted_count
                                print(f"Inserted {inserted_count} documents into {coll_name} (skipped {len(docs) - inserted_count} duplicates)")
                                batch_by_collection[coll_name] = []

                        if row_num % 10000 == 0:
                            print(f"  Processed {row_num} rows from {os.path.basename(csv_file)}")

                    print(f"Completed processing {os.path.basename(csv_file)} - {row_num} rows")

            except Exception as e:
                print(f"Error reading CSV file {csv_file}: {e}")
                continue

        for coll_name, docs in batch_by_collection.items():
            if docs:
                inserted_count = insert_batch(mongo_db[coll_name], docs)
                total_documents += inserted_count
                print(f"Inserted {inserted_count} documents into {coll_name} (skipped {len(docs) - inserted_count} duplicates)")

        print("\nMigration Summary:")
        print(f"Total CSV files processed: {len(csv_files)}")
        print(f"Total rows processed: {total_rows_processed}")
        print(f"Total rows skipped (filters/errors): {total_rows_skipped}")
        print(f"Total collections created: {total_collections}")
        for collection_name, count in collection_counts.items():
            print(f"  {collection_name}: {count} documents")
        print(f"Total documents migrated: {total_documents}")
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
    success = migrate_data_from_csv(csv_directory, destination_uri)
    sys.exit(0 if success else 1)
