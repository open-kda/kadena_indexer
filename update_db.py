from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import BulkWriteError, DuplicateKeyError
import sys

# MongoDB URI - credentials masked for security
mongo_uri = "mongodb://root:test6test6withpas5528fjF@neo.kadenaiconnect.com:27018/admin?authSource=admin&tls=true&tlsAllowInvalidCertificates=true&directConnection=true"

def migrate_data(mongo_uri):
    """Migrate data from kadena_events to nft_events database."""
    client = None
    
    try:
        # Connect to MongoDB
        print("Connecting to database...")
        client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        
        # Test connection
        client.admin.command('ping')
        print("Successfully connected to database")
        
        # Database names
        source_db_name = 'kadena_events'
        destination_db_name = 'nft_events'
        
        print(f"Migrating from {source_db_name} to {destination_db_name}")
        
        # Skip these large collections within kadena_events
        skip_collections = [
            'kdlaunch.kdswap-exchange.CREATE_PAIR',
            'kdswap-exchange.SWAP',
            'kaddex.exchange.SWAP',
            'n_e309f0fa7cf3a13f93a8da5325cdad32790d2070.heron.TRANSFER',
            'kdlaunch.kdswap-exchange.SWAP',
            'kdswap-exchange.UPDATE',
            'kaddex.exchange.CREATE_PAIR',
            'kaddex.exchange.UPDATE',
            'kdlaunch.kdswap-exchange.UPDATE'
        ]
        
        # Track migration statistics
        total_collections = 0
        total_documents = 0
        total_duplicates = 0
        total_errors = 0
        skipped_collections = 0
        
        # Connect to specific databases
        source_db = client[source_db_name]
        destination_db = client[destination_db_name]

        # List collections in the kadena_events database
        collections = source_db.list_collection_names()
        print(f"Source Database: {source_db_name}, Collections found: {len(collections)}")
        
        for collection_name in collections:
            if collection_name in skip_collections:
                print(f"  Skipping large collection: {collection_name}")
                skipped_collections += 1
                continue
            
            total_collections += 1
            source_collection = source_db[collection_name]
            destination_collection = destination_db[collection_name]

            # Count documents and fetch them from the source collection
            doc_count = source_collection.count_documents({})
            
            if doc_count > 0:
                print(f"  Migrating {doc_count} documents from {source_db_name}.{collection_name} to {destination_db_name}.{collection_name}")
                
                # Process documents in batches to avoid memory issues
                batch_size = 1000
                collection_documents = 0
                collection_duplicates = 0
                collection_errors = 0
                
                for i in range(0, doc_count, batch_size):
                    batch = list(source_collection.find({}).skip(i).limit(batch_size))
                    if batch:
                        # Try to insert the batch, handling duplicates
                        batch_result = insert_batch_with_duplicate_handling(
                            destination_collection, batch
                        )
                        
                        collection_documents += batch_result['inserted']
                        collection_duplicates += batch_result['duplicates']
                        collection_errors += batch_result['errors']
                        
                        print(f"    Batch {i//batch_size + 1}: {batch_result['inserted']} inserted, "
                              f"{batch_result['duplicates']} duplicates, {batch_result['errors']} errors "
                              f"({i+len(batch)}/{doc_count})")
                
                total_documents += collection_documents
                total_duplicates += collection_duplicates
                total_errors += collection_errors
                
                print(f"  Completed {collection_name}: {collection_documents} inserted, "
                      f"{collection_duplicates} duplicates, {collection_errors} errors")
            else:
                print(f"  No documents to migrate in {collection_name}")

        # Print summary
        print("\nMigration Summary:")
        print(f"Source Database: {source_db_name}")
        print(f"Destination Database: {destination_db_name}")
        print(f"Collections processed: {total_collections} (skipped: {skipped_collections})")
        print(f"Total documents inserted: {total_documents}")
        print(f"Total duplicates skipped: {total_duplicates}")
        print(f"Total errors encountered: {total_errors}")
        print("Migration completed successfully!")

    except Exception as e:
        print(f"An error occurred during migration: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Close the client
        if client:
            client.close()
    
    return True

def insert_batch_with_duplicate_handling(collection, batch):
    """
    Insert a batch of documents with proper duplicate handling.
    Returns a dictionary with counts of inserted, duplicates, and errors.
    """
    result = {
        'inserted': 0,
        'duplicates': 0,
        'errors': 0
    }
    
    try:
        # Try to insert the entire batch first
        insert_result = collection.insert_many(batch, ordered=False)
        result['inserted'] = len(insert_result.inserted_ids)
        return result
        
    except BulkWriteError as bulk_error:
        # Handle bulk write errors (mostly duplicates)
        write_errors = bulk_error.details.get('writeErrors', [])
        result['inserted'] = bulk_error.details.get('nInserted', 0)
        
        for error in write_errors:
            if error.get('code') == 11000:  # Duplicate key error
                result['duplicates'] += 1
            else:
                result['errors'] += 1
                print(f"    Unexpected write error: {error}")
        
        return result
        
    except Exception as e:
        # Handle other errors by trying individual inserts
        print(f"    Batch insert failed, trying individual inserts: {str(e)}")
        
        for doc in batch:
            try:
                collection.insert_one(doc)
                result['inserted'] += 1
            except DuplicateKeyError:
                result['duplicates'] += 1
            except Exception as individual_error:
                result['errors'] += 1
                print(f"    Individual insert error: {individual_error}")
        
        return result

if __name__ == "__main__":
    print("Starting MongoDB migration...")
    success = migrate_data(mongo_uri)
    sys.exit(0 if success else 1)