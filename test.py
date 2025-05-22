from pymongo import MongoClient
from pymongo.server_api import ServerApi
import sys

# MongoDB connection URI for source only
source_uri = "mongodb://root:test6test6withpas5528fjF@neo.kadenaiconnect.com:27018/admin?authSource=admin&tls=true&tlsAllowInvalidCertificates=true&directConnection=true"

def inspect_collections(source_uri):
    """Inspect collections in admin and kadena_events databases."""
    source_client = None

    try:
        print("Connecting to source database...")
        source_client = MongoClient(source_uri, server_api=ServerApi('1'))
        source_client.admin.command('ping')
        print("Successfully connected to source database")

        # List and inspect collections in admin and kadena_events
        focus_databases = ["nft_events"]
        source_databases = source_client.list_database_names()
        
        print("\n--- Source MongoDB Collections in admin and kadena_events ---")
        
        for db_name in focus_databases:
            if db_name in source_databases:
                print(f"\nDatabase: {db_name}")
                db = source_client[db_name]
                try:
                    collections = db.list_collection_names()
                    print(f"  Found {len(collections)} collections:")
                    for coll_name in collections:
                        try:
                            collection = db[coll_name]
                            count = collection.count_documents({})
                            print(f"  - Collection: {coll_name} (Documents: {count})")
                            if count > 0:
                                print(f"    → Showing one sample document:")
                                sample = collection.find_one()
                                if sample:
                                    # Format the sample document for better readability
                                    import json
                                    from bson import json_util
                                    sample_json = json.dumps(json.loads(json_util.dumps(sample)), indent=2)
                                    # Print the formatted JSON with indentation preserved
                                    lines = sample_json.split('\n')
                                    for i, line in enumerate(lines):
                                        if i > 20:  # Limit to first 20 lines to avoid overwhelming output
                                            print("      ... (truncated, showing first 20 lines)")
                                            break
                                        print(f"      {line}")
                                    print()  # Add a blank line after each document for readability
                        except Exception as inner_e:
                            print(f"    ⚠️ Error with collection '{coll_name}': {inner_e}")
                except Exception as db_e:
                    print(f"  ⚠️ Error accessing database '{db_name}': {db_e}")
            else:
                print(f"\nDatabase: {db_name} not found in the source MongoDB")

    except Exception as e:
        print(f"\nAn error occurred during inspection: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if source_client:
            source_client.close()

    return True

if __name__ == "__main__":
    print("Starting MongoDB inspection for admin and kadena_events collections...")
    success = inspect_collections(source_uri)
    sys.exit(0 if success else 1)