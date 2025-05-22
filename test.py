from pymongo import MongoClient
from pymongo.server_api import ServerApi
import sys

# MongoDB connection URI
mongo_uri = "mongodb://root:test6test6withpas5528fjF@neo.kadenaiconnect.com:27018/admin?authSource=admin&tls=true&tlsAllowInvalidCertificates=true&directConnection=true"

# Target database and collections to drop
database_name = "nft_events"
collections_to_delete = [
    'marmalade-v2.collection-policy-v1',
    'marmalade-v2.ledger',
    'marmalade-sale.conventional-auction'
]

def delete_collections(uri, db_name, collections):
    client = None
    try:
        print(f"Connecting to MongoDB at {uri}...")
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        print("Successfully connected.")

        db = client[db_name]
        existing_collections = db.list_collection_names()

        for coll in collections:
            if coll in existing_collections:
                print(f"Deleting collection: {coll}")
                db.drop_collection(coll)
                print(f"  ✔ Dropped: {coll}")
            else:
                print(f"  ⚠️ Collection not found: {coll}")

        print("\nCompleted collection deletion.")
        return True

    except Exception as e:
        print(f"Error during collection deletion: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if client:
            client.close()

if __name__ == "__main__":
    success = delete_collections(mongo_uri, database_name, collections_to_delete)
    sys.exit(0 if success else 1)
