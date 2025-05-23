from pymongo import MongoClient
from pymongo.server_api import ServerApi

# MongoDB connection URI
mongo_uri = "mongodb://root:test6test6withpas5528fjF@neo.kadenaiconnect.com:27018/admin?authSource=admin&tls=true&tlsAllowInvalidCertificates=true&directConnection=true"

# Target database and collection
database_name = "nft_events"
collection_name = "chain8v2tokens"

def fetch_last_100(uri, db_name, coll_name):
    try:
        print(f"Connecting to MongoDB")
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        print("Successfully connected.")

        db = client[db_name]
        collection = db[coll_name]

        print(f"Fetching last 100 inserted records from '{coll_name}'...")
        records = list(collection.find().sort("$natural", -1).limit(100))

        print(f"Fetched {len(records)} records.\n")
        for doc in records:
            print(doc)

        return records

    except Exception as e:
        print(f"Error fetching records: {e}")
        import traceback
        traceback.print_exc()
        return []

    finally:
        client.close()

if __name__ == "__main__":
    fetch_last_100(mongo_uri, database_name, collection_name)
