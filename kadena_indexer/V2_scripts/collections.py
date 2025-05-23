import tempfile
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
from dotenv import load_dotenv

load_dotenv()

def process_v2_collections():
    lock_file_path = os.path.join(tempfile.gettempdir(), "ng_collection_script.lock")
    
    # Ensure only one instance of the script runs at a time
    if os.path.exists(lock_file_path):
        print("Another instance of the script is running. Exiting.")
        sys.exit()

    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")
        print("Lock file created.")

    try:
        # Database configuration
        mongo_uri = os.environ.get('Mongo_URI')
        mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        mongo_db = mongo_client['nft_events']
        tokens_collection = mongo_db['chain8v2tokens']
        collections_collection = mongo_db['chain8v2collections']
        metadata_collection = mongo_db['script_metadata']
        
        # Fetch the last processed collection height
        metadata_doc = metadata_collection.find_one({"key": "last_v2_collection_height"})
        last_collection_height = int(metadata_doc["value"]) if metadata_doc else 0
        print(f"Starting from block height: {last_collection_height}")

        max_height_processed = last_collection_height
        temp_max_height_processed = last_collection_height

        # Process CREATE-COLLECTION events
        create_collection_events = mongo_db['marmalade-v2.collection-policy-v1.COLLECTION'].find(
            {"height": {"$gt": last_collection_height}}
        ).sort("height", 1)

        for event in create_collection_events:
            params = event['params']
            collection_id, collection_name, maxSize, collection_creator = params[:4]

            maxSize_value = maxSize if isinstance(maxSize, int) else maxSize.get('int', 0)

            # Insert or update collection information in MongoDB
            collections_collection.update_one(
                {"_id": collection_id},
                {"$setOnInsert": {
                    "creator": collection_creator,
                    "maxSize": maxSize_value,
                    "name": collection_name,
                    "size": 0,
                    "approved": False,
                    "tokens": []
                }},
                upsert=True
            )
            print(f"Processed V2 COLLECTION for {collection_id}")
            temp_max_height_processed = max(temp_max_height_processed, event['height'])

        # Process ADD-TO-COLLECTION events
        add_to_collection_events = mongo_db['marmalade-v2.collection-policy-v1.TOKEN-COLLECTION'].find(
            {"height": {"$gt": max_height_processed}}
        ).sort("height", 1)

        for event in add_to_collection_events:
            collection_id, nft_id = event['params']

            # Fetch collection document from MongoDB
            collection_document = collections_collection.find_one({"_id": collection_id})
            if not collection_document:
                print(f"Warning: Collection {collection_id} not found. Skipping.")
                continue

            collection_name = collection_document['name']

            # Update the token's associated collection info
            update_result = tokens_collection.update_one(
                {"_id": nft_id},
                {"$set": {
                    "collection": {"collectionId": collection_id, "collectionName": collection_name}
                }},
                upsert=True
            )

            # Check if this NFT is new to the collection
            if update_result.modified_count > 0 or update_result.upserted_id:
                # NFT is new to the collection, update the collection document
                collections_collection.update_one(
                    {"_id": collection_id},
                    {
                        "$addToSet": {"tokens": nft_id},
                        "$inc": {"size": 1}
                    }
                )
                print(f"Added new NFT {nft_id} to V2 collection {collection_id} and incremented size")
            else:
                print(f"NFT {nft_id} already in V2 collection {collection_id}. Size not incremented.")

            temp_max_height_processed = max(temp_max_height_processed, event['height'])

        # Update the last processed height for collections after processing all events
        if temp_max_height_processed > max_height_processed:
            metadata_collection.update_one(
                {"key": "last_v2_collection_height"},
                {"$set": {"value": temp_max_height_processed}},
                upsert=True
            )
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
        if 'mongo_client' in locals():
            mongo_client.close()

# Allow direct execution but avoid automatic execution during imports
if __name__ == "__main__":
    process_v2_collections()

