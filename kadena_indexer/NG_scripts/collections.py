import tempfile
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

def process_collections():
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
        tokens_collection = mongo_db['chain8v2ngtokens']
        collections_collection = mongo_db['chain8ngcollections']
        metadata_collection = mongo_db['script_metadata']
        
        # Ping MongoDB to verify the connection
        mongo_client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        
        # Fetch the last processed collection height
        metadata_doc = metadata_collection.find_one({"key": "last_collection_height"})
        last_collection_height = int(metadata_doc["value"]) if metadata_doc else 0
        print(f"Starting from height: {last_collection_height + 1}")

        # Collection names for NG events
        create_collection_name = 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-collection.CREATE-COLLECTION'
        add_to_collection_name = 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-collection.ADD-TO-COLLECTION'

        # Get all unique heights greater than last processed height for both event types
        all_heights = set()
        
        # Get heights from CREATE-COLLECTION events
        create_heights = mongo_db[create_collection_name].distinct(
            "height", 
            {"height": {"$gt": last_collection_height}}
        )
        all_heights.update(create_heights)
        
        # Get heights from ADD-TO-COLLECTION events  
        add_heights = mongo_db[add_to_collection_name].distinct(
            "height",
            {"height": {"$gt": last_collection_height}}
        )
        all_heights.update(add_heights)
        
        # Sort heights to process them in order
        sorted_heights = sorted(all_heights)
        
        if not sorted_heights:
            print("No new heights to process.")
            return
            
        print(f"Found {len(sorted_heights)} heights to process: {min(sorted_heights)} to {max(sorted_heights)}")

        # Process each height sequentially
        for current_height in sorted_heights:
            print(f"\n=== Processing height {current_height} ===")
            
            # Group all events at this height by type
            events_at_height = defaultdict(list)
            
            # Get CREATE-COLLECTION events at this height
            create_events = list(
                mongo_db[create_collection_name].find(
                    {"height": current_height}
                ).sort("_id", 1)
            )
            if create_events:
                events_at_height['CREATE-COLLECTION'] = create_events
                print(f"  Found {len(create_events)} CREATE-COLLECTION events at height {current_height}")
            
            # Get ADD-TO-COLLECTION events at this height
            add_events = list(
                mongo_db[add_to_collection_name].find(
                    {"height": current_height}
                ).sort("_id", 1)
            )
            if add_events:
                events_at_height['ADD-TO-COLLECTION'] = add_events
                print(f"  Found {len(add_events)} ADD-TO-COLLECTION events at height {current_height}")

            # Process all events at this height
            height_processed_successfully = True
            
            try:
                # Process CREATE-COLLECTION events first (create collections before adding tokens)
                if 'CREATE-COLLECTION' in events_at_height:
                    for event in events_at_height['CREATE-COLLECTION']:
                        success = process_create_collection_event(event, collections_collection)
                        if not success:
                            height_processed_successfully = False
                            print(f"Failed to process CREATE-COLLECTION event at height {current_height}")
                            break
                
                # Then process ADD-TO-COLLECTION events (add tokens to collections)
                if height_processed_successfully and 'ADD-TO-COLLECTION' in events_at_height:
                    for event in events_at_height['ADD-TO-COLLECTION']:
                        success = process_add_to_collection_event(
                            event, collections_collection, tokens_collection
                        )
                        if not success:
                            height_processed_successfully = False
                            print(f"Failed to process ADD-TO-COLLECTION event at height {current_height}")
                            break

                # Only update the height if all events at this height were processed successfully
                if height_processed_successfully:
                    metadata_collection.update_one(
                        {"key": "last_collection_height"},
                        {"$set": {"value": current_height}},
                        upsert=True
                    )
                    print(f"Successfully processed all events at height {current_height}")
                else:
                    print(f"Stopping at height {current_height} due to processing errors")
                    break
                    
            except Exception as e:
                print(f"Error processing height {current_height}: {e}")
                height_processed_successfully = False
                break

        final_height = current_height if height_processed_successfully else last_collection_height
        print(f"\nProcessing completed. Last processed height: {final_height}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
            print("Lock file removed.")
        if 'mongo_client' in locals():
            mongo_client.close()

def process_create_collection_event(event, collections_collection):
    """Process a CREATE-COLLECTION event and return True if successful, False otherwise"""
    try:
        params = event['params']
        event_height = event['height']
        
        if len(params) < 4:
            print(f"Invalid CREATE-COLLECTION event parameters: expected at least 4, got {len(params)}")
            return False
            
        collection_id, collection_name, maxSize, collection_creator = params[:4]
        
        print(f"  Processing CREATE-COLLECTION event at height {event_height} for {collection_id}")

        # Handle maxSize which might be an int or a dict with 'int' key
        if isinstance(maxSize, int):
            maxSize_value = maxSize
        elif isinstance(maxSize, dict) and 'int' in maxSize:
            maxSize_value = maxSize['int']
        else:
            print(f"Warning: Unexpected maxSize format: {maxSize}, defaulting to 0")
            maxSize_value = 0

        # Insert or update collection information in MongoDB
        result = collections_collection.update_one(
            {"_id": collection_id},
            {"$setOnInsert": {
                "creator": collection_creator,
                "maxSize": maxSize_value,
                "name": collection_name,
                "size": 0,
                "tokens": []
            }},
            upsert=True
        )
        
        if result.upserted_id:
            print(f"    Created new NG collection {collection_id} with name '{collection_name}'")
        else:
            print(f"    NG collection {collection_id} already exists")
        
        return True
        
    except Exception as e:
        print(f"Error processing CREATE-COLLECTION event: {e}")
        return False

def process_add_to_collection_event(event, collections_collection, tokens_collection):
    """Process an ADD-TO-COLLECTION event and return True if successful, False otherwise"""
    try:
        params = event['params']
        event_height = event['height']
        
        if len(params) < 2:
            print(f"Invalid ADD-TO-COLLECTION event parameters: expected at least 2, got {len(params)}")
            return False
            
        collection_id, nft_id = params[0], params[1]
        
        print(f"  Processing ADD-TO-COLLECTION event at height {event_height} for NFT {nft_id}")

        # Fetch collection document from MongoDB
        collection_document = collections_collection.find_one({"_id": collection_id})
        if not collection_document:
            print(f"    Warning: NG collection {collection_id} not found. Skipping.")
            return False

        collection_name = collection_document['name']

        # Check if the token is already in this collection
        existing_token = tokens_collection.find_one({
            "_id": nft_id,
            "collection.collectionId": collection_id
        })
        
        if existing_token:
            print(f"    NFT {nft_id} already in NG collection {collection_id}. Skipping.")
            return True

        # Update the token's associated collection info
        token_update_result = tokens_collection.update_one(
            {"_id": nft_id},
            {"$set": {
                "collection": {"collectionId": collection_id, "collectionName": collection_name}
            }},
            upsert=True
        )

        if token_update_result.modified_count > 0 or token_update_result.upserted_id:
            # Token was updated/created, now update the collection document
            collection_update_result = collections_collection.update_one(
                {"_id": collection_id},
                {
                    "$addToSet": {"tokens": nft_id},
                    "$inc": {"size": 1}
                }
            )
            
            if collection_update_result.modified_count > 0:
                print(f"    Added NFT {nft_id} to NG collection {collection_id} and incremented size")
            else:
                print(f"    NFT {nft_id} was already in NG collection tokens array")
        else:
            print(f"    No changes made for NFT {nft_id}")

        return True
        
    except Exception as e:
        print(f"Error processing ADD-TO-COLLECTION event: {e}")
        return False

# Allow direct execution but avoid automatic execution during imports
if __name__ == "__main__":
    process_collections()