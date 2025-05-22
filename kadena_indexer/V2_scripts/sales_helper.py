from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
from dotenv import load_dotenv
import tempfile
load_dotenv()

lock_file_path = os.path.join(tempfile.gettempdir(), "mongo_script.lock")

def process_v2_sales_helper():
    if os.path.exists(lock_file_path):
        print("Another instance of the script is running. Exiting.")
        sys.exit()

    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")
        print("Lock file created.")

    try:
        mongo_uri = os.environ.get('Mongo_URI')

        mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        mongo_db = mongo_client['kadena_events']
        events_collection = mongo_db['events']
        blocks_collection = mongo_db['blocks']
        items_for_sale_collection = mongo_db['chain8v2sales']
        sales_data_collection = mongo_db['chain8v2salesdata']
        metadata_collection = mongo_db['script_metadata']
        
        mongo_client.admin.command('ping')
        print("Successfully connected to MongoDB!")

        # Fetch the last processed height from metadata
        metadata = metadata_collection.find_one({"key": "last_v2_sales_processed_height"})
        last_processed_height = int(metadata["value"]) if metadata else 0

        max_height_processed = last_processed_height

        gas_addresses = [
            '6d87fd6e5e47185cb421459d2888bddba7a6c0f2c4ae5246d5f38f993818bb89',
            '99cb7008d7d70c94f138cc366a825f0d9c83a8a2f4ba82c86c666e0ab6fecf3a',
        ]

        for event_type in ['BUY', 'WITHDRAW']:
            events = events_collection.find({
                "qual_name": f"marmalade-v2.ledger.{event_type}",
                "height": {"$gt": last_processed_height}
            }).sort("height", 1)

            for event in events:
                request_key = event['regKey']
                block_hash = event['block']
                event_params = event['params']
                mongo_db_id = event_params[-1]
                event_height = event['height']

                max_height_processed = max(max_height_processed, event_height)

                if event_type == 'BUY':
                    nftid = event_params[0]
                    buyer = event_params[2]
                    seller = event_params[1]

                    block_data = blocks_collection.find_one({"hash": block_hash})
                    creation_time = block_data['creation_time'] if block_data else None

                    if not creation_time:
                        print(f"No data found for block hash: {block_hash}")
                        continue

                    associated_events = events_collection.find({
                        "request_key": request_key,
                        "qual_name": {"$regex": "^coin\\.TRANSFER"}
                    })

                    relevant_transfer = None
                    max_amount = 0

                    for transfer_event in associated_events:
                        transfer_params = transfer_event['params']
                        if not any(gas in transfer_params for gas in gas_addresses):
                            amount = transfer_params[-1]
                            if isinstance(amount, (int, float)) and amount > max_amount:
                                max_amount = amount
                                relevant_transfer = transfer_params

                    if relevant_transfer:
                        if not sales_data_collection.find_one({"_id": mongo_db_id}):
                            sales_data_collection.insert_one({
                                "_id": mongo_db_id,
                                "nftid": nftid,
                                "buyer": buyer,
                                "seller": seller,
                                "sale_amount": max_amount,
                                "creation_time": creation_time,
                            })

                items_for_sale_collection.update_one({"_id": mongo_db_id}, {"$set": {"isActive": False}})

        if max_height_processed > last_processed_height:
            metadata_collection.update_one(
                {"key": "last_v2_sales_processed_height"},
                {"$set": {"value": max_height_processed}},
                upsert=True
            )

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)

if __name__ == "__main__":
    process_v2_sales_helper()
