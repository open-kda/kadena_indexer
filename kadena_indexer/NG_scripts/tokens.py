from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import tempfile
load_dotenv()

lock_file_path = os.path.join(tempfile.gettempdir(), "token_tk_script.lock")

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_tokens():
    if os.path.exists(lock_file_path):
        logging.info(f"Lock file path: {lock_file_path}")
        logging.info("Another instance of the script is running. Exiting.")
        sys.exit()

    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")
        logging.info("Tokens Lock file created.")

    def get_last_token_height(tokens_collection):
        """Get the last processed token height from metadata"""
        logging.info("Fetching last token height...")
        result = tokens_collection.find_one({"_id": "last_token_height"})
        return int(result['value']) if result else 0

    def update_last_token_height(tokens_collection, height):
        """Update the last processed token height"""
        tokens_collection.update_one(
            {"_id": "last_token_height"},
            {"$set": {"value": height, "updated_at": datetime.utcnow()}},
            upsert=True
        )
        logging.info(f"Updated last processed token height to {height}")

    def get_all_token_events_at_height_range(mongo_db, event_queries, start_height, end_height=None):
        """Get all token events within a height range, sorted by height"""
        all_events = []
        
        query = {"height": {"$gt": start_height}}
        if end_height is not None:
            query["height"]["$lte"] = end_height
            
        for event_type, collection_name in event_queries.items():
            try:
                events = list(
                    mongo_db[collection_name].find(query).sort("height", 1)
                )
                for event in events:
                    event['event_type'] = event_type
                    all_events.append(event)
                logging.info(f"Found {len(events)} {event_type} events")
            except Exception as e:
                logging.error(f"Error fetching {event_type} events: {e}")
                continue
        
        # Sort all events by height to ensure sequential processing
        all_events.sort(key=lambda x: x['height'])
        return all_events

    try:
        mongo_uri = os.environ.get('Mongo_URI')
        if not mongo_uri:
            raise ValueError("Mongo_URI environment variable not set")

        # MongoDB client connection
        mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        mongo_db = mongo_client['nft_events']
        tokens_collection = mongo_db['chain8v2ngtokens']
        ledger_collection = mongo_db['chain8v2ngledger']

        # Ping MongoDB to verify the connection
        mongo_client.admin.command('ping')
        logging.info("Successfully connected to MongoDB!")

        # Get the last processed token height
        last_token_height = get_last_token_height(tokens_collection)
        logging.info(f"Starting from token height: {last_token_height}")

        event_queries = {
            'MINT': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.MINT",
            'RECONCILE': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.RECONCILE",
            'SUPPLY': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.SUPPLY",
            'TOKEN-CREATE': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.TOKEN-CREATE",
        }

        # Get all events after the last processed height, sorted by height
        all_events = get_all_token_events_at_height_range(mongo_db, event_queries, last_token_height)
        
        if not all_events:
            logging.info("No new token events to process")
            return

        logging.info(f"Found {len(all_events)} total token events to process")

        current_height = last_token_height
        events_processed_count = 0
        
        # Process events sequentially by height
        for event in all_events:
            event_height = event["height"]
            event_type = event["event_type"]
            event_params = event["params"]
            
            # Log progress every 100 events or when height changes
            if events_processed_count % 100 == 0 or event_height != current_height:
                logging.info(f"Processing {event_type} event at height {event_height} (Event #{events_processed_count + 1})")
            
            current_height = event_height

            try:
                if event_type == 'MINT':
                    # Parse MINT event parameters
                    nft_id, account, balance = event_params
                    ledger_collection.update_one(
                        {"nftId": nft_id},
                        {"$set": {
                            "account": account, 
                            "balance": balance,
                            "updatedAt": event_height
                        }},
                        upsert=True
                    )

                elif event_type == 'SUPPLY':
                    # Parse SUPPLY event parameters
                    nft_id, supply = event_params
                    tokens_collection.update_one(
                        {"_id": nft_id},
                        {"$set": {
                            "supply": supply,
                            "updatedAt": event_height
                        }},
                        upsert=True
                    )

                elif event_type == 'TOKEN-CREATE':
                    nft_id, uri = event_params[0], event_params[1]
                    policies = event_params[3] if len(event_params) > 3 else []
                    
                    if not isinstance(policies, list):
                        logging.warning(f"Expected policies to be a list, but got: {type(policies)} for NFT {nft_id}")
                        policies = []

                    grouped_policies = {}
                    for policy_str in policies:
                        try:
                            if isinstance(policy_str, str):
                                parts = policy_str.split(".")
                                if len(parts) >= 2:
                                    namespace = parts[0]
                                    policy_name = ".".join(parts[1:])

                                    if namespace not in grouped_policies:
                                        grouped_policies[namespace] = {}

                                    if policy_name not in grouped_policies[namespace]:
                                        grouped_policies[namespace][policy_name] = []

                                    grouped_policies[namespace][policy_name].append(policy_str)
                                else:
                                    logging.warning(f"Unexpected policy format: {policy_str}")
                            else:
                                logging.warning(f"Policy is not a string: {policy_str}")
                        except Exception as policy_error:
                            logging.error(f"Error processing policy '{policy_str}': {policy_error}")

                    tokens_collection.update_one(
                        {"_id": nft_id},
                        {"$set": {
                            "uri": uri, 
                            "groupedPolicies": grouped_policies,
                            "updatedAt": event_height,
                            "createdAt": event_height
                        }},
                        upsert=True
                    )

                elif event_type == 'RECONCILE':
                    if len(event_params) < 4:
                        logging.error(f"RECONCILE event has insufficient parameters: {event_params}")
                        continue
                        
                    nft_id = event_params[0]
                    previous_owner_info = event_params[2]
                    new_owner_info = event_params[3]

                    if not isinstance(previous_owner_info, dict) or not isinstance(new_owner_info, dict):
                        logging.error(f"Invalid owner info format in RECONCILE event: prev={previous_owner_info}, new={new_owner_info}")
                        continue

                    previous_account = previous_owner_info.get('account')
                    new_account = new_owner_info.get('account')

                    if not previous_account or not new_account:
                        logging.error(f"Missing account information in RECONCILE event: prev={previous_account}, new={new_account}")
                        continue

                    # Update previous owner's balance to 0
                    ledger_collection.update_one(
                        {"nftId": nft_id, "account": previous_account},
                        {"$set": {
                            "balance": 0,
                            "updatedAt": event_height
                        }}
                    )

                    # Update new owner's balance to 1
                    ledger_collection.update_one(
                        {"nftId": nft_id, "account": new_account},
                        {"$set": {
                            "balance": 1,
                            "updatedAt": event_height
                        }},
                        upsert=True
                    )

                    logging.debug(f"Processed RECONCILE event for NFT {nft_id}: {previous_account} -> {new_account}")

                events_processed_count += 1
                
                # Update height checkpoint every 1000 events to avoid losing progress
                if events_processed_count % 1000 == 0:
                    update_last_token_height(tokens_collection, current_height)
                    logging.info(f"Checkpoint: Processed {events_processed_count} token events up to height {current_height}")

            except Exception as event_error:
                logging.error(f"Error processing {event_type} event at height {event_height}: {event_error}")
                logging.error(f"Event params: {event_params}")
                # Continue processing other events instead of failing completely
                continue

        # Update the final height after processing all events
        if current_height > last_token_height:
            update_last_token_height(tokens_collection, current_height)
            logging.info(f"Successfully processed {events_processed_count} token events. Final height: {current_height}")
        else:
            logging.info("No new token heights processed")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise  # Re-raise to ensure the error is visible
    finally:
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
            logging.info("Tokens lock file removed.")

if __name__ == "__main__":
    process_tokens()