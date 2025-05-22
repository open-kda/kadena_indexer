from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
import json
from dotenv import load_dotenv
import tempfile
from collections import defaultdict
load_dotenv()

lock_file_path = os.path.join(tempfile.gettempdir(), "v2_token_script.lock")

def process_v2_tokens():
    if os.path.exists(lock_file_path):
        print(lock_file_path)
        print("Another instance of the script is running. Exiting.")
        sys.exit()

    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")
        print("Tokens Lock file created.")

    try:
        mongo_uri = os.environ.get('Mongo_URI')

        # MongoDB client connection
        mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        mongo_db = mongo_client['nft_events']
        tokens_collection = mongo_db['chain8v2tokens']
        ledger_collection = mongo_db['chain8v2ledger']

        # Ping MongoDB to verify the connection
        mongo_client.admin.command('ping')
        print("Successfully connected to MongoDB!")

        # Query MongoDB for the last processed token height
        last_token_height_doc = tokens_collection.find_one({"_id": "last_v2_token_height"})
        last_token_height = last_token_height_doc['value'] if last_token_height_doc else 0
        
        print(f"Starting from height: {last_token_height + 1}")

        event_queries = {
            'MINT': "marmalade-v2.ledger.MINT",
            'RECONCILE': "marmalade-v2.ledger.RECONCILE",
            'SUPPLY': "marmalade-v2.ledger.SUPPLY",
            'TOKEN': "marmalade-v2.ledger.TOKEN",
        }

        # Get all unique heights greater than last processed height
        all_heights = set()
        for event_type in ['MINT', 'RECONCILE', 'SUPPLY', 'TOKEN']:
            heights = mongo_db[event_queries.get(event_type)].distinct(
                "height", 
                {"height": {"$gt": last_token_height}}
            )
            all_heights.update(heights)
        
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
            
            for event_type in ['MINT', 'RECONCILE', 'SUPPLY', 'TOKEN']:
                events = list(
                    mongo_db[event_queries.get(event_type)].find(
                        {"height": current_height}
                    ).sort("_id", 1)  # Use _id for consistent ordering
                )
                if events:
                    events_at_height[event_type] = events
                    print(f"  Found {len(events)} {event_type} events at height {current_height}")

            # Process all events at this height
            height_processed_successfully = True
            
            try:
                for event_type, events in events_at_height.items():
                    for event in events:
                        success = process_single_event(event, event_type, tokens_collection, ledger_collection)
                        if not success:
                            height_processed_successfully = False
                            print(f"Failed to process {event_type} event at height {current_height}")
                            break
                    
                    if not height_processed_successfully:
                        break

                # Only update the height if all events at this height were processed successfully
                if height_processed_successfully:
                    tokens_collection.update_one(
                        {"_id": "last_v2_token_height"},
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

        print(f"\nProcessing completed. Last processed height: {current_height if height_processed_successfully else last_token_height}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
            print("Lock file removed.")

def process_single_event(event, event_type, tokens_collection, ledger_collection):
    """Process a single event and return True if successful, False otherwise"""
    try:
        event_params = event['params']
        event_height = event['height']
        
        print(f"  Processing {event_type} event at height {event_height}...")

        if event_type == 'MINT':
            # Parse MINT event parameters
            if len(event_params) < 3:
                print(f"Invalid MINT event parameters: {event_params}")
                return False
                
            nft_id, account, balance = event_params[0], event_params[1], event_params[2]
            ledger_collection.update_one(
                {"nftId": nft_id},
                {"$set": {"account": account, "balance": balance}},
                upsert=True
            )

        elif event_type == 'SUPPLY':
            # Parse SUPPLY event parameters
            if len(event_params) < 2:
                print(f"Invalid SUPPLY event parameters: {event_params}")
                return False
                
            nft_id, supply = event_params[0], event_params[1]
            tokens_collection.update_one(
                {"_id": nft_id},
                {"$set": {"supply": supply}},
                upsert=True
            )

        elif event_type == 'TOKEN':
            if len(event_params) < 4:
                print(f"Invalid TOKEN event parameters: {event_params}")
                return False
                
            nft_id, uri = event_params[0], event_params[3]
            policies = event_params[2]
            
            if not isinstance(policies, list):
                print(f"Expected policies to be a list, but got: {type(policies)}")
                return False

            grouped_policies = {}
            for policy_str in policies:
                try:
                    if not isinstance(policy_str, str):
                        print(f"Expected policy to be a string, but got: {type(policy_str)}")
                        continue
                        
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
                        print(f"Unexpected policy format: {policy_str}")
                except AttributeError:
                    print(f"Error processing policy string '{policy_str}': AttributeError")
                    continue

            tokens_collection.update_one(
                {"_id": nft_id},
                {"$set": {"uri": uri, "groupedPolicies": grouped_policies}},
                upsert=True
            )

        elif event_type == 'RECONCILE':
            if len(event_params) < 4:
                print(f"Invalid RECONCILE event parameters: {event_params}")
                return False
                
            nft_id = event_params[0]
            previous_owner_info = event_params[2]
            new_owner_info = event_params[3]

            if not isinstance(previous_owner_info, dict) or not isinstance(new_owner_info, dict):
                print(f"Invalid owner info format in RECONCILE event")
                return False

            if 'account' not in previous_owner_info or 'account' not in new_owner_info:
                print(f"Missing account info in RECONCILE event")
                return False

            previous_account = previous_owner_info['account']
            new_account = new_owner_info['account']

            ledger_collection.update_one(
                {"nftId": nft_id, "account": previous_account},
                {"$set": {"balance": 0}}
            )

            ledger_collection.update_one(
                {"nftId": nft_id, "account": new_account},
                {"$set": {"balance": 1}},
                upsert=True
            )

            print(f"    Processed RECONCILE event for NFT {nft_id}: {previous_account} -> {new_account}")

        return True
        
    except Exception as e:
        print(f"Error processing {event_type} event: {e}")
        return False

if __name__ == "__main__":
    process_v2_tokens()