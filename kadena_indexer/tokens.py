from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
import json
from dotenv import load_dotenv
import tempfile
load_dotenv()

lock_file_path = os.path.join(tempfile.gettempdir(), "token_tk_script.lock")

def process_tokens():
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
        mongo_db = mongo_client['kadena_events']
        tokens_collection = mongo_db['chain8v2ngtokens']
        ledger_collection = mongo_db['chain8v2ngledger']

        # Ping MongoDB to verify the connection
        mongo_client.admin.command('ping')
        print("Successfully connected to MongoDB!")

        # Query MongoDB for the last processed token height, here I simulate it by getting from tokens_collection.
        last_token_height = tokens_collection.find_one({"_id": "last_token_height"})
        last_token_height = last_token_height['value'] if last_token_height else 0

        event_queries = {
            'MINT': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.MINT",
            'RECONCILE': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.RECONCILE",
            'SUPPLY': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.SUPPLY",
            'TOKEN-CREATE': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.TOKEN-CREATE",
        }

        # Processing logic for different event types
        #'MINT',
        for event_type in ['MINT','RECONCILE','SUPPLY','TOKEN-CREATE']:
            print(f"Processing {event_type} events...")

            # Fetching relevant events from MongoDB (for simulation purposes)
            events = list(
                mongo_db[event_queries.get(event_type)].find(
                    {
                        "height": {"$gt": last_token_height}
                    }
                ).sort("height", 1)
            )
            #print("events:", events)

            if not events:
                print("No events found for the given criteria.")
            else:
                print(f"Number of events: {len(events)}")


            for event in events:
                #request_key = event['request_key']
                #block_hash = event['block']
                event_params = event['params']
                event_height = event['height']
                print(f"Processing {event_type} event at height {event['height']}...")

                if event_type == 'MINT':
                    # Parse MINT event parameters
                    nft_id, account, balance = event_params
                    ledger_collection.update_one(
                        {"nftId": nft_id},
                        {"$set": {"account": account, "balance": balance}},
                        upsert=True
                    )

                elif event_type == 'SUPPLY':
                    # Parse SUPPLY event parameters
                    nft_id, supply = event_params
                    tokens_collection.update_one(
                        {"_id": nft_id},
                        {"$set": {"supply": supply}},
                        upsert=True
                    )

                elif event_type == 'TOKEN-CREATE':
                    nft_id, uri = event_params[0], event_params[1]
                    policies = event_params[3]
                    
                    if not isinstance(policies, list):
                        print(f"Expected policies to be a list, but got: {type(policies)}")
                        continue

                    grouped_policies = {}
                    for policy_str in policies:
                        try:
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

                    tokens_collection.update_one(
                        {"_id": nft_id},
                        {"$set": {"uri": uri, "groupedPolicies": grouped_policies}},
                        upsert=True
                    )

                elif event_type == 'RECONCILE':
                    nft_id = event_params[0]
                    previous_owner_info = event_params[2]
                    new_owner_info = event_params[3]

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

                    print(f"Processed RECONCILE event for NFT {nft_id}: {previous_account} -> {new_account}")

                # Update last processed height (if needed)
                last_token_height = max(last_token_height, event_height)
                tokens_collection.update_one(
                    {"_id": "last_token_height"},
                    {"$set": {"value": last_token_height}},
                    upsert=True
                )

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)

if __name__ == "__main__":
    process_tokens()
