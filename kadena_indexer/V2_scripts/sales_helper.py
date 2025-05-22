from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
from dotenv import load_dotenv
import tempfile
from collections import defaultdict
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
        mongo_db = mongo_client['nft_events']
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
        
        print(f"Starting from height: {last_processed_height + 1}")

        gas_addresses = [
            '6d87fd6e5e47185cb421459d2888bddba7a6c0f2c4ae5246d5f38f993818bb89',
            '99cb7008d7d70c94f138cc366a825f0d9c83a8a2f4ba82c86c666e0ab6fecf3a',
        ]

        # Get all unique heights greater than last processed height for both event types
        all_heights = set()
        for event_type in ['BUY', 'WITHDRAW']:
            heights = events_collection.distinct(
                "height", 
                {
                    "qual_name": f"marmalade-v2.ledger.{event_type}",
                    "height": {"$gt": last_processed_height}
                }
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
            
            for event_type in ['BUY', 'WITHDRAW']:
                events = list(events_collection.find({
                    "qual_name": f"marmalade-v2.ledger.{event_type}",
                    "height": current_height
                }).sort("_id", 1))  # Use _id for consistent ordering
                
                if events:
                    events_at_height[event_type] = events
                    print(f"  Found {len(events)} {event_type} events at height {current_height}")

            # Process all events at this height
            height_processed_successfully = True
            
            try:
                for event_type, events in events_at_height.items():
                    for event in events:
                        success = process_single_event(
                            event, event_type, gas_addresses,
                            events_collection, blocks_collection, 
                            items_for_sale_collection, sales_data_collection
                        )
                        if not success:
                            height_processed_successfully = False
                            print(f"Failed to process {event_type} event at height {current_height}")
                            break
                    
                    if not height_processed_successfully:
                        break

                # Only update the height if all events at this height were processed successfully
                if height_processed_successfully:
                    metadata_collection.update_one(
                        {"key": "last_v2_sales_processed_height"},
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

        final_height = current_height if height_processed_successfully else last_processed_height
        print(f"\nProcessing completed. Last processed height: {final_height}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
            print("Lock file removed.")

def process_single_event(event, event_type, gas_addresses, events_collection, 
                        blocks_collection, items_for_sale_collection, sales_data_collection):
    """Process a single event and return True if successful, False otherwise"""
    try:
        request_key = event['regKey']
        block_hash = event['block']
        event_params = event['params']
        event_height = event['height']
        
        if not event_params or len(event_params) == 0:
            print(f"Invalid event parameters for {event_type} at height {event_height}")
            return False
            
        mongo_db_id = event_params[-1]
        
        print(f"  Processing {event_type} event at height {event_height}...")

        if event_type == 'BUY':
            if len(event_params) < 3:
                print(f"Invalid BUY event parameters: expected at least 3, got {len(event_params)}")
                return False
                
            nftid = event_params[0]
            seller = event_params[1]  
            buyer = event_params[2]

            # Get block data for creation time
            block_data = blocks_collection.find_one({"hash": block_hash})
            creation_time = block_data['creation_time'] if block_data else None

            if not creation_time:
                print(f"No creation_time found for block hash: {block_hash}")
                return False

            # Find associated transfer events
            associated_events = events_collection.find({
                "request_key": request_key,
                "qual_name": {"$regex": "^coin\\.TRANSFER"}
            })

            relevant_transfer = None
            max_amount = 0

            for transfer_event in associated_events:
                try:
                    transfer_params = transfer_event.get('params', [])
                    if not transfer_params:
                        continue
                        
                    # Check if this transfer involves gas addresses (skip if it does)
                    contains_gas_address = any(gas in str(param) for param in transfer_params for gas in gas_addresses)
                    
                    if not contains_gas_address:
                        amount = transfer_params[-1]
                        if isinstance(amount, (int, float)) and amount > max_amount:
                            max_amount = amount
                            relevant_transfer = transfer_params
                except (IndexError, TypeError) as e:
                    print(f"Error processing transfer event: {e}")
                    continue

            if relevant_transfer and max_amount > 0:
                # Check if sales record already exists
                existing_sale = sales_data_collection.find_one({"_id": mongo_db_id})
                if not existing_sale:
                    try:
                        sales_data_collection.insert_one({
                            "_id": mongo_db_id,
                            "nftid": nftid,
                            "buyer": buyer,
                            "seller": seller,
                            "sale_amount": max_amount,
                            "creation_time": creation_time,
                        })
                        print(f"    Inserted sale record: NFT {nftid}, Amount: {max_amount}")
                    except Exception as e:
                        print(f"Error inserting sale record: {e}")
                        return False
                else:
                    print(f"    Sale record already exists for {mongo_db_id}")
                    
        elif event_type == 'WITHDRAW':
            # For WITHDRAW events, we might need different processing logic
            # For now, we just acknowledge the event
            print(f"    Processed WITHDRAW event for {mongo_db_id}")

        # Update the sale item status regardless of event type
        try:
            result = items_for_sale_collection.update_one(
                {"_id": mongo_db_id}, 
                {"$set": {"isActive": False}}
            )
            if result.matched_count > 0:
                print(f"    Updated sale item {mongo_db_id} to inactive")
            else:
                print(f"    Sale item {mongo_db_id} not found for status update")
        except Exception as e:
            print(f"Error updating sale item status: {e}")
            return False

        return True
        
    except Exception as e:
        print(f"Error processing {event_type} event: {e}")
        return False

if __name__ == "__main__":
    process_v2_sales_helper()