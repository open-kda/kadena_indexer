from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
import tempfile
load_dotenv()

lock_file_path = os.path.join(tempfile.gettempdir(), "ngsales_script.lock")

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_sales():
    if os.path.exists(lock_file_path):
        logging.info("Another instance of the script is running. Exiting.")
        sys.exit()

    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")

    logging.info("Lock file created.")

    def get_last_ngsales_height(metadata_collection):
        logging.info("Fetching last ngsales height...")
        result = metadata_collection.find_one({"key": "last_ngsales_height"})
        return int(result["value"]) if result else 0

    def update_last_ngsales_height(metadata_collection, last_height):
        metadata_collection.update_one(
            {"key": "last_ngsales_height"},
            {"$set": {"value": last_height, "updated_at": datetime.utcnow()}},
            upsert=True
        )
        logging.info(f"Updated last processed height to {last_height}")

    def extract_price(price_data):
        if isinstance(price_data, dict):
            return float(price_data.get('decimal', price_data.get('amount', 0)))
        elif isinstance(price_data, (int, float, str)):
            return float(price_data)
        else:
            logging.warning(f"Unexpected price format: {price_data}")
            return 0

    def get_all_events_at_height_range(mongo_db, event_queries, start_height, end_height=None):
        """Get all events within a height range, sorted by height"""
        all_events = []
        
        query = {"height": {"$gt": start_height}}
        if end_height is not None:
            query["height"]["$lte"] = end_height
            
        for event_type, collection_name in event_queries.items():
            events = list(
                mongo_db[collection_name].find(query).sort("height", 1)
            )
            for event in events:
                event['event_type'] = event_type
                all_events.append(event)
        
        # Sort all events by height to ensure sequential processing
        all_events.sort(key=lambda x: x['height'])
        return all_events

    try:
        mongo_uri = os.environ.get('Mongo_URI')
        if not mongo_uri:
            raise ValueError("Mongo_URI environment variable not set")

        mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        mongo_db = mongo_client['nft_events']

        metadata_collection = mongo_db['script_metadata']
        sales_collection = mongo_db['chain8ngsales']
        auctions_collection = mongo_db['chain8ngauctions']
        sales_data_collection = mongo_db['chain8ngsalesdata']

        mongo_client.admin.command('ping')
        logging.info("Successfully connected to MongoDB!")

        last_ngsales_height = get_last_ngsales_height(metadata_collection)
        logging.info(f"Starting from height: {last_ngsales_height}")

        event_queries = {
            'SALE': 'n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.SALE',
            'FIXED-SALE-OFFER': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-fixed-sale.FIXED-SALE-OFFER',
            'FIXED-SALE-WITHDRAWN': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-fixed-sale.FIXED-SALE-WITHDRAWN',
            'FIXED-SALE-BOUGHT': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-fixed-sale.FIXED-SALE-BOUGHT',
            'AUCTION-SALE-OFFER': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-auction-sale.AUCTION-SALE-OFFER',
            'AUCTION-SALE-BOUGHT': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-auction-sale.AUCTION-SALE-BOUGHT',
            'PLACE-BID': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-auction-sale.PLACE-BID',
            'AUCTION-SALE-WITHDRAWN': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-auction-sale.AUCTION-SALE-WITHDRAWN'
        }

        # Get all events after the last processed height, sorted by height
        all_events = get_all_events_at_height_range(mongo_db, event_queries, last_ngsales_height)
        
        if not all_events:
            logging.info("No new events to process")
            return

        logging.info(f"Found {len(all_events)} total events to process")

        current_height = last_ngsales_height
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
                if event_type == 'SALE':
                    nft_id, seller, amount, timeout, transaction_id = event_params
                    sale_data = {
                        "_id": transaction_id,
                        "nftid": nft_id,
                        "seller": seller,
                        "amount": amount,
                        "timeout": timeout,
                        "updatedAt": event_height,
                        "initiatedat": event_height
                    }
                    sales_collection.update_one(
                        {"_id": transaction_id},
                        {"$set": sale_data},
                        upsert=True
                    )

                elif event_type == 'FIXED-SALE-OFFER':
                    sale_id, nft_id, sale_price = event_params
                    sale_price = extract_price(sale_price)
                    sale_data = {
                        "_id": sale_id,
                        "nftid": nft_id,
                        "salePrice": sale_price,
                        "isActive": True,
                        "saleType": "fixed",
                        "updatedAt": event_height,
                        "initiatedat": event_height
                    }
                    sales_collection.update_one(
                        {"_id": sale_id},
                        {"$set": sale_data},
                        upsert=True
                    )
                    sales_data_collection.update_one(
                        {"_id": sale_id},
                        {"$set": {
                            "nftid": nft_id,
                            "seller": sale_data.get('seller'),
                            "pendingSale": sale_price,
                            "sale_amount": 0
                        }},
                        upsert=True
                    )

                elif event_type == 'FIXED-SALE-WITHDRAWN':
                    sale_id, nft_id = event_params
                    sales_collection.update_one(
                        {"_id": sale_id},
                        {"$set": {"isActive": False, "updatedAt": event_height}}
                    )

                elif event_type == 'FIXED-SALE-BOUGHT':
                    sale_id, nft_id = event_params
                    sales_collection.update_one(
                        {"_id": sale_id},
                        {"$set": {"isActive": False, "updatedAt": event_height}}
                    )
                    current_doc = sales_data_collection.find_one({"_id": sale_id})
                    pending_sale_amount = current_doc.get('pendingSale', 0) if current_doc else 0
                    sales_data_collection.update_one(
                        {"_id": sale_id},
                        {"$set": {
                            "nftid": nft_id,
                            "sale_amount": pending_sale_amount,
                            "pendingSale": 0
                        }},
                        upsert=True
                    )

                elif event_type == 'AUCTION-SALE-OFFER':
                    sale_id, nft_id, offer_price = event_params
                    offer_price = extract_price(offer_price)
                    auction_data = {
                        "_id": sale_id,
                        "nftid": nft_id,
                        "bids": [],
                        "highest-bid": offer_price,
                        "reserve-price": offer_price,
                        "updatedAt": event_height,
                        "initiatedat": event_height
                    }
                    auctions_collection.update_one(
                        {"_id": sale_id},
                        {"$set": auction_data},
                        upsert=True
                    )

                elif event_type == 'AUCTION-SALE-BOUGHT':
                    sale_id, nft_id, price = event_params
                    price = extract_price(price)
                    sales_collection.update_one(
                        {"_id": sale_id},
                        {"$set": {"isActive": False, "updatedAt": event_height}}
                    )
                    sales_data_collection.update_one(
                        {"_id": sale_id},
                        {"$set": {
                            "nftid": nft_id,
                            "sale_amount": price,
                            "updatedAt": event_height
                        }},
                        upsert=True
                    )

                elif event_type == 'PLACE-BID':
                    sale_id, nft_id, buyer, bid_price = event_params
                    bid_price = extract_price(bid_price)
                    bid_data = {
                        "bid": bid_price,
                        "bidder": buyer,
                        "timestamp": event_height
                    }
                    auctions_collection.update_one(
                        {"_id": sale_id},
                        {
                            "$push": {"bids": bid_data}, 
                            "$set": {"highest-bid": bid_price, "updatedAt": event_height}
                        }
                    )

                elif event_type == 'AUCTION-SALE-WITHDRAWN':
                    sale_id, nft_id = event_params
                    sales_collection.update_one(
                        {"_id": sale_id},
                        {"$set": {"isActive": False, "updatedAt": event_height}}
                    )

                events_processed_count += 1
                
                # Update height checkpoint every 1000 events to avoid losing progress
                if events_processed_count % 1000 == 0:
                    update_last_ngsales_height(metadata_collection, current_height)
                    logging.info(f"Checkpoint: Processed {events_processed_count} events up to height {current_height}")

            except Exception as event_error:
                logging.error(f"Error processing {event_type} event at height {event_height}: {event_error}")
                logging.error(f"Event params: {event_params}")
                # Continue processing other events instead of failing completely
                continue

        # Update the final height after processing all events
        if current_height > last_ngsales_height:
            update_last_ngsales_height(metadata_collection, current_height)
            logging.info(f"Successfully processed {events_processed_count} events. Final height: {current_height}")
        else:
            logging.info("No new heights processed")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise  # Re-raise to ensure the error is visible
    finally:
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
            logging.info("Lock file removed.")

if __name__ == "__main__":
    process_sales()