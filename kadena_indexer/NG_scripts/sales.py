from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
import tempfile
from collections import defaultdict

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

lock_file_path = os.path.join(tempfile.gettempdir(), "ngsales_script.lock")

class SalesProcessor:
    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self.metadata_collection = None
        self.sales_collection = None
        self.auctions_collection = None
        self.sales_data_collection = None
        
        self.event_queries = {
            'SALE': 'n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.SALE',
            'FIXED-SALE-OFFER': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-fixed-sale.FIXED-SALE-OFFER',
            'FIXED-SALE-WITHDRAWN': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-fixed-sale.FIXED-SALE-WITHDRAWN',
            'FIXED-SALE-BOUGHT': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-fixed-sale.FIXED-SALE-BOUGHT',
            'AUCTION-SALE-OFFER': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-auction-sale.AUCTION-SALE-OFFER',
            'AUCTION-SALE-BOUGHT': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-auction-sale.AUCTION-SALE-BOUGHT',
            'PLACE-BID': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-auction-sale.PLACE-BID',
            'AUCTION-SALE-WITHDRAWN': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-auction-sale.AUCTION-SALE-WITHDRAWN'
        }
        
        # Batch operations storage
        self.sales_updates = []
        self.auctions_updates = []
        self.sales_data_updates = []
        self.batch_size = 100

    def connect_to_mongodb(self):
        """Establish MongoDB connection"""
        try:
            mongo_uri = os.environ.get('Mongo_URI')
            if not mongo_uri:
                raise ValueError("Mongo_URI environment variable not set")
            
            self.mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
            self.mongo_db = self.mongo_client['nft_events']
            
            self.metadata_collection = self.mongo_db['script_metadata']
            self.sales_collection = self.mongo_db['chain8ngsales']
            self.auctions_collection = self.mongo_db['chain8ngauctions']
            self.sales_data_collection = self.mongo_db['chain8ngsalesdata']
            
            # Verify connection
            self.mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB!")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def get_last_ngsales_height(self):
        """Get the last processed ngsales height"""
        try:
            result = self.metadata_collection.find_one({"key": "last_ngsales_height"})
            return int(result["value"]) if result else 0
        except Exception as e:
            logger.error(f"Error getting last ngsales height: {e}")
            return 0

    def update_last_ngsales_height(self, last_height):
        """Update the last processed height"""
        try:
            self.metadata_collection.update_one(
                {"key": "last_ngsales_height"},
                {"$set": {"value": last_height}},
                upsert=True
            )
            logger.info(f"Updated last ngsales height to: {last_height}")
        except Exception as e:
            logger.error(f"Error updating last ngsales height: {e}")

    def extract_price(self, price_data):
        """Extract price from various formats"""
        try:
            if isinstance(price_data, dict):
                return float(price_data.get('decimal', price_data.get('amount', 0)))
            elif isinstance(price_data, (int, float, str)):
                return float(price_data)
            else:
                logger.warning(f"Unexpected price format: {price_data}")
                return 0
        except (ValueError, TypeError) as e:
            logger.error(f"Error extracting price from {price_data}: {e}")
            return 0

    def validate_event_params(self, event_type, params):
        """Validate event parameters based on event type"""
        if not isinstance(params, list):
            return False, f"Expected params to be a list, got {type(params)}"
        
        required_lengths = {
            'SALE': 5,                      # [nft_id, seller, amount, timeout, transaction_id]
            'FIXED-SALE-OFFER': 3,         # [sale_id, nft_id, sale_price]
            'FIXED-SALE-WITHDRAWN': 2,     # [sale_id, nft_id]
            'FIXED-SALE-BOUGHT': 2,        # [sale_id, nft_id]
            'AUCTION-SALE-OFFER': 3,       # [sale_id, nft_id, offer_price]
            'AUCTION-SALE-BOUGHT': 3,      # [sale_id, nft_id, price]
            'PLACE-BID': 4,                # [sale_id, nft_id, buyer, bid_price]
            'AUCTION-SALE-WITHDRAWN': 2    # [sale_id, nft_id]
        }
        
        expected_length = required_lengths.get(event_type)
        if expected_length and len(params) < expected_length:
            return False, f"Expected at least {expected_length} parameters for {event_type}, got {len(params)}"
        
        return True, ""

    def process_sale_event(self, event_params, height):
        """Process SALE event"""
        try:
            nft_id, seller, amount, timeout, transaction_id = event_params[:5]
            
            sale_data = {
                "_id": transaction_id,
                "nftid": nft_id,
                "seller": seller,
                "amount": amount,
                "timeout": timeout,
                "updatedAt": height,
                "initiatedat": height
            }
            
            self.sales_updates.append({
                'filter': {"_id": transaction_id},
                'update': {"$set": sale_data},
                'upsert': True
            })
            
            logger.debug(f"Queued SALE update for transaction {transaction_id}")
            
        except Exception as e:
            logger.error(f"Error processing SALE event: {e}")

    def process_fixed_sale_offer_event(self, event_params, height):
        """Process FIXED-SALE-OFFER event"""
        try:
            sale_id, nft_id, sale_price = event_params[:3]
            sale_price = self.extract_price(sale_price)
            
            sale_data = {
                "_id": sale_id,
                "nftid": nft_id,
                "salePrice": sale_price,
                "isActive": True,
                "saleType": "fixed",
                "updatedAt": height,
                "initiatedat": height
            }
            
            self.sales_updates.append({
                'filter': {"_id": sale_id},
                'update': {"$set": sale_data},
                'upsert': True
            })
            
            self.sales_data_updates.append({
                'filter': {"_id": sale_id},
                'update': {"$set": {
                    "nftid": nft_id,
                    "pendingSale": sale_price,
                    "sale_amount": 0
                }},
                'upsert': True
            })
            
            logger.debug(f"Queued FIXED-SALE-OFFER updates for sale {sale_id}")
            
        except Exception as e:
            logger.error(f"Error processing FIXED-SALE-OFFER event: {e}")

    def process_fixed_sale_withdrawn_event(self, event_params, height):
        """Process FIXED-SALE-WITHDRAWN event"""
        try:
            sale_id, nft_id = event_params[:2]
            
            self.sales_updates.append({
                'filter': {"_id": sale_id},
                'update': {"$set": {"isActive": False, "updatedAt": height}},
                'upsert': False
            })
            
            logger.debug(f"Queued FIXED-SALE-WITHDRAWN update for sale {sale_id}")
            
        except Exception as e:
            logger.error(f"Error processing FIXED-SALE-WITHDRAWN event: {e}")

    def process_fixed_sale_bought_event(self, event_params, height):
        """Process FIXED-SALE-BOUGHT event"""
        try:
            sale_id, nft_id = event_params[:2]
            
            self.sales_updates.append({
                'filter': {"_id": sale_id},
                'update': {"$set": {"isActive": False, "updatedAt": height}},
                'upsert': False
            })
            
            # Get pending sale amount for sales data update
            current_doc = self.sales_data_collection.find_one({"_id": sale_id})
            pending_sale_amount = current_doc.get('pendingSale', 0) if current_doc else 0
            
            self.sales_data_updates.append({
                'filter': {"_id": sale_id},
                'update': {"$set": {
                    "nftid": nft_id,
                    "sale_amount": pending_sale_amount,
                    "pendingSale": 0
                }},
                'upsert': False
            })
            
            logger.debug(f"Queued FIXED-SALE-BOUGHT updates for sale {sale_id}")
            
        except Exception as e:
            logger.error(f"Error processing FIXED-SALE-BOUGHT event: {e}")

    def process_auction_sale_offer_event(self, event_params, height):
        """Process AUCTION-SALE-OFFER event"""
        try:
            sale_id, nft_id, offer_price = event_params[:3]
            
            auction_data = {
                "_id": sale_id,
                "nftid": nft_id,
                "bids": [],
                "highest-bid": offer_price,
                "reserve-price": offer_price
            }
            
            self.auctions_updates.append({
                'filter': {"_id": sale_id},
                'update': {"$set": auction_data},
                'upsert': True
            })
            
            logger.debug(f"Queued AUCTION-SALE-OFFER update for auction {sale_id}")
            
        except Exception as e:
            logger.error(f"Error processing AUCTION-SALE-OFFER event: {e}")

    def process_auction_sale_bought_event(self, event_params, height):
        """Process AUCTION-SALE-BOUGHT event"""
        try:
            sale_id, nft_id, price = event_params[:3]
            price = self.extract_price(price)
            
            self.sales_updates.append({
                'filter': {"_id": sale_id},
                'update': {"$set": {"isActive": False, "updatedAt": height}},
                'upsert': False
            })
            
            self.sales_data_updates.append({
                'filter': {"_id": sale_id},
                'update': {"$set": {
                    "nftid": nft_id,
                    "sale_amount": price
                }},
                'upsert': True
            })
            
            logger.debug(f"Queued AUCTION-SALE-BOUGHT updates for sale {sale_id}")
            
        except Exception as e:
            logger.error(f"Error processing AUCTION-SALE-BOUGHT event: {e}")

    def process_place_bid_event(self, event_params, height):
        """Process PLACE-BID event"""
        try:
            sale_id, nft_id, buyer, bid_price = event_params[:4]
            
            bid_data = {
                "bid": bid_price,
                "bidder": buyer
            }
            
            self.auctions_updates.append({
                'filter': {"_id": sale_id},
                'update': {"$push": {"bids": bid_data}, "$set": {"highest-bid": bid_price}},
                'upsert': False
            })
            
            logger.debug(f"Queued PLACE-BID update for auction {sale_id}")
            
        except Exception as e:
            logger.error(f"Error processing PLACE-BID event: {e}")

    def process_auction_sale_withdrawn_event(self, event_params, height):
        """Process AUCTION-SALE-WITHDRAWN event"""
        try:
            sale_id, nft_id = event_params[:2]
            
            self.sales_updates.append({
                'filter': {"_id": sale_id},
                'update': {"$set": {"isActive": False, "updatedAt": height}},
                'upsert': False
            })
            
            logger.debug(f"Queued AUCTION-SALE-WITHDRAWN update for sale {sale_id}")
            
        except Exception as e:
            logger.error(f"Error processing AUCTION-SALE-WITHDRAWN event: {e}")

    def execute_batch_updates(self):
        """Execute all queued batch updates"""
        try:
            # Execute sales updates
            if self.sales_updates:
                for update in self.sales_updates:
                    self.sales_collection.update_one(
                        update['filter'],
                        update['update'],
                        upsert=update['upsert']
                    )
                logger.info(f"Executed {len(self.sales_updates)} sales updates")
                self.sales_updates.clear()

            # Execute auctions updates
            if self.auctions_updates:
                for update in self.auctions_updates:
                    self.auctions_collection.update_one(
                        update['filter'],
                        update['update'],
                        upsert=update['upsert']
                    )
                logger.info(f"Executed {len(self.auctions_updates)} auctions updates")
                self.auctions_updates.clear()

            # Execute sales data updates
            if self.sales_data_updates:
                for update in self.sales_data_updates:
                    self.sales_data_collection.update_one(
                        update['filter'],
                        update['update'],
                        upsert=update['upsert']
                    )
                logger.info(f"Executed {len(self.sales_data_updates)} sales data updates")
                self.sales_data_updates.clear()

        except Exception as e:
            logger.error(f"Error executing batch updates: {e}")
            raise

    def process_events_by_height(self, events_by_height):
        """Process all events for a specific height"""
        for height in sorted(events_by_height.keys()):
            logger.info(f"Processing events at height {height}")
            
            # Process events in logical order
            event_order = [
                'SALE', 'FIXED-SALE-OFFER', 'AUCTION-SALE-OFFER', 
                'PLACE-BID', 'FIXED-SALE-BOUGHT', 'AUCTION-SALE-BOUGHT',
                'FIXED-SALE-WITHDRAWN', 'AUCTION-SALE-WITHDRAWN'
            ]
            
            for event_type in event_order:
                if event_type in events_by_height[height]:
                    events = events_by_height[height][event_type]
                    for event in events:
                        event_params = event.get('params', [])
                        
                        # Validate event parameters
                        is_valid, error_msg = self.validate_event_params(event_type, event_params)
                        if not is_valid:
                            logger.error(f"Invalid {event_type} event at height {height}: {error_msg}")
                            continue
                        
                        # Process the event
                        if event_type == 'SALE':
                            self.process_sale_event(event_params, height)
                        elif event_type == 'FIXED-SALE-OFFER':
                            self.process_fixed_sale_offer_event(event_params, height)
                        elif event_type == 'FIXED-SALE-WITHDRAWN':
                            self.process_fixed_sale_withdrawn_event(event_params, height)
                        elif event_type == 'FIXED-SALE-BOUGHT':
                            self.process_fixed_sale_bought_event(event_params, height)
                        elif event_type == 'AUCTION-SALE-OFFER':
                            self.process_auction_sale_offer_event(event_params, height)
                        elif event_type == 'AUCTION-SALE-BOUGHT':
                            self.process_auction_sale_bought_event(event_params, height)
                        elif event_type == 'PLACE-BID':
                            self.process_place_bid_event(event_params, height)
                        elif event_type == 'AUCTION-SALE-WITHDRAWN':
                            self.process_auction_sale_withdrawn_event(event_params, height)
            
            # Execute batch updates for this height
            self.execute_batch_updates()
            
            # Update last processed height after successfully processing all events at this height
            self.update_last_ngsales_height(height)

    def fetch_and_organize_events(self, last_ngsales_height):
        """Fetch all events and organize them by height and type"""
        events_by_height = defaultdict(lambda: defaultdict(list))
        total_events = 0
        
        for event_type, collection_name in self.event_queries.items():
            logger.info(f"Fetching {event_type} events...")
            
            try:
                events = list(
                    self.mongo_db[collection_name].find(
                        {"height": {"$gt": last_ngsales_height}}
                    ).sort("height", 1)
                )
                
                logger.info(f"Found {len(events)} {event_type} events")
                total_events += len(events)
                
                for event in events:
                    height = event.get('height')
                    if height is not None:
                        events_by_height[height][event_type].append(event)
                        
            except Exception as e:
                logger.error(f"Error fetching {event_type} events: {e}")
                continue
        
        logger.info(f"Total events to process: {total_events}")
        return events_by_height

def process_sales():
    """Main processing function"""
    if os.path.exists(lock_file_path):
        logger.info(f"Lock file exists at {lock_file_path}")
        logger.info("Another instance of the script is running. Exiting.")
        sys.exit()

    # Create lock file
    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")
        logger.info("Sales Lock file created.")

    processor = SalesProcessor()
    
    try:
        # Connect to MongoDB
        processor.connect_to_mongodb()
        
        # Get last processed height
        last_ngsales_height = processor.get_last_ngsales_height()
        logger.info(f"Last processed height: {last_ngsales_height}")
        
        # Fetch and organize all events
        events_by_height = processor.fetch_and_organize_events(last_ngsales_height)
        
        if not events_by_height:
            logger.info("No events found to process.")
            return
        
        # Process events by height in ascending order
        processor.process_events_by_height(events_by_height)
        
        logger.info("Sales processing completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")
        raise
    finally:
        # Clean up
        if processor.mongo_client:
            processor.mongo_client.close()
        
        if os.path.exists(lock_file_path):
            os.remove(lock_file_path)
            logger.info("Lock file removed.")

if __name__ == "__main__":
    process_sales()