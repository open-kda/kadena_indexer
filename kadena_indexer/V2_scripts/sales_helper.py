from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
from dotenv import load_dotenv
import tempfile
import logging
from collections import defaultdict

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

lock_file_path = os.path.join(tempfile.gettempdir(), "mongo_script.lock")

class V2SalesProcessor:
    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self.events_collection = None
        self.blocks_collection = None
        self.items_for_sale_collection = None
        self.sales_data_collection = None
        self.metadata_collection = None
        
        # Gas addresses to exclude from transfer calculations
        self.gas_addresses = [
            '6d87fd6e5e47185cb421459d2888bddba7a6c0f2c4ae5246d5f38f993818bb89',
            '99cb7008d7d70c94f138cc366a825f0d9c83a8a2f4ba82c86c666e0ab6fecf3a',
        ]
        
        # Batch operations
        self.sales_inserts = []
        self.sale_updates = []
        self.batch_size = 100

    def connect_to_mongodb(self):
        """Establish MongoDB connection"""
        try:
            mongo_uri = os.environ.get('Mongo_URI')
            if not mongo_uri:
                raise ValueError("Mongo_URI environment variable not set")
            
            self.mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
            self.mongo_db = self.mongo_client['nft_events']
            self.events_collection = self.mongo_db['events']
            self.blocks_collection = self.mongo_db['blocks']
            self.items_for_sale_collection = self.mongo_db['chain8v2sales']
            self.sales_data_collection = self.mongo_db['chain8v2salesdata']
            self.metadata_collection = self.mongo_db['script_metadata']
            
            # Verify connection
            self.mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB!")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def get_last_processed_height(self):
        """Get the last processed sales height"""
        try:
            metadata = self.metadata_collection.find_one({"key": "last_v2_sales_processed_height"})
            return int(metadata["value"]) if metadata else 0
        except Exception as e:
            logger.error(f"Error getting last processed height: {e}")
            return 0

    def update_last_processed_height(self, height):
        """Update the last processed height"""
        try:
            self.metadata_collection.update_one(
                {"key": "last_v2_sales_processed_height"},
                {"$set": {"value": height}},
                upsert=True
            )
            logger.info(f"Updated last processed height to: {height}")
        except Exception as e:
            logger.error(f"Error updating last processed height: {e}")

    def validate_event_params(self, event_type, params):
        """Validate event parameters"""
        if not isinstance(params, list):
            return False, f"Expected params to be a list, got {type(params)}"
        
        required_lengths = {
            'BUY': 4,      # [nftid, seller, buyer, mongo_db_id]
            'WITHDRAW': 2   # [?, mongo_db_id]
        }
        
        expected_length = required_lengths.get(event_type)
        if expected_length and len(params) < expected_length:
            return False, f"Expected at least {expected_length} parameters for {event_type}, got {len(params)}"
        
        return True, ""

    def get_block_creation_time(self, block_hash):
        """Get block creation time with caching"""
        try:
            block_data = self.blocks_collection.find_one({"hash": block_hash}, {"creation_time": 1})
            if block_data:
                return block_data.get('creation_time')
            else:
                logger.warning(f"No block data found for hash: {block_hash}")
                return None
        except Exception as e:
            logger.error(f"Error fetching block data for hash {block_hash}: {e}")
            return None

    def find_relevant_transfer(self, request_key):
        """Find the relevant transfer event with the highest amount (excluding gas)"""
        try:
            associated_events = self.events_collection.find({
                "request_key": request_key,
                "qual_name": {"$regex": "^coin\\.TRANSFER"}
            })

            relevant_transfer = None
            max_amount = 0

            for transfer_event in associated_events:
                transfer_params = transfer_event.get('params', [])
                
                # Skip if any gas address is involved
                if any(gas in str(param) for param in transfer_params for gas in self.gas_addresses):
                    continue
                
                # Get the amount (usually the last parameter)
                if transfer_params:
                    amount = transfer_params[-1]
                    if isinstance(amount, (int, float)) and amount > max_amount:
                        max_amount = amount
                        relevant_transfer = transfer_params

            return relevant_transfer, max_amount
            
        except Exception as e:
            logger.error(f"Error finding relevant transfer for request_key {request_key}: {e}")
            return None, 0

    def process_buy_event(self, event):
        """Process BUY event"""
        try:
            request_key = event['regKey']
            block_hash = event['block']
            event_params = event['params']
            mongo_db_id = event_params[-1]
            
            # Validate parameters
            is_valid, error_msg = self.validate_event_params('BUY', event_params)
            if not is_valid:
                logger.error(f"Invalid BUY event: {error_msg}")
                return
            
            nftid = event_params[0]
            seller = event_params[1]
            buyer = event_params[2]

            # Get block creation time
            creation_time = self.get_block_creation_time(block_hash)
            if not creation_time:
                logger.warning(f"Skipping BUY event due to missing block data: {block_hash}")
                return

            # Find relevant transfer
            relevant_transfer, max_amount = self.find_relevant_transfer(request_key)

            if relevant_transfer and max_amount > 0:
                # Check if sale data already exists
                existing_sale = self.sales_data_collection.find_one({"_id": mongo_db_id})
                if not existing_sale:
                    self.sales_inserts.append({
                        "_id": mongo_db_id,
                        "nftid": nftid,
                        "buyer": buyer,
                        "seller": seller,
                        "sale_amount": max_amount,
                        "creation_time": creation_time,
                    })
                    logger.debug(f"Queued sale data insert for NFT {nftid}")

            # Queue sale status update
            self.sale_updates.append({
                'filter': {"_id": mongo_db_id},
                'update': {"$set": {"isActive": False}}
            })
            
        except Exception as e:
            logger.error(f"Error processing BUY event: {e}")

    def process_withdraw_event(self, event):
        """Process WITHDRAW event"""
        try:
            event_params = event['params']
            mongo_db_id = event_params[-1]
            
            # Queue sale status update
            self.sale_updates.append({
                'filter': {"_id": mongo_db_id},
                'update': {"$set": {"isActive": False}}
            })
            
            logger.debug(f"Queued WITHDRAW update for sale {mongo_db_id}")
            
        except Exception as e:
            logger.error(f"Error processing WITHDRAW event: {e}")

    def execute_batch_operations(self):
        """Execute all queued batch operations"""
        try:
            # Insert new sales data
            if self.sales_inserts:
                self.sales_data_collection.insert_many(self.sales_inserts, ordered=False)
                logger.info(f"Inserted {len(self.sales_inserts)} new sales records")
                self.sales_inserts.clear()

            # Update sale statuses
            if self.sale_updates:
                for update in self.sale_updates:
                    self.items_for_sale_collection.update_one(
                        update['filter'],
                        update['update']
                    )
                logger.info(f"Updated {len(self.sale_updates)} sale statuses")
                self.sale_updates.clear()

        except Exception as e:
            logger.error(f"Error executing batch operations: {e}")
            raise

    def fetch_and_organize_events(self, last_processed_height):
        """Fetch all events and organize them by height"""
        events_by_height = defaultdict(list)
        total_events = 0
        
        for event_type in ['BUY', 'WITHDRAW']:
            logger.info(f"Fetching {event_type} events...")
            
            try:
                events = list(
                    self.events_collection.find({
                        "qual_name": f"marmalade-v2.ledger.{event_type}",
                        "height": {"$gt": last_processed_height}
                    }).sort("height", 1)
                )
                
                logger.info(f"Found {len(events)} {event_type} events")
                total_events += len(events)
                
                for event in events:
                    height = event.get('height')
                    if height is not None:
                        event['event_type'] = event_type
                        events_by_height[height].append(event)
                        
            except Exception as e:
                logger.error(f"Error fetching {event_type} events: {e}")
                continue
        
        logger.info(f"Total events to process: {total_events}")
        return events_by_height

    def process_events_by_height(self, events_by_height):
        """Process all events organized by height"""
        for height in sorted(events_by_height.keys()):
            logger.info(f"Processing events at height {height}")
            
            # Process all events at this height
            for event in events_by_height[height]:
                event_type = event['event_type']
                
                if event_type == 'BUY':
                    self.process_buy_event(event)
                elif event_type == 'WITHDRAW':
                    self.process_withdraw_event(event)
            
            # Execute batch operations for this height
            self.execute_batch_operations()
            
            # Update last processed height after successfully processing all events at this height
            self.update_last_processed_height(height)

def process_v2_sales_helper():
    """Main processing function"""
    if os.path.exists(lock_file_path):
        logger.info("Another instance of the script is running. Exiting.")
        sys.exit()

    # Create lock file
    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")
        logger.info("Lock file created.")

    processor = V2SalesProcessor()
    
    try:
        # Connect to MongoDB
        processor.connect_to_mongodb()
        
        # Get last processed height
        last_processed_height = processor.get_last_processed_height()
        logger.info(f"Last processed height: {last_processed_height}")
        
        # Fetch and organize all events
        events_by_height = processor.fetch_and_organize_events(last_processed_height)
        
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
    process_v2_sales_helper()