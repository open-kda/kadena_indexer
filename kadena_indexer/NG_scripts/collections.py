import tempfile
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
import logging
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

lock_file_path = os.path.join(tempfile.gettempdir(), "ng_collection_script.lock")

class CollectionsProcessor:
    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self.tokens_collection = None
        self.collections_collection = None
        self.metadata_collection = None
        
        self.event_queries = {
            'CREATE-COLLECTION': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-collection.CREATE-COLLECTION',
            'ADD-TO-COLLECTION': 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-collection.ADD-TO-COLLECTION'
        }
        
        # Batch operations storage
        self.collections_updates = []
        self.tokens_updates = []
        self.batch_size = 100

    def connect_to_mongodb(self):
        """Establish MongoDB connection"""
        try:
            mongo_uri = os.environ.get('Mongo_URI')
            if not mongo_uri:
                raise ValueError("Mongo_URI environment variable not set")
            
            self.mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
            self.mongo_db = self.mongo_client['nft_events']
            
            self.tokens_collection = self.mongo_db['chain8v2ngtokens']
            self.collections_collection = self.mongo_db['chain8ngcollections']
            self.metadata_collection = self.mongo_db['script_metadata']
            
            # Verify connection
            self.mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB!")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def get_last_collection_height(self):
        """Get the last processed collection height"""
        try:
            metadata_doc = self.metadata_collection.find_one({"key": "last_collection_height"})
            return int(metadata_doc["value"]) if metadata_doc else 0
        except Exception as e:
            logger.error(f"Error getting last collection height: {e}")
            return 0

    def update_last_collection_height(self, height):
        """Update the last processed height"""
        try:
            self.metadata_collection.update_one(
                {"key": "last_collection_height"},
                {"$set": {"value": height}},
                upsert=True
            )
            logger.info(f"Updated last collection height to: {height}")
        except Exception as e:
            logger.error(f"Error updating last collection height: {e}")

    def validate_event_params(self, event_type, params):
        """Validate event parameters based on event type"""
        if not isinstance(params, list):
            return False, f"Expected params to be a list, got {type(params)}"
        
        required_lengths = {
            'CREATE-COLLECTION': 4,    # [collection_id, collection_name, maxSize, collection_creator]
            'ADD-TO-COLLECTION': 2     # [collection_id, nft_id]
        }
        
        expected_length = required_lengths.get(event_type)
        if expected_length and len(params) < expected_length:
            return False, f"Expected at least {expected_length} parameters for {event_type}, got {len(params)}"
        
        return True, ""

    def extract_max_size(self, max_size_data):
        """Extract max size from various formats"""
        try:
            if isinstance(max_size_data, int):
                return max_size_data
            elif isinstance(max_size_data, dict):
                return max_size_data.get('int', 0)
            elif isinstance(max_size_data, str):
                return int(max_size_data)
            else:
                logger.warning(f"Unexpected maxSize format: {max_size_data}")
                return 0
        except (ValueError, TypeError) as e:
            logger.error(f"Error extracting maxSize from {max_size_data}: {e}")
            return 0

    def process_create_collection_event(self, event_params, height):
        """Process CREATE-COLLECTION event"""
        try:
            collection_id, collection_name, max_size, collection_creator = event_params[:4]
            max_size_value = self.extract_max_size(max_size)
            
            collection_data = {
                "_id": collection_id,
                "creator": collection_creator,
                "maxSize": max_size_value,
                "name": collection_name,
                "size": 0,
                "approved": False,
                "tokens": [],
                "lastUpdated": height
            }
            
            self.collections_updates.append({
                'filter': {"_id": collection_id},
                'update': {"$setOnInsert": collection_data},
                'upsert': True,
                'operation_type': 'create'
            })
            
            logger.debug(f"Queued CREATE-COLLECTION update for collection {collection_id}")
            
        except Exception as e:
            logger.error(f"Error processing CREATE-COLLECTION event: {e}")

    def process_add_to_collection_event(self, event_params, height):
        """Process ADD-TO-COLLECTION event"""
        try:
            collection_id, nft_id = event_params[:2]
            
            # First, check if collection exists and get its info
            collection_document = self.collections_collection.find_one({"_id": collection_id})
            if not collection_document:
                logger.warning(f"Collection {collection_id} not found. Skipping NFT {nft_id}")
                return
            
            collection_name = collection_document['name']
            
            # Check if NFT already has this collection assigned
            existing_token = self.tokens_collection.find_one({
                "_id": nft_id,
                "collection.collectionId": collection_id
            })
            
            if existing_token:
                logger.debug(f"NFT {nft_id} already in collection {collection_id}. Skipping.")
                return
            
            # Update token with collection info
            self.tokens_updates.append({
                'filter': {"_id": nft_id},
                'update': {"$set": {
                    "collection": {
                        "collectionId": collection_id, 
                        "collectionName": collection_name
                    },
                    "lastUpdated": height
                }},
                'upsert': True
            })
            
            # Add NFT to collection and increment size
            self.collections_updates.append({
                'filter': {"_id": collection_id},
                'update': {
                    "$addToSet": {"tokens": nft_id},
                    "$inc": {"size": 1},
                    "$set": {"lastUpdated": height}
                },
                'upsert': False,
                'operation_type': 'add_token'
            })
            
            logger.debug(f"Queued ADD-TO-COLLECTION updates for NFT {nft_id} -> collection {collection_id}")
            
        except Exception as e:
            logger.error(f"Error processing ADD-TO-COLLECTION event: {e}")

    def execute_batch_updates(self):
        """Execute all queued batch updates"""
        try:
            # Execute collections updates first (creates before additions)
            create_updates = [u for u in self.collections_updates if u.get('operation_type') == 'create']
            add_updates = [u for u in self.collections_updates if u.get('operation_type') == 'add_token']
            
            # Process creates first
            if create_updates:
                for update in create_updates:
                    result = self.collections_collection.update_one(
                        update['filter'],
                        update['update'],
                        upsert=update['upsert']
                    )
                    if result.upserted_id:
                        logger.debug(f"Created new collection: {result.upserted_id}")
                
                logger.info(f"Executed {len(create_updates)} collection create updates")
            
            # Execute token updates
            if self.tokens_updates:
                for update in self.tokens_updates:
                    self.tokens_collection.update_one(
                        update['filter'],
                        update['update'],
                        upsert=update['upsert']
                    )
                logger.info(f"Executed {len(self.tokens_updates)} token updates")
                self.tokens_updates.clear()
            
            # Process additions after tokens are updated
            if add_updates:
                for update in add_updates:
                    self.collections_collection.update_one(
                        update['filter'],
                        update['update'],
                        upsert=update['upsert']
                    )
                logger.info(f"Executed {len(add_updates)} collection add updates")
            
            # Clear all collections updates
            self.collections_updates.clear()

        except Exception as e:
            logger.error(f"Error executing batch updates: {e}")
            raise

    def process_events_by_height(self, events_by_height):
        """Process all events for a specific height"""
        for height in sorted(events_by_height.keys()):
            logger.info(f"Processing events at height {height}")
            
            # Process CREATE-COLLECTION events first, then ADD-TO-COLLECTION
            event_order = ['CREATE-COLLECTION', 'ADD-TO-COLLECTION']
            
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
                        if event_type == 'CREATE-COLLECTION':
                            self.process_create_collection_event(event_params, height)
                        elif event_type == 'ADD-TO-COLLECTION':
                            self.process_add_to_collection_event(event_params, height)
            
            # Execute batch updates for this height
            self.execute_batch_updates()
            
            # Update last processed height after successfully processing all events at this height
            self.update_last_collection_height(height)

    def fetch_and_organize_events(self, last_collection_height):
        """Fetch all events and organize them by height and type"""
        events_by_height = defaultdict(lambda: defaultdict(list))
        total_events = 0
        
        for event_type, collection_name in self.event_queries.items():
            logger.info(f"Fetching {event_type} events...")
            
            try:
                events = list(
                    self.mongo_db[collection_name].find(
                        {"height": {"$gt": last_collection_height}}
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

def process_collections():
    """Main processing function"""
    if os.path.exists(lock_file_path):
        logger.info(f"Lock file exists at {lock_file_path}")
        logger.info("Another instance of the script is running. Exiting.")
        sys.exit()

    # Create lock file
    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")
        logger.info("Collections Lock file created.")

    processor = CollectionsProcessor()
    
    try:
        # Connect to MongoDB
        processor.connect_to_mongodb()
        
        # Get last processed height
        last_collection_height = processor.get_last_collection_height()
        logger.info(f"Last processed height: {last_collection_height}")
        
        # Fetch and organize all events
        events_by_height = processor.fetch_and_organize_events(last_collection_height)
        
        if not events_by_height:
            logger.info("No events found to process.")
            return
        
        # Process events by height in ascending order
        processor.process_events_by_height(events_by_height)
        
        logger.info("Collections processing completed successfully!")
        
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

# Allow direct execution but avoid automatic execution during imports
if __name__ == "__main__":
    process_collections()