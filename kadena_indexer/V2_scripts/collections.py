import tempfile
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
from dotenv import load_dotenv
import logging
from collections import defaultdict

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class V2CollectionsProcessor:
    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self.tokens_collection = None
        self.collections_collection = None
        self.metadata_collection = None
        
        # Event collection names
        self.event_collections = {
            'COLLECTION': 'marmalade-v2.collection-policy-v1.COLLECTION',
            'TOKEN_COLLECTION': 'marmalade-v2.collection-policy-v1.TOKEN-COLLECTION'
        }
        
        # Batch operations
        self.collection_updates = []
        self.token_updates = []
        self.batch_size = 100

    def connect_to_mongodb(self):
        """Establish MongoDB connection"""
        try:
            mongo_uri = os.environ.get('Mongo_URI')
            if not mongo_uri:
                raise ValueError("Mongo_URI environment variable not set")
            
            self.mongo_client = MongoClient(mongo_uri, server_api=ServerApi('1'))
            self.mongo_db = self.mongo_client['nft_events']
            self.tokens_collection = self.mongo_db['chain8v2tokens']
            self.collections_collection = self.mongo_db['chain8v2collections']
            self.metadata_collection = self.mongo_db['script_metadata']
            
            # Verify connection
            self.mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB!")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def get_last_processed_height(self):
        """Get the last processed collection height"""
        try:
            metadata_doc = self.metadata_collection.find_one({"key": "last_v2_collection_height"})
            return int(metadata_doc["value"]) if metadata_doc else 0
        except Exception as e:
            logger.error(f"Error getting last processed height: {e}")
            return 0

    def update_last_processed_height(self, height):
        """Update the last processed height"""
        try:
            self.metadata_collection.update_one(
                {"key": "last_v2_collection_height"},
                {"$set": {"value": height}},
                upsert=True
            )
            logger.info(f"Updated last processed height to: {height}")
        except Exception as e:
            logger.error(f"Error updating last processed height: {e}")

    def validate_collection_params(self, params):
        """Validate collection creation parameters"""
        if not isinstance(params, list) or len(params) < 4:
            return False, f"Expected at least 4 parameters for COLLECTION, got {len(params) if isinstance(params, list) else 'non-list'}"
        
        collection_id, collection_name, maxSize, collection_creator = params[:4]
        
        if not collection_id or not collection_name:
            return False, "Collection ID and name are required"
        
        return True, ""

    def validate_token_collection_params(self, params):
        """Validate token-collection assignment parameters"""
        if not isinstance(params, list) or len(params) < 2:
            return False, f"Expected at least 2 parameters for TOKEN-COLLECTION, got {len(params) if isinstance(params, list) else 'non-list'}"
        
        collection_id, nft_id = params[:2]
        
        if not collection_id or not nft_id:
            return False, "Collection ID and NFT ID are required"
        
        return True, ""

    def extract_max_size_value(self, maxSize):
        """Extract numeric value from maxSize parameter"""
        try:
            if isinstance(maxSize, int):
                return maxSize
            elif isinstance(maxSize, dict) and 'int' in maxSize:
                return maxSize['int']
            elif isinstance(maxSize, str) and maxSize.isdigit():
                return int(maxSize)
            else:
                logger.warning(f"Unexpected maxSize format: {maxSize}, defaulting to 0")
                return 0
        except Exception as e:
            logger.error(f"Error extracting maxSize value: {e}")
            return 0

    def process_collection_creation(self, event):
        """Process COLLECTION creation event"""
        try:
            params = event.get('params', [])
            height = event.get('height')
            
            # Validate parameters
            is_valid, error_msg = self.validate_collection_params(params)
            if not is_valid:
                logger.error(f"Invalid COLLECTION event at height {height}: {error_msg}")
                return
            
            collection_id, collection_name, maxSize, collection_creator = params[:4]
            maxSize_value = self.extract_max_size_value(maxSize)
            
            # Queue collection creation/update
            self.collection_updates.append({
                'type': 'upsert',
                'filter': {"_id": collection_id},
                'update': {
                    "$setOnInsert": {
                        "creator": collection_creator,
                        "maxSize": maxSize_value,
                        "name": collection_name,
                        "size": 0,
                        "approved": False,
                        "tokens": [],
                        "createdAt": height
                    }
                }
            })
            
            logger.debug(f"Queued collection creation for {collection_id}")
            
        except Exception as e:
            logger.error(f"Error processing COLLECTION event: {e}")

    def get_collection_info(self, collection_id):
        """Get collection information from database"""
        try:
            return self.collections_collection.find_one(
                {"_id": collection_id}, 
                {"name": 1, "tokens": 1}
            )
        except Exception as e:
            logger.error(f"Error fetching collection info for {collection_id}: {e}")
            return None

    def process_token_to_collection(self, event):
        """Process TOKEN-COLLECTION assignment event"""
        try:
            params = event.get('params', [])
            height = event.get('height')
            
            # Validate parameters
            is_valid, error_msg = self.validate_token_collection_params(params)
            if not is_valid:
                logger.error(f"Invalid TOKEN-COLLECTION event at height {height}: {error_msg}")
                return
            
            collection_id, nft_id = params[:2]
            
            # Get collection information
            collection_document = self.get_collection_info(collection_id)
            if not collection_document:
                logger.warning(f"Collection {collection_id} not found. Skipping token assignment.")
                return

            collection_name = collection_document.get('name', '')
            existing_tokens = collection_document.get('tokens', [])
            
            # Check if token is already in collection
            is_new_token = nft_id not in existing_tokens
            
            # Queue token update
            self.token_updates.append({
                'filter': {"_id": nft_id},
                'update': {
                    "$set": {
                        "collection": {
                            "collectionId": collection_id, 
                            "collectionName": collection_name
                        }
                    }
                }
            })
            
            # If it's a new token, queue collection update
            if is_new_token:
                self.collection_updates.append({
                    'type': 'update',
                    'filter': {"_id": collection_id},
                    'update': {
                        "$addToSet": {"tokens": nft_id},
                        "$inc": {"size": 1}
                    }
                })
                logger.debug(f"Queued new token {nft_id} addition to collection {collection_id}")
            else:
                logger.debug(f"Token {nft_id} already in collection {collection_id}")
            
        except Exception as e:
            logger.error(f"Error processing TOKEN-COLLECTION event: {e}")

    def execute_batch_operations(self):
        """Execute all queued batch operations"""
        try:
            # Execute collection updates
            if self.collection_updates:
                for update in self.collection_updates:
                    if update['type'] == 'upsert':
                        result = self.collections_collection.update_one(
                            update['filter'],
                            update['update'],
                            upsert=True
                        )
                        if result.upserted_id:
                            logger.debug(f"Created new collection: {update['filter']['_id']}")
                    else:  # regular update
                        self.collections_collection.update_one(
                            update['filter'],
                            update['update']
                        )
                
                logger.info(f"Executed {len(self.collection_updates)} collection updates")
                self.collection_updates.clear()

            # Execute token updates
            if self.token_updates:
                for update in self.token_updates:
                    self.tokens_collection.update_one(
                        update['filter'],
                        update['update'],
                        upsert=True
                    )
                
                logger.info(f"Executed {len(self.token_updates)} token updates")
                self.token_updates.clear()

        except Exception as e:
            logger.error(f"Error executing batch operations: {e}")
            raise

    def fetch_and_organize_events(self, last_processed_height):
        """Fetch all events and organize them by height"""
        events_by_height = defaultdict(list)
        total_events = 0
        
        # Process COLLECTION events first (creation must happen before assignment)
        for event_type in ['COLLECTION', 'TOKEN_COLLECTION']:
            logger.info(f"Fetching {event_type} events...")
            
            try:
                collection_name = self.event_collections[event_type]
                events = list(
                    self.mongo_db[collection_name].find(
                        {"height": {"$gt": last_processed_height}}
                    ).sort("height", 1)
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
            
            # Sort events by type to ensure COLLECTION events are processed before TOKEN_COLLECTION
            events = sorted(events_by_height[height], key=lambda x: x['event_type'])
            
            # Process all events at this height
            for event in events:
                event_type = event['event_type']
                
                if event_type == 'COLLECTION':
                    self.process_collection_creation(event)
                elif event_type == 'TOKEN_COLLECTION':
                    self.process_token_to_collection(event)
            
            # Execute batch operations for this height
            self.execute_batch_operations()
            
            # Update last processed height after successfully processing all events at this height
            self.update_last_processed_height(height)

def process_v2_collections():
    """Main processing function"""
    lock_file_path = os.path.join(tempfile.gettempdir(), "ng_collection_script.lock")
    
    # Ensure only one instance of the script runs at a time
    if os.path.exists(lock_file_path):
        logger.info("Another instance of the script is running. Exiting.")
        sys.exit()

    # Create lock file
    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")
        logger.info("Lock file created.")

    processor = V2CollectionsProcessor()
    
    try:
        # Connect to MongoDB
        processor.connect_to_mongodb()
        
        # Get last processed height
        last_processed_height = processor.get_last_processed_height()
        logger.info(f"Starting from block height: {last_processed_height}")
        
        # Fetch and organize all events
        events_by_height = processor.fetch_and_organize_events(last_processed_height)
        
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
    process_v2_collections()