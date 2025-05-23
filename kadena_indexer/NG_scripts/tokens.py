from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
import sys
import json
from dotenv import load_dotenv
import tempfile
from collections import defaultdict
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

lock_file_path = os.path.join(tempfile.gettempdir(), "token_tk_script.lock")

class TokenProcessor:
    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self.tokens_collection = None
        self.ledger_collection = None
        
        self.event_queries = {
            'MINT': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.MINT",
            'RECONCILE': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.RECONCILE",
            'SUPPLY': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.SUPPLY",
            'TOKEN-CREATE': "n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.TOKEN-CREATE",
        }
        
        # Batch operations storage
        self.token_updates = []
        self.ledger_updates = []
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
            self.ledger_collection = self.mongo_db['chain8v2ngledger']
            
            # Verify connection
            self.mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB!")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def get_last_processed_height(self):
        """Get the last processed token height"""
        try:
            last_height_doc = self.tokens_collection.find_one({"_id": "last_token_height"})
            return last_height_doc['value'] if last_height_doc else 0
        except Exception as e:
            logger.error(f"Error getting last processed height: {e}")
            return 0

    def update_last_processed_height(self, height):
        """Update the last processed height"""
        try:
            self.tokens_collection.update_one(
                {"_id": "last_token_height"},
                {"$set": {"value": height}},
                upsert=True
            )
            logger.info(f"Updated last processed height to: {height}")
        except Exception as e:
            logger.error(f"Error updating last processed height: {e}")

    def validate_event_params(self, event_type, params):
        """Validate event parameters based on event type"""
        if not isinstance(params, list):
            return False, f"Expected params to be a list, got {type(params)}"
        
        required_lengths = {
            'MINT': 3,        # [nft_id, account, balance]
            'SUPPLY': 2,      # [nft_id, supply]
            'TOKEN-CREATE': 4, # [nft_id, uri, ?, policies]
            'RECONCILE': 4    # [nft_id, ?, previous_owner_info, new_owner_info]
        }
        
        expected_length = required_lengths.get(event_type)
        if expected_length and len(params) < expected_length:
            return False, f"Expected at least {expected_length} parameters for {event_type}, got {len(params)}"
        
        return True, ""

    def process_mint_event(self, event_params, height):
        """Process MINT event"""
        try:
            nft_id, account, balance = event_params[:3]
            
            self.ledger_updates.append({
                'filter': {"nftId": nft_id},
                'update': {"$set": {"account": account, "balance": balance, "lastUpdated": height}},
                'upsert': True
            })
            
            logger.debug(f"Queued MINT update for NFT {nft_id}, account {account}")
            
        except Exception as e:
            logger.error(f"Error processing MINT event: {e}")

    def process_supply_event(self, event_params, height):
        """Process SUPPLY event"""
        try:
            nft_id, supply = event_params[:2]
            
            self.token_updates.append({
                'filter': {"_id": nft_id},
                'update': {"$set": {"supply": supply, "lastUpdated": height}},
                'upsert': True
            })
            
            logger.debug(f"Queued SUPPLY update for NFT {nft_id}")
            
        except Exception as e:
            logger.error(f"Error processing SUPPLY event: {e}")

    def process_token_create_event(self, event_params, height):
        """Process TOKEN-CREATE event"""
        try:
            nft_id = event_params[0]
            uri = event_params[1] if len(event_params) > 1 else ""
            policies = event_params[3] if len(event_params) > 3 else []
            
            if not isinstance(policies, list):
                logger.warning(f"Expected policies to be a list for NFT {nft_id}, got: {type(policies)}")
                policies = []

            grouped_policies = self.group_policies(policies)
            
            self.token_updates.append({
                'filter': {"_id": nft_id},
                'update': {"$set": {
                    "uri": uri, 
                    "groupedPolicies": grouped_policies,
                    "lastUpdated": height
                }},
                'upsert': True
            })
            
            logger.debug(f"Queued TOKEN-CREATE update for NFT {nft_id}")
            
        except Exception as e:
            logger.error(f"Error processing TOKEN-CREATE event: {e}")

    def group_policies(self, policies):
        """Group policies by namespace"""
        grouped_policies = defaultdict(lambda: defaultdict(list))
        
        for policy_str in policies:
            try:
                if not isinstance(policy_str, str):
                    logger.warning(f"Policy is not a string: {policy_str}")
                    continue
                    
                parts = policy_str.split(".")
                if len(parts) >= 2:
                    namespace = parts[0]
                    policy_name = ".".join(parts[1:])
                    grouped_policies[namespace][policy_name].append(policy_str)
                else:
                    logger.warning(f"Unexpected policy format: {policy_str}")
                    
            except Exception as e:
                logger.error(f"Error processing policy string '{policy_str}': {e}")
        
        # Convert defaultdict to regular dict for MongoDB storage
        return {ns: dict(policies) for ns, policies in grouped_policies.items()}

    def process_reconcile_event(self, event_params, height):
        """Process RECONCILE event"""
        try:
            nft_id = event_params[0]
            previous_owner_info = event_params[2]
            new_owner_info = event_params[3]

            if not isinstance(previous_owner_info, dict) or not isinstance(new_owner_info, dict):
                logger.error(f"Invalid owner info format for RECONCILE event, NFT {nft_id}")
                return

            previous_account = previous_owner_info.get('account')
            new_account = new_owner_info.get('account')
            
            if not previous_account or not new_account:
                logger.error(f"Missing account information in RECONCILE event for NFT {nft_id}")
                return

            # Set previous owner balance to 0
            self.ledger_updates.append({
                'filter': {"nftId": nft_id, "account": previous_account},
                'update': {"$set": {"balance": 0, "lastUpdated": height}},
                'upsert': False
            })

            # Set new owner balance to 1
            self.ledger_updates.append({
                'filter': {"nftId": nft_id, "account": new_account},
                'update': {"$set": {"nftId": nft_id, "account": new_account, "balance": 1, "lastUpdated": height}},
                'upsert': True
            })

            logger.debug(f"Queued RECONCILE updates for NFT {nft_id}: {previous_account} -> {new_account}")
            
        except Exception as e:
            logger.error(f"Error processing RECONCILE event: {e}")

    def execute_batch_updates(self):
        """Execute all queued batch updates"""
        try:
            # Execute token updates
            if self.token_updates:
                for update in self.token_updates:
                    self.tokens_collection.update_one(
                        update['filter'],
                        update['update'],
                        upsert=update['upsert']
                    )
                logger.info(f"Executed {len(self.token_updates)} token updates")
                self.token_updates.clear()

            # Execute ledger updates
            if self.ledger_updates:
                for update in self.ledger_updates:
                    if update['upsert']:
                        self.ledger_collection.update_one(
                            update['filter'],
                            update['update'],
                            upsert=True
                        )
                    else:
                        self.ledger_collection.update_one(
                            update['filter'],
                            update['update']
                        )
                logger.info(f"Executed {len(self.ledger_updates)} ledger updates")
                self.ledger_updates.clear()

        except Exception as e:
            logger.error(f"Error executing batch updates: {e}")
            raise

    def process_events_by_height(self, events_by_height):
        """Process all events for a specific height"""
        for height in sorted(events_by_height.keys()):
            logger.info(f"Processing events at height {height}")
            
            # Process all events at this height in order: TOKEN-CREATE first, then others
            event_order = ['TOKEN-CREATE', 'MINT', 'SUPPLY', 'RECONCILE']
            
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
                        if event_type == 'MINT':
                            self.process_mint_event(event_params, height)
                        elif event_type == 'SUPPLY':
                            self.process_supply_event(event_params, height)
                        elif event_type == 'TOKEN-CREATE':
                            self.process_token_create_event(event_params, height)
                        elif event_type == 'RECONCILE':
                            self.process_reconcile_event(event_params, height)
            
            # Execute batch updates for this height
            self.execute_batch_updates()
            
            # Update last processed height after successfully processing all events at this height
            self.update_last_processed_height(height)

    def fetch_and_organize_events(self, last_token_height):
        """Fetch all events and organize them by height and type"""
        events_by_height = defaultdict(lambda: defaultdict(list))
        total_events = 0
        
        for event_type in ['TOKEN-CREATE', 'MINT', 'SUPPLY', 'RECONCILE']:
            logger.info(f"Fetching {event_type} events...")
            
            try:
                events = list(
                    self.mongo_db[self.event_queries[event_type]].find(
                        {"height": {"$gt": last_token_height}}
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

def process_tokens():
    """Main processing function"""
    if os.path.exists(lock_file_path):
        logger.info(f"Lock file exists at {lock_file_path}")
        logger.info("Another instance of the script is running. Exiting.")
        sys.exit()

    # Create lock file
    with open(lock_file_path, 'w') as lock_file:
        lock_file.write("LOCKED")
        logger.info("Tokens Lock file created.")

    processor = TokenProcessor()
    
    try:
        # Connect to MongoDB
        processor.connect_to_mongodb()
        
        # Get last processed height
        last_token_height = processor.get_last_processed_height()
        logger.info(f"Last processed height: {last_token_height}")
        
        # Fetch and organize all events
        events_by_height = processor.fetch_and_organize_events(last_token_height)
        
        if not events_by_height:
            logger.info("No events found to process.")
            return
        
        # Process events by height in ascending order
        processor.process_events_by_height(events_by_height)
        
        logger.info("Token processing completed successfully!")
        
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
    process_tokens()