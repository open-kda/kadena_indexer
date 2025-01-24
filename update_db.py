from pymongo import MongoClient
from pymongo.server_api import ServerApi

# MongoDB URIs
source_uri = (
    ""
)
destination_uri = (
    ""
)

def migrate_data(source_uri, destination_uri):
    """Migrate data from the source MongoDB to the destination."""
    try:
        # Connect to source and destination MongoDB
        source_client = MongoClient(source_uri, server_api=ServerApi("1"))
        destination_client = MongoClient(destination_uri, server_api=ServerApi("1"))
        
        # List databases in the source
        source_databases = source_client.list_database_names()
        print(f"Source Databases: {source_databases}")
        
        for db_name in source_databases:
            if db_name in ['admin', 'config', 'local', 'nftDatabase']:
                # Skip system databases
                continue
            
            source_db = source_client[db_name]
            destination_db = destination_client[db_name]

            # List collections in the database
            collections = source_db.list_collection_names()
            print(f"Copying database: {db_name}, Collections: {collections}")
            
            for collection_name in collections:
                if collection_name in ['n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.RECONCILE', 'coin.TRANSFER', 'n_4e470a97222514a8662dd1219000a0431451b0ee.policy-auction-sale.AUCTION-SALE-WITHDRAWN', 'n_4e470a97222514a8662dd1219000a0431451b0ee.ledger.MINT','n_4e470a97222514a8662dd1219000a0431451b0ee.policy-fixed-sale.FIXED-SALE-OFFER','kdlaunch.kdswap-exchange.CREATE_PAIR','kdswap-exchange.SWAP','n_4e470a97222514a8662dd1219000a0431451b0ee.policy-auction-sale.AUCTION-SALE-OFFER','n_4e470a97222514a8662dd1219000a0431451b0ee.policy-collection.ADD-TO-COLLECTION','kaddex.exchange.SWAP','n_e309f0fa7cf3a13f93a8da5325cdad32790d2070.heron.TRANSFER','kdlaunch.kdswap-exchange.SWAP','kdswap-exchange.UPDATE','kaddex.exchange.CREATE_PAIR','kaddex.exchange.UPDATE','kdlaunch.kdswap-exchange.UPDATE']:
                    # Skip system collection
                    continue
            
                source_collection = source_db[collection_name]
                destination_collection = destination_db[collection_name]

                # Fetch all documents from the source collection
                documents = list(source_collection.find({}))
                if documents:
                    print(f"Inserting {len(documents)} documents into {db_name}.{collection_name}")
                    # Insert documents into the destination collection
                    destination_collection.insert_many(documents)
                else:
                    print(f"No documents to migrate in {db_name}.{collection_name}")
        
        print("Migration completed successfully!")

    except Exception as e:
        print(f"An error occurred during migration: {e}")

    finally:
        # Close the clients
        source_client.close()
        destination_client.close()

if __name__ == "__main__":
    migrate_data(source_uri, destination_uri)
