import sys
import os

# Add the kadena_indexer directory to Python path
kadena_indexer_path = os.path.join(os.path.dirname(__file__), 'kadena_indexer')
sys.path.insert(0, kadena_indexer_path)

from kadena_indexer.scriptsMaster import run_scripts

if __name__ == "__main__":
    run_scripts()