import os
import sys

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from NG_scripts.collections import process_collections
from NG_scripts.tokens import process_tokens
from NG_scripts.sales import process_sales
from V2_scripts.collections import process_v2_collections
from V2_scripts.tokens import process_v2_tokens
from V2_scripts.sales_helper import process_v2_sales_helper

def run_scripts():
    """Run all processing scripts in sequence"""
    print("Starting script execution...")
    
    try:
        print("1. Processing collections...")
        process_collections()
        print("✓ Collections processed successfully")
    except Exception as e:
        print(f"✗ Error processing collections: {e}")
    
    try:
        print("2. Processing tokens...")
        process_tokens()
        print("✓ Tokens processed successfully")
    except Exception as e:
        print(f"✗ Error processing tokens: {e}")
    
    try:
        print("3. Processing sales...")
        process_sales()
        print("✓ Sales processed successfully")
    except Exception as e:
        print(f"✗ Error processing sales: {e}")
    
    try:
        print("4. Processing V2 collections...")
        process_v2_collections()
        print("✓ V2 Collections processed successfully")
    except Exception as e:
        print(f"✗ Error processing V2 collections: {e}")
    
    try:
        print("5. Processing V2 tokens...")
        process_v2_tokens()
        print("✓ V2 Tokens processed successfully")
    except Exception as e:
        print(f"✗ Error processing V2 tokens: {e}")
    
    try:
        print("6. Processing V2 sales helper...")
        process_v2_sales_helper()
        print("✓ V2 Sales helper processed successfully")
    except Exception as e:
        print(f"✗ Error processing V2 sales helper: {e}")
    
    print("All scripts execution completed!")

if __name__ == "__main__":
    run_scripts()