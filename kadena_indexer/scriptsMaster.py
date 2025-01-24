from .collections import process_collections
from .tokens import process_tokens
from .sales import process_sales
from .sales_helper import process_sales_helper

def run_sripts():
    process_collections()
    process_tokens()
    process_sales()
    #process_sales_helper()
    
if __name__ == "__main__":
    run_sripts()