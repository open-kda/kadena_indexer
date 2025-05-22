from .NG_scripts.collections import process_collections
from .NG_scripts.tokens import process_tokens
from .NG_scripts.sales import process_sales
from .V2_scripts.collections import process_v2_collections
from .V2_scripts.tokens import process_v2_tokens
from .V2_scripts.sales_helper import process_v2_sales_helper

def run_sripts():
    process_collections()
    process_tokens()
    process_sales()
    process_v2_collections()
    process_v2_tokens()
    process_v2_sales_helper()
    
if __name__ == "__main__":
    run_sripts()