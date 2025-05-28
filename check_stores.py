import argparse
import json
import logging
import os
import time
from datetime import datetime
from colorama import Fore, Style, init

from config import OUTPUT_DIR
from review_scraper import process_stores_for_reviews
from dataseo_checker import check_stores_for_mobile
from data_manager import save_intermediate_results, save_results_with_versioning

init(autoreset=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"walmart_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_stores_from_json(filename):

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            stores = json.load(f)
        
        logger.info(f"{Fore.GREEN}Loaded {len(stores)} stores from {filename}{Style.RESET_ALL}")
        return stores
    
    except Exception as e:
        logger.error(f"{Fore.RED}Error loading stores from {filename}: {str(e)}{Style.RESET_ALL}")
        return []


def main():
    """Main function to run store checks"""
    
    parser = argparse.ArgumentParser(description="Check Walmart stores for review counts and nearby mobile stores")
    parser.add_argument("--input", default="json_data/small_space_properties.json", 
                        help="Input JSON file with stores that meet space criteria")
    parser.add_argument("--skip-reviews", action="store_true", 
                        help="Skip review checking and use existing review data")
    parser.add_argument("--skip-mobile", action="store_true", 
                        help="Skip mobile store checking")
    parser.add_argument("--workers", type=int, default=None, 
                        help="Number of parallel workers to use")
    parser.add_argument("--limit", type=int, default=None, 
                        help="Limit the number of stores to process")
    args = parser.parse_args()
    
    
    if args.workers:
        from config import API_WORKERS
        API_WORKERS = args.workers
        logger.info(f"{Fore.CYAN}Set workers to {API_WORKERS}{Style.RESET_ALL}")
    
    
    start_time = time.time()
    
    
    logger.info(f"{Fore.CYAN}Step 1: Loading stores from {args.input}{Style.RESET_ALL}")
    stores = load_stores_from_json(args.input)
    
    if not stores:
        logger.error(f"{Fore.RED}No stores loaded. Exiting.{Style.RESET_ALL}")
        return
    
    
    if args.limit and args.limit > 0:
        stores = stores[:args.limit]
        logger.info(f"{Fore.CYAN}Limited to processing {len(stores)} stores{Style.RESET_ALL}")
    
    
    if not args.skip_reviews:
        logger.info(f"{Fore.CYAN}Step 2: Checking review counts for {len(stores)} stores{Style.RESET_ALL}")
        stores = process_stores_for_reviews(stores)
    else:
        logger.info(f"{Fore.CYAN}Skipping review checks as requested{Style.RESET_ALL}")
    
    
    if not args.skip_mobile:
        logger.info(f"{Fore.CYAN}Step 3: Checking for nearby mobile stores{Style.RESET_ALL}")
        stores = check_stores_for_mobile(stores)
    else:
        logger.info(f"{Fore.CYAN}Skipping mobile store checks as requested{Style.RESET_ALL}")
    
    
    eligible_stores = [store for store in stores if 
                      store.get("meets_criteria", False) and 
                      not store.get("has_mobile_store", False)]
    
    logger.info(f"{Fore.GREEN}Found {len(eligible_stores)} eligible stores out of {len(stores)} total{Style.RESET_ALL}")
    
    
    logger.info(f"{Fore.CYAN}Step 5: Saving final results{Style.RESET_ALL}")
    save_results_with_versioning(eligible_stores)
    
    
    elapsed_time = time.time() - start_time
    logger.info(f"{Fore.GREEN}Process completed in {elapsed_time:.2f} seconds{Style.RESET_ALL}")
    
    
    logger.info(f"{Fore.GREEN}=" * 50)
    logger.info(f"{Fore.GREEN}SUMMARY:")
    logger.info(f"{Fore.GREEN}Total stores processed: {len(stores)}")
    logger.info(f"{Fore.GREEN}Stores meeting review criteria: {sum(1 for s in stores if s.get('meets_criteria', False))}")
    logger.info(f"{Fore.GREEN}Stores with mobile stores: {sum(1 for s in stores if s.get('has_mobile_store', False))}")
    logger.info(f"{Fore.GREEN}Final eligible stores: {len(eligible_stores)}")
    logger.info(f"{Fore.GREEN}=" * 50)


if __name__ == "__main__":
    main()
