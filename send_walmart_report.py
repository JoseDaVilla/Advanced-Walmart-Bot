"""
Script to generate and send email reports of eligible Walmart stores
Can be run independently to send a report from existing JSON data
"""

import os
import json
import argparse
import logging
import sys
from datetime import datetime
from colorama import Fore, Style, init

from config import OUTPUT_DIR, MAX_SPACE_SIZE, MIN_REVIEWS, EMAIL_RECEIVER
from email_notifier import send_email

# Initialize colorama
init(autoreset=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("email_report.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)


def load_eligible_stores(filename=None):
    """
    Load eligible stores from a JSON file.
    
    Args:
        filename: Optional specific JSON filename to load. If None, will try common filenames.
        
    Returns:
        List of eligible store dictionaries
    """
    # If no filename specified, try common filenames in priority order
    if not filename:
        possible_files = [
            "eligible_stores.json",  # From review_scraper.py
            "properties_with_reviews.json",  # From main.py intermediate results
            "matching_properties.json",  # Final matching properties
            "small_space_properties.json",  # Initial eligible small space properties
        ]
        
        # Try each file in order
        for file in possible_files:
            filepath = os.path.join(OUTPUT_DIR, file)
            if os.path.exists(filepath):
                logger.info(f"Found data file: {filepath}")
                filename = filepath
                break
                
        if not filename:
            # Look for timestamped eligible stores files
            timestamp_files = [f for f in os.listdir(OUTPUT_DIR) 
                               if f.startswith("eligible_stores_") and f.endswith(".json")]
            if timestamp_files:
                # Use the most recent one
                newest_file = sorted(timestamp_files)[-1]
                filename = os.path.join(OUTPUT_DIR, newest_file)
                logger.info(f"Using most recent eligible stores file: {newest_file}")
    else:
        # If filename was explicitly provided
        if not os.path.isabs(filename):
            filename = os.path.join(OUTPUT_DIR, filename)
    
    # Check if file exists
    if not filename or not os.path.exists(filename):
        logger.error(f"Could not find eligible stores data file")
        return []
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            stores = json.load(f)
            
        logger.info(f"Loaded {len(stores)} stores from {os.path.basename(filename)}")
        return stores
    except Exception as e:
        logger.error(f"Error loading JSON file: {e}")
        return []


def filter_eligible_stores(stores):
    """
    Filter stores to ensure only truly eligible ones are included.
    
    Args:
        stores: List of store dictionaries
        
    Returns:
        List of filtered eligible store dictionaries
    """
    if not stores:
        return []
        
    eligible = []
    
    for store in stores:
        eligible_spaces = [s for s in store.get("spaces", []) 
                          if s.get("sqft", 0) < MAX_SPACE_SIZE]
        
        # Check if it meets all eligibility criteria
        if (eligible_spaces and 
            store.get("review_count", 0) >= MIN_REVIEWS and
            not store.get("has_mobile_store", False)):
            
            # Ensure spaces field only includes eligible spaces
            store_copy = store.copy()
            store_copy["spaces"] = eligible_spaces
            eligible.append(store_copy)
            
    logger.info(f"Filtered to {len(eligible)} eligible stores")
    return eligible


def print_store_details(stores):
    """
    Print details of eligible stores to console for review.
    
    Args:
        stores: List of eligible store dictionaries
    """
    if not stores:
        print(f"{Fore.YELLOW}No eligible stores found{Style.RESET_ALL}")
        return
        
    print(f"\n{Fore.GREEN}========== ELIGIBLE WALMART STORES =========={Style.RESET_ALL}")
    print(f"{Fore.GREEN}Found {len(stores)} stores matching criteria:{Style.RESET_ALL}")
    print(f"• Spaces smaller than {MAX_SPACE_SIZE} sq ft")
    print(f"• At least {MIN_REVIEWS:,} Google reviews")
    print(f"• No mobile phone stores inside or nearby")
    print()
    
    for i, store in enumerate(stores):
        store_id = store.get("store_id", "Unknown")
        store_num = store.get("store_number", f"Store #{store_id}")
        address = store.get("address", "Unknown address")
        city = store.get("city", "Unknown city")
        zip_code = store.get("zip_code", "Unknown ZIP")
        reviews = store.get("review_count", "Unknown")
        spaces = store.get("spaces", [])
        
        print(f"{Fore.CYAN}Store #{i+1}: {store_num}{Style.RESET_ALL}")
        print(f"  Address: {address}")
        print(f"  City: {city}, ZIP: {zip_code}")
        print(f"  Reviews: {reviews:,}")
        print(f"  Available spaces: {len(spaces)}")
        
        for space in spaces:
            suite = space.get("suite", "Unknown")
            sqft = space.get("sqft", "Unknown")
            print(f"    • Suite {suite}: {sqft} sq ft")
            
        print()


def main():
    """Main function to load data, generate report, and send email."""
    parser = argparse.ArgumentParser(description="Generate and send email report of eligible Walmart stores")
    parser.add_argument("-f", "--file", help="Specific JSON file to load data from")
    parser.add_argument("-p", "--print-only", action="store_true", help="Only print results, don't send email")
    parser.add_argument("-e", "--email", help=f"Send email to specific address (default: {EMAIL_RECEIVER})")
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Load and filter stores
    stores = load_eligible_stores(args.file)
    eligible_stores = filter_eligible_stores(stores)
    
    # Print stores to console
    print_store_details(eligible_stores)
    
    # Send email if requested
    if not args.print_only:
        if eligible_stores:
            receiver = args.email or EMAIL_RECEIVER
            try:
                print(f"\n{Fore.BLUE}Sending email to {receiver}...{Style.RESET_ALL}")
                send_email(eligible_stores)
                print(f"{Fore.GREEN}Email sent successfully!{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error sending email: {e}{Style.RESET_ALL}")
        else:
            print(f"{Fore.YELLOW}No eligible stores to include in email{Style.RESET_ALL}")


if __name__ == "__main__":
    main()
