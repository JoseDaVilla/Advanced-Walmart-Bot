"""
Walmart Leasing Space Checker - Main Entry Point

This script orchestrates the three-step process:
1. Scrapes Walmart leasing page for properties with available spaces < 1000 sqft
2. Checks Google Maps for review counts and coordinates of each property
3. Uses DataForSEO API to check for mobile stores near or inside each Walmart
"""

import os
import sys
import time
import logging
import json
from datetime import datetime

from config import OUTPUT_DIR
from scraper import get_walmart_properties_with_small_spaces
from review_scraper import process_stores_for_reviews
from dataforseo_checker import check_stores_for_mobile
from data_manager import save_results_with_versioning
from email_notifier import send_email

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("walmart_checker.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_pipeline(test_mode=False, quick_mode=False):
    """
    Run the complete scraping pipeline.
    
    Args:
        test_mode: If True, use sample data instead of scraping
        quick_mode: If True, process fewer properties for faster testing
    
    Returns:
        list: Final properties matching all criteria
    """
    start_time = time.time()
    logger.info(f"Starting Walmart Leasing Space Checker at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # STEP 1: Get properties with small spaces
    if test_mode:
        logger.info("Test mode: Using sample data")
        small_space_properties = get_sample_data()
    else:
        logger.info("Step 1: Scraping Walmart properties with small spaces")
        small_space_properties = get_walmart_properties_with_small_spaces()
    
    # Save intermediate results
    step1_file = os.path.join(OUTPUT_DIR, "step1_small_spaces.json")
    with open(step1_file, 'w', encoding='utf-8') as f:
        json.dump(small_space_properties, f, indent=2)
    logger.info(f"Step 1 complete: Found {len(small_space_properties)} properties with small spaces")
    
    if not small_space_properties:
        logger.info("No properties with small spaces found. Exiting.")
        return []
    
    # STEP 2: Check reviews and get coordinates
    if quick_mode:
        # In quick mode, process only a subset of properties
        sample_size = min(10, len(small_space_properties))
        properties_to_check = small_space_properties[:sample_size]
        logger.info(f"Quick mode: Processing {sample_size} properties")
    else:
        properties_to_check = small_space_properties
        
    logger.info(f"Step 2: Checking Google Maps reviews for {len(properties_to_check)} properties")
    properties_with_reviews = process_stores_for_reviews(properties_to_check)
    
    # Save intermediate results
    step2_file = os.path.join(OUTPUT_DIR, "step2_with_reviews.json")
    with open(step2_file, 'w', encoding='utf-8') as f:
        json.dump(properties_with_reviews, f, indent=2)
        
    # Filter for properties that meet review criteria
    qualifying_properties = [p for p in properties_with_reviews if p.get("meets_criteria", False)]
    logger.info(f"Step 2 complete: {len(qualifying_properties)} of {len(properties_with_reviews)} properties meet review criteria")
    
    if not qualifying_properties:
        logger.info("No properties meet review criteria. Exiting.")
        return []
    
    # STEP 3: Check for mobile stores using DataForSEO API
    logger.info(f"Step 3: Checking for mobile stores for {len(qualifying_properties)} properties")
    final_properties = check_stores_for_mobile(qualifying_properties)
    
    # Save final results
    step3_file = os.path.join(OUTPUT_DIR, "step3_final_results.json")
    with open(step3_file, 'w', encoding='utf-8') as f:
        json.dump(final_properties, f, indent=2)
    
    # Filter for properties that meet all criteria
    matching_properties = [p for p in final_properties if p.get("meets_criteria", False)]
    logger.info(f"Step 3 complete: {len(matching_properties)} of {len(final_properties)} properties meet all criteria")
    
    # Save versioned results for historical tracking
    if matching_properties:
        save_results_with_versioning(matching_properties)
    
    # Calculate and log total execution time
    execution_time = time.time() - start_time
    minutes = int(execution_time // 60)
    seconds = int(execution_time % 60)
    logger.info(f"Pipeline complete in {minutes} minutes {seconds} seconds")
    
    return matching_properties


def get_sample_data():
    """Get sample data for testing purposes."""
    return [
        {
            "store_id": "1234",
            "store_number": "Store #1234",
            "address": "123 Test St, Anytown, TX 12345",
            "spaces": [
                {"suite": "100", "sqft": 800, "text": "Suite 100 | 800 sqft"},
                {"suite": "101", "sqft": 600, "text": "Suite 101 | 600 sqft"},
            ],
        },
        {
            "store_id": "5678",
            "store_number": "Store #5678",
            "address": "456 Sample Ave, Testville, CA 67890",
            "spaces": [
                {"suite": "200", "sqft": 950, "text": "Suite 200 | 950 sqft"}
            ],
        },
    ]


def send_notification(matching_properties):
    """Send email notification with matching properties."""
    if not matching_properties:
        logger.info("No matching properties to notify about")
        return
    
    logger.info(f"Sending email notification with {len(matching_properties)} matching properties")
    try:
        send_email(matching_properties)
        logger.info("Email notification sent successfully")
    except Exception as e:
        logger.error(f"Failed to send email notification: {str(e)}")


def main():
    """Main entry point with command line argument handling."""
    # Parse command line arguments
    test_mode = "--test" in sys.argv
    quick_mode = "--quick" in sys.argv
    
    if "--help" in sys.argv or "-h" in sys.argv:
        print("Walmart Leasing Space Checker")
        print("\nUsage:")
        print("  python main.py [options]")
        print("\nOptions:")
        print("  --test       Run in test mode with sample data")
        print("  --quick      Process fewer properties for faster testing")
        print("  --email      Send email notification with results")
        print("  --resume     Resume from previous step (provide step number)")
        print("  --help, -h   Show this help message")
        return
    
    # Run the pipeline
    matching_properties = run_pipeline(test_mode, quick_mode)
    
    # Send email notification if requested or if we found matching properties
    if "--email" in sys.argv or matching_properties:
        send_notification(matching_properties)
    
    # Print summary at the end
    if matching_properties:
        print("\n--- MATCHING PROPERTIES ---")
        print(f"Found {len(matching_properties)} properties that meet all criteria:")
        for prop in matching_properties:
            print(f"Store #{prop['store_id']}: {prop['address']}")
            for space in prop.get('spaces', []):
                print(f"  - Suite {space['suite']}: {space['sqft']} sqft")
        print("\nCheck the output files in the json_data directory for complete details.")
    else:
        print("\nNo properties found matching all criteria.")


if __name__ == "__main__":
    main()
