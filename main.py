"""
Walmart Leasing Space Checker - Main Entry Point

This script:
1. Scrapes Walmart leasing page for properties with available spaces < 1000 sqft
2. Uses parallel processing for faster execution
3. Checks reviews and for mobile stores using direct Google Maps search
4. Sends email notifications about matching properties
5. Runs on a daily schedule or one-time
"""

import os
import sys
import time
import logging
import schedule
import platform
import subprocess
import locale
from datetime import datetime

from config import OUTPUT_DIR
from scraper import get_walmart_properties_with_small_spaces
from location_checker import check_locations_in_parallel
from email_notifier import send_email
from data_manager import save_results_with_versioning, save_intermediate_results

# Configure better logging with encoding fixes
system_encoding = locale.getpreferredencoding()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # Specify UTF-8 encoding for the log file to support all characters
        logging.FileHandler("walmart_leasing_parallel.log", encoding='utf-8'),
        # Use system encoding for console output (avoids encoding errors)
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_chrome_driver_status():
    """Verify Chrome and ChromeDriver status and restart if needed."""
    logger.info("Checking Chrome and ChromeDriver status...")
    
    try:
        # Test if we can create a WebDriver instance
        from selenium_utils import setup_selenium_driver
        test_driver = setup_selenium_driver(headless=True)
        
        if test_driver:
            logger.info("Chrome and ChromeDriver are working correctly")
            test_driver.quit()
            return True
        else:
            logger.warning("Could not create WebDriver. Attempting to restart Chrome processes...")
            # Try to kill and restart Chrome processes
            if platform.system() == "Windows":
                try:
                    subprocess.run(["taskkill", "/f", "/im", "chrome.exe"], 
                                  stdout=subprocess.DEVNULL, 
                                  stderr=subprocess.DEVNULL)
                    subprocess.run(["taskkill", "/f", "/im", "chromedriver.exe"], 
                                  stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL)
                    logger.info("Chrome processes terminated. Waiting to restart...")
                    time.sleep(5)
                except Exception as e:
                    logger.error(f"Error terminating Chrome processes: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"Error checking ChromeDriver status: {str(e)}")
        return False

def job():
    """Main job function to run the scraper and send notifications."""
    logger.info(f"Running job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    start_time = time.time()
    
    # Verify WebDriver is working
    check_chrome_driver_status()
    
    # Check if test mode
    test_mode = "--test" in sys.argv
    
    if test_mode:
        logger.info("Running in TEST MODE")
        # Use sample data for testing
        test_properties = [
            {
                "store_id": "1234",
                "store_number": "Store #1234",
                "store_name": "Store #1234",
                "address": "123 Test St, Anytown, TX 12345",
                "review_count": 15000,
                "spaces": [
                    {"suite": "100", "sqft": 800, "text": "Suite 100 | 800 sqft"},
                    {"suite": "101", "sqft": 600, "text": "Suite 101 | 600 sqft"}
                ],
                "meets_criteria": True
            },
            {
                "store_id": "5678",
                "store_number": "Store #5678",
                "store_name": "Store #5678",
                "address": "456 Sample Ave, Testville, CA 67890",
                "review_count": 12000,
                "spaces": [
                    {"suite": "200", "sqft": 950, "text": "Suite 200 | 950 sqft"}
                ],
                "meets_criteria": True
            }
        ]
        send_email(test_properties)
        return
    
    # Process with quick mode if specified
    quick_mode = "--quick" in sys.argv
    if quick_mode:
        logger.info("Running in QUICK MODE - processing limited properties")
    
    # Step 1: Get properties with small spaces
    logger.info("Step 1: Scraping Walmart properties with small spaces")
    small_space_properties = get_walmart_properties_with_small_spaces()
    
    # Save intermediate results
    save_intermediate_results(small_space_properties, "small_space_properties.json")
    
    if not small_space_properties:
        logger.info("No properties with spaces under 1000 sqft found. Exiting.")
        return
    
    # Step 2: Check Google Maps for reviews and mobile stores
    logger.info(f"Step 2: Checking Google Maps data for {len(small_space_properties)} properties")
    checked_properties = check_locations_in_parallel(small_space_properties)
    
    # Save intermediate results
    save_intermediate_results(checked_properties, "checked_properties.json")
    
    # Step 3: Filter for matching properties that meet all criteria
    logger.info("Step 3: Filtering for properties that meet all criteria")
    matching_properties = [prop for prop in checked_properties if prop.get('meets_criteria', False)]
    
    # Step 4: Save final results with versioning
    logger.info("Step 4: Saving final results")
    final_properties = save_results_with_versioning(matching_properties)
    
    logger.info(f"Found {len(matching_properties)} properties matching ALL criteria")
    logger.info(f"Total execution time: {time.time() - start_time:.2f} seconds")
    
    # Step 5: Send email notification for matching properties
    if matching_properties:
        logger.info("Step 5: Sending email notification")
        send_email(matching_properties)
        logger.info(f"Email sent with {len(matching_properties)} matching properties")
    else:
        logger.info("No matching properties found, no email sent")

def main():
    """Main function to run the script once and set up scheduling."""
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Print help message if requested
    if "--help" in sys.argv or "-h" in sys.argv:
        print("Usage: python main.py [options]")
        print("Options:")
        print("  --test       Run in test mode with sample data")
        print("  --schedule   Run once and then schedule daily execution")
        print("  --quick      Process only a limited number of properties (faster)")
        print("  --workers N  Use N parallel browser workers (default: 15)")
        print("  --api N      Use N parallel API/location check workers (default: 8)")
        return
    
    # Check if parallel workers specified
    global WEB_WORKERS
    if "--workers" in sys.argv:
        try:
            idx = sys.argv.index("--workers")
            if idx + 1 < len(sys.argv) and sys.argv[idx + 1].isdigit():
                from config import WEB_WORKERS as config_web_workers
                worker_count = int(sys.argv[idx + 1])
                if 1 <= worker_count <= 30:  # Reasonable limits
                    logger.info(f"Using {worker_count} parallel browser workers")
                    # Update the WEB_WORKERS in config
                    import config
                    config.WEB_WORKERS = worker_count
        except Exception as e:
            logger.error(f"Error setting worker count: {str(e)}")
    
    # Check if API workers specified
    if "--api" in sys.argv:
        try:
            idx = sys.argv.index("--api")
            if idx + 1 < len(sys.argv) and sys.argv[idx + 1].isdigit():
                api_worker_count = int(sys.argv[idx + 1])
                if 1 <= api_worker_count <= 20:  # Reasonable limits
                    logger.info(f"Using {api_worker_count} parallel API workers")
                    # Update the API_WORKERS in config
                    import config
                    config.API_WORKERS = api_worker_count
        except Exception as e:
            logger.error(f"Error setting API worker count: {str(e)}")
    
    # Run the job immediately
    logger.info("Starting Walmart Leasing Space Checker (Parallel Version)")
    job()
    
    # Schedule to run daily at 8:00 AM if requested
    if "--schedule" in sys.argv:
        schedule.every().day.at("08:00").do(job)
        logger.info("Scheduled to run daily at 8:00 AM")
        
        # Keep the script running to execute scheduled jobs
        while True:
            schedule.run_pending()
            time.sleep(60)

if __name__ == "__main__":
    main()
