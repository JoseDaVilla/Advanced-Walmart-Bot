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
import psutil
import threading

from config import OUTPUT_DIR, MAX_SPACE_SIZE, WEB_WORKERS, API_WORKERS
from scraper import get_walmart_properties_with_small_spaces
from location_checker import check_locations_in_parallel
from email_notifier import send_email
from data_manager import save_results_with_versioning, save_intermediate_results
from playwright_utils import setup_playwright_browser, close_browser

# Configure better logging with encoding fixes
system_encoding = locale.getpreferredencoding()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        # Specify UTF-8 encoding for the log file to support all characters
        logging.FileHandler("walmart_leasing_parallel.log", encoding="utf-8"),
        # Use system encoding for console output (avoids encoding errors)
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def check_playwright_status():
    """Verify Playwright browser can be created."""
    logger.info("Checking Playwright browser status...")

    try:
        # Test if we can create a Playwright browser instance
        browser_info = setup_playwright_browser(headless=True, retries=1)

        if browser_info:
            logger.info("Playwright browser is working correctly")
            close_browser(browser_info)
            return True
        else:
            logger.warning("Could not create Playwright browser.")
            return False
    except Exception as e:
        logger.error(f"Error checking Playwright browser status: {str(e)}")
        return False


def monitor_resources():
    """Monitor and log system resource usage during execution."""
    stop_monitor = threading.Event()

    def _monitor_thread():
        start_time = time.time()
        while not stop_monitor.is_set():
            # Get CPU and memory usage
            cpu_percent = psutil.cpu_percent(interval=None)
            memory_percent = psutil.virtual_memory().percent

            # Log usage every 30 seconds
            elapsed = time.time() - start_time
            if elapsed % 30 < 1:
                logger.info(
                    f"Resource monitor: CPU: {cpu_percent}%, Memory: {memory_percent}% (Elapsed: {int(elapsed)}s)"
                )

            time.sleep(5)

    # Set daemon=True to prevent blocking program exit
    monitor_thread = threading.Thread(target=_monitor_thread, daemon=True)
    monitor_thread.start()

    return stop_monitor


def job():
    """Main job function to run the scraper and send notifications."""
    logger.info(f"Running job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    start_time = time.time()

    # Start monitoring resources
    stop_monitor = monitor_resources()

    try:
        # Verify Playwright is working
        check_playwright_status()

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
                        {"suite": "101", "sqft": 600, "text": "Suite 101 | 600 sqft"},
                    ],
                    "meets_criteria": True,
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
                    "meets_criteria": True,
                },
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

        # Add an extra filter to ensure all spaces are under the limit
        for prop in small_space_properties:
            # Filter out any spaces over the MAX_SPACE_SIZE
            prop["spaces"] = [
                space
                for space in prop.get("spaces", [])
                if space.get("sqft", 0) < MAX_SPACE_SIZE
            ]

        # Filter out properties that no longer have any valid spaces
        small_space_properties = [
            prop for prop in small_space_properties if prop.get("spaces", [])
        ]

        # Save intermediate results
        save_intermediate_results(small_space_properties, "small_space_properties.json")

        if not small_space_properties:
            logger.info("No properties with spaces under 1000 sqft found. Exiting.")
            return

        # Step 2: Check Google Maps for reviews and mobile stores
        logger.info(
            f"Step 2: Checking Google Maps data for {len(small_space_properties)} properties"
        )
        checked_properties = check_locations_in_parallel(small_space_properties)

        # Save intermediate results
        save_intermediate_results(checked_properties, "checked_properties.json")

        # Step 3: Filter for matching properties that meet all criteria
        logger.info("Step 3: Filtering for properties that meet all criteria")

        # More detailed logging about filtering
        total_checked = len(checked_properties)
        meeting_criteria = [
            prop for prop in checked_properties if prop.get("meets_criteria", False)
        ]
        not_meeting_criteria = [
            prop for prop in checked_properties if not prop.get("meets_criteria", False)
        ]

        logger.info(f"Total properties checked: {total_checked}")
        logger.info(f"Properties meeting criteria: {len(meeting_criteria)}")
        logger.info(f"Properties NOT meeting criteria: {len(not_meeting_criteria)}")

        # Log detailed reasons for failures
        failure_reasons = {}
        for prop in not_meeting_criteria:
            reason = prop.get("fail_reason", "Unknown")
            store_num = prop.get("store_number", "Unknown")
            if reason in failure_reasons:
                failure_reasons[reason].append(store_num)
            else:
                failure_reasons[reason] = [store_num]

        for reason, stores in failure_reasons.items():
            logger.info(f"Failure reason '{reason}': {len(stores)} stores")
            if "mobile" in reason.lower() and len(stores) <= 10:
                logger.info(f"Stores with mobile stores: {stores}")

        matching_properties = meeting_criteria

        # Additional filter to remove properties with unknown city or ZIP code
        filtered_properties = [
            prop
            for prop in matching_properties
            if prop.get("city", "Unknown") != "Unknown"
            and prop.get("zip_code", "Unknown") != "Unknown"
        ]

        logger.info(
            f"Filtered out {len(matching_properties) - len(filtered_properties)} properties with unknown city or ZIP"
        )

        # Log how many stores remained
        logger.info(
            f"Final properties meeting ALL criteria: {len(filtered_properties)}"
        )

        if filtered_properties:
            # Log some details about the matching properties
            for i, prop in enumerate(filtered_properties[:5]):  # Show first 5
                store_id = prop.get("store_id", "Unknown")
                address = prop.get("address", "Unknown")
                logger.info(f"Match {i+1}: Store #{store_id} at {address}")

        # Step 4: Save final results with versioning
        logger.info("Step 4: Saving final results")
        final_properties = save_results_with_versioning(filtered_properties)

        logger.info(
            f"Found {len(filtered_properties)} properties matching ALL criteria with known locations"
        )
        logger.info(f"Total execution time: {time.time() - start_time:.2f} seconds")

        # Step 5: Send email notification for matching properties
        if filtered_properties:
            logger.info("Step 5: Sending email notification")
            send_email(filtered_properties)
            logger.info(
                f"Email sent with {len(filtered_properties)} matching properties"
            )
        else:
            logger.info("No matching properties found, no email sent")

    finally:
        # Stop resource monitoring
        stop_monitor.set()


def main():
    """Main function to run the script once and set up scheduling."""
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Print help message if requested
    if "--help" in sys.argv or "-h" in sys.argv:
        print("Usage: python main.py [options]")
        print("Options:")
        print("  --test                  Run in test mode with sample data")
        print("  --schedule              Run once and then schedule daily execution")
        print(
            "  --quick                 Process only a limited number of properties (faster)"
        )
        print("  --workers N             Use N parallel browser workers (default: 15)")
        print(
            "  --api N                 Use N parallel API/location check workers (default: 8)"
        )
        print(
            "  --min-reviews N         Set minimum review count to N (default: 10000)"
        )
        print("  --debug-screenshots     Save screenshots for debugging")
        return

    # Apply command line overrides to configuration
    import config

    # Check for min-reviews override
    if "--min-reviews" in sys.argv:
        try:
            idx = sys.argv.index("--min-reviews")
            if idx + 1 < len(sys.argv) and sys.argv[idx + 1].isdigit():
                min_reviews = int(sys.argv[idx + 1])
                logger.info(f"Overriding minimum review count to {min_reviews}")
                # Update the MIN_REVIEWS in config
                config.MIN_REVIEWS = min_reviews
        except Exception as e:
            logger.error(f"Error setting minimum reviews: {str(e)}")

    # Enable debug screenshots if requested
    if "--debug-screenshots" in sys.argv:
        global SAVE_DEBUG_SCREENSHOTS
        SAVE_DEBUG_SCREENSHOTS = True
        logger.info("Debug screenshots enabled")

    # Check if parallel workers specified - IMPORTANT: This needs to be applied to the config
    if "--workers" in sys.argv:
        try:
            idx = sys.argv.index("--workers")
            if idx + 1 < len(sys.argv) and sys.argv[idx + 1].isdigit():
                worker_count = int(sys.argv[idx + 1])
                if 1 <= worker_count <= 30:  # Reasonable limits
                    logger.info(f"Using {worker_count} parallel browser workers")
                    # Update the WEB_WORKERS in config
                    config.WEB_WORKERS = worker_count
                else:
                    logger.warning(
                        f"Invalid worker count: {worker_count}, using default of {config.WEB_WORKERS}"
                    )
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
                    config.API_WORKERS = api_worker_count
                else:
                    logger.warning(
                        f"Invalid API worker count: {api_worker_count}, using default of {config.API_WORKERS}"
                    )
        except Exception as e:
            logger.error(f"Error setting API worker count: {str(e)}")

    # Run the job immediately (fixes the asyncio warning)
    logger.info("Starting Walmart Leasing Space Checker (Playwright Version)")

    # Fix for asyncio warning: use get_running_loop instead of get_event_loop
    try:
        import asyncio

        try:
            loop = (
                asyncio.get_running_loop()
            )  # This will raise a RuntimeError if no loop is running
            logger.warning(
                "Detected running asyncio event loop - this may cause issues with Playwright"
            )
        except RuntimeError:
            # No running loop, this is the expected case
            pass
    except Exception:
        pass

    # Run the main job
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
