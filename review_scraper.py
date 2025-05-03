"""
Review scraper for Walmart stores
Scrapes Google Maps to get review counts and store coordinates
"""

import logging
import time
import re
import urllib.parse
import random
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from config import MIN_REVIEWS, GOOGLE_MAPS_URL
from playwright_utils import setup_playwright_browser, close_browser, wait_for_element

# Configure logging
logger = logging.getLogger(__name__)


def extract_review_count_from_page(page):
    """
    Extract review count from Google Maps page.
    
    Args:
        page: Playwright page object
        
    Returns:
        int: Number of reviews
    """
    review_count = 0
    
    try:
        # Method 1: Direct extraction from F7nice element
        f7nice_element = page.query_selector(".F7nice")
        if f7nice_element:
            full_text = f7nice_element.inner_text()
            reviews_match = re.search(r"\(([0-9.,]+)\)", full_text)
            if reviews_match:
                review_str = reviews_match.group(1).replace(".", "").replace(",", "")
                try:
                    review_count = int(review_str)
                    logger.info(f"Found {review_count} reviews from F7nice element")
                    return review_count
                except ValueError:
                    pass
        
        # Method 2: Look for spans with review count in aria-label
        aria_elements = page.query_selector_all(
            'span[aria-label*="reseñas"], span[aria-label*="reviews"], span[aria-label*="reseña"]'
        )
        
        for elem in aria_elements:
            aria_text = elem.get_attribute("aria-label") or elem.inner_text()
            review_match = re.search(
                r"([\d.,]+)\s*(?:reseñas|reviews|review|reseñas|reseña)",
                aria_text,
                re.IGNORECASE,
            )
            if review_match:
                review_str = review_match.group(1).replace(",", "").replace(".", "")
                try:
                    count = int(review_str)
                    if count > review_count:
                        review_count = count
                        logger.info(f"Found {review_count} reviews via aria-label")
                except ValueError:
                    pass
        
        # Method 3: JavaScript extraction as final attempt
        if review_count == 0:
            try:
                js_result = page.evaluate("""
                    () => {
                        // Try multiple methods to find review count
                        // Method 1: Check for spans with reviews in aria-label
                        const reviewSpans = Array.from(document.querySelectorAll('span[aria-label*="review"]'));
                        for (const span of reviewSpans) {
                            const match = span.getAttribute('aria-label')?.match(/([\d.,]+)\\s*review/i);
                            if (match) return match[1].replace(/[,.]/g, '');
                        }
                        
                        // Method 2: Check for F7nice div with review count
                        const f7nice = document.querySelector('.F7nice');
                        if (f7nice) {
                            const text = f7nice.textContent;
                            const match = text.match(/\\(([\d.,]+)\\)/);
                            if (match) return match[1].replace(/[,.]/g, '');
                        }
                        
                        return "0";
                    }
                """)
                try:
                    js_count = int(js_result)
                    review_count = js_count
                    logger.info(f"Found {review_count} reviews via JavaScript")
                except ValueError:
                    pass
            except Exception as js_error:
                logger.warning(f"JavaScript review extraction failed: {str(js_error)}")
    
    except Exception as e:
        logger.error(f"Error extracting review count: {str(e)}")
    
    return review_count


def extract_coordinates_from_url(page):
    """
    Extract coordinates from the Google Maps URL.
    
    Args:
        page: Playwright page object
        
    Returns:
        str: Coordinates in format "lat,lng" or None if not found
    """
    try:
        # Get the current URL
        current_url = page.url
        
        # Extract coordinates using regex patterns
        patterns = [
            r"@(-?\d+\.\d+),(-?\d+\.\d+)",  # @lat,lng format
            r"ll=(-?\d+\.\d+),(-?\d+\.\d+)", # ll=lat,lng format
            r"q=(-?\d+\.\d+),(-?\d+\.\d+)"   # q=lat,lng format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, current_url)
            if match:
                lat, lng = match.groups()
                logger.info(f"Extracted coordinates: {lat},{lng}")
                return f"{lat},{lng}"
        
        # Try JavaScript extraction if regex fails
        try:
            coords = page.evaluate("""
                () => {
                    // Try to find coordinates in the URL or in page metadata
                    if (window.APP_INITIALIZATION_STATE) {
                        const appState = window.APP_INITIALIZATION_STATE;
                        // Look for coordinate patterns in the app state
                        const match = appState.match(/"(-?\\d+\\.\\d+),(-?\\d+\\.\\d+)"/);
                        if (match) return `${match[1]},${match[2]}`;
                    }
                    return null;
                }
            """)
            
            if coords:
                logger.info(f"Extracted coordinates via JavaScript: {coords}")
                return coords
        except Exception as js_error:
            logger.debug(f"JavaScript coordinate extraction failed: {str(js_error)}")
        
        logger.warning("Could not extract coordinates from URL")
        return None
    
    except Exception as e:
        logger.error(f"Error extracting coordinates: {str(e)}")
        return None


def process_store_for_reviews(store_property):
    """
    Process a single store to get review counts and coordinates.
    
    Args:
        store_property: Dictionary with store information
        
    Returns:
        dict: Updated store property with review count and coordinates
    """
    store_id = store_property.get("store_id", "Unknown")
    address = store_property.get("address", "")
    store_number = f"Store #{store_id}"
    
    logger.info(f"Checking reviews for {store_number}: {address}")
    
    # Prepare search query
    search_query = f"Walmart {store_number} {address}"
    
    # Create browser instance
    browser_info = setup_playwright_browser(headless=True)
    if not browser_info:
        logger.error(f"Failed to create browser for {store_number}")
        store_property["meets_criteria"] = False
        store_property["fail_reason"] = "Browser setup failed"
        return store_property
    
    try:
        page = browser_info["page"]
        
        # Navigate to Google Maps
        encoded_query = urllib.parse.quote(search_query)
        page.goto(f"{GOOGLE_MAPS_URL}{encoded_query}", wait_until="domcontentloaded")
        
        # Wait for content to load
        try:
            wait_for_element(page, 'div[role="main"], div.section-hero-header, .F7nice', timeout=30)
            time.sleep(3)  # Additional wait for dynamic content
        except PlaywrightTimeoutError:
            logger.warning(f"Timeout waiting for Maps content for {store_number}")
        
        # Extract review count
        review_count = extract_review_count_from_page(page)
        store_property["review_count"] = review_count
        
        # Extract coordinates
        coordinates = extract_coordinates_from_url(page)
        if coordinates:
            store_property["location_coordinate"] = coordinates
        
        # Extract more complete address
        try:
            formatted_address_elem = page.query_selector('button[data-item-id="address"], span.section-info-text')
            if formatted_address_elem:
                formatted_address = formatted_address_elem.inner_text().strip()
                store_property["full_address"] = formatted_address
        except Exception as address_error:
            logger.debug(f"Error extracting formatted address: {str(address_error)}")
        
        # Check if it meets review criteria
        if review_count >= MIN_REVIEWS:
            store_property["meets_criteria"] = True
            logger.info(f"✓ {store_number} has {review_count} reviews (minimum {MIN_REVIEWS})")
        else:
            store_property["meets_criteria"] = False
            store_property["fail_reason"] = f"Only {review_count} reviews (minimum {MIN_REVIEWS})"
            logger.info(f"✗ {store_number} has only {review_count} reviews (minimum {MIN_REVIEWS})")
        
    except Exception as e:
        logger.error(f"Error processing {store_number}: {str(e)}")
        store_property["meets_criteria"] = False
        store_property["fail_reason"] = f"Error checking reviews: {str(e)}"
    finally:
        close_browser(browser_info)
    
    return store_property


def process_stores_for_reviews(store_properties, max_workers=4):
    """
    Process multiple stores to get review counts and coordinates.
    
    Args:
        store_properties: List of store property dictionaries
        max_workers: Maximum number of parallel workers
        
    Returns:
        list: Updated store properties with review counts and coordinates
    """
    import concurrent.futures
    
    logger.info(f"Processing reviews for {len(store_properties)} stores with {max_workers} workers")
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(process_store_for_reviews, store): store for store in store_properties}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            store = futures[future]
            try:
                result = future.result()
                results.append(result)
                
                # Add progress logging
                if len(results) % 10 == 0 or len(results) == len(store_properties):
                    logger.info(f"Progress: {len(results)}/{len(store_properties)} stores processed")
            except Exception as e:
                logger.error(f"Error processing store {store.get('store_id', 'Unknown')}: {str(e)}")
                # Add failed store with error info
                store["meets_criteria"] = False
                store["fail_reason"] = f"Processing error: {str(e)}"
                results.append(store)
    
    # Count how many meet criteria
    meeting_criteria = sum(1 for p in results if p.get("meets_criteria", False))
    logger.info(f"Finished processing {len(results)} stores: {meeting_criteria} meet review criteria")
    
    return results


# For direct testing
if __name__ == "__main__":
    import json
    import os
    from config import OUTPUT_DIR
    
    # Configure root logger for direct execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Input file path
    input_file = os.path.join(OUTPUT_DIR, "small_space_properties.json")
    
    if os.path.exists(input_file):
        with open(input_file, 'r', encoding='utf-8') as f:
            stores = json.load(f)
            
        # Process first 3 stores if in test mode
        test_stores = stores[:3] if len(sys.argv) > 1 and sys.argv[1] == "--test" else stores
        processed_stores = process_stores_for_reviews(test_stores, max_workers=2)
        
        # Save results
        output_file = os.path.join(OUTPUT_DIR, "stores_with_reviews.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(processed_stores, f, indent=2)
            
        logger.info(f"Results saved to {output_file}")
        
        # Print summary
        meeting_criteria = [s for s in processed_stores if s.get("meets_criteria", False)]
        logger.info(f"Summary: {len(meeting_criteria)}/{len(processed_stores)} stores meet review criteria")
    else:
        logger.error(f"Input file not found: {input_file}")
        logger.info("Please run the sqft scraper first")
