"""
Location checker for Walmart properties
Uses direct Google Maps searches instead of API calls
"""

import re
import time
import logging
import urllib.parse
import concurrent.futures
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

from config import MOBILE_STORE_KEYWORDS, MIN_REVIEWS, GOOGLE_MAPS_URL, SEARCH_RADIUS_METERS, API_WORKERS
from selenium_utils import setup_selenium_driver

# Configure logging
logger = logging.getLogger(__name__)

def extract_city_zip_from_address(address):
    """Extract city and zip code from a formatted address string."""
    try:
        # Try to extract US format ZIP code (5 digits, sometimes with 4 digit extension)
        zip_match = re.search(r'(\d{5}(?:-\d{4})?)', address)
        zip_code = zip_match.group(1) if zip_match else "Unknown"
        
        # Try to extract city
        # Pattern looks for "City, STATE ZIP" or "City STATE ZIP" format
        city_match = re.search(r'([A-Za-z\s\.]+),?\s+[A-Z]{2}\s+\d{5}', address)
        if city_match:
            city = city_match.group(1).strip()
        else:
            # Try to match without requiring ZIP code in the pattern
            city_match = re.search(r'([A-Za-z\s\.]+),\s+[A-Z]{2}', address)
            city = city_match.group(1).strip() if city_match else "Unknown"
        
        return {'city': city, 'zip_code': zip_code}
    except Exception as e:
        logger.error(f"Error extracting city/zip: {str(e)}")
        return {'city': "Unknown", 'zip_code': "Unknown"}

def address_similarity_check(address1, address2):
    """Check if two addresses are similar enough to likely be the same location."""
    # Convert both to lowercase for comparison
    addr1 = address1.lower()
    addr2 = address2.lower()
    
    # Basic exact match check
    if addr1 == addr2:
        return True
    
    # Extract numbers - if both addresses have numbers and they match, good sign
    numbers1 = re.findall(r'\d+', addr1)
    numbers2 = re.findall(r'\d+', addr2)
    
    # Extract street names
    streets1 = re.findall(r'([a-z]+\s+(?:street|st|avenue|ave|road|rd|drive|dr|blvd|boulevard))', addr1)
    streets2 = re.findall(r'([a-z]+\s+(?:street|st|avenue|ave|road|rd|drive|dr|blvd|boulevard))', addr2)
    
    # If both have numbers but they don't match at all, likely different places
    if numbers1 and numbers2 and not any(n in addr2 for n in numbers1):
        return False
        
    # If both have street names but none match, likely different places
    if streets1 and streets2 and not any(s in addr2 for s in streets1):
        return False
    
    # If we get here, there's enough similarity or ambiguity to pass
    return True

def check_google_reviews_and_stores(property_info):
    """
    Check Google Maps for review counts and nearby mobile stores 
    using direct web search instead of API.
    """
    # Keep original address and store ID
    original_address = property_info['address']
    original_store_id = property_info['store_id']
    
    # Format search query with Walmart prefix for better matching
    search_query = f"Walmart Store #{original_store_id} {original_address}"
    
    # Add retries for Google Maps access
    max_retries = 3
    for attempt in range(max_retries):
        driver = setup_selenium_driver(headless=True)
        if not driver:
            property_info['meets_criteria'] = False
            property_info['fail_reason'] = "Failed to create browser instance"
            return property_info
        
        try:
            # Add random delay to avoid throttling
            time.sleep(random.uniform(1, 3))
            
            # On retry attempts, try a more general query
            if attempt > 0:
                fallback_query = f"Walmart {original_address}"
                encoded_query = urllib.parse.quote(fallback_query)
                logger.info(f"Retry {attempt}/{max_retries} with fallback query: {fallback_query}")
            else:
                encoded_query = urllib.parse.quote(search_query)
            
            try:
                # Set a longer page load timeout for Google Maps
                driver.set_page_load_timeout(45)  # Increased timeout
                driver.get(f"{GOOGLE_MAPS_URL}{encoded_query}")
            except WebDriverException as e:
                logger.warning(f"WebDriver error when accessing Google Maps: {str(e)}")
                # Continue to next retry
                driver.quit()
                continue
            
            # Wait for results to load with increased timeout
            try:
                WebDriverWait(driver, 20).until(  # Increased timeout
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"], div.section-result-content, div[role="main"]'))
                )
            except TimeoutException:
                logger.warning(f"Timeout waiting for Google Maps results for {search_query}")
                # Don't return immediately, still try to process what's loaded
            
            # Get the Walmart info
            time.sleep(3)  # Extra wait for stabilization
            
            try:
                # Try to get the main result panel
                store_panel = driver.find_element(By.CSS_SELECTOR, 'div[role="main"], div.section-hero-header')
                
                # Extract full address
                try:
                    formatted_address_elem = store_panel.find_element(
                        By.CSS_SELECTOR, 
                        'button[data-item-id="address"], span.section-info-text'
                    )
                    formatted_address = formatted_address_elem.text.strip()
                    property_info['full_address'] = formatted_address
                    property_info['google_address'] = formatted_address
                    
                    # Extract city and zip
                    location_details = extract_city_zip_from_address(formatted_address)
                    property_info['city'] = location_details['city']
                    property_info['zip_code'] = location_details['zip_code']
                    
                    # Check for address mismatch
                    if not address_similarity_check(original_address, formatted_address):
                        property_info['address_mismatch_warning'] = True
                        logger.warning(f"Address mismatch: '{original_address}' vs '{formatted_address}'")
                except NoSuchElementException:
                    property_info['full_address'] = property_info['address']
                    property_info['city'] = "Unknown"
                    property_info['zip_code'] = "Unknown"
                
                # Extract review count
                try:
                    reviews_elem = store_panel.find_element(
                        By.CSS_SELECTOR, 
                        'span[aria-label*="stars"], span.rating-score'
                    )
                    if reviews_elem:
                        # The reviews element text might be like "4.2 stars 11,482 reviews"
                        reviews_text = reviews_elem.get_attribute('aria-label') or reviews_elem.text
                        review_count_match = re.search(r'([\d,]+)\s+review', reviews_text)
                        
                        if review_count_match:
                            review_count_str = review_count_match.group(1).replace(',', '')
                            property_info['review_count'] = int(review_count_str)
                        else:
                            # Try to find a separate element with review count
                            review_count_elem = store_panel.find_element(
                                By.CSS_SELECTOR,
                                'span[aria-label*="review"], span.reviews-count'
                            )
                            if review_count_elem:
                                review_text = review_count_elem.text
                                review_count_str = re.search(r'([\d,]+)', review_text)
                                if review_count_str:
                                    property_info['review_count'] = int(review_count_str.group(1).replace(',', ''))
                except (NoSuchElementException, ValueError):
                    property_info['review_count'] = 0
                
                # Extract phone number
                try:
                    phone_elem = store_panel.find_element(
                        By.CSS_SELECTOR,
                        'button[data-item-id="phone:tel"], span.phone-number'
                    )
                    if phone_elem:
                        property_info['phone_number'] = phone_elem.text.strip()
                except NoSuchElementException:
                    pass
                
                # Extract website if available
                try:
                    website_elem = store_panel.find_element(
                        By.CSS_SELECTOR,
                        'a[data-item-id="authority"], button[data-item-id*="website"], a[href*="walmart.com"]'
                    )
                    if website_elem:
                        website = website_elem.get_attribute('href') or website_elem.text
                        property_info['website'] = website
                        
                        # Extract store ID from website URL
                        if "walmart.com/store/" in website:
                            store_url_match = re.search(r'walmart\.com/store/(\d+)', website)
                            if store_url_match:
                                website_store_id = store_url_match.group(1)
                                property_info['website_store_id'] = website_store_id
                                property_info['leasing_id'] = original_store_id
                                
                                # Flag if IDs don't match
                                if website_store_id != original_store_id:
                                    property_info['id_mismatch'] = True
                except NoSuchElementException:
                    pass
                
                # Skip further checking if it doesn't meet review threshold
                if property_info.get('review_count', 0) < MIN_REVIEWS:
                    property_info['meets_criteria'] = False
                    property_info['fail_reason'] = f"Only {property_info.get('review_count', 0)} reviews (minimum {MIN_REVIEWS})"
                    return property_info
                
                # Now check for nearby mobile stores
                mobile_store_result = check_nearby_mobile_stores(driver, property_info)
                
                if mobile_store_result['has_mobile']:
                    property_info['meets_criteria'] = False
                    property_info['fail_reason'] = "Has a mobile phone store"
                else:
                    property_info['meets_criteria'] = True
                    property_info['fail_reason'] = None
                    
            except NoSuchElementException:
                logger.warning(f"Could not find store panel for {search_query}")
                property_info['meets_criteria'] = False
                property_info['fail_reason'] = "Could not find store in Google Maps"
                
        except Exception as e:
            logger.error(f"Error checking Google data for {property_info['store_name']}: {str(e)}")
            if attempt < max_retries - 1:
                # Try again with next retry
                logger.info(f"Will retry Google Maps search for {property_info['store_name']}")
                driver.quit()
                continue
            else:
                property_info['error'] = str(e)
                property_info['meets_criteria'] = False
                property_info['fail_reason'] = f"Error checking Google data: {str(e)}"
        
        finally:
            # Clean up
            if driver:
                driver.quit()
                
        # If we got here without triggering a continue, break out of retry loop
        break
            
    return property_info

def check_nearby_mobile_stores(driver, property_info):
    """
    Check for nearby mobile phone repair stores by directly searching on Google Maps.
    Returns a dictionary with results.
    """
    result = {
        'has_mobile': False,
        'stores': []
    }
    
    try:
        # Get current Walmart location URL
        walmart_url = driver.current_url
        
        # For each keyword, search for nearby stores
        found_stores = []
        
        # Prepare search terms based on keywords
        search_terms = [
            "mobile phone repair",
            "cell phone repair",
            "CPR Cell Phone Repair", 
            "Techy Cellaris Casemate"
        ]
        
        # Search for each term
        for term in search_terms:
            # Construct search query to look near current location
            # This simulates the "search nearby" feature of Google Maps
            search_query = f"{term} near {property_info['address']}"
            encoded_query = urllib.parse.quote(search_query)
            driver.get(f"{GOOGLE_MAPS_URL}{encoded_query}")
            
            # Wait for results
            time.sleep(3)
            
            try:
                # Wait for search results to appear
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, 'div[role="feed"], div.section-result-content')
                    )
                )
                
                # Get all result elements
                result_elements = driver.find_elements(
                    By.CSS_SELECTOR, 
                    'div[role="article"], div.section-result'
                )
                
                # Process each result
                for elem in result_elements[:10]:  # Limit to first 10 results
                    try:
                        store_name = elem.find_element(
                            By.CSS_SELECTOR, 
                            'h3, div.section-result-title'
                        ).text.strip()
                        
                        # Check if distance is shown and is within our target radius (100m)
                        distance_elem = elem.find_elements(
                            By.CSS_SELECTOR, 
                            'div[class*="distance"], span.section-result-distance'
                        )
                        
                        if distance_elem:
                            distance_text = distance_elem[0].text
                            # Extract distance value and unit
                            distance_match = re.search(r'([\d.]+)\s*(mi|km|m|ft)', distance_text)
                            
                            if distance_match:
                                distance_val = float(distance_match.group(1))
                                distance_unit = distance_match.group(2)
                                
                                # Convert to meters for consistency
                                if distance_unit == 'km':
                                    distance_meters = distance_val * 1000
                                elif distance_unit == 'mi':
                                    distance_meters = distance_val * 1609.34
                                elif distance_unit == 'ft':
                                    distance_meters = distance_val * 0.3048
                                else:  # Already in meters
                                    distance_meters = distance_val
                                
                                # Check if within our target radius
                                if distance_meters <= SEARCH_RADIUS_METERS:
                                    # Check if matches any of our keywords
                                    store_name_lower = store_name.lower()
                                    matches = [term for term in MOBILE_STORE_KEYWORDS 
                                            if term.lower() in store_name_lower]
                                    
                                    if matches:
                                        store_entry = {
                                            'name': store_name,
                                            'matched_keywords': matches,
                                            'distance': f"{distance_val} {distance_unit}"
                                        }
                                        
                                        # Avoid duplicates
                                        if not any(s.get('name') == store_name for s in found_stores):
                                            found_stores.append(store_entry)
                    
                    except NoSuchElementException:
                        continue
                    
            except TimeoutException:
                logger.warning(f"Timeout waiting for nearby {term} results")
                
            # Return to the Walmart location
            driver.get(walmart_url)
            time.sleep(1)
        
        # Update result with all found stores
        if found_stores:
            result['has_mobile'] = True
            result['stores'] = found_stores
            
        # Add info about the search method to the property info
        property_info['mobile_store_search_method'] = "Google Maps Web Search"
        property_info['mobile_store_search_radius'] = f"{SEARCH_RADIUS_METERS} meters"
        property_info['mobile_store_keywords_checked'] = MOBILE_STORE_KEYWORDS
        property_info['has_mobile_store'] = result['has_mobile']
        
        # Store any found matches
        if result['stores']:
            property_info['mobile_stores_found'] = result['stores']
            
    except Exception as e:
        logger.error(f"Error checking for mobile stores: {str(e)}")
        result['has_mobile'] = True  # Assume yes for safety
        result['error'] = str(e)
        property_info['has_mobile_store'] = True
        property_info['mobile_store_error'] = str(e)
    
    return result

def check_locations_in_parallel(small_space_properties):
    """Check Google Maps data for properties in parallel."""
    logger.info(f"Checking Google Maps data for {len(small_space_properties)} properties in parallel")
    
    checked_properties = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=API_WORKERS) as executor:
        # Process Google data in parallel
        future_to_property = {
            executor.submit(check_google_reviews_and_stores, prop): i 
            for i, prop in enumerate(small_space_properties)
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_property):
            prop_idx = future_to_property[future]
            try:
                result = future.result()
                if result:
                    checked_properties.append(result)
                    # Use ASCII-safe status indicators instead of Unicode symbols
                    status = "MATCH" if result.get('meets_criteria') else "NO MATCH"
                    logger.info(f"Property {prop_idx+1}/{len(small_space_properties)}: {status} - {result.get('store_number')}")
            except Exception as e:
                logger.error(f"Error processing property {prop_idx}: {str(e)}")
    
    return checked_properties
