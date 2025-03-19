"""
Walmart Leasing Space Checker - Parallel Processing Version

This script:
1. Scrapes Walmart leasing page for properties with available spaces < 1000 sqft
2. Uses parallel processing for faster execution
3. Makes parallel API calls to check reviews and mobile stores
4. Sends email notifications about matching properties
5. Runs on a daily schedule or one-time
"""

import os
import re
import json
import sys
import time
import logging
import requests
import smtplib
import schedule
import concurrent.futures
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, WebDriverException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("walmart_leasing_parallel.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
WALMART_LEASING_URL = "https://leasing.walmart.com/viewspaces"
GOOGLE_API_KEY = "AIzaSyC6av-FESCOQG9F-G4oZ0k9KVweacH3KIU"
MAX_SPACE_SIZE = 1000  
MIN_REVIEWS = 10000
MOBILE_STORE_KEYWORDS = ["CPR", "TalknFix", "iTalkandRepair", "mobile repair", "phone repair", 
                        "cell phone", "cellular", "smartphone repair", "iphone repair", "wireless repair",
                        "Cell Phone Repair", "Ifixandrepair", "Cellaris", "Thefix", "Casemate", "Techy",
                        "iFixandRepair", "IFixAndRepair", "The Fix", "Case Mate", "CaseMate"]

# Email configuration
EMAIL_SENDER = "testproject815@gmail.com"
EMAIL_PASSWORD = "bhkf idoc twdj hidb"
EMAIL_RECEIVER = "josedvilla18@gmail.com"

# Output directory for JSON data
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Concurrency settings
WEB_WORKERS = 15    # Increase from 2 to 3 for even faster processing  
API_WORKERS = 8    # Increase from 4 to 8 for faster API calls


def setup_selenium_driver(headless=True):
    """Set up and return a Selenium WebDriver."""
    try:
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless=new')
        
        # Other useful options
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        
        # Suppress browser warning messages
        chrome_options.add_argument('--log-level=3')  # Only show fatal errors
        chrome_options.add_argument('--silent')
        
        # Try to use webdriver-manager for ChromeDriver installation
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except ImportError:
            # Fallback if webdriver-manager is not available
            driver = webdriver.Chrome(options=chrome_options)
            
        return driver
    except WebDriverException as e:
        logger.error(f"Failed to set up Selenium driver: {str(e)}")
        logger.error("Make sure you have Chrome and ChromeDriver installed.")
        return None


def extract_property_info(button_html):
    """Extract basic property info from button HTML."""
    soup = BeautifulSoup(button_html, 'html.parser')
    
    # Extract store info div
    store_info_div = soup.select_one('.jss58')
    if not store_info_div:
        return None
    
    # Extract store number - improved extraction
    store_number_elem = store_info_div.select_one('b.jss53')
    store_number_text = store_number_elem.text if store_number_elem else "Unknown"
    
    # Extract the numeric store ID more carefully
    store_id_match = re.search(r'Store #(\d+)', store_number_text)
    store_id = store_id_match.group(1) if store_id_match else store_number_text.replace("Store #", "").strip()
    
    # Extract available spaces
    available_spaces_elem = store_info_div.select('b.jss53')
    available_spaces = available_spaces_elem[-1].text if len(available_spaces_elem) > 2 else "Unknown"
    
    # Extract address
    address_elem = store_info_div.select_one('p.jss54')
    address = address_elem.text.strip() if address_elem else "Unknown"
    
    # Extract Google Maps URL
    maps_link = store_info_div.select_one('a.jss55')
    maps_url = maps_link['href'] if maps_link and maps_link.has_attr('href') else ""
    
    return {
        "store_id": store_id,              # Just the numeric ID
        "store_number": f"Store #{store_id}",  # Full store number with prefix
        "store_name": store_number_text,    # Original store name text
        "address": address,
        "available_spaces": available_spaces.strip(),
        "google_maps_url": maps_url,
        "spaces": []
    }


def extract_modal_data(modal_html):
    """Extract spaces information from modal HTML."""
    soup = BeautifulSoup(modal_html, 'html.parser')
    spaces = []
    
    # Try to use the modal reference data if available
    modal_reference = load_modal_reference()
    if modal_reference:
        # Use patterns from the reference modal to enhance extraction
        reference_spaces = extract_spaces_from_reference(modal_reference)
        if reference_spaces:
            logger.info("Using modal reference patterns to improve extraction")
    
    # First try the exact structure from modal_data.html
    suite_spans = soup.select('.jss98 span[style*="font-weight: bold"]')
    if suite_spans:
        for suite_span in suite_spans:
            try:
                suite_text = suite_span.text.strip()
                # Look for the sibling span with the sqft
                sqft_span = suite_span.find_next_sibling('span')
                if sqft_span:
                    sqft_text = sqft_span.text.strip()
                    # Extract the suite number and sqft
                    suite_match = re.search(r'Suite\s+(\w+)', suite_text)
                    suite = suite_match.group(1) if suite_match else None
                    
                    sqft_match = re.search(r'(\d+)\s*(?:sq\s*ft|sqft)', sqft_text, re.IGNORECASE)
                    if sqft_match:
                        sqft = int(sqft_match.group(1))
                        spaces.append({
                            'suite': suite or "TBD",  # Use "TBD" instead of "Unknown"
                            'sqft': sqft,
                            'text': f"{suite_text} {sqft_text}"
                        })
            except Exception as e:
                logger.error(f"Error extracting space from span: {str(e)}")
    
    # If we didn't find spaces with the exact structure, fall back to our existing method
    if not spaces:
        # Try multiple patterns to extract suite information
        space_text_patterns = [
            # Try to find any text in the modal with sqft mentioned
            r"(?:Suite\s+([A-Za-z0-9-]+))?\s*(?:[|:])?\s*(\d+)\s*(?:sq\s*ft|sqft)",
            r"Suite\s+([A-Za-z0-9-]+)",
            r"(\d+)\s*(?:sq\s*ft|sqft)"
        ]
        
        # Get all text from the modal
        modal_text = soup.text
        
        # Test each pattern
        for pattern in space_text_patterns:
            matches = re.findall(pattern, modal_text, re.IGNORECASE)
            if matches:
                if isinstance(matches[0], tuple):
                    # If pattern captured groups in tuple format
                    for suite_match, size_match in matches:
                        try:
                            suite = suite_match if suite_match else "TBD"
                            sqft = int(size_match)
                            spaces.append({
                                'suite': suite,
                                'sqft': sqft,
                                'text': f"Suite {suite} | {sqft} sqft"
                            })
                        except (ValueError, IndexError):
                            continue
                else:
                    # If pattern captured just one group
                    space_selectors = ['.jss96', '.jss98', 'p[class*="jss"] span']
                    for selector in space_selectors:
                        space_elements = soup.select(selector)
                        for space_elem in space_elements:
                            space_text = space_elem.text.strip()
                            
                            # Look for square footage pattern
                            sqft_match = re.search(r'(\d+)\s*(?:sq\s*ft|sqft)', space_text, re.IGNORECASE)
                            if sqft_match:
                                sqft = int(sqft_match.group(1))
                                
                                # Extract suite number
                                suite_match = re.search(r'Suite\s+([A-Za-z0-9-]+)', space_text)
                                suite = suite_match.group(1) if suite_match else "TBD"
                                
                                spaces.append({
                                    'suite': suite,
                                    'sqft': sqft,
                                    'text': space_text
                                })
    
    # Deduplicate spaces - remove duplicate TBD entries
    if spaces:
        # Group spaces by square footage
        spaces_by_sqft = {}
        for space in spaces:
            sqft = space.get('sqft')
            if sqft not in spaces_by_sqft:
                spaces_by_sqft[sqft] = []
            spaces_by_sqft[sqft].append(space)
        
        # For each square footage, prefer entries with actual suite numbers over TBD
        deduplicated_spaces = []
        for sqft, space_group in spaces_by_sqft.items():
            # Filter spaces with actual suite numbers (not TBD)
            named_suites = [s for s in space_group if s.get('suite') != 'TBD']
            if named_suites:
                # Add all spaces with actual suite numbers
                deduplicated_spaces.extend(named_suites)
            else:
                # If all are TBD, just add one
                deduplicated_spaces.append(space_group[0])
        
        spaces = deduplicated_spaces
    
    return spaces


def extract_spaces_from_reference(modal_soup):
    """Extract space patterns from reference modal."""
    spaces = []
    suite_spans = modal_soup.select('.jss98 span[style*="font-weight: bold"]')
    if suite_spans:
        for suite_span in suite_spans:
            try:
                suite_text = suite_span.text.strip()
                sqft_span = suite_span.find_next_sibling('span')
                if sqft_span:
                    sqft_text = sqft_span.text.strip()
                    spaces.append({
                        'pattern': suite_text + " " + sqft_text,
                        'selectors': {
                            'suite': '.jss98 span[style*="font-weight: bold"]',
                            'sqft': '.jss98 span:not([style*="font-weight: bold"])'
                        }
                    })
            except Exception:
                pass
    return spaces


def process_property_chunk(buttons_chunk, worker_id=0):
    """Process a chunk of property buttons with a single browser instance."""
    logger.info(f"Worker {worker_id}: Processing {len(buttons_chunk)} property buttons")
    
    # Set up a new browser instance
    driver = setup_selenium_driver(headless=True)  # Changed to headless=True for faster performance
    if not driver:
        logger.error(f"Worker {worker_id}: Failed to create browser instance")
        return []
    
    try:
        # Load the Walmart leasing page
        driver.get(WALMART_LEASING_URL)
        logger.info(f"Worker {worker_id}: Loaded Walmart leasing page")
        
        # Wait for the page to load
        try:
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'button[class*="jss"]')))
        except TimeoutException:
            logger.warning(f"Worker {worker_id}: Timeout waiting for page to load")
        
        # Extra wait for JavaScript
        time.sleep(5)
        
        # Find all property buttons - each worker should count its own buttons
        all_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.jss56')
        worker_button_count = len(all_buttons)
        logger.info(f"Worker {worker_id}: Found {worker_button_count} property buttons")
        
        # Create a map of what buttons this worker should process
        # Using modulo operation to distribute buttons across workers
        button_indices_to_process = [i for i in buttons_chunk if i < worker_button_count]
        
        if len(button_indices_to_process) < len(buttons_chunk):
            logger.warning(f"Worker {worker_id}: Some buttons ({len(buttons_chunk) - len(button_indices_to_process)}) are out of range. Will process {len(button_indices_to_process)} buttons.")
        
        properties = []
        
        # Process each valid button index
        for button_idx in button_indices_to_process:
            try:
                # Only log every 50 buttons to reduce log spam
                if button_idx % 50 == 0:
                    logger.info(f"Worker {worker_id}: Processing button {button_idx}")
                
                # Verify the index is still valid before trying to access it
                if button_idx >= len(all_buttons):
                    # Refresh the button list if needed
                    all_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.jss56')
                    if button_idx >= len(all_buttons):
                        logger.warning(f"Worker {worker_id}: Button index {button_idx} out of range after refresh. Skipping.")
                        continue
                
                # Get the button at this index
                button = all_buttons[button_idx]
                
                # Scroll to button
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", button)
                time.sleep(0.3)  # Reduced from 0.5
                
                # Get button HTML and extract basic info
                button_html = button.get_attribute('outerHTML')
                prop_info = extract_property_info(button_html)
                
                if not prop_info:
                    logger.warning(f"Worker {worker_id}: Could not extract info from button {button_idx}")
                    continue
                
                logger.info(f"Worker {worker_id}: Found property {prop_info['store_name']} with {prop_info['available_spaces']}")
                
                # Click the button to open modal
                try:
                    # Try JavaScript click
                    driver.execute_script("arguments[0].click();", button)
                    # Short wait for modal to appear
                    time.sleep(1)
                    
                    # Check if modal appeared by looking for any new elements
                    page_html_after_click = driver.page_source
                    spaces = extract_modal_data(page_html_after_click)
                    
                    # Filter spaces by size
                    prop_info['spaces'] = [space for space in spaces if space['sqft'] < MAX_SPACE_SIZE]
                    
                    if prop_info['spaces']:
                        logger.info(f"Worker {worker_id}: Found {len(prop_info['spaces'])} spaces under 1000 sqft")
                        properties.append(prop_info)
                    
                    # Try to close modal
                    try:
                        driver.find_element(By.CSS_SELECTOR, '.MuiSvgIcon-root path[d*="M19"]').click()
                    except:
                        try:
                            driver.find_element(By.CSS_SELECTOR, 'svg.MuiSvgIcon-root').click()
                        except:
                            # Press Escape as a last resort
                            webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                    
                    time.sleep(0.5)  # Wait for modal to close
                    
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error clicking button or processing modal: {str(e)}")
            
            except Exception as e:
                logger.error(f"Worker {worker_id}: Error processing button {button_idx}: {str(e)}")
        
        return properties
    
    except Exception as e:
        logger.error(f"Worker {worker_id}: Error in worker: {str(e)}")
        return []
    
    finally:
        # Close the browser
        if driver:
            driver.quit()


def check_google_data(property_info):
    """Check Google reviews and mobile store presence for a property."""
    try:
        # Keep original address from leasing site
        original_address = property_info['address']
        original_store_id = property_info['store_id']
        
        # Format search query with Walmart prefix for better matching
        property_address = f"Walmart {original_address}"
        
        # Use more specific search to get more accurate results
        # Add store number to improve match accuracy
        search_query = f"Walmart Store #{original_store_id} {original_address}"
        url = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={requests.utils.quote(search_query)}&inputtype=textquery&fields=place_id,formatted_address,geometry&key={GOOGLE_API_KEY}"
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "candidates" in data and data["candidates"]:
            place_info = data["candidates"][0]
            place_id = place_info["place_id"]
            
            # Save the full formatted address from Google but keep original as primary
            if "formatted_address" in place_info:
                formatted_address = place_info["formatted_address"]
                property_info['full_address'] = formatted_address
                property_info['google_address'] = formatted_address  # Save separately for clarity
                logger.info(f"Got Google Maps address: {formatted_address}")
                
                # Check for major address mismatch
                if not address_similarity_check(original_address, formatted_address):
                    logger.warning(f"Possible address mismatch: Leasing: '{original_address}' vs Google: '{formatted_address}'")
                    property_info['address_mismatch_warning'] = True
                
                # Extract city and zip code from the formatted address
                location_details = extract_city_zip_from_address(formatted_address)
                property_info['city'] = location_details['city']
                property_info['zip_code'] = location_details['zip_code']
                logger.info(f"Extracted city: {location_details['city']}, zip: {location_details['zip_code']}")
            else:
                property_info['full_address'] = property_info['address']
                property_info['city'] = "Unknown"
                property_info['zip_code'] = "Unknown"

            # Get the details including review count
            details_url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=user_ratings_total,formatted_address,formatted_phone_number,website&key={GOOGLE_API_KEY}"
            details_response = requests.get(details_url, timeout=10)
            details_response.raise_for_status()
            details = details_response.json()
            
            # Store additional information
            property_info['review_count'] = details.get("result", {}).get("user_ratings_total", 0)
            
            if "formatted_phone_number" in details.get("result", {}):
                property_info['phone_number'] = details["result"]["formatted_phone_number"]
                
            if "website" in details.get("result", {}):
                website = details["result"]["website"]
                property_info['website'] = website
                
                # Extract store ID from website but DON'T replace original store ID
                if "walmart.com/store/" in website:
                    store_url_match = re.search(r'walmart\.com/store/(\d+)', website)
                    if store_url_match:
                        website_store_id = store_url_match.group(1)
                        logger.info(f"Found store ID in website URL: {website_store_id} (leasing ID: {original_store_id})")
                        
                        # Save website store ID separately but keep original as primary
                        property_info['website_store_id'] = website_store_id
                        property_info['leasing_id'] = original_store_id
                        
                        # Only add a flag if they don't match
                        if website_store_id != original_store_id:
                            property_info['id_mismatch'] = True
                
            # Skip further checking if it doesn't meet review threshold
            if property_info['review_count'] < MIN_REVIEWS:
                property_info['meets_criteria'] = False
                property_info['fail_reason'] = f"Only {property_info['review_count']} reviews (minimum {MIN_REVIEWS})"
                return property_info
            
            # If we have location geometry, check for mobile stores
            if "geometry" in place_info:
                location = place_info["geometry"]["location"]
                lat = location["lat"]
                lng = location["lng"]
                
                # Get nearby mobile stores
                mobile_store_status = check_mobile_stores(lat, lng)
                property_info['mobile_store_search_method'] = "Google Places Nearby Search API"
                property_info['mobile_store_search_radius'] = "100 meters"
                property_info['mobile_store_keywords_checked'] = MOBILE_STORE_KEYWORDS
                property_info['has_mobile_store'] = mobile_store_status['has_mobile']
                
                # Store any found matches for reference
                if mobile_store_status['stores']:
                    property_info['mobile_stores_found'] = mobile_store_status['stores']
                
                if property_info['has_mobile_store']:
                    property_info['meets_criteria'] = False
                    property_info['fail_reason'] = "Has a mobile phone store"
                else:
                    property_info['meets_criteria'] = True
                    property_info['fail_reason'] = None
            else:
                # Fallback to old method
                has_mobile = has_mobile_store(property_address)
                property_info['has_mobile_store'] = has_mobile
                
                if has_mobile:
                    property_info['meets_criteria'] = False
                    property_info['fail_reason'] = "Has a mobile phone store"
                else:
                    property_info['meets_criteria'] = True
                    property_info['fail_reason'] = None
        else:
            # Try a more generic search if specific one fails
            fallback_url = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={requests.utils.quote(property_address)}&inputtype=textquery&fields=place_id,formatted_address,geometry&key={GOOGLE_API_KEY}"
            fallback_response = requests.get(fallback_url, timeout=10)
            fallback_response.raise_for_status()
            fallback_data = fallback_response.json()
            
            if "candidates" in fallback_data and fallback_data["candidates"]:
                # Process fallback results similarly
                # ...condensed for brevity, would mirror the above code...
                logger.info(f"Used fallback search for {property_info['store_name']}")
                return check_google_data_from_place_info(property_info, fallback_data["candidates"][0])
            else:
                # Couldn't find the place in Google
                property_info['review_count'] = 0
                property_info['meets_criteria'] = False
                property_info['fail_reason'] = "Location not found in Google Places API"
            
        return property_info
        
    except Exception as e:
        logger.error(f"Error checking Google data for {property_info['store_name']}: {str(e)}")
        property_info['error'] = str(e)
        property_info['meets_criteria'] = False
        property_info['fail_reason'] = f"Error checking Google data: {str(e)}"
        return property_info


def address_similarity_check(address1, address2):
    """Check if two addresses are similar enough to likely be the same location.
    Returns True if addresses appear to be the same location."""
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


def check_google_data_from_place_info(property_info, place_info):
    """Process Google place info for a property - extracted for reuse with fallback search."""
    try:
        # Keep original values
        original_address = property_info['address']
        original_store_id = property_info['store_id']
        
        place_id = place_info["place_id"]
        
        # Save the full formatted address from Google but keep original as primary
        if "formatted_address" in place_info:
            formatted_address = place_info["formatted_address"]
            property_info['full_address'] = formatted_address
            property_info['google_address'] = formatted_address  # Save separately for clarity
            logger.info(f"Got Google Maps address (fallback): {formatted_address}")
            
            # Check for major address mismatch
            if not address_similarity_check(original_address, formatted_address):
                logger.warning(f"Possible address mismatch (fallback): Leasing: '{original_address}' vs Google: '{formatted_address}'")
                property_info['address_mismatch_warning'] = True
            
            # Extract city and zip code from the formatted address
            location_details = extract_city_zip_from_address(formatted_address)
            property_info['city'] = location_details['city']
            property_info['zip_code'] = location_details['zip_code']
        else:
            property_info['full_address'] = property_info['address']
            property_info['city'] = "Unknown"
            property_info['zip_code'] = "Unknown"

        # Get the details including review count
        details_url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=user_ratings_total,formatted_address,formatted_phone_number,website&key={GOOGLE_API_KEY}"
        details_response = requests.get(details_url, timeout=10)
        details_response.raise_for_status()
        details = details_response.json()
        
        # Store additional information
        property_info['review_count'] = details.get("result", {}).get("user_ratings_total", 0)
        
        if "formatted_phone_number" in details.get("result", {}):
            property_info['phone_number'] = details["result"]["formatted_phone_number"]
            
        if "website" in details.get("result", {}):
            website = details["result"]["website"]
            property_info['website'] = website
            
            # Extract store ID from website but DON'T replace original store ID
            if "walmart.com/store/" in website:
                store_url_match = re.search(r'walmart\.com/store/(\d+)', website)
                if store_url_match:
                    website_store_id = store_url_match.group(1)
                    logger.info(f"Found store ID in website URL (fallback): {website_store_id} (leasing ID: {original_store_id})")
                    
                    # Save website store ID separately but keep original as primary
                    property_info['website_store_id'] = website_store_id
                    property_info['leasing_id'] = original_store_id
                    
                    # Only add a flag if they don't match
                    if website_store_id != original_store_id:
                        property_info['id_mismatch'] = True
            
        # Skip further checking if it doesn't meet review threshold
        if property_info['review_count'] < MIN_REVIEWS:
            property_info['meets_criteria'] = False
            property_info['fail_reason'] = f"Only {property_info['review_count']} reviews (minimum {MIN_REVIEWS})"
            return property_info
        
        # If we have location geometry, check for mobile stores
        if "geometry" in place_info:
            location = place_info["geometry"]["location"]
            lat = location["lat"]
            lng = location["lng"]
            
            # Get nearby mobile stores
            mobile_store_status = check_mobile_stores(lat, lng)
            property_info['mobile_store_search_method'] = "Google Places Nearby Search API (Fallback)"
            property_info['mobile_store_search_radius'] = "100 meters"
            property_info['mobile_store_keywords_checked'] = MOBILE_STORE_KEYWORDS
            property_info['has_mobile_store'] = mobile_store_status['has_mobile']
            
            # Store any found matches for reference
            if mobile_store_status['stores']:
                property_info['mobile_stores_found'] = mobile_store_status['stores']
            
            if property_info['has_mobile_store']:
                property_info['meets_criteria'] = False
                property_info['fail_reason'] = "Has a mobile phone store"
            else:
                property_info['meets_criteria'] = True
                property_info['fail_reason'] = None
        else:
            # No location data
            property_info['meets_criteria'] = False
            property_info['fail_reason'] = "Missing location data in Google Place API"
        
        return property_info
    
    except Exception as e:
        logger.error(f"Error in fallback Google data check: {str(e)}")
        property_info['error'] = str(e)
        property_info['meets_criteria'] = False
        property_info['fail_reason'] = f"Error in fallback check: {str(e)}"
        return property_info


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


def get_google_reviews(address):
    """Get the number of Google reviews for a location."""
    try:
        # First find the place
        url = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={requests.utils.quote(address)}&inputtype=textquery&fields=place_id&key={GOOGLE_API_KEY}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if "candidates" in data and data["candidates"]:
            place_id = data["candidates"][0]["place_id"]

            # Then get the details including review count
            details_url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&fields=user_ratings_total&key={GOOGLE_API_KEY}"
            details_response = requests.get(details_url, timeout=10)
            details_response.raise_for_status()
            return details_response.json().get("result", {}).get("user_ratings_total", 0)
        return 0
    except Exception as e:
        logger.error(f"Error getting Google reviews for {address}: {str(e)}")
        return 0


def has_mobile_store(address):
    """Check if there is a mobile phone store in the location."""
    try:
        # Format address
        if "walmart" not in address.lower():
            formatted_address = f"Walmart {address}"
        else:
            formatted_address = address
            
        formatted_address = requests.utils.quote(formatted_address)
        
        # First query: Look for Walmart location
        walmart_url = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={formatted_address}&inputtype=textquery&fields=place_id,geometry&key={GOOGLE_API_KEY}"
        walmart_response = requests.get(walmart_url, timeout=10)
        walmart_response.raise_for_status()
        walmart_data = walmart_response.json()
        
        if "candidates" not in walmart_data or not walmart_data["candidates"]:
            logger.warning(f"Could not find Walmart at address: {address}")
            return True  # Assume there is a store (to be safe)
        
        # Get the Walmart place_id and location
        location = walmart_data["candidates"][0].get("geometry", {}).get("location", {})
        
        if location:
            lat = location.get("lat")
            lng = location.get("lng")
            radius = "100"  # Search within 100 meters of the Walmart
            
            # Check for each special brand name separately
            for brand in ["mobile phone repair", "cell phone repair", "techy", "cellaris", "thefix", "casemate", "ifixandrepair"]:
                keyword = requests.utils.quote(brand)
                nearby_url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lng}&radius={radius}&keyword={keyword}&key={GOOGLE_API_KEY}"
                nearby_response = requests.get(nearby_url, timeout=10)
                nearby_response.raise_for_status()
                nearby_data = nearby_response.json()
                
                if "results" in nearby_data and nearby_data["results"]:
                    for result in nearby_data["results"]:
                        name = result.get("name", "").lower()
                        if any(term.lower() in name for term in MOBILE_STORE_KEYWORDS):
                            return True  # Found a mobile store
        
        return False
        
    except Exception as e:
        logger.error(f"Error checking for mobile stores: {str(e)}")
        return True  # Assume there is a store in case of error (to be safe)


def check_mobile_stores(lat, lng):
    """Check for mobile phone repair stores near a specific location."""
    result = {
        'has_mobile': False,
        'stores': []
    }
    
    try:
        radius = "100"  # Search within 100 meters
        # Use multiple keyword combinations for better coverage
        keyword_sets = [
            "mobile+phone+repair",
            "cell+phone+repair",
            "iphone+repair",
            "cellaris+cpr+thefix+techy"
        ]
        
        found_stores = []
        
        # Try each keyword set
        for keywords in keyword_sets:
            nearby_url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lng}&radius={radius}&keyword={keywords}&key={GOOGLE_API_KEY}"
            nearby_response = requests.get(nearby_url, timeout=10)
            nearby_response.raise_for_status()
            nearby_data = nearby_response.json()
            
            if "results" in nearby_data and nearby_data["results"]:
                for place in nearby_data["results"]:
                    place_name = place.get("name", "").lower()
                    
                    # Check if any keywords match
                    matches = [term for term in MOBILE_STORE_KEYWORDS if term.lower() in place_name]
                    
                    # Extra check for partial matches with special brands
                    special_brands = ["techy", "cellaris", "casemate", "thefix"]
                    for brand in special_brands:
                        if brand in place_name and brand not in [m.lower() for m in matches]:
                            matches.append(brand.capitalize())
                            
                    if matches:
                        # Avoid duplicates
                        if not any(store.get('place_id') == place.get('place_id') for store in found_stores):
                            found_stores.append({
                                'name': place.get("name"),
                                'matched_keywords': matches,
                                'place_id': place.get("place_id")
                            })
        
        # Update result with all found stores
        if found_stores:
            result['has_mobile'] = True
            result['stores'] = found_stores
    
    except Exception as e:
        logger.error(f"Error in check_mobile_stores: {str(e)}")
        result['has_mobile'] = True  # Assume yes in case of error
        result['error'] = str(e)
    
    return result


def send_email(properties):
    """Send email notification about matching properties."""
    if not properties:
        logger.info("No properties to notify about")
        return
    
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER
        msg["Subject"] = f"Walmart Leasing Opportunities - {datetime.now().strftime('%Y-%m-%d')}"
        
        # Create HTML content
        html_content = f"""
        <html>
        <head>
            <style>
                table {{
                    border-collapse: collapse;
                    width: 100%;
                }}
                th, td {{
                    border: 1px solid #dddddd;
                    text-align: left;
                    padding: 8px;
                }}
                tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                th {{
                    background-color: #0071ce;
                    color: white;
                }}
                .check {{
                    color: green;
                    font-weight: bold;
                }}
                .x {{
                    color: red;
                    font-weight: bold;
                }}
                .note {{
                    font-size: 0.8em;
                    color: #666;
                }}
                .warning {{
                    color: #ff6600;
                    font-weight: bold;
                }}
            </style>
        </head>
        <body>
            <h2>Walmart Leasing Opportunities</h2>
            <p>Found {len(properties)} locations matching your criteria:</p>
            <table>
                <tr>
                    <th>Store #</th>
                    <th>Address</th>
                    <th>City</th>
                    <th>ZIP</th>
                    <th>Spaces</th>
                    <th>Reviews</th>
                    <th>Mobile Store</th>
                </tr>
        """
        
        # Add a plain text version as well
        text_content = "Walmart Leasing Opportunities\n\n"
        text_content += f"Found {len(properties)} locations matching your criteria:\n\n"
        
        # Add each property to the email
        for prop in properties:
            # Use the leasing site store ID as the primary ID
            store_id = prop.get("store_id", "Unknown")
            store_num = f"Store #{store_id}"
            
            # Use website ID information if available
            website = prop.get("website", "")
            website_store_id = prop.get("website_store_id", "")
            
            # Show ID warning if there's a mismatch between leasing site and website
            id_note = ""
            if prop.get('id_mismatch', False):
                id_note = f" <span class='warning'>(Website ID: {website_store_id})</span>"
            
            # Original address from leasing site
            leasing_address = prop.get('address', "Unknown")
            
            # Google Maps address if available
            google_address = prop.get('google_address', prop.get('full_address', ""))
            
            # Show address warning if needed
            address_note = ""
            if prop.get('address_mismatch_warning', False):
                address_note = f"<br><span class='note'>Verified address: {google_address}</span>"
            
            # Use city and zip from Google data
            city = prop.get('city', "Unknown")
            zip_code = prop.get('zip_code', "Unknown")
            reviews = prop.get("review_count", "N/A")
            
            # Create space details HTML - now with deduplication
            space_html = "<ul>"
            space_text = ""
            
            for space in prop.get("spaces", []):
                suite = space.get("suite", "TBD")
                sqft = space.get("sqft", "Unknown")
                space_html += f"<li>Suite {suite}: {sqft} sqft</li>"
                space_text += f"- Suite {suite}: {sqft} sqft\n"
            
            space_html += "</ul>"
            
            # All properties in the final list have been confirmed to NOT have mobile stores
            method = prop.get('mobile_store_search_method', 'Google Places API')
            radius = prop.get('mobile_store_search_radius', '100m')
            mobile_store = f"No mobile stores detected within {radius} <span class='check'>✓</span><br><small>Method: {method}</small>"
            
            # Add website link
            website_html = f"<a href='{website}' target='_blank'>Store Website</a>" if website else ""
            
            # Add to HTML content
            html_content += f"""
                <tr>
                    <td>{store_num}{id_note}<br>{website_html}</td>
                    <td>{leasing_address}{address_note}</td>
                    <td>{city}</td>
                    <td>{zip_code}</td>
                    <td>{space_html}</td>
                    <td>{reviews}</td>
                    <td>{mobile_store}</td>
                </tr>
            """
            
            # Add to text content
            text_content += f"• {store_num} at {leasing_address} - {city}, {zip_code} - {reviews} reviews - No mobile store ✓\n"
            text_content += space_text
            text_content += "\n"
        
        # Close the HTML
        html_content += """
            </table>
            <p>This is an automated message from your Walmart Leasing Checker.</p>
            <p><strong>Note:</strong> All listings above have been verified to meet the following criteria:</p>
            <ul>
                <li>Available space under 1000 sqft</li>
                <li>Over 10,000 Google reviews</li>
                <li>No mobile phone repair stores present (checked with Google Places API)</li>
            </ul>
            <p><strong>How Mobile Store Detection Works:</strong> We use the Google Places API to find the exact 
            geographic coordinates of each Walmart location, then search for nearby 
            businesses within 100 meters that match mobile phone repair keywords including Talknfix, 
            Ifixandrepair, Cellaris, Thefix, Casemate, Techy and other repair brands. This ensures we only recommend 
            locations without competing mobile repair shops.</p>
        </body>
        </html>
        """
        
        # Attach both text and HTML parts
        msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))
        
        # Send the email
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
            logger.info(f"Email sent successfully to {EMAIL_RECEIVER}")
            
    except Exception as e:
        logger.error(f"Error sending email: {str(e)}")


def load_previous_results():
    """Load previously identified matching properties from file."""
    file_path = os.path.join(OUTPUT_DIR, "matching_properties.json")
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"Loaded {len(data)} properties from previous results")
                return data
        except Exception as e:
            logger.error(f"Failed to load previous matching properties: {str(e)}")
    return []

def is_duplicate_property(new_prop, existing_props):
    """Check if a property is a duplicate of one already in our results."""
    for prop in existing_props:
        # Compare store ID and address for uniqueness
        store_match = new_prop.get('store_id') == prop.get('store_id')
        website_match = (
            new_prop.get('website_store_id') == prop.get('website_store_id') 
            and new_prop.get('website_store_id') is not None
        )
        
        # If either store ID or website store ID match, consider it the same store
        if store_match or website_match:
            return True
    return False

def save_results_with_versioning(properties):
    """Save results with versioning to avoid overwriting previous data."""
    # Create base filename
    base_filename = "matching_properties"
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # First, save current version with timestamp
    versioned_filename = f"{base_filename}_{current_time}.json"
    versioned_path = os.path.join(OUTPUT_DIR, versioned_filename)
    
    # Always save the current results with timestamp
    with open(versioned_path, "w", encoding="utf-8") as f:
        json.dump(properties, f, indent=2)
    logger.info(f"Saved current results to {versioned_filename}")
    
    # Now update the main file (merged with previous if applicable)
    main_path = os.path.join(OUTPUT_DIR, f"{base_filename}.json")
    
    # Load previous results
    previous_results = load_previous_results()
    
    # If we have previous results, merge with current results
    if previous_results:
        # Add new properties that aren't duplicates
        merged_properties = previous_results.copy()
        new_count = 0
        
        for prop in properties:
            if not is_duplicate_property(prop, previous_results):
                merged_properties.append(prop)
                new_count += 1
        
        # Save merged results
        with open(main_path, "w", encoding="utf-8") as f:
            json.dump(merged_properties, f, indent=2)
        
        logger.info(f"Updated matching_properties.json with {new_count} new properties (total: {len(merged_properties)})")
        return merged_properties
    else:
        # If no previous results, just save current results as the main file
        with open(main_path, "w", encoding="utf-8") as f:
            json.dump(properties, f, indent=2)
        logger.info(f"Created new matching_properties.json with {len(properties)} properties")
        return properties

def scrape_walmart_leasing_parallel():
    """
    Scrape Walmart leasing properties using parallel processing.
    """
    logger.info("Starting parallel Walmart leasing scraper...")
    start_time = time.time()
    
    # Step 1: Get property button count first with a single browser
    driver = setup_selenium_driver(headless=True)
    if not driver:
        logger.error("Failed to create browser instance for initial scan")
        return []
    
    try:
        # Load the page
        driver.get(WALMART_LEASING_URL)
        logger.info("Loaded Walmart leasing page for initial scan")
        
        # Wait for page to load
        try:
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'button[class*="jss"]')))
        except TimeoutException:
            logger.warning("Timeout waiting for page to load during initial scan")
        
        # Extra wait for JavaScript
        time.sleep(5)
        
        # Get button count
        buttons = driver.find_elements(By.CSS_SELECTOR, 'button.jss56')
        button_count = len(buttons)
        logger.info(f"Found {button_count} property buttons")
        
        if button_count == 0:
            logger.error("No property buttons found. Exiting.")
            driver.quit()
            return []
            
    except Exception as e:
        logger.error(f"Error during initial scan: {str(e)}")
        if driver:
            driver.quit()
        return []
    
    driver.quit()
    
    # Step 2: Divide buttons into chunks for parallel processing
    button_indices = list(range(button_count))
    
    # Process only the first 300 properties for initial run to save time (optional)
    if "--quick" in sys.argv:
        logger.info("Quick mode: Processing only first 300 properties")
        button_indices = button_indices[:300]
    
    # Distribute button indices evenly across workers
    chunk_size = max(1, len(button_indices) // WEB_WORKERS)
    button_chunks = []
    
    # Create proper chunks based on number of workers (no overlaps)
    for i in range(WEB_WORKERS):
        start_idx = i * chunk_size
        # Last worker gets any remaining buttons
        end_idx = min((i + 1) * chunk_size, len(button_indices))
        button_chunks.append(button_indices[start_idx:end_idx])
    
    logger.info(f"Divided {button_count} buttons into {len(button_chunks)} chunks for {WEB_WORKERS} workers")
    
    # Step 3: Process each chunk in parallel
    all_properties = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=WEB_WORKERS) as executor:
        # Submit all tasks
        future_to_chunk = {
            executor.submit(process_property_chunk, chunk, worker_id): worker_id 
            for worker_id, chunk in enumerate(button_chunks)
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_chunk):
            worker_id = future_to_chunk[future]
            try:
                properties = future.result()
                logger.info(f"Worker {worker_id} completed, found {len(properties)} properties with small spaces")
                all_properties.extend(properties)
            except Exception as e:
                logger.error(f"Worker {worker_id} generated an exception: {str(e)}")
    
    # Step 4: Filter properties with spaces under 1000 sqft (safety check)
    # Note: process_property_chunk already filters for spaces < 1000 sqft, this is a second verification
    small_space_properties = []
    for prop in all_properties:
        small_spaces = [s for s in prop.get('spaces', []) if s.get('sqft', 9999) < MAX_SPACE_SIZE]
        if small_spaces:
            prop['spaces'] = small_spaces
            small_space_properties.append(prop)
    
    logger.info(f"Found {len(small_space_properties)} properties with spaces under {MAX_SPACE_SIZE} sqft")
    
    # Save small space properties
    with open(os.path.join(OUTPUT_DIR, "small_space_properties.json"), "w", encoding="utf-8") as f:
        json.dump(small_space_properties, f, indent=2)
    
    # No small space properties found
    if not small_space_properties:
        logger.info("No properties with spaces under 1000 sqft found")
        return []
    
    # Step 5: Check Google reviews and mobile stores in parallel
    logger.info(f"Checking Google data for {len(small_space_properties)} properties in parallel (only for properties with spaces < 1000 sqft)")
    with concurrent.futures.ThreadPoolExecutor(max_workers=API_WORKERS) as executor:
        # Process Google data in parallel
        checked_properties = list(executor.map(check_google_data, small_space_properties))
    
    # Filter out None values in case any property checks failed completely
    checked_properties = [prop for prop in checked_properties if prop is not None]
    
    # Step 6: Filter for final matching properties - now with None check
    matching_properties = []
    for prop in checked_properties:
        # Use get() with default to prevent AttributeError
        if prop is not None and prop.get('meets_criteria', False):
            matching_properties.append(prop)
    
    # Save final results with versioning
    matching_properties = save_results_with_versioning(matching_properties)
    
    logger.info(f"Found {len(matching_properties)} properties matching ALL criteria")
    logger.info(f"Total execution time: {time.time() - start_time:.2f} seconds")
    
    return matching_properties


def load_modal_reference():
    """Load modal reference data for extraction patterns."""
    modal_file = "c:\\Users\\usuario\\Desktop\\scraping\\modal_data.html"
    if os.path.exists(modal_file):
        try:
            with open(modal_file, 'r', encoding='utf-8') as f:
                modal_html = f.read()
            modal_soup = BeautifulSoup(modal_html, 'html.parser')
            return modal_soup
        except Exception as e:
            logger.error(f"Error loading modal reference: {str(e)}")
    return None


def job():
    """Main job function to run the scraper and send notifications."""
    logger.info(f"Running job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if test mode
    test_mode = "--test" in sys.argv
    
    if test_mode:
        logger.info("Running in TEST MODE")
        # Use sample data for testing
        test_properties = [
            {
                "store_name": "Store #1234",
                "store_number": "1234",
                "address": "123 Test St, Anytown, TX 12345",
                "review_count": 15000,
                "spaces": [
                    {"suite": "100", "sqft": 800, "text": "Suite 100 | 800 sqft"},
                    {"suite": "101", "sqft": 600, "text": "Suite 101 | 600 sqft"}
                ],
                "meets_criteria": True
            },
            {
                "store_name": "Store #5678",
                "store_number": "5678",
                "address": "456 Sample Ave, Testville, CA 67890",
                "review_count": 12000,
                "spaces": [
                    {"suite": "200", "sqft": 950, "text": "Suite 200 | 950 sqft"}
                ],
                "meets_criteria": True
            }
        ]
        send_email(test_properties)
    else:
        # Run the actual parallel scraper
        matching_properties = scrape_walmart_leasing_parallel()
        
        # Send email if new matches found
        if matching_properties:
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
        print("Usage: python walmart_leasing_checker_parallel.py [options]")
        print("Options:")
        print("  --test       Run in test mode with sample data")
        print("  --schedule   Run once and then schedule daily execution")
        print("  --quick      Process only the first 300 properties (faster)")
        print("  --parallel N Use N parallel browser workers (default: 6)")
        return
    
    # Check if parallel workers specified
    if "--parallel" in sys.argv:
        try:
            idx = sys.argv.index("--parallel")
            if idx + 1 < len(sys.argv) and sys.argv[idx + 1].isdigit():
                global WEB_WORKERS
                WEB_WORKERS = int(sys.argv[idx + 1])
                logger.info(f"Using {WEB_WORKERS} parallel browser workers")
        except:
            pass
    
    # Run the job immediately
    logger.info("Starting Walmart Leasing Space Checker (Parallel Version)")
    job()
    
    # Schedule to run daily at 8:00 AM
    if "--schedule" in sys.argv:
        schedule.every().day.at("08:00").do(job)
        logger.info("Scheduled to run daily at 8:00 AM")
        
        # Keep the script running to execute scheduled jobs
        while True:
            schedule.run_pending()
            time.sleep(60)


if __name__ == "__main__":
    main()
