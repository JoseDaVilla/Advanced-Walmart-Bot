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
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

from config import MOBILE_STORE_KEYWORDS, MIN_REVIEWS, GOOGLE_MAPS_URL, SEARCH_RADIUS_METERS, API_WORKERS
from selenium_utils import setup_selenium_driver

logger = logging.getLogger(__name__)

#* ================================================
#* ========= EXTRACT CITY AND ZIP FROM ADDRESS ====
#* ================================================

def extract_city_zip_from_address(address):
    """Extract city and zip code from a formatted address string with better handling of different formats."""
    try:
        # Handle empty or None addresses
        if not address or address.strip() in ["Unknown", ""]:
            return {'city': "Unknown", 'zip_code': "Unknown"}
        
        # Remove any non-printable characters like \ue0c8 (location pin icon)
        address = ''.join(c for c in address if c.isprintable()).strip()
        
        # First try to extract US format ZIP code (5 digits, sometimes with 4 digit extension)
        zip_match = re.search(r'(\d{5}(?:-\d{4})?)', address)
        zip_code = zip_match.group(1) if zip_match else "Unknown"
        
        # Try to extract Puerto Rico specific ZIP codes (common in the results)
        if "Puerto Rico" in address and zip_code == "Unknown":
            zip_match = re.search(r'(\d{5})', address)
            zip_code = zip_match.group(1) if zip_match else "Unknown"
        
        # Try more patterns for city extraction
        city = "Unknown"
        
        # Pattern 1: Look for "City, STATE ZIP" format
        city_match = re.search(r'([A-Za-z\s\.]+),\s+[A-Z]{2}\s+\d{5}', address)
        if city_match:
            city = city_match.group(1).strip()
        
        # Pattern 2: Look for "City, STATE" format
        if city == "Unknown":
            city_match = re.search(r'([A-Za-z\s\.]+),\s+[A-Z]{2}', address)
            if city_match:
                city = city_match.group(1).strip()
        
        # Pattern 3: Look for ", City, " format (common in international addresses)
        if city == "Unknown":
            city_match = re.search(r',\s*([A-Za-z\s\.]+),', address)
            if city_match:
                city = city_match.group(1).strip()
        
        # Pattern 4: Look for Puerto Rico specific format "City, Puerto Rico"
        if city == "Unknown" and "Puerto Rico" in address:
            city_match = re.search(r'([A-Za-z\s\.]+),\s+(?:\d{5},\s+)?Puerto Rico', address)
            if city_match:
                city = city_match.group(1).strip()
        
        return {'city': city, 'zip_code': zip_code}
    except Exception as e:
        logger.error(f"Error extracting city/zip: {str(e)}")
        return {'city': "Unknown", 'zip_code': "Unknown"}

#* ================================================
#* ========= CHECK ADDRESS SIMILARITY =============
#* ================================================

def address_similarity_check(address1, address2):
    """Check if two addresses are similar enough to likely be the same location."""
    # Convert both to lowercase for comparison
    addr1 = address1.lower()
    addr2 = address2.lower()
    
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

#* ================================================
#* ========= EXTRACT REVIEW COUNT FROM PAGE ======
#* ================================================

def extract_review_count_from_page(driver, store_panel=None):
    """Extract review count from Google Maps page using multiple methods."""
    review_count = 0
    
    try:
        # If no store panel provided, try to find it
        if not store_panel:
            store_panel = driver.find_element(By.CSS_SELECTOR, 'div[role="main"], div.section-hero-header, .xtuJJ')
        
        # Method 1: Look specifically for spans with aria-label containing "reseñas" or "reviews"
        aria_elements = driver.find_elements(By.CSS_SELECTOR, 'span[aria-label*="reseñas"], span[aria-label*="reviews"], span[aria-label*="reseña"]')
        
        for elem in aria_elements:
            aria_text = elem.get_attribute('aria-label') or elem.text
            logger.info(f"Found review element with aria-label: {aria_text}")
            
            # Extract numeric value from aria-label text
            # Handle both formats: "11.958 reseñas" (Spanish) or "11,958 reviews" (English)
            review_match = re.search(r'([\d.,]+)\s*(?:reseñas|reviews|reseña)', aria_text, re.IGNORECASE)
            if review_match:
                # Clean up number - replace both commas and periods with empty strings
                # then convert to int (handles both European and US number formats)
                review_str = review_match.group(1)
                # For European format (11.958), keep only the last period/comma as decimal point
                if '.' in review_str and ',' not in review_str:
                    parts = review_str.split('.')
                    if len(parts[-1]) == 3:  # If last part has 3 digits, it's likely a thousand separator
                        review_str = ''.join(parts)  # Remove all periods
                elif ',' in review_str and '.' not in review_str:
                    parts = review_str.split(',')
                    if len(parts[-1]) == 3:  # If last part has 3 digits, it's likely a thousand separator
                        review_str = ''.join(parts)  # Remove all commas
                else:
                    # Just remove all separators
                    review_str = review_str.replace(',', '').replace('.', '')
                
                try:
                    count = int(review_str)
                    if count > review_count:
                        review_count = count
                        logger.info(f"Found review count via aria-label: {review_count}")
                except ValueError:
                    pass
        
        # Method 2: Look for spans with parentheses containing numbers
        if review_count == 0:
            review_spans = driver.find_elements(By.CSS_SELECTOR, '.F7nice span span span, .fontBodyMedium span span span')
            for span in review_spans:
                span_text = span.text.strip()
                # Look for text like "(11.958)" - common format in Google Maps
                paren_match = re.match(r'^\(([0-9.,]+)\)$', span_text)
                if paren_match:
                    review_str = paren_match.group(1).replace('.', '').replace(',', '')
                    try:
                        count = int(review_str)
                        if count > review_count:
                            review_count = count
                            logger.info(f"Found review count from parentheses: {review_count}")
                    except ValueError:
                        pass
                        
        # Method 3: Check for aria-labels more generally by looking for spans in the F7nice div
        if review_count == 0:
            f7nice = driver.find_elements(By.CSS_SELECTOR, '.F7nice')
            for elem in f7nice:
                # Try to find the rating number (ignore it)
                rating_span = elem.find_elements(By.CSS_SELECTOR, 'span[aria-hidden="true"]')
                # Find spans after the rating that might have the review count
                all_spans = elem.find_elements(By.CSS_SELECTOR, 'span')
                
                for span in all_spans:
                    # Skip rating spans (these will have aria-hidden="true")
                    if span.get_attribute('aria-hidden') == 'true':
                        continue
                    
                    # Look for review count spans (these typically have aria-label with "reseñas" or "reviews")
                    aria_label = span.get_attribute('aria-label')
                    if aria_label and ('reseñas' in aria_label.lower() or 'reviews' in aria_label.lower()):
                        # Extract the number from the aria-label
                        num_match = re.search(r'([\d.,]+)', aria_label)
                        if num_match:
                            review_str = num_match.group(1).replace('.', '').replace(',', '')
                            try:
                                count = int(review_str)
                                if count > review_count:
                                    review_count = count
                                    logger.info(f"Found review count from F7nice: {review_count}")
                            except ValueError:
                                pass
                    
                    # Also check text content for parenthesized numbers
                    span_text = span.text.strip()
                    if span_text and span_text.startswith('(') and span_text.endswith(')'):
                        num_text = span_text[1:-1]  # Remove parentheses
                        try:
                            count = int(num_text.replace('.', '').replace(',', ''))
                            if count > review_count:
                                review_count = count
                                logger.info(f"Found review count from span text: {review_count}")
                        except ValueError:
                            pass
        
    except Exception as e:
        logger.error(f"Error extracting review count: {str(e)}")
    
    return review_count

#* ================================================
#* ========= CHECK GOOGLE REVIEWS AND STORES ======
#* ================================================

def check_google_reviews_and_stores(property_info, worker_id=0):
    """
    Check Google Maps for review counts and nearby mobile stores.
    Each call has a worker_id to ensure truly independent operation.
    """
    # Use the worker_id to ensure this instance is completely independent
    # Keep original address and store ID
    original_address = property_info['address']
    store_id = property_info['store_id']
    store_number = f"Store #{store_id}"
    
    # Format search query with Walmart prefix exactly as recommended
    search_query = f"Walmart {store_number} {original_address}"
    logger.info(f"Worker {worker_id}: Searching for: {search_query}")
    
    # Add retries for Google Maps access
    max_retries = 3
    for attempt in range(max_retries):
        # Create a completely independent browser instance for this worker
        unique_port = 9500 + worker_id + (attempt * 100)  # Ensure unique ports even across retries
        driver = setup_selenium_driver(headless=True, worker_id=worker_id, debugging_port=unique_port)
        
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
            
            # Access Google Maps with the formatted query
            try:
                driver.set_page_load_timeout(45)
                driver.get(f"{GOOGLE_MAPS_URL}{encoded_query}")
            except WebDriverException as e:
                logger.warning(f"WebDriver error when accessing Google Maps: {str(e)}")
                driver.quit()
                continue
            
            # Wait for results to load
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="feed"], div.section-result-content, div[role="main"], .F7nice, .DkEaL'))
                )
            except TimeoutException:
                logger.warning(f"Timeout waiting for Google Maps results for {search_query}")
            
            # Additional wait to make sure page elements are fully loaded
            time.sleep(5)
            
            try:
                # Try to get the store panel
                store_panel = driver.find_element(By.CSS_SELECTOR, 'div[role="main"], div.section-hero-header, .xtuJJ')
                
                # Extract the address
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
                
                # Extract review count using the specialized function
                review_count = extract_review_count_from_page(driver, store_panel)
                property_info['review_count'] = review_count
                
                logger.info(f"Found {review_count} reviews for {store_number}")
                
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
                                property_info['leasing_id'] = store_id
                                
                                # Flag if IDs don't match
                                if website_store_id != store_id:
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

#* ================================================
#* ========= CHECK NEARBY MOBILE STORES ===========
#* ================================================

def check_nearby_mobile_stores(driver, property_info):
    """
    Check for nearby mobile phone repair stores by directly searching on Google Maps.
    Returns a dictionary with results.
    """
    result = {
        'has_mobile': False,
        'stores': []
    }
    
    found_stores = []  # Initialize found_stores at the top level
    
    try:
        # Get current Walmart location URL
        walmart_url = driver.current_url
        logger.info(f"Checking for mobile stores near Walmart at {property_info['address']}")
        
        # First approach: Use the built-in "nearby" search in Google Maps
        nearby_results_found = False
        try:
            # Look for the "Nearby" button - updated selectors based on the HTML examples
            nearby_button = None
            try:
                # Try various selectors for the nearby button in different languages and UI versions
                selectors = [
                    'button[aria-label="Nearby"]', 
                    'button[aria-label="Cercano"]',
                    'button[data-value="Nearby"]',
                    'button[data-value="Cercano"]',
                    'button.g88MCb[jsaction*="pane.wfvdle35"]',  # From your HTML example
                    'button[jsaction*="nearbysearch"]',
                    'button.g88MCb',  # Simplified selector matching the nearby HTML example
                    'button[jsaction*="pane.action.nearby"]',
                    'button.gm2-icon-button[jsaction*="pane.nearbysearch"]',
                    'button[data-item-id="nearby"]'
                ]
                
                for selector in selectors:
                    try:
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements:
                            # Look for elements containing "Nearby" or "Cercano" text
                            for element in elements:
                                element_text = element.text.lower() or ''
                                if 'cerca' in element_text or 'near' in element_text:
                                    nearby_button = element
                                    break
                            
                            # If nothing specific was found, just use the first element
                            if not nearby_button and elements:
                                element_text = elements[0].text or elements[0].get_attribute('aria-label') or elements[0].get_attribute('data-value') or ''
                                logger.info(f"Found potential nearby button: {element_text} using selector: {selector}")
                                nearby_button = elements[0]
                                break
                    except Exception:
                        continue
                
                # If we couldn't find with CSS, try looking for text content
                if not nearby_button:
                    # Try to find by visible text - looking for buttons with "Nearby" or "Cercano" text
                    nearby_texts = ["Nearby", "Cercano", "Near", "Cerca"]
                    for text in nearby_texts:
                        try:
                            xpath_expr = f"//button[contains(., '{text}')]"
                            elements = driver.find_elements(By.XPATH, xpath_expr)
                            if elements:
                                logger.info(f"Found nearby button via text: {text}")
                                nearby_button = elements[0]
                                break
                        except Exception:
                            pass
                
                if nearby_button:
                    logger.info(f"Found 'Nearby' button - using direct nearby search")
                    
                    # Try JavaScript click first (most reliable)
                    try:
                        driver.execute_script("arguments[0].click();", nearby_button)
                        logger.info("Clicked nearby button with JavaScript")
                    except:
                        try:
                            # Try action chain click next
                            from selenium.webdriver.common.action_chains import ActionChains
                            actions = ActionChains(driver)
                            actions.move_to_element(nearby_button).click().perform()
                            logger.info("Clicked nearby button with ActionChains")
                        except:
                            # Direct click as last resort
                            nearby_button.click()
                            logger.info("Clicked nearby button with direct click")
                    
                    # Wait for the search box to appear
                    time.sleep(2)  # Give UI time to update
                    
                    # Try different selectors for the search input
                    search_box = None
                    search_input_selectors = [
                        'input#searchboxinput',
                        'input[name="q"]',
                        'input.searchboxinput',
                        'input[role="combobox"]',
                        'input[jsaction*="omnibox"]'
                    ]
                    
                    for selector in search_input_selectors:
                        try:
                            search_elements = WebDriverWait(driver, 5).until(
                                EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                            )
                            if search_elements:
                                search_box = search_elements[0]
                                logger.info(f"Found search box using selector: {selector}")
                                break
                        except:
                            continue
                    
                    if search_box:
                        # Clear any existing text and enter our search
                        search_box.clear()
                        search_box.send_keys("mobile phone repair")
                        time.sleep(1)  # Brief pause
                        search_box.send_keys(Keys.ENTER)
                        logger.info("Entered 'mobile phone repair' in nearby search")
                        
                        # Wait for search results
                        time.sleep(3)
                        
                        # Take a screenshot for debugging
                        try:
                            screenshot_path = f"nearby_search_{property_info['store_id']}.png"
                            driver.save_screenshot(screenshot_path)
                            logger.info(f"Saved nearby search screenshot to {screenshot_path}")
                        except:
                            pass
                        
                        # Look for results
                        result_elements = driver.find_elements(
                            By.CSS_SELECTOR, 
                            'div[role="article"], div.section-result, .Nv2PK, div[role="feed"] > div'
                        )
                        
                        nearby_results_found = len(result_elements) > 0
                        logger.info(f"Found {len(result_elements)} results from nearby search")
                        
                        # Process these results to find mobile stores
                        found_stores = process_result_elements(driver, result_elements, [])
                    else:
                        logger.info("Could not find search box after clicking nearby button")
                        nearby_results_found = False
                else:
                    logger.info("Nearby button not found, will use manual searches")
                    nearby_results_found = False
            except Exception as e:
                logger.info(f"Could not use nearby button: {str(e)}")
                nearby_results_found = False
        except Exception as e:
            logger.warning(f"Error trying to use direct nearby search: {str(e)}")
            nearby_results_found = False
        
        # Second approach: Manual searches with keywords (fallback)
        if not nearby_results_found or not found_stores:
            # Use just a few specific search terms to avoid throttling
            search_terms = [
                "cell phone repair near",
                "mobile repair store near"
            ]
            
            for term in search_terms:
                # Construct search query
                search_query = f"{term} {property_info['address']}"
                encoded_query = urllib.parse.quote(search_query)
                search_url = f"{GOOGLE_MAPS_URL}{encoded_query}"
                
                logger.info(f"Searching for: {search_query}")
                
                try:
                    driver.get(search_url)
                    time.sleep(3)  # Wait for results to load
                    
                    # Wait for search results to appear
                    result_selectors = [
                        'div[role="feed"]', 
                        'div[role="main"]', 
                        'div.section-result-content',
                        '.Nv2PK'  # Newer Google Maps result container class
                    ]
                    
                    for selector in result_selectors:
                        try:
                            WebDriverWait(driver, 8).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                            )
                            break
                        except TimeoutException:
                            continue
                    
                    # Look for newer Google Maps result layout
                    result_elements = driver.find_elements(
                        By.CSS_SELECTOR, 
                        'div[role="article"], div.section-result, .Nv2PK'
                    )
                    
                    logger.info(f"Found {len(result_elements)} results for '{term}'")
                    
                    # Process results using the helper function
                    more_stores = process_result_elements(driver, result_elements, [])
                    
                    # Add to overall found stores, avoiding duplicates
                    for store in more_stores:
                        if not any(s.get('name') == store.get('name') for s in found_stores):
                            found_stores.append(store)
                    
                except Exception as e:
                    logger.warning(f"Error searching for term '{term}': {str(e)}")
                
                # Wait a bit between searches to avoid rate limits
                time.sleep(1)
        
        # Return to the original URL so we're ready for the next search
        try:
            driver.get(walmart_url)
            time.sleep(1)
        except:
            pass
        
        # Update result with all found stores
        if found_stores:
            result['has_mobile'] = True
            result['stores'] = found_stores
            logger.info(f"Found {len(found_stores)} mobile stores nearby: {[s['name'] for s in found_stores]}")
        else:
            logger.info("No mobile stores found in the vicinity")
            
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

#* ================================================
#* ========= PROCESS SEARCH RESULTS ===============
#* ================================================

def process_result_elements(driver, result_elements, found_stores=None):
    """Helper function to process search result elements and extract store information."""
    if found_stores is None:
        found_stores = []
        
    for elem in result_elements[:12]:  # Limit to first 12 for performance
        try:
            # Try multiple selectors for store name
            name_selectors = ['h1', 'h2', 'h3', '.fontHeadlineSmall', '[role="heading"]', 'span.section-result-title']
            store_name = None
            
            for selector in name_selectors:
                try:
                    name_elem = elem.find_element(By.CSS_SELECTOR, selector)
                    if name_elem:
                        store_name = name_elem.text.strip()
                        break
                except:
                    continue
                    
            if not store_name:
                continue
            
            # Check for distance info with multiple selectors
            distance_text = "Unknown"
            distance_selectors = [
                'span[aria-label*="miles"]', 
                'span[aria-label*="mi"]',
                'span.fontBodyMedium > span:nth-child(2)',  # Newer format
                '.UY7F9',  # Distance class in newer Google Maps
                'span:contains("mi")'  # jQuery-like, adapting for Selenium
            ]
            
            for selector in distance_selectors:
                try:
                    distance_elems = elem.find_elements(By.CSS_SELECTOR, selector)
                    if distance_elems:
                        distance_text = distance_elems[0].text.strip()
                        break
                except:
                    continue
            
            # Extract distance value more flexibly
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
                
                # Debugging output to show what's being found
                logger.info(f"Found store: {store_name}, Distance: {distance_val} {distance_unit} ({distance_meters} meters)")
                
                # Check if within our target radius
                if distance_meters <= SEARCH_RADIUS_METERS * 1.5:  # Allow slightly wider radius to be safe
                    # Check if matches any of our keywords - add "The Fix" to the manual check
                    store_name_lower = store_name.lower()
                    matches = [term for term in MOBILE_STORE_KEYWORDS 
                             if term.lower() in store_name_lower]
                    
                    # Special manual keyword check - Note 'the fix' is included in MOBILE_STORE_KEYWORDS now
                    manual_keywords = ["cell", "phone", "repair", "mobile", "fix", "wireless", "the fix"]
                    word_count = sum(1 for word in manual_keywords if word.lower() in store_name_lower)
                    
                    # Match if matches keywords or if has multiple repair-related keywords
                    if matches or (word_count >= 2 and any(word in store_name_lower for word in ["repair", "fix"])):
                        matched_terms = matches if matches else ["keyword combination match"]
                        store_entry = {
                            'name': store_name,
                            'matched_keywords': matched_terms,
                            'distance': f"{distance_val} {distance_unit}",
                            'distance_meters': distance_meters  # Include actual meters for reference
                        }
                        
                        # Avoid duplicates
                        if not any(s.get('name') == store_name for s in found_stores):
                            found_stores.append(store_entry)
                            logger.info(f"MATCH: {store_name} - {distance_val} {distance_unit} - located within {distance_meters} meters (threshold: {SEARCH_RADIUS_METERS * 1.5}m)")
            
        except Exception as e:
            logger.debug(f"Error processing a result: {str(e)}")
    
    return found_stores

#* ================================================
#* ========= RUN PARALLEL LOCATION CHECKS =========
#* ================================================

def check_locations_in_parallel(small_space_properties):
    """Check Google Maps data for properties in parallel with true independence."""
    logger.info(f"Checking Google Maps data for {len(small_space_properties)} properties in parallel")
    
    # Determine effective number of workers based on workload size
    effective_workers = min(API_WORKERS, max(1, len(small_space_properties) // 10 + 1))
    if effective_workers < API_WORKERS:
        logger.info(f"Reducing number of workers to {effective_workers} due to workload size")
    
    checked_properties = []
    match_count = 0
    
    # Use smaller batch size for better distribution
    batch_size = 10  # Smaller batch size for more even distribution
    total_batches = (len(small_space_properties) + batch_size - 1) // batch_size
    
    # Create a function that each worker will run
    def process_property_independently(property_info, worker_id):
        try:
            logger.info(f"Worker {worker_id}: Processing {property_info.get('store_number', 'Unknown')}")
            result = check_google_reviews_and_stores(property_info, worker_id)
            return result
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {str(e)}")
            property_info['meets_criteria'] = False
            property_info['fail_reason'] = f"Processing error: {str(e)}"
            return property_info
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as executor:
        # Create a more efficient distribution - one property at a time to each worker
        futures = []
        for idx, prop in enumerate(small_space_properties):
            worker_id = idx % effective_workers  # Distribute properties evenly across workers
            futures.append(executor.submit(process_property_independently, prop, worker_id))
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    checked_properties.append(result)
                    status = "MATCH" if result.get('meets_criteria') else "NO MATCH"
                    if result.get('meets_criteria'):
                        match_count += 1
                        logger.info(f"Property {result.get('store_number', 'Unknown')}: {status} - FOUND MATCH! ({match_count} matches so far)")
                    else:
                        reason = result.get('fail_reason', 'Unknown')
                        logger.info(f"Property {result.get('store_number', 'Unknown')}: {status} - Reason: {reason}")
            except Exception as e:
                logger.error(f"Error processing property result: {str(e)}")
    
    return checked_properties
