"""
Location checker for Walmart properties
Uses direct Google Maps searches with Playwright
"""

import re
import time
import logging
import urllib.parse
import concurrent.futures
import random
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from config import MOBILE_STORE_KEYWORDS, MIN_REVIEWS, GOOGLE_MAPS_URL, SEARCH_RADIUS_METERS, API_WORKERS
from playwright_utils import setup_playwright_browser, close_browser, wait_for_element

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
    """
    Enhanced check if two addresses are similar enough to likely be the same location.
    Now better detects stores inside Walmart with the same address.
    """
    # First convert both to lowercase and clean whitespace
    addr1 = address1.lower().strip()
    addr2 = address2.lower().strip()
    
    if addr1 == addr2:
        return True
    
    # Look for indicators that the store is inside Walmart
    inside_indicators = ["inside walmart", "inside the walmart", "#the fix", "# the fix", 
                          "in walmart", "walmart supercenter", "in-store"]
    for indicator in inside_indicators:
        if indicator in addr1 or indicator in addr2:
            # If one address has an inside indicator, extract core components from both
            # Extract street numbers, street names, city, state, zip
            numbers1 = re.findall(r'\b(\d+)\b', addr1)
            numbers2 = re.findall(r'\b(\d+)\b', addr2)
            
            # Extract street name patterns
            streets1 = re.findall(r'([a-z]+\s+(?:street|st|avenue|ave|road|rd|drive|dr|blvd|boulevard))', addr1)
            streets2 = re.findall(r'([a-z]+\s+(?:street|st|avenue|ave|road|rd|drive|dr|blvd|boulevard))', addr2)
            
            # Extract zip codes
            zip1 = re.findall(r'\b(\d{5})\b', addr1)
            zip2 = re.findall(r'\b(\d{5})\b', addr2)
            
            # Extract city, state patterns
            city_state1 = re.findall(r'([a-z\s]+),\s+([a-z]{2})', addr1)
            city_state2 = re.findall(r'([a-z\s]+),\s+([a-z]{2})', addr2)
            
            # Check for matching components - if street number and ZIP match, it's likely the same location
            if numbers1 and numbers2 and any(n in numbers2 for n in numbers1):
                if (zip1 and zip2 and zip1[0] == zip2[0]) or (streets1 and streets2 and any(s in addr2 for s in streets1)):
                    logger.info(f"Address similarity detected - likely inside Walmart - '{addr1}' vs '{addr2}'")
                    return True
    
    # Original checks
    numbers1 = re.findall(r'\b(\d+)\b', addr1)
    numbers2 = re.findall(r'\b(\d+)\b', addr2)
    
    streets1 = re.findall(r'([a-z]+\s+(?:street|st|avenue|ave|road|rd|drive|dr|blvd|boulevard))', addr1)
    streets2 = re.findall(r'([a-z]+\s+(?:street|st|avenue|ave|road|rd|drive|dr|blvd|boulevard))', addr2)
    
    # Extract city names
    city1 = re.search(r'([a-z\s]+),\s+[a-z]{2}', addr1)
    city2 = re.search(r'([a-z\s]+),\s+[a-z]{2}', addr2)
    
    # Extract zip codes
    zip1 = re.search(r'\b(\d{5})\b', addr1)
    zip2 = re.search(r'\b(\d{5})\b', addr2)
    
    # If both have numbers but they don't match at all, likely different places
    if numbers1 and numbers2 and not any(n in numbers2 for n in numbers1):
        return False
        
    # If both have street names but none match, likely different places
    if streets1 and streets2 and not any(s in addr2 for s in streets1):
        return False
    
    # If numbers match AND (zip codes match OR city names match), high probability of same location
    if numbers1 and numbers2 and any(n in numbers2 for n in numbers1):
        if (zip1 and zip2 and zip1.group(1) == zip2.group(1)) or (city1 and city2 and city1.group(1).strip() == city2.group(1).strip()):
            logger.info(f"High confidence address match: '{addr1}' vs '{addr2}'")
            return True
    
    # If we get here, there's enough similarity or ambiguity to pass
    return True

#* ================================================
#* ========= EXTRACT REVIEW COUNT FROM PAGE ======
#* ================================================

def extract_review_count_from_page(page, store_panel_selector=None):
    """Extract review count from Google Maps page using multiple methods."""
    review_count = 0
    
    try:
        # If no store panel selector provided, use default
        if not store_panel_selector:
            store_panel_selector = 'div[role="main"], div.section-hero-header, .xtuJJ'
        
        # Method 1: Look specifically for spans with aria-label containing "reseñas" or "reviews"
        aria_elements = page.query_selector_all('span[aria-label*="reseñas"], span[aria-label*="reviews"], span[aria-label*="reseña"]')
        
        for elem in aria_elements:
            aria_text = elem.get_attribute('aria-label') or elem.inner_text()
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
            review_spans = page.query_selector_all('.F7nice span span span, .fontBodyMedium span span span')
            for span in review_spans:
                span_text = span.inner_text().strip()
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
            f7nice_elements = page.query_selector_all('.F7nice')
            for elem in f7nice_elements:
                # Find spans in this element
                spans = elem.query_selector_all('span')
                
                for span in spans:
                    # Skip rating spans (these will have aria-hidden="true")
                    if span.get_attribute('aria-hidden') == 'true':
                        continue
                    
                    # Look for review count spans
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
                    span_text = span.inner_text().strip()
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
    Check Google Maps for review counts and nearby mobile stores using Playwright.
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
    
    browser_info = None
    # Add retries for Google Maps access
    max_retries = 3
    for attempt in range(max_retries):
        # Create a completely independent browser instance for this worker
        browser_info = setup_playwright_browser(
            headless=True, 
            worker_id=worker_id
        )
        
        if not browser_info:
            property_info['meets_criteria'] = False
            property_info['fail_reason'] = "Failed to create browser instance"
            return property_info
        
        page = browser_info["page"]
        
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
                page.goto(f"{GOOGLE_MAPS_URL}{encoded_query}", wait_until="domcontentloaded")
            except Exception as e:
                logger.warning(f"Error when accessing Google Maps: {str(e)}")
                close_browser(browser_info)
                browser_info = None
                continue
            
            # Wait for results to load
            try:
                page.wait_for_selector('div[role="feed"], div.section-result-content, div[role="main"], .F7nice, .DkEaL', 
                                     timeout=20000)
            except PlaywrightTimeoutError:
                logger.warning(f"Timeout waiting for Google Maps results for {search_query}")
            
            # Additional wait to make sure page elements are fully loaded
            time.sleep(5)
            
            try:
                # Try to get the store panel
                store_panel_selector = 'div[role="main"], div.section-hero-header, .xtuJJ'
                store_panel = page.query_selector(store_panel_selector)
                
                if not store_panel:
                    raise Exception("Store panel not found")
                
                # Extract the address
                try:
                    formatted_address_elem = page.query_selector(
                        'button[data-item-id="address"], span.section-info-text'
                    )
                    formatted_address = formatted_address_elem.inner_text().strip() if formatted_address_elem else ""
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
                except Exception:
                    property_info['full_address'] = property_info['address']
                    property_info['city'] = "Unknown"
                    property_info['zip_code'] = "Unknown"
                
                # Extract review count using the specialized function
                review_count = extract_review_count_from_page(page)
                property_info['review_count'] = review_count
                
                logger.info(f"Found {review_count} reviews for {store_number}")
                
                # Extract phone number
                try:
                    phone_elem = page.query_selector(
                        'button[data-item-id="phone:tel"], span.phone-number'
                    )
                    if phone_elem:
                        property_info['phone_number'] = phone_elem.inner_text().strip()
                except Exception:
                    pass
                
                # Extract website if available
                try:
                    website_elem = page.query_selector(
                        'a[data-item-id="authority"], button[data-item-id*="website"], a[href*="walmart.com"]'
                    )
                    if website_elem:
                        website = website_elem.get_attribute('href') or website_elem.inner_text()
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
                except Exception:
                    pass
                
                # Skip further checking if it doesn't meet review threshold
                if property_info.get('review_count', 0) < MIN_REVIEWS:
                    property_info['meets_criteria'] = False
                    property_info['fail_reason'] = f"Only {property_info.get('review_count', 0)} reviews (minimum {MIN_REVIEWS})"
                    close_browser(browser_info)
                    return property_info
                
                # Now check for nearby mobile stores
                mobile_store_result = check_nearby_mobile_stores(browser_info, property_info)
                
                if mobile_store_result['has_mobile']:
                    property_info['meets_criteria'] = False
                    property_info['fail_reason'] = "Has a mobile phone store"
                else:
                    property_info['meets_criteria'] = True
                    property_info['fail_reason'] = None
                    
            except Exception as e:
                logger.warning(f"Could not find store panel for {search_query}: {str(e)}")
                property_info['meets_criteria'] = False
                property_info['fail_reason'] = "Could not find store in Google Maps"
                
        except Exception as e:
            logger.error(f"Error checking Google data for {property_info['store_name']}: {str(e)}")
            if attempt < max_retries - 1:
                # Try again with next retry
                logger.info(f"Will retry Google Maps search for {property_info['store_name']}")
                close_browser(browser_info)
                browser_info = None
                continue
            else:
                property_info['error'] = str(e)
                property_info['meets_criteria'] = False
                property_info['fail_reason'] = f"Error checking Google data: {str(e)}"
        
        finally:
            # Clean up
            if browser_info:
                close_browser(browser_info)
                browser_info = None
                
        # If we got here without triggering a continue, break out of retry loop
        break
            
    return property_info

#* ================================================
#* ========= CHECK NEARBY MOBILE STORES ===========
#* ================================================

def check_nearby_mobile_stores(browser_info, property_info):
    """
    Check for nearby mobile phone repair stores by directly searching on Google Maps.
    Enhanced to better detect stores at the same location as Walmart.
    """
    result = {
        'has_mobile': False,
        'stores': []
    }
    
    found_stores = []  # Initialize found_stores at the top level
    browser = browser_info["browser"]
    context = browser_info["context"]
    page = browser_info["page"]
    walmart_url = page.url
    walmart_address = property_info.get('full_address') or property_info.get('address')
    
    try:
        # Get current Walmart location URL
        logger.info(f"Checking for mobile stores near Walmart at {walmart_address}")
        
        # Function to restore browser session if it becomes invalid
        def safe_search_execution(search_url, description):
            """Execute a search with session recovery if needed."""
            nonlocal page, context, browser
            
            for retry in range(3):
                try:
                    # Add error handling to the initial navigation
                    try:
                        page.goto(search_url, wait_until="domcontentloaded")
                    except Exception as e:
                        logger.warning(f"Error navigating to URL during {description}, retry {retry+1}: {str(e)}")
                        if retry < 2:
                            # Create new page on connection errors
                            try:
                                page.close()
                            except:
                                pass
                                
                            # Sleep before recreating to let resources free up
                            time.sleep(5)
                            
                            # Create a new page
                            page = context.new_page()
                            page.set_default_timeout(30000)
                            continue
                        else:
                            return None
                            
                    # Give more time for results to load
                    time.sleep(5)
                    
                    # Check if page is still valid
                    try:
                        # Simple check - get title will throw exception if session is invalid
                        if not page.url.startswith('http'):
                            raise Exception("Invalid page state")
                            
                        page.title()
                    except Exception as e:
                        if retry < 2:
                            logger.warning(f"Page became invalid during {description}, recreating... Error: {str(e)}")
                            try:
                                # Create a new page
                                page.close()
                                page = context.new_page()
                                page.set_default_timeout(30000)
                                
                                # Try again with the new page
                                page.goto(search_url, wait_until="domcontentloaded")
                                time.sleep(5)
                            except Exception as e2:
                                logger.error(f"Failed to recreate page: {str(e2)}")
                                return None
                        else:
                            logger.error(f"Page still invalid after {retry+1} retries, aborting {description}")
                            return None
                            
                    # Look for results with multiple selector attempts
                    for selector in [
                        'div[role="article"], div.section-result, .Nv2PK',
                        '.Nv2PK',
                        'div.section-result',
                        '.fontHeadlineSmall'
                    ]:
                        try:
                            result_elements = page.query_selector_all(selector)
                            if result_elements and len(result_elements) > 0:
                                logger.info(f"Found {len(result_elements)} results for {description}")
                                return result_elements
                        except:
                            continue
                    
                    # If we get here but found no elements, return empty list instead of None
                    logger.info(f"Found 0 results for {description}")
                    return []
                    
                except Exception as e:
                    if "Failed to establish a new connection" in str(e) or "WebDriver exception" in str(e):
                        # Connection related errors deserve a fresh page
                        if retry < 2:
                            logger.warning(f"Connection error during {description}, retrying with new page: {str(e)}")
                            # Try to recover
                            try:
                                page.close()
                            except:
                                pass
                                
                            # Add a significant delay to let resources free up
                            time.sleep(10)
                            
                            # Create new page
                            page = context.new_page()
                            page.set_default_timeout(30000)
                        else:
                            logger.warning(f"Error with {description} after final retry: {str(e)}")
                            return []
                    elif retry < 2:  # For non-connection errors, retry with same page
                        logger.warning(f"Error during {description}, retrying: {str(e)}")
                        time.sleep(5)
                    else:
                        logger.warning(f"Error with {description} after all retries: {str(e)}")
                        return []
            
            return []  # Return empty list if we exhaust all retries
        
        # First approach: Use the built-in "nearby" search in Google Maps
        nearby_results_found = False
        try:
            # Look for the "Nearby" button - updated selectors based on the HTML examples
            nearby_button = None
            selectors = [
                'button[aria-label="Nearby"]', 
                'button[aria-label="Cercano"]',
                'button[data-value="Nearby"]',
                'button[data-value="Cercano"]',
                'button.g88MCb[jsaction*="pane.wfvdle35"]',
                'button[jsaction*="nearbysearch"]',
                'button.g88MCb',
                'button[jsaction*="pane.action.nearby"]',
                'button.gm2-icon-button[jsaction*="pane.nearbysearch"]',
                'button[data-item-id="nearby"]'
            ]
            
            for selector in selectors:
                elements = page.query_selector_all(selector)
                if elements:
                    # Look for elements containing "Nearby" or "Cercano" text
                    for element in elements:
                        element_text = element.inner_text().lower() or ''
                        if 'cerca' in element_text or 'near' in element_text:
                            nearby_button = element
                            break
                    
                    # If nothing specific was found, just use the first element
                    if not nearby_button and elements:
                        element_text = (elements[0].inner_text() or 
                                       elements[0].get_attribute('aria-label') or 
                                       elements[0].get_attribute('data-value') or '')
                        logger.info(f"Found potential nearby button: {element_text} using selector: {selector}")
                        nearby_button = elements[0]
                        break
            
            # If we couldn't find with CSS, try looking for text content
            if not nearby_button:
                # Try to find by visible text
                nearby_texts = ["Nearby", "Cercano", "Near", "Cerca"]
                for text in nearby_texts:
                    elements = page.query_selector_all(f"//button[contains(., '{text}')]")
                    if elements:
                        logger.info(f"Found nearby button via text: {text}")
                        nearby_button = elements[0]
                        break
            
            if nearby_button:
                logger.info(f"Found 'Nearby' button - using direct nearby search")
                
                # Try JavaScript click first (most reliable)
                try:
                    nearby_button.click()
                    logger.info("Clicked nearby button")
                except Exception:
                    # Try evaluating JavaScript as fallback
                    try:
                        page.evaluate("arguments[0].click()", nearby_button)
                        logger.info("Clicked nearby button with JavaScript")
                    except:
                        logger.warning("Failed to click nearby button")
                
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
                        search_box = page.wait_for_selector(selector, timeout=5000)
                        if search_box:
                            logger.info(f"Found search box using selector: {selector}")
                            break
                    except:
                        continue
                
                if search_box:
                    # Clear any existing text and enter our search
                    search_box.fill("")
                    search_box.type("mobile phone repair")
                    time.sleep(1)  # Brief pause
                    search_box.press("Enter")
                    logger.info("Entered 'mobile phone repair' in nearby search")
                    
                    # Wait for search results
                    time.sleep(3)
                    
                    # Take a screenshot for debugging
                    try:
                        screenshot_path = f"nearby_search_{property_info['store_id']}.png"
                        page.screenshot(path=screenshot_path)
                        logger.info(f"Saved nearby search screenshot to {screenshot_path}")
                    except:
                        pass
                    
                    # Look for results
                    result_elements = page.query_selector_all(
                        'div[role="article"], div.section-result, .Nv2PK, div[role="feed"] > div'
                    )
                    
                    nearby_results_found = len(result_elements) > 0
                    logger.info(f"Found {len(result_elements)} results from nearby search")
                    
                    # Process these results to find mobile stores
                    found_stores = process_result_elements(page, result_elements, [], walmart_address)
                else:
                    logger.info("Could not find search box after clicking nearby button")
                    nearby_results_found = False
            else:
                logger.info("Nearby button not found, will use manual searches")
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
                
                # Use safe search execution
                result_elements = safe_search_execution(search_url, f"search '{term}'")
                
                if result_elements:
                    # Process results using the helper function
                    more_stores = process_result_elements(page, result_elements, [], walmart_address)
                    
                    # Add to overall found stores, avoiding duplicates
                    for store in more_stores:
                        if not any(s.get('name') == store.get('name') for s in found_stores):
                            found_stores.append(store)
                
                # Wait a bit between searches to avoid rate limits
                time.sleep(1)
        
        # Combined approach for all targeted and direct searches
        all_search_queries = []
        
        # Add targeted brand searches
        targeted_brands = [
            "The Fix Walmart",
            "iFixAndRepair Walmart",
            "Cellaris Walmart",
            "Talk N Fix Walmart",
            "Techy Walmart"
        ]
        
        # Add general repair searches
        general_searches = [
            "phone repair Walmart",
            "mobile repair Walmart",
            "cell phone repair Walmart"
        ]
        
        # Add direct in-store searches
        inside_searches = [
            "The Fix inside Walmart",
            "iFixAndRepair",
            "Tech repair inside Walmart",
            "mobile repair inside Walmart",
            "phone repair inside Walmart", 
            "cell phone repair at Walmart"
        ]
        
        # Prepare all search queries with address
        for term in targeted_brands + general_searches + inside_searches:
            all_search_queries.append(f"{term} {walmart_address}")
        
        # Execute all searches with improved resilience
        for search_query in all_search_queries:
            encoded_query = urllib.parse.quote(search_query)
            search_url = f"{GOOGLE_MAPS_URL}{encoded_query}"
            
            logger.info(f"Executing search: {search_query}")
            
            # Use safe search execution
            result_elements = safe_search_execution(search_url, f"search '{search_query}'")
            
            if result_elements:
                # Process results
                more_stores = process_result_elements(page, result_elements, [], walmart_address)
                
                # Add to overall found stores, avoiding duplicates
                for store in more_stores:
                    if not any(s.get('name') == store.get('name') for s in found_stores):
                        found_stores.append(store)
        
        # The final check - look for any stores with our keywords at exactly the same address
        # Extract street number and street name from Walmart address
        walmart_addr_parts = {}
        if walmart_address:
            # Get street number
            street_num_match = re.search(r'\b(\d+)\b', walmart_address.lower())
            if street_num_match:
                walmart_addr_parts['street_num'] = street_num_match.group(1)
                
            # Get zip code
            zip_match = re.search(r'\b(\d{5})\b', walmart_address)
            if zip_match:
                walmart_addr_parts['zip'] = zip_match.group(1)
        
        # If we have a street number and ZIP, do a direct search with it
        if 'street_num' in walmart_addr_parts and 'zip' in walmart_addr_parts:
            exact_address_search = f"{walmart_addr_parts['street_num']} phone repair {walmart_addr_parts['zip']}"
            
            encoded_query = urllib.parse.quote(exact_address_search)
            search_url = f"{GOOGLE_MAPS_URL}{encoded_query}"
            
            logger.info(f"Final exact address search: {exact_address_search}")
            
            # Use safe search execution
            result_elements = safe_search_execution(search_url, "exact address search")
            
            if result_elements:
                more_stores = process_result_elements(page, result_elements, [], walmart_address)
                
                for store in more_stores:
                    if not any(s.get('name') == store.get('name') for s in found_stores):
                        found_stores.append(store)
        
        # Update result with all found stores
        if found_stores:
            result['has_mobile'] = True
            result['stores'] = found_stores
            logger.info(f"Found {len(found_stores)} mobile stores nearby or at the same address: {[s['name'] for s in found_stores]}")
        else:
            logger.info("No mobile stores found in the vicinity or at the same address")
            
        # Add info about the search method to the property info
        property_info['mobile_store_search_method'] = "Google Maps Web Search with same-address detection"
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
    
    # Try to return to the original URL
    try:
        page.goto(walmart_url, wait_until="domcontentloaded")
    except Exception:
        pass
    
    return result

#* ================================================
#* ========= PROCESS SEARCH RESULTS ===============
#* ================================================

def process_result_elements(page, result_elements, found_stores=None, walmart_address=None):
    """
    Helper function to process search result elements and extract store information.
    Now also checks for stores within Walmart address.
    """
    if found_stores is None:
        found_stores = []
    
    # Clean walmart address for better comparisons
    clean_walmart_address = walmart_address.lower() if walmart_address else ""
    
    # Extract Walmart's address components for comparison
    walmart_street_num = None
    walmart_street_name = None
    walmart_city = None
    walmart_state = None
    walmart_zip = None
    
    if walmart_address:
        # Extract components using regex
        street_num_match = re.search(r'\b(\d+)\b', clean_walmart_address)
        walmart_street_num = street_num_match.group(1) if street_num_match else None
        
        # More extraction patterns similar to your existing code
        # ... (similar to existing code)
            
    for idx, elem in enumerate(result_elements[:12]):  # Limit to first 12 for performance
        try:
            # Try multiple selectors for store name
            name_selectors = ['h1', 'h2', 'h3', '.fontHeadlineSmall', '[role="heading"]', 'span.section-result-title']
            store_name = None
            
            for selector in name_selectors:
                name_elem = elem.query_selector(selector)
                if name_elem:
                    store_name = name_elem.inner_text().strip()
                    break
                    
            if not store_name:
                continue
            
            # Check for distance info with multiple selectors
            distance_text = "Unknown"
            distance_selectors = [
                'span[aria-label*="miles"]', 
                'span[aria-label*="mi"]',
                'span.fontBodyMedium > span:nth-child(2)',
                '.UY7F9'
            ]
            
            for selector in distance_selectors:
                distance_elem = elem.query_selector(selector)
                if distance_elem:
                    distance_text = distance_elem.inner_text().strip()
                    break
            
            # Extract store address using improved validation
            store_address = None
            address_selectors = [
                '.fontBodySmall[jsan*="address"]', 
                '.fontBodyMedium > div[jsan*="address"]',
                'div[class*="address"]',
                'div[jscontroller*="address"]',
                'div.W4Efsd > div.fontBodyMedium:nth-child(1)',
                'div[aria-label*="address"]'
            ]
            
            # Collect address candidates
            address_candidates = []
            for selector in address_selectors:
                address_elems = elem.query_selector_all(selector)
                for addr_elem in address_elems:
                    text = addr_elem.inner_text().strip()
                    if text and len(text) > 8:
                        address_candidates.append(text)
            
            # Select best address candidate
            for candidate in address_candidates:
                # Skip if it looks like a review
                if candidate.startswith('"') or candidate.endswith('"') or candidate.count('.') > 3:
                    continue
                    
                # Skip if it's too long (likely a description)
                if len(candidate) > 200:
                    continue
                    
                # Detect if it looks like an address
                address_indicators = [
                    re.search(r'\d+\s+[A-Za-z]', candidate),  # Street number pattern
                    re.search(r'(?:Ave|St|Rd|Dr|Blvd|Lane|Calle|Avenida|Carretera),?', candidate, re.IGNORECASE),  # Street suffix
                    re.search(r'\b[A-Z]{2}\s+\d{5}\b', candidate),  # US ZIP code pattern
                    re.search(r'\b\d{5}\b', candidate)  # Generic ZIP pattern
                ]
                
                if any(address_indicators):
                    store_address = candidate
                    break
            
            # If no good candidate found, use shortest one
            if not store_address and address_candidates:
                store_address = min(address_candidates, key=len)
            
            # Log the store address if found
            if store_address:
                logger.info(f"Store address for '{store_name}': {store_address}")
            
            # Check for Walmart-owned services
            is_walmart_owned_service = False
            excluded_walmart_services = [
                "walmart tech", "walmart electronics", "walmart cell phone services", 
                "walmart wireless", "walmart service desk", "walmart photo center",
                "walmart supercenter", "walmart electronics department"
            ]
            
            if store_name and "walmart" in store_name.lower():
                if any(service in store_name.lower() for service in excluded_walmart_services):
                    if not any(repair_term in store_name.lower() for repair_term in ["repair", "fix", "iclinic"]):
                        is_walmart_owned_service = True
                        logger.info(f"Detected Walmart-owned service: '{store_name}' - Not a competing mobile store")
            
            # Skip Walmart's own services
            if is_walmart_owned_service:
                continue
            
            # Check for indicators of being inside Walmart
            store_name_lower = store_name.lower() if store_name else ""
            store_addr_lower = store_address.lower() if store_address else ""
            
            # Inside Walmart indicators
            inside_walmart_indicators = [
                "inside walmart", "in walmart", "walmart #", "walmart store", 
                "walmart supercenter", "inside the walmart", "inside the store",
                "#the fix", "# the fix", "in-store", "inside", "at walmart",
                "walmart supercenter", "walmart center", "walmart location"
            ]
            
            # Only consider "inside Walmart" with valid address
            is_inside_walmart = False
            has_valid_addresses = store_address and walmart_address and len(store_address) > 10
            
            if has_valid_addresses:
                # Check for explicit indicators
                for indicator in inside_walmart_indicators:
                    if indicator in store_name_lower or indicator in store_addr_lower:
                        if not (store_address.startswith('"') and store_address.endswith('"')):
                            is_inside_walmart = True
                            logger.info(f"Store '{store_name}' appears to be inside Walmart (indicator: '{indicator}')")
                            break
                
                # Check address components for exact matches
                if not is_inside_walmart:
                    # Implementation similar to your existing code
                    # ... (extracting store components and comparing them)
                    pass
            
            # Extract distance value
            distance_meters = None
            distance_val = None
            distance_unit = None
            distance_match = re.search(r'([\d.]+)\s*(mi|km|m|ft)', distance_text)
            if distance_match:
                # Implementation similar to your existing code
                # ... (distance conversion)
                pass
            
            # Keyword matching logic
            store_name_lower = store_name.lower()
            matches = [term for term in MOBILE_STORE_KEYWORDS if term.lower() in store_name_lower]
            
            # Keyword counting
            manual_keywords = ["cell", "phone", "repair", "mobile", "fix", "wireless", "device"]
            word_count = sum(1 for word in manual_keywords if word.lower() in store_name_lower)
            
            # Detection flags
            is_within_distance = distance_meters <= SEARCH_RADIUS_METERS * 1.5 if distance_meters is not None else False
            is_keyword_match = len(matches) > 0
            is_multi_keyword_match = (word_count >= 2 and any(word in store_name_lower for word in ["repair", "fix"]))
            
            # Final matching logic
            if ((is_within_distance or is_inside_walmart) and (is_keyword_match or is_multi_keyword_match)):
                # Implementation similar to your existing code
                # ... (building store entry and adding to found_stores)
                pass
            
            # Check for same address matches
            elif (store_address and walmart_address and len(store_address) > 10):
                # Implementation similar to your existing code
                # ... (address-based matching logic)
                pass
            
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
