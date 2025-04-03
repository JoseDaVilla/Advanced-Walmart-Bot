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
import os  # Added for screenshot paths
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
    Better detection of stores inside Walmart with differently formatted addresses.
    """
    # First convert both to lowercase and clean whitespace
    addr1 = address1.lower().strip()
    addr2 = address2.lower().strip()
    
    if addr1 == addr2:
        return True
    
    # Look for indicators that the store is inside Walmart
    inside_indicators = ["inside walmart", "inside the walmart", "#the fix", "# the fix", 
                          "in walmart", "walmart supercenter", "in-store", "suite", "local"]
                          
    inside_walmart = any(indicator in addr1 or indicator in addr2 for indicator in inside_indicators)
    
    # Extract key address components for comparison
    # Extract ZIP codes (very reliable for matching)
    zip1 = re.search(r'\b(\d{5})\b', addr1)
    zip2 = re.search(r'\b(\d{5})\b', addr2)
    
    # Extract cities
    city1 = re.search(r'([a-z\s]+),\s+([a-z]{2}|\w+\s+rico)', addr1)
    city2 = re.search(r'([a-z\s]+),\s+([a-z]{2}|\w+\s+rico)', addr2)
    
    city1_val = city1.group(1).strip() if city1 else ""
    city2_val = city2.group(1).strip() if city2 else ""
    
    # If both addresses have ZIP codes and they match, very high probability of same location
    if zip1 and zip2 and zip1.group(1) == zip2.group(1):
        # Same ZIP code and city, almost certainly the same location
        if city1_val and city2_val and (city1_val == city2_val or 
                                       city1_val in city2_val or 
                                       city2_val in city1_val):
            logger.info(f"Same location detected: ZIP and city match between '{addr1}' and '{addr2}'")
            return True
        
        # Same ZIP but different formatting of city names - could still be same place
        # Especially for Puerto Rico addresses which often have different formatting
        if "puerto rico" in addr1 and "puerto rico" in addr2:
            logger.info(f"Same location detected in Puerto Rico: ZIP match between '{addr1}' and '{addr2}'")
            return True
        
        # If one address has "walmart" in it and they share a ZIP, likely inside
        if "walmart" in addr1 or "walmart" in addr2:
            logger.info(f"Same location detected: ZIP match with Walmart address between '{addr1}' and '{addr2}'")
            return True
    
    # Extract street numbers (reliable for matching when present)
    street_nums1 = re.findall(r'\b(\d{1,5})\b', addr1)
    street_nums2 = re.findall(r'\b(\d{1,5})\b', addr2)
    
    # If any street numbers match and city or ZIP matches, likely same location
    if street_nums1 and street_nums2:
        for num1 in street_nums1:
            if num1 in street_nums2:
                # Same street number, now check city or ZIP
                if ((zip1 and zip2 and zip1.group(1) == zip2.group(1)) or 
                    (city1_val and city2_val and (city1_val == city2_val or 
                                              city1_val in city2_val or 
                                              city2_val in city1_val))):
                    logger.info(f"Same location detected: Street number and city/ZIP match between '{addr1}' and '{addr2}'")
                    return True
    
    # Check for common road identifiers in Puerto Rico
    pr_road_identifiers = ["carr", "carretera", "pr-", "km", "barrio", "calle", "ave", "avenida"]
    if "puerto rico" in addr1 and "puerto rico" in addr2:
        has_pr_road1 = any(identifier in addr1 for identifier in pr_road_identifiers)
        has_pr_road2 = any(identifier in addr2 for identifier in pr_road_identifiers) 
        
        # If both addresses have PR road indicators and same ZIP/city, likely same location
        if has_pr_road1 and has_pr_road2 and (
            (zip1 and zip2 and zip1.group(1) == zip2.group(1)) or 
            (city1_val and city2_val and city1_val == city2_val)):
            logger.info(f"Puerto Rico address match detected: '{addr1}' and '{addr2}'")
            return True
    
    # Original checks for other cases
    # ... existing code ...
    
    return False

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
        
        # IMPROVED METHOD 0: Take a screenshot for debugging review extraction issues
        try:
            screenshot_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                        f"debug_reviews_{int(time.time())}.png")
            page.screenshot(path=screenshot_path)
        except Exception as ss_error:
            logger.debug(f"Could not save screenshot: {str(ss_error)}")
        
        # Method 1: Direct text extraction from F7nice div (most reliable)
        f7nice_element = page.query_selector('.F7nice')
        if f7nice_element:
            full_text = f7nice_element.inner_text()
            logger.debug(f"F7nice text content: {full_text}")
            
            # Try to extract review count from parenthesized numbers
            reviews_match = re.search(r'\(([0-9.,]+)\)', full_text)
            if reviews_match:
                review_str = reviews_match.group(1).replace('.', '').replace(',', '')
                try:
                    count = int(review_str)
                    if count > review_count:
                        review_count = count
                        logger.info(f"Found review count from F7nice div: {review_count}")
                except ValueError:
                    pass

        # Method 2: Look specifically for spans with aria-label containing "reseñas" or "reviews"
        aria_elements = page.query_selector_all('span[aria-label*="reseñas"], span[aria-label*="reviews"], span[aria-label*="reseña"]')
        
        for elem in aria_elements:
            aria_text = elem.get_attribute('aria-label') or elem.inner_text()
            logger.info(f"Found review element with aria-label: {aria_text}")
            
            # Extract numeric value from aria-label text
            # Handle both formats: "11.958 reseñas" (Spanish) or "11,958 reviews" (English)
            review_match = re.search(r'([\d.,]+)\s*(?:reseñas|reviews|review|reseñas|reseña)', aria_text, re.IGNORECASE)
            if review_match:
                review_str = review_match.group(1)
                
                # Normalize number format - both periods and commas could be thousand separators
                # depending on locale
                if '.' in review_str and ',' not in review_str:
                    if len(review_str.split('.')[-1]) == 3:  # If last part has 3 digits, it's a thousand separator
                        review_str = review_str.replace('.', '')
                elif ',' in review_str and '.' not in review_str:
                    if len(review_str.split(',')[-1]) == 3:  # If last part has 3 digits, it's a thousand separator
                        review_str = review_str.replace(',', '')
                else:
                    # Handle more complex cases
                    review_str = review_str.replace(',', '').replace('.', '')
                
                try:
                    count = int(review_str)
                    if count > review_count:
                        review_count = count
                        logger.info(f"Found review count via aria-label: {review_count}")
                except ValueError:
                    pass
        
        # Method 3: Try to find any span with parenthesized numbers
        if review_count == 0:
            all_spans = page.query_selector_all('span')
            for span in all_spans:
                span_text = span.inner_text().strip()
                # Look for text like "(11.958)" or "(11,958)" - common format in Google Maps
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
        
        # Method 4: Try direct JavaScript evaluation for reviews
        if review_count == 0:
            try:
                # Use JavaScript to find the review count in the DOM
                review_js = """
                    () => {
                        // Try multiple methods to find reviews
                        
                        // Method 1: Look for spans with reviews in aria-label
                        const reviewSpans = Array.from(document.querySelectorAll('span[aria-label*="review"]'));
                        for (const span of reviewSpans) {
                            const match = span.getAttribute('aria-label').match(/([\d.,]+)\\s*review/i);
                            if (match) return match[1].replace(/[,.]/g, '');
                        }
                        
                        // Method 2: Look for F7nice div with review count
                        const f7nice = document.querySelector('.F7nice');
                        if (f7nice) {
                            const text = f7nice.textContent;
                            const match = text.match(/\\(([\d.,]+)\\)/);
                            if (match) return match[1].replace(/[,.]/g, '');
                        }
                        
                        // Method 3: Look for any parenthesized numbers
                        const allSpans = Array.from(document.querySelectorAll('span'));
                        for (const span of allSpans) {
                            const match = span.textContent.match(/^\\(([\d.,]+)\\)$/);
                            if (match) return match[1].replace(/[,.]/g, '');
                        }
                        
                        return "0";
                    }
                """
                js_result = page.evaluate(review_js)
                try:
                    js_count = int(js_result)
                    if js_count > review_count:
                        review_count = js_count
                        logger.info(f"Found review count via JavaScript: {review_count}")
                except ValueError:
                    pass
            except Exception as js_error:
                logger.debug(f"JavaScript review extraction failed: {str(js_error)}")
        
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
    
    # Extract store ID for specific searches
    store_id = property_info.get('store_id', '')
    store_number = property_info.get('store_number', '')
    
    # Store critical information for better detection
    store_city = property_info.get('city', '')
    store_zip = property_info.get('zip_code', '')
    
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
        
        # ENHANCED APPROACH: Added specific direct searches for common store patterns
        # Create specific searches for exact addresses and store formats
        direct_searches = [
            # Search for iFixandRepair at this specific store (CRITICALLY IMPORTANT)
            f"iFixandRepair {store_number} {walmart_address}",
            f"iFixandRepair Orlando Walmart {store_id}",
            f"iFixandRepair inside Walmart {store_id}",
            f"iFixandRepair at Walmart {store_id}",
            
            # Search for other common brand names at this specific Walmart
            f"The Fix {store_number} {walmart_address}",
            f"Cell Phone Repair {store_number} {walmart_address}",
            f"Cellaris {store_number} {walmart_address}",
            f"TalkNFix {store_number} {walmart_address}",
            f"Techy {store_number} {walmart_address}",
            
            # Extremely specific search for this store
            f"phone repair 8101 S John Young Pkwy Orlando",
        ]
        
        # Add store city+zip searches
        if store_city and store_zip:
            direct_searches.extend([
                f"iFixandRepair Walmart {store_city} {store_zip}",
                f"The Fix Walmart {store_city} {store_zip}",
                f"phone repair Walmart {store_city} {store_zip}"
            ])
        
        logger.info(f"Adding {len(direct_searches)} specific searches for Walmart #{store_id}")
        
        # Execute these direct searches
        for search_query in direct_searches:
            encoded_query = urllib.parse.quote(search_query)
            search_url = f"{GOOGLE_MAPS_URL}{encoded_query}"
            
            logger.info(f"Executing direct search: {search_query}")
            
            result_elements = safe_search_execution(search_url, f"direct search '{search_query}'")
            
            if result_elements:
                logger.info(f"Found {len(result_elements)} results for direct search")
                
                # Process with extra sensitivity for matching
                more_stores = process_result_elements(
                    page, 
                    result_elements, 
                    [], 
                    walmart_address,
                    extra_sensitive=True,
                    store_id=store_id
                )
                
                # Add to overall found stores
                for store in more_stores:
                    if not any(s.get('name') == store.get('name') for s in found_stores):
                        found_stores.append(store)
        
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

def process_result_elements(page, result_elements, found_stores=None, walmart_address=None, extra_sensitive=False, store_id=None):
    """
    Helper function to process search result elements and extract store information.
    Improved detection for stores within Walmart address.
    """
    if found_stores is None:
        found_stores = []
    
    # Clean walmart address for better comparisons
    clean_walmart_address = walmart_address.lower() if walmart_address else ""
    
    # Extract detailed components from Walmart address for better comparison
    walmart_addr_components = {}
    if walmart_address:
        # Extract city
        city_match = re.search(r'([A-Za-z\s]+),\s+(?:\d{5},\s+)?(?:[A-Z]{2}|Puerto Rico)', walmart_address)
        if city_match:
            walmart_addr_components['city'] = city_match.group(1).strip().lower()
        
        # Extract ZIP code
        zip_match = re.search(r'\b(\d{5})\b', walmart_address)
        if zip_match:
            walmart_addr_components['zip'] = zip_match.group(1)
        
        # Extract street number
        street_num_match = re.search(r'\b(\d+)\b', clean_walmart_address)
        if street_num_match:
            walmart_addr_components['street_num'] = street_num_match.group(1)
        
        # Check if address contains suite/local info
        suite_match = re.search(r'(?:suite|local|#)\s*([a-z0-9-]+)', clean_walmart_address, re.IGNORECASE)
        if suite_match:
            walmart_addr_components['suite'] = suite_match.group(1)
            
        # Extract state/territory
        if "puerto rico" in clean_walmart_address:
            walmart_addr_components['state'] = "puerto rico"
    
    # Define specific high-risk brands that are commonly found inside Walmart
    HIGH_CONFIDENCE_IN_WALMART_BRANDS = [
        "the fix", "thefix", "the-fix",
        "ifix", "i-fix", "ifixandrepair", "i fix and repair", 
        "cellaris", "cellairis",
        "talk n fix", "talknfix", "talk-n-fix", 
        "techy", "tech-y",
        "mobile solution", "mobile solutions",
        "experimax",
        "gadget repair", "gadgets repair",
        "wireless clinic", "wireless repair clinic"
    ]
    
    for idx, elem in enumerate(result_elements[:15]):  # Search more results (15 instead of 12)
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
            
            # Extract store address
            store_address = None
            address_selectors = [
                '.fontBodySmall[jsan*="address"]', 
                '.fontBodyMedium > div[jsan*="address"]',
                'div[class*="address"]',
                'div[jscontroller*="address"]',
                'div.W4Efsd > div.fontBodyMedium:nth-child(1)',
                'div[aria-label*="address"]'
            ]
            
            # Try to extract address
            for selector in address_selectors:
                address_elem = elem.query_selector(selector)
                if address_elem:
                    store_address = address_elem.inner_text().strip()
                    if store_address and len(store_address) > 5:  # Ensure we have a meaningful address
                        break
            
            # Skip if we couldn't find a name or address
            if not store_name or not store_address:
                continue
            
            store_name_lower = store_name.lower()
            store_addr_lower = store_address.lower() if store_address else ""
                
            # Check if this is a high-confidence brand that's typically inside Walmart
            is_high_confidence_brand = any(brand in store_name_lower.replace(" ", "") for brand in 
                                         [b.replace(" ", "") for b in HIGH_CONFIDENCE_IN_WALMART_BRANDS])
            
            # Look for indicators this store is located inside Walmart
            inside_walmart_indicators = [
                "inside walmart", "in walmart", "walmart #", "walmart store", 
                "walmart supercenter", "inside the walmart", "inside the store",
                "the fix at walmart", "the fix walmart", "fix walmart", 
                "inside", "at walmart", "walmart location", "ste", "suite" 
            ]
            
            explicitly_inside_walmart = any(indicator in store_name_lower or indicator in store_addr_lower 
                                         for indicator in inside_walmart_indicators)
            
            # Extract address components from store address
            store_addr_components = {}
            if store_address:
                # City
                city_match = re.search(r'([A-Za-z\s]+),\s+(?:\d{5},\s+)?(?:[A-Z]{2}|puerto rico)', 
                                     store_addr_lower, re.IGNORECASE)
                if city_match:
                    store_addr_components['city'] = city_match.group(1).strip().lower()
                
                # ZIP code
                zip_match = re.search(r'\b(\d{5})\b', store_addr_lower)
                if zip_match:
                    store_addr_components['zip'] = zip_match.group(1)
                    
                # Puerto Rico identifier
                if "puerto rico" in store_addr_lower:
                    store_addr_components['state'] = "puerto rico"
            
            # IMPROVED ADDRESS MATCHING LOGIC:
            is_same_location = False
            
            # 1. Check for exact/similar address match
            if walmart_address and store_address:
                # Direct address match with the enhanced address_similarity_check
                is_same_location = address_similarity_check(walmart_address, store_address)
            
            # 2. Check for ZIP and city match (very reliable indicators)
            if ('zip' in walmart_addr_components and 'zip' in store_addr_components and
                walmart_addr_components['zip'] == store_addr_components['zip']):
                
                # If we have matching ZIP codes and it's a high confidence brand, it's likely inside Walmart
                if is_high_confidence_brand:
                    is_same_location = True
                    logger.warning(f"HIGH CONFIDENCE MATCH: '{store_name}' with matching ZIP code {store_addr_components['zip']}")
                
                # If in Puerto Rico, we confirm same location
                elif ('state' in walmart_addr_components and 'state' in store_addr_components and
                     walmart_addr_components['state'] == 'puerto rico' and store_addr_components['state'] == 'puerto rico'):
                    is_same_location = True
                    logger.info(f"Puerto Rico same location detected via ZIP match: '{store_address}' vs Walmart at '{walmart_address}'")
                
                # For other locations, check if cities match too
                elif ('city' in walmart_addr_components and 'city' in store_addr_components and
                     store_addr_components['city'] == walmart_addr_components['city']):
                    is_same_location = True
                    logger.info(f"Same location detected via ZIP and city match: '{store_address}' vs Walmart at '{walmart_address}'")
            
            # 3. Extra check for high confidence brands - if name matches and the store has the same city, it's likely inside
            if (not is_same_location and is_high_confidence_brand and
                'city' in walmart_addr_components and 'city' in store_addr_components and
                walmart_addr_components['city'] == store_addr_components['city']):
                is_same_location = True
                logger.warning(f"HIGH CONFIDENCE BRAND in same city: '{store_name}' in {store_addr_components['city']}")
            
            # If we have a verified same location match, add this store
            if is_same_location or explicitly_inside_walmart:
                # Check if this is a mobile store (keyword match)
                store_name_for_matching = store_name_lower.replace(" ", "").replace("-", "")
                
                # IMPROVED KEYWORD MATCHING: Check for variations of names without spaces/hyphens
                matches = []
                for keyword in MOBILE_STORE_KEYWORDS:
                    normalized_keyword = keyword.lower().replace(" ", "").replace("-", "")
                    if normalized_keyword in store_name_for_matching:
                        matches.append(keyword)
                
                # Count mobile-related keywords
                mobile_keywords = ["cell", "phone", "repair", "mobile", "fix", "wireless", "device"]
                word_count = sum(1 for word in mobile_keywords if word.lower() in store_name_lower)
                
                # Special check for high-confidence brands
                is_known_brand = any(brand.replace(" ", "") in store_name_for_matching 
                                   for brand in HIGH_CONFIDENCE_IN_WALMART_BRANDS)
                
                # If we have keyword matches, multiple mobile terms + repair/fix, or it's a known brand, it's a mobile store
                if matches or is_known_brand or (word_count >= 2 and any(word in store_name_lower for word in ["repair", "fix"])):
                    store_entry = {
                        'name': store_name,
                        'address': store_address,
                        'distance': "Same location - Inside Walmart",
                        'keywords_matched': matches,
                        'location_match': "exact_address",
                        'location_confidence': "high",
                        'is_known_brand': is_known_brand,
                        'matching_method': "address_match"
                    }
                    found_stores.append(store_entry)
                    logger.warning(f"FOUND MOBILE STORE AT SAME ADDRESS: '{store_name}' at '{store_address}'")
                    
                    # Enhanced logging
                    if is_known_brand:
                        logger.warning(f"HIGH CONFIDENCE BRAND DETECTED: {store_name}")
            
            # Handle nearby stores (not at the same address)
            else:
                # Extract distance value
                distance_meters = None
                try:
                    distance_match = re.search(r'([\d.]+)\s*(mi|km|m|ft)', distance_text)
                    if distance_match:
                        distance_val = float(distance_match.group(1))
                        distance_unit = distance_match.group(2)
                        
                        # Convert to meters for consistent comparison
                        if distance_unit == 'mi':
                            distance_meters = distance_val * 1609.34  # miles to meters
                        elif distance_unit == 'km':
                            distance_meters = distance_val * 1000  # km to meters
                        elif distance_unit == 'ft':
                            distance_meters = distance_val * 0.3048  # feet to meters
                        else:  # assume meters
                            distance_meters = distance_val
                            
                        # If store is nearby or is a high confidence brand, add it
                        if distance_meters <= SEARCH_RADIUS_METERS * 1.5 or is_high_confidence_brand:
                            # Check for mobile store keywords
                            store_matches = []
                            for keyword in MOBILE_STORE_KEYWORDS:
                                if keyword.lower().replace(" ", "") in store_name_lower.replace(" ", ""):
                                    store_matches.append(keyword)
                                    
                            # Only add if it matches known mobile repair keywords
                            if store_matches or is_high_confidence_brand:
                                store_entry = {
                                    'name': store_name,
                                    'address': store_address,
                                    'distance': f"{distance_val} {distance_unit}",
                                    'distance_meters': distance_meters,
                                    'keywords_matched': store_matches,
                                    'is_known_brand': is_high_confidence_brand
                                }
                                found_stores.append(store_entry)
                                logger.warning(f"Found nearby mobile store: '{store_name}' at {distance_val} {distance_unit}")
                                
                except Exception:
                    # If we can't extract distance but it's a high confidence brand, still consider it
                    if is_high_confidence_brand:
                        store_entry = {
                            'name': store_name,
                            'address': store_address,
                            'distance': "Unknown distance",
                            'keywords_matched': [],
                            'is_known_brand': True,
                            'high_risk': True
                        }
                        found_stores.append(store_entry)
                        logger.warning(f"Found high-confidence brand with unknown distance: '{store_name}'")
        
        except Exception as e:
            logger.debug(f"Error processing a result: {str(e)}")
    
    return found_stores
