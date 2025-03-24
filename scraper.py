"""
Core scraper functionality for Walmart leasing properties
"""

import re
import time
import logging
import concurrent.futures
from bs4 import BeautifulSoup
from selenium import webdriver  # Add this import for ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains  # Add this import
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

from config import WALMART_LEASING_URL, MAX_SPACE_SIZE, WEB_WORKERS
from selenium_utils import setup_selenium_driver, scroll_to_element, safe_click

# Configure logging
logger = logging.getLogger(__name__)

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

def process_property_chunk(button_indices, worker_id=0):
    """Process a chunk of property indices with a single browser instance."""
    logger.info(f"Worker {worker_id}: Starting to process {len(button_indices)} buttons")
    
    # Set up a new browser instance with retry mechanism
    driver = setup_selenium_driver(headless=True, retries=3)
    if not driver:
        logger.error(f"Worker {worker_id}: Failed to create browser instance after multiple attempts")
        return []
    
    properties = []
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        try:
            # Load the Walmart leasing page
            driver.get(WALMART_LEASING_URL)
            logger.info(f"Worker {worker_id}: Loaded Walmart leasing page (attempt {retry_count + 1})")
            
            # Wait for the page to load - looking specifically for property buttons
            try:
                wait = WebDriverWait(driver, 30)  # Increased timeout
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'button.jss56')))
                logger.info(f"Worker {worker_id}: Page loaded successfully")
            except TimeoutException:
                logger.warning(f"Worker {worker_id}: Timeout waiting for page to load, retrying...")
                retry_count += 1
                continue
            
            # Extra wait to ensure JavaScript is fully loaded
            time.sleep(5)
            
            # Find all property buttons
            all_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.jss56')
            button_count = len(all_buttons)
            
            # Verify we actually found buttons
            if button_count == 0:
                logger.warning(f"Worker {worker_id}: No buttons found, retrying...")
                retry_count += 1
                continue
                
            logger.info(f"Worker {worker_id}: Found {button_count} property buttons")
            
            # Process buttons based on their index
            processed_button_count = 0
            for idx in button_indices:
                if idx >= button_count:
                    continue
                    
                try:
                    # Only log every 10 buttons to reduce log spam
                    if processed_button_count % 10 == 0:
                        logger.info(f"Worker {worker_id}: Processing button {idx} (progress: {processed_button_count}/{len(button_indices)})")
                    
                    # Get a fresh reference to all buttons to avoid stale elements
                    if processed_button_count % 20 == 0:  # Refresh button list periodically
                        all_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.jss56')
                        if len(all_buttons) == 0:
                            logger.warning(f"Worker {worker_id}: Buttons disappeared, refreshing page...")
                            driver.refresh()
                            time.sleep(3)
                            all_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.jss56')
                    
                    # Skip if button index is out of range after refresh
                    if idx >= len(all_buttons):
                        continue
                    
                    # Get the button at this index
                    try:
                        button = all_buttons[idx]
                        
                        # Scroll to make button visible
                        try:
                            scroll_to_element(driver, button)
                            time.sleep(0.2)  # Brief pause after scrolling
                        except:
                            logger.warning(f"Worker {worker_id}: Failed to scroll to button {idx}, skipping")
                            continue
                        
                        # Extract info from button before clicking
                        try:
                            button_html = button.get_attribute('outerHTML')
                            prop_info = extract_property_info(button_html)
                            
                            if not prop_info:
                                continue
                                
                            logger.info(f"Worker {worker_id}: Found property {prop_info['store_name']} with {prop_info['available_spaces']}")
                            
                            # Click the button to open modal
                            try:
                                driver.execute_script("arguments[0].click();", button)
                                time.sleep(1)  # Wait for modal
                                
                                # Extract space data from modal
                                page_html = driver.page_source
                                spaces = extract_modal_data(page_html)
                                
                                # Filter spaces by size
                                small_spaces = [space for space in spaces if space['sqft'] < MAX_SPACE_SIZE]
                                
                                if small_spaces:
                                    logger.info(f"Worker {worker_id}: Found {len(small_spaces)} spaces under {MAX_SPACE_SIZE} sqft")
                                    prop_info['spaces'] = small_spaces
                                    properties.append(prop_info)
                                
                                # Close the modal using different methods
                                try:
                                    # Try finding the close button first
                                    close_buttons = driver.find_elements(By.CSS_SELECTOR, '.MuiSvgIcon-root path[d*="M19"]')
                                    if close_buttons:
                                        driver.execute_script("arguments[0].click();", close_buttons[0])
                                    else:
                                        # Try any SVG icon
                                        svg_icons = driver.find_elements(By.CSS_SELECTOR, 'svg.MuiSvgIcon-root')
                                        if svg_icons:
                                            driver.execute_script("arguments[0].click();", svg_icons[0])
                                        else:
                                            # Last resort: press Escape key
                                            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                                except:
                                    # If all else fails, just press Escape
                                    ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                                
                                time.sleep(0.5)  # Wait for modal to close
                                
                            except Exception as e:
                                logger.error(f"Worker {worker_id}: Error processing modal: {str(e)}")
                                # Try to recover by pressing Escape
                                try:
                                    ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                                    time.sleep(0.5)
                                except:
                                    pass
                                    
                        except StaleElementReferenceException:
                            logger.warning(f"Worker {worker_id}: Stale element for button {idx}")
                            continue
                        except Exception as e:
                            logger.error(f"Worker {worker_id}: Error extracting info from button {idx}: {str(e)}")
                            continue
                            
                    except Exception as e:
                        logger.error(f"Worker {worker_id}: Error accessing button {idx}: {str(e)}")
                        continue
                        
                    processed_button_count += 1
                    
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Error in button processing loop: {str(e)}")
                    continue
                    
            # If we processed any buttons successfully, break the retry loop
            if processed_button_count > 0:
                logger.info(f"Worker {worker_id}: Successfully processed {processed_button_count} buttons")
                break
                
            retry_count += 1
            
        except Exception as e:
            logger.error(f"Worker {worker_id}: Critical error in worker: {str(e)}")
            retry_count += 1
            # Try to restart the browser if needed
            try:
                driver.quit()
                driver = setup_selenium_driver(headless=True)
            except:
                pass
    
    # Clean up
    if driver:
        driver.quit()
        
    logger.info(f"Worker {worker_id}: Completed with {len(properties)} properties found")
    return properties

def get_walmart_properties_with_small_spaces():
    """
    Main function to scrape Walmart leasing properties using parallel processing.
    Modified for better performance and reliability.
    """
    logger.info("Starting parallel Walmart leasing scraper with improved distribution...")
    
    # First, determine total number of buttons with a single browser instance
    buttons_count = get_total_button_count()
    
    if buttons_count == 0:
        logger.error("No property buttons found. Exiting.")
        return []
    
    logger.info(f"Found {buttons_count} total property buttons to process")
    
    # Use a different approach - each worker gets non-consecutive buttons
    # This ensures workers aren't all trying to load the same part of the page
    all_indices = list(range(buttons_count))
    
    # Create worker tasks that distribute indices more efficiently
    # Each worker gets a stride pattern of indices (0, n, 2n, 3n, etc.)
    worker_tasks = []
    for i in range(WEB_WORKERS):
        # Worker i gets buttons i, i+WEB_WORKERS, i+2*WEB_WORKERS, etc.
        worker_indices = all_indices[i::WEB_WORKERS]
        worker_tasks.append(worker_indices)
    
    logger.info(f"Distributed {buttons_count} buttons across {WEB_WORKERS} workers")
    
    # Process all tasks in parallel
    all_properties = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=WEB_WORKERS) as executor:
        future_to_worker = {
            executor.submit(process_property_chunk, indices, worker_id): worker_id
            for worker_id, indices in enumerate(worker_tasks)
        }
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_worker):
            worker_id = future_to_worker[future]
            try:
                properties = future.result()
                logger.info(f"Worker {worker_id} completed, found {len(properties)} properties with small spaces")
                all_properties.extend(properties)
            except Exception as e:
                logger.error(f"Worker {worker_id} generated an exception: {str(e)}")
    
    # Deduplicate properties based on store_id
    deduplicated = {}
    for prop in all_properties:
        store_id = prop.get('store_id')
        if store_id and (store_id not in deduplicated or 
                        len(prop.get('spaces', [])) > len(deduplicated[store_id].get('spaces', []))):
            deduplicated[store_id] = prop
    
    deduplicated_properties = list(deduplicated.values())
    logger.info(f"Found {len(deduplicated_properties)} unique properties with spaces under {MAX_SPACE_SIZE} sqft")
    
    return deduplicated_properties

def get_total_button_count(max_retries=3):
    """Get the total number of property buttons from the leasing page."""
    for attempt in range(max_retries):
        driver = setup_selenium_driver(headless=True)
        if not driver:
            logger.error(f"Failed to create browser instance for button count (attempt {attempt + 1})")
            time.sleep(5)
            continue
            
        try:
            logger.info(f"Loading Walmart leasing page to count buttons (attempt {attempt + 1})")
            driver.get(WALMART_LEASING_URL)
            
            # Wait for page to load
            try:
                wait = WebDriverWait(driver, 30)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'button.jss56')))
            except TimeoutException:
                logger.warning("Timeout waiting for buttons to load")
                driver.quit()
                time.sleep(5)
                continue
                
            # Extra wait to ensure all buttons are loaded
            time.sleep(5)
            
            buttons = driver.find_elements(By.CSS_SELECTOR, 'button.jss56')
            count = len(buttons)
            
            if count > 0:
                logger.info(f"Found {count} total property buttons")
                driver.quit()
                return count
                
            logger.warning(f"No buttons found on attempt {attempt + 1}")
            driver.quit()
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"Error counting buttons: {str(e)}")
            if driver:
                driver.quit()
            time.sleep(5)
    
    # If we get here, all attempts failed
    logger.error("Failed to count buttons after all attempts")
    return 0
