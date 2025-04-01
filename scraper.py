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
    
    # Find the modal content
    modal_content = soup.select_one('.MuiDialogContent-root') or soup
    
    # Find all text blocks that look like spaces
    space_patterns = [
        # Pattern for "Suite XXX | YYY sqft"
        r'Suite\s+([A-Za-z0-9-]+)\s*\|\s*(\d+)\s*(?:sq\s*ft|sqft)',
        # Pattern for just "Suite XXX" followed by a separate "YYY sqft"
        r'Suite\s+([A-Za-z0-9-]+)[\s\S]*?(\d+)\s*(?:sq\s*ft|sqft)',
        # General pattern for any combination of suite and square footage
        r'(?:Suite\s+)?([A-Za-z0-9-]+)[\s\r\n\t]*(?:[:|])?\s*(\d+)\s*(?:sq\s*ft|sqft)',
    ]
    
    # Extract text content
    modal_text = modal_content.get_text(separator=' ')
    
    # Test each pattern in order of specificity
    spaces_found = False
    for pattern in space_patterns:
        matches = re.findall(pattern, modal_text, re.IGNORECASE | re.MULTILINE)
        if matches:
            for suite, sqft in matches:
                try:
                    spaces.append({
                        'suite': suite.strip(),
                        'sqft': int(sqft.strip()),
                        'text': f"Suite {suite} | {sqft} sqft"
                    })
                    spaces_found = True
                except (ValueError, AttributeError):
                    continue
            
            # If we found spaces with this pattern, stop trying others
            if spaces_found:
                logger.info(f"Found {len(spaces)} spaces using pattern: {pattern}")
                break
    
    # If we didn't find spaces with regular expressions, try HTML structure
    if not spaces:
        # Look for suite info in paragraphs, spans, or divs
        for element in modal_content.select('p, span, div'):
            text = element.get_text().strip()
            if 'suite' in text.lower() and ('sqft' in text.lower() or 'sq ft' in text.lower()):
                # Extract suite number and square footage
                suite_match = re.search(r'Suite\s+([A-Za-z0-9-]+)', text, re.IGNORECASE)
                sqft_match = re.search(r'(\d+)\s*(?:sq\s*ft|sqft)', text, re.IGNORECASE)
                
                if suite_match and sqft_match:
                    suite = suite_match.group(1)
                    sqft = int(sqft_match.group(1))
                    spaces.append({
                        'suite': suite,
                        'sqft': sqft,
                        'text': text
                    })
    
    # Try one more method: look for lines with both 'Suite' and square footage
    if not spaces:
        lines = modal_text.split('\n')
        for line in lines:
            if 'suite' in line.lower() and any(s in line.lower() for s in ['sqft', 'sq ft']):
                suite_match = re.search(r'Suite\s+([A-Za-z0-9-]+)', line, re.IGNORECASE)
                sqft_match = re.search(r'(\d+)\s*(?:sq\s*ft|sqft)', line, re.IGNORECASE)
                
                if suite_match and sqft_match:
                    suite = suite_match.group(1)
                    sqft = int(sqft_match.group(1))
                    spaces.append({
                        'suite': suite,
                        'sqft': sqft,
                        'text': line.strip()
                    })
    
    # Deduplicate spaces based on suite numbers
    if spaces:
        unique_spaces = {}
        for space in spaces:
            suite = space.get('suite')
            if suite not in unique_spaces or space.get('sqft', 0) < unique_spaces[suite].get('sqft', 9999):
                unique_spaces[suite] = space
        
        spaces = list(unique_spaces.values())
    
    return spaces

def process_property_chunk(button_indices, worker_id=0):
    """
    Process a chunk of property indices with a truly independent browser instance.
    Each worker has its own browser to enable real parallelism.
    """
    logger.info(f"Worker {worker_id}: Starting to process {len(button_indices)} buttons")
    
    # Set up a new browser instance with retry mechanism
    # Use a unique user agent and port to ensure true independence
    unique_port = 9222 + worker_id  # Use a unique debugging port for each Chrome instance
    driver = setup_selenium_driver(
        headless=True, 
        retries=3, 
        worker_id=worker_id,
        debugging_port=unique_port
    )
    
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
                                
                                # Filter spaces by size more strictly
                                small_spaces = [space for space in spaces if space['sqft'] < MAX_SPACE_SIZE]
                                
                                # Only add property if it has at least one small space
                                if small_spaces:
                                    logger.info(f"Worker {worker_id}: Found {len(small_spaces)} spaces under {MAX_SPACE_SIZE} sqft")
                                    prop_info['spaces'] = small_spaces
                                    properties.append(prop_info)
                                else:
                                    logger.info(f"Worker {worker_id}: No spaces under {MAX_SPACE_SIZE} sqft, skipping property")
                                
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
    Main function to scrape Walmart leasing properties using true parallel processing.
    """
    logger.info("Starting true parallel Walmart leasing scraper...")
    
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
    
    # Use multiprocessing instead of threading for true parallelism
    # Process all tasks in parallel using ProcessPoolExecutor instead of ThreadPoolExecutor
    all_properties = []
    
    # Create a function that each worker will run in its own process
    def worker_process(worker_id, indices):
        try:
            # Set up process-specific logging
            process_logger = logging.getLogger(f"Worker-{worker_id}")
            process_logger.setLevel(logging.INFO)
            
            # Process the button indices with this worker's own browser
            process_logger.info(f"Worker {worker_id} starting with {len(indices)} buttons")
            properties = process_property_chunk(indices, worker_id)
            process_logger.info(f"Worker {worker_id} completed with {len(properties)} properties found")
            return properties
        except Exception as e:
            process_logger.error(f"Worker {worker_id} failed: {str(e)}")
            return []
    
    # Use concurrent.futures.ProcessPoolExecutor for true parallel execution
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=WEB_WORKERS) as executor:
        # Submit tasks to the executor
        futures = []
        for worker_id, indices in enumerate(worker_tasks):
            futures.append(executor.submit(worker_process, worker_id, indices))
        
        # Process results as they complete (not waiting for all to finish)
        for future in concurrent.futures.as_completed(futures):
            try:
                properties = future.result()
                all_properties.extend(properties)
                logger.info(f"Received {len(properties)} properties from worker (total so far: {len(all_properties)})")
            except Exception as e:
                logger.error(f"Worker process generated an exception: {str(e)}")
    
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
