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
                        "Cell Phone Repair"]

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
    
    # Extract store number
    store_number_elem = store_info_div.select_one('b.jss53')
    store_number = store_number_elem.text if store_number_elem else "Unknown"
    store_id = store_number.replace("Store #", "").strip()
    
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
        "store_number": store_id,
        "store_name": store_number,
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
                    suite = suite_match.group(1) if suite_match else "Unknown"
                    
                    sqft_match = re.search(r'(\d+)\s*(?:sq\s*ft|sqft)', sqft_text, re.IGNORECASE)
                    if sqft_match:
                        sqft = int(sqft_match.group(1))
                        spaces.append({
                            'suite': suite,
                            'sqft': sqft,
                            'text': f"{suite_text} {sqft_text}"
                        })
            except Exception as e:
                logger.error(f"Error extracting space from span: {str(e)}")
    
    # If we didn't find spaces with the exact structure, fall back to our existing method
    if not spaces:
        # Extract spaces information - try multiple selectors
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
                    suite_match = re.search(r'Suite\s+(\w+)', space_text)
                    suite = suite_match.group(1) if suite_match else "Unknown"
                    
                    spaces.append({
                        'suite': suite,
                        'sqft': sqft,
                        'text': space_text
                    })
    
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
        property_address = f"Walmart {property_info['address']}"
        
        # Get Google reviews
        review_count = get_google_reviews(property_address)
        property_info['review_count'] = review_count
        
        # Skip further checking if it doesn't meet review threshold
        if review_count < MIN_REVIEWS:
            property_info['meets_criteria'] = False
            property_info['fail_reason'] = f"Only {review_count} reviews (minimum {MIN_REVIEWS})"
            return property_info
        
        # Check for mobile stores
        has_mobile = has_mobile_store(property_address)
        property_info['has_mobile_store'] = has_mobile
        
        if has_mobile:
            property_info['meets_criteria'] = False
            property_info['fail_reason'] = "Has a mobile phone store"
        else:
            property_info['meets_criteria'] = True
            property_info['fail_reason'] = None
            
        return property_info
        
    except Exception as e:
        logger.error(f"Error checking Google data for {property_info['store_name']}: {str(e)}")
        property_info['error'] = str(e)
        property_info['meets_criteria'] = False
        property_info['fail_reason'] = f"Error checking Google data: {str(e)}"
        return property_info


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
            
            # Look for mobile stores near this Walmart
            nearby_url = f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lng}&radius={radius}&keyword=mobile+phone+repair+cell&key={GOOGLE_API_KEY}"
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


def send_email(properties):
    """ todo Send email notification about matching properties."""
    if not properties:
        logger.info("No properties to notify about")
        return
    
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER
        msg["Subject"] = f"Walmart Leasing Opportunities - {datetime.now().strftime('%Y-%m-%d')}"
        
        #! Create HTML content
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
            </style>
        </head>
        <body>
            <h2>Walmart Leasing Opportunities</h2>
            <p>Found {len(properties)} locations matching your criteria:</p>
            <table>
                <tr>
                    <th>Store</th>
                    <th>Address</th>
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
            store_num = prop["store_name"]
            address = prop["address"]
            reviews = prop.get("review_count", "N/A")
            
            # Create space details HTML
            space_html = "<ul>"
            space_text = ""
            
            for space in prop.get("spaces", []):
                suite = space.get("suite", "Unknown")
                sqft = space.get("sqft", "Unknown")
                space_html += f"<li>Suite {suite}: {sqft} sqft</li>"
                space_text += f"- Suite {suite}: {sqft} sqft\n"
            
            space_html += "</ul>"
            
            # All properties in the final list have been confirmed to NOT have mobile stores
            # So we can display this information
            mobile_store = "No mobile store detected <span class='check'>✓</span>"
            
            # Add to HTML content
            html_content += f"""
                <tr>
                    <td>{store_num}</td>
                    <td>{address}</td>
                    <td>{space_html}</td>
                    <td>{reviews}</td>
                    <td>{mobile_store}</td>
                </tr>
            """
            
            # Add to text content
            text_content += f"• {store_num} at {address} - {reviews} reviews - No mobile store ✓\n"
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
                <li>No mobile phone repair stores present</li>
            </ul>
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
    # OPTIMIZATION: Only make API calls for properties that already match the square footage criteria
    # This avoids unnecessary API requests for properties we won't use anyway
    logger.info(f"Checking Google data for {len(small_space_properties)} properties in parallel (only for properties with spaces < 1000 sqft)")
    with concurrent.futures.ThreadPoolExecutor(max_workers=API_WORKERS) as executor:
        # Process Google data in parallel
        checked_properties = list(executor.map(check_google_data, small_space_properties))
    
    # Step 6: Filter for final matching properties
    matching_properties = [prop for prop in checked_properties if prop.get('meets_criteria', False)]
    
    # Save final results
    with open(os.path.join(OUTPUT_DIR, "matching_properties.json"), "w", encoding="utf-8") as f:
        json.dump(matching_properties, f, indent=2)
    
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
        
        # Send email if matches found
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
