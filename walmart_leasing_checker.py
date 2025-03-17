"""
Walmart Leasing Space Checker

This script:
1. Scrapes Walmart leasing page for available properties
2. Extracts listings with spaces < 1000 sqft
3. Checks Google reviews (must be > 10,000)
4. Ensures no mobile phone repair stores inside
5. Sends email notifications
6. Runs on a daily schedule
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
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException, WebDriverException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("walmart_leasing_checker.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
WALMART_LEASING_URL = "https://leasing.walmart.com/viewspaces"
GOOGLE_API_KEY = "AIzaSyC6av-FESCOQG9F-G4oZ0k9KVweacH3KIU"
MAX_SPACE_SIZE = 1000  # sq ft
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


def scrape_walmart_leasing():
    """
    Scrape Walmart leasing page and extract all available properties.
    Returns a list of properties with their details.
    """
    logger.info("Starting to scrape Walmart leasing page...")
    
    driver = setup_selenium_driver(headless=False)  # Use visible browser for better handling of modals
    if not driver:
        logger.error("Failed to set up the WebDriver. Exiting.")
        return []
    
    all_properties = []
    
    try:
        # Load the Walmart leasing page
        driver.get(WALMART_LEASING_URL)
        logger.info("Loaded Walmart leasing page")
        
        # Wait for the page to load by looking for property buttons
        try:
            wait = WebDriverWait(driver, 30)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'button[class*="jss"]')))
            logger.info("Page loaded successfully")
        except TimeoutException:
            logger.warning("Timeout waiting for page to load. Taking a screenshot.")
            driver.save_screenshot(os.path.join(OUTPUT_DIR, "page_load_timeout.png"))
        
        # Give extra time for JavaScript to execute
        time.sleep(5)
        
        # Save the page HTML for reference
        with open(os.path.join(OUTPUT_DIR, "walmart_page.html"), "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        
        # Find all property buttons - try multiple class patterns
        button_selectors = [
            'button[class*="jss"]', 
            'button.jss56', 
            'button:has(svg)'
        ]
        
        property_buttons = []
        for selector in button_selectors:
            property_buttons = driver.find_elements(By.CSS_SELECTOR, selector)
            if property_buttons:
                logger.info(f"Found {len(property_buttons)} property buttons using selector: {selector}")
                break
        
        if not property_buttons:
            logger.error("No property buttons found. Saving screenshot for debugging.")
            driver.save_screenshot(os.path.join(OUTPUT_DIR, "no_buttons.png"))
            return []
        
        # Extract basic information from all buttons first
        logger.info(f"Processing {len(property_buttons)} properties for basic information...")
        
        # Create a list to store properties with available spaces < 1000 sqft
        small_space_properties = []
        
        # First pass: Get basic information from all buttons
        for idx, button in enumerate(property_buttons):
            try:
                # Scroll the button into view
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                time.sleep(0.2)
                
                # Get button HTML and extract basic information
                button_html = button.get_attribute('outerHTML')
                soup = BeautifulSoup(button_html, 'html.parser')
                
                # Extract store number
                store_num_elem = soup.select_one('b[class*="jss"]')
                if not store_num_elem:
                    logger.warning(f"Could not find store number for property {idx+1}")
                    continue
                    
                store_num = store_num_elem.text.strip()
                
                # Extract address
                address_elem = soup.select_one('p[class*="jss"]')
                address = address_elem.text.strip() if address_elem else "Unknown"
                
                # Extract available spaces text
                spaces_elems = soup.select('b[class*="jss"]')
                spaces_text = None
                for elem in spaces_elems:
                    if 'available space' in elem.text:
                        spaces_text = elem.text.strip()
                        break
                
                # If we couldn't find spaces information, skip this property
                if not spaces_text:
                    logger.warning(f"No spaces information for property {idx+1}")
                    continue
                    
                logger.info(f"Property {idx+1}: {store_num} at {address} - {spaces_text}")
                
                # Add to the list of properties to process further
                property_info = {
                    'store_num': store_num,
                    'address': address,
                    'spaces_text': spaces_text,
                    'button_index': idx
                }
                
                small_space_properties.append(property_info)
                
            except Exception as e:
                logger.error(f"Error extracting basic info for property {idx+1}: {str(e)}")
        
        logger.info(f"Found {len(small_space_properties)} properties with basic information")
        
        # Second pass: Process each property to get detailed space information
        matching_properties = []
        
        for idx, prop in enumerate(small_space_properties):
            try:
                logger.info(f"Processing property {idx+1}/{len(small_space_properties)}: {prop['store_num']} at {prop['address']}")
                
                # Get the button for this property
                button = property_buttons[prop['button_index']]
                
                # Scroll button into view
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                time.sleep(0.5)
                
                # Click the button to open the modal
                try:
                    # Try JavaScript click for better reliability
                    driver.execute_script("arguments[0].click();", button)
                    logger.info(f"Clicked button for {prop['store_num']}")
                except:
                    logger.warning(f"JavaScript click failed, trying direct click for {prop['store_num']}")
                    button.click()
                
                # Wait for modal to appear
                modal_found = False
                try:
                    modal_selectors = [
                        '[class*="modal"]', '.jss84', '.jss96', '.jss95', 
                        'div[class*="Modal"]', 'p[class*="jss"]'
                    ]
                    
                    for selector in modal_selectors:
                        try:
                            wait = WebDriverWait(driver, 5)
                            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                            modal_found = True
                            logger.info(f"Modal found with selector: {selector}")
                            break
                        except TimeoutException:
                            continue
                    
                    if not modal_found:
                        logger.warning(f"Modal not detected for {prop['store_num']}. Taking screenshot.")
                        driver.save_screenshot(os.path.join(OUTPUT_DIR, f"modal_not_found_{idx}.png"))
                    
                    # Wait a bit for modal content to load
                    time.sleep(1)
                    
                    # Get the modal content
                    page_source = driver.page_source
                    modal_soup = BeautifulSoup(page_source, 'html.parser')
                    
                    # Save modal HTML for debugging
                    with open(os.path.join(OUTPUT_DIR, f"modal_{idx}.html"), "w", encoding="utf-8") as f:
                        f.write(page_source)
                    
                    # Extract spaces information from modal
                    spaces = []
                    space_selectors = ['.jss96', '.jss98', 'p[class*="jss"] span']
                    
                    for selector in space_selectors:
                        space_elements = modal_soup.select(selector)
                        for space_elem in space_elements:
                            space_text = space_elem.text.strip()
                            
                            # Look for square footage
                            sqft_match = re.search(r'(\d+)\s*(?:sq\s*ft|sqft)', space_text, re.IGNORECASE)
                            if sqft_match:
                                sqft = int(sqft_match.group(1))
                                
                                # Check if space meets our size requirement
                                if sqft < MAX_SPACE_SIZE:
                                    # Extract suite number
                                    suite_match = re.search(r'Suite\s+(\w+)', space_text)
                                    suite = suite_match.group(1) if suite_match else "Unknown"
                                    
                                    logger.info(f"Found space under {MAX_SPACE_SIZE} sqft: {suite} - {sqft} sqft")
                                    spaces.append({
                                        'suite': suite,
                                        'sqft': sqft,
                                        'text': space_text
                                    })
                    
                    # Close the modal
                    try:
                        # Try multiple ways to close the modal
                        close_selectors = [
                            '.MuiSvgIcon-root path[d*="M19"]', 
                            'svg[viewBox="0 0 24 24"]',
                            'button.close',
                            '.close-modal'
                        ]
                        
                        close_clicked = False
                        for selector in close_selectors:
                            try:
                                close_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                                for elem in close_elements:
                                    try:
                                        parent = elem.find_element(By.XPATH, './..')
                                        parent.click()
                                        close_clicked = True
                                        break
                                    except:
                                        continue
                                
                                if close_clicked:
                                    break
                            except:
                                continue
                        
                        if not close_clicked:
                            # Press Escape key as fallback
                            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
                            logger.info("Used Escape key to close modal")
                        else:
                            logger.info("Closed modal using click")
                            
                    except Exception as e:
                        logger.error(f"Error closing modal: {str(e)}")
                    
                    # Wait for modal to disappear
                    time.sleep(1)
                    
                    # If we found spaces under 1000 sqft, add property to our list for Google checking
                    if spaces:
                        prop['spaces'] = spaces
                        matching_properties.append(prop)
                    
                except TimeoutException:
                    logger.warning(f"Timeout waiting for modal for {prop['store_num']}")
                except Exception as e:
                    logger.error(f"Error processing modal for {prop['store_num']}: {str(e)}")
                    
            except Exception as e:
                logger.error(f"Error processing property {idx+1}: {str(e)}")
        
        # Close the WebDriver
        driver.quit()
        logger.info(f"Found {len(matching_properties)} properties with spaces under {MAX_SPACE_SIZE} sqft")
        
        # Save the properties to JSON
        with open(os.path.join(OUTPUT_DIR, "small_space_properties.json"), "w", encoding="utf-8") as f:
            json.dump(matching_properties, f, indent=2)
        
        # Now use Google Places API to check reviews and mobile stores
        final_matches = []
        
        for prop in matching_properties:
            try:
                # Format address to ensure it includes "Walmart"
                full_address = f"Walmart {prop['address']}"
                
                # Check Google reviews
                review_count = get_google_reviews(full_address)
                logger.info(f"Google reviews for {prop['store_num']}: {review_count}")
                
                # Check if it meets review threshold
                if review_count < MIN_REVIEWS:
                    logger.info(f"Skipping {prop['store_num']} - only {review_count} reviews")
                    continue
                
                # Check for mobile stores
                has_mobile = has_mobile_store(full_address)
                logger.info(f"Mobile store check for {prop['store_num']}: {has_mobile}")
                
                # Skip if mobile store present
                if has_mobile:
                    logger.info(f"Skipping {prop['store_num']} - has mobile store")
                    continue
                
                # This property matches all criteria
                prop['review_count'] = review_count
                final_matches.append(prop)
                logger.info(f"✓ Match found: {prop['store_num']} at {prop['address']}")
                
            except Exception as e:
                logger.error(f"Error checking Google data for {prop['store_num']}: {str(e)}")
        
        # Save final matches to JSON
        with open(os.path.join(OUTPUT_DIR, "matching_properties.json"), "w", encoding="utf-8") as f:
            json.dump(final_matches, f, indent=2)
            
        logger.info(f"Found {len(final_matches)} properties matching ALL criteria")
        return final_matches
        
    except Exception as e:
        logger.error(f"Error in scrape_walmart_leasing: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Make sure to quit the driver
        if driver:
            driver.quit()
            
        return []


def get_google_reviews(address):
    """Get the number of Google reviews for a Walmart location."""
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
            return (
                details_response.json().get("result", {}).get("user_ratings_total", 0)
            )
        return 0
    except Exception as e:
        logger.error(f"Error getting Google reviews: {str(e)}")
        return 0


def has_mobile_store(address):
    """Check if there is a mobile phone store in the Walmart at the given address."""
    try:
        # Format address to ensure it includes "Walmart"
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
        walmart_place_id = walmart_data["candidates"][0]["place_id"]
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
                        logger.info(f"Found mobile store: {result.get('name')} near Walmart {address}")
                        return True
        
        logger.info(f"No mobile stores detected in Walmart at {address}")
        return False
        
    except Exception as e:
        logger.error(f"Error checking for mobile stores: {str(e)}")
        return True  # Assume there is a store in case of error (to be safe)


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
                </tr>
        """
        
        # Add a plain text version as well
        text_content = "Walmart Leasing Opportunities\n\n"
        text_content += f"Found {len(properties)} locations matching your criteria:\n\n"
        
        # Add each property to the email
        for prop in properties:
            store_num = prop["store_num"]
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
            
            # Add to HTML content
            html_content += f"""
                <tr>
                    <td>{store_num}</td>
                    <td>{address}</td>
                    <td>{space_html}</td>
                    <td>{reviews}</td>
                </tr>
            """
            
            # Add to text content
            text_content += f"• {store_num} at {address} - {reviews} reviews\n"
            text_content += space_text
            text_content += "\n"
        
        # Close the HTML
        html_content += """
            </table>
            <p>This is an automated message from your Walmart Leasing Checker.</p>
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


def job():
    """Main job function to run the scraper and send notifications."""
    logger.info(f"Running scheduled job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if test mode
    test_mode = "--test" in sys.argv
    
    if test_mode:
        logger.info("Running in TEST MODE")
        # Use sample data for testing
        test_properties = [
            {
                "store_num": "Store #1234",
                "address": "123 Test St, Anytown, TX 12345",
                "review_count": 15000,
                "spaces": [
                    {"suite": "100", "sqft": 800, "text": "Suite 100 | 800 sqft"},
                    {"suite": "101", "sqft": 600, "text": "Suite 101 | 600 sqft"}
                ]
            },
            {
                "store_num": "Store #5678",
                "address": "456 Sample Ave, Testville, CA 67890",
                "review_count": 12000,
                "spaces": [
                    {"suite": "200", "sqft": 950, "text": "Suite 200 | 950 sqft"}
                ]
            }
        ]
        send_email(test_properties)
    else:
        # Run the actual scraper
        matching_properties = scrape_walmart_leasing()
        
        # Send email if we found matches
        if matching_properties:
            send_email(matching_properties)
        else:
            logger.info("No matching properties found, no email sent")


def main():
    """Main function to run the script once and set up scheduling."""
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Run the job immediately
    logger.info("Starting Walmart Leasing Space Checker")
    job()
    
    # Schedule to run daily at 8:00 AM
    schedule.every().day.at("08:00").do(job)
    logger.info("Scheduled to run daily at 8:00 AM")
    
    # Keep the script running to execute scheduled jobs
    while "--schedule" in sys.argv:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
