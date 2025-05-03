"""
Core scraper functionality for Walmart leasing properties
Using Playwright for browser automation
"""

import re
import time
import logging
import os
import random
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from colorama import Fore, Back, Style, init

from config import WALMART_LEASING_URL, MAX_SPACE_SIZE
from playwright_utils import (
    setup_playwright_browser,
    close_browser,
    wait_for_element,
    scroll_to_element,
    safe_click,
    force_click,
)

# Initialize colorama for cross-platform colored terminal output
init(autoreset=True)

# Configure logging
logger = logging.getLogger(__name__)


def extract_property_info(button_html):
    """Extract basic property info from button HTML."""
    soup = BeautifulSoup(button_html, "html.parser")

    # Extract store info div
    store_info_div = soup.select_one(".jss58")
    if not store_info_div:
        return None

    # Extract store number - improved extraction
    store_number_elem = store_info_div.select_one("b.jss53")
    store_number_text = store_number_elem.text if store_number_elem else "Unknown"

    # Extract the numeric store ID more carefully
    store_id_match = re.search(r"Store #(\d+)", store_number_text)
    store_id = (
        store_id_match.group(1)
        if store_id_match
        else store_number_text.replace("Store #", "").strip()
    )

    # Extract available spaces
    available_spaces_elem = store_info_div.select("b.jss53")
    available_spaces = (
        available_spaces_elem[-1].text if len(available_spaces_elem) > 2 else "Unknown"
    )

    # Extract address
    address_elem = store_info_div.select_one("p.jss54")
    address = address_elem.text.strip() if address_elem else "Unknown"

    # Extract Google Maps URL
    maps_link = store_info_div.select_one("a.jss55")
    maps_url = maps_link["href"] if maps_link and maps_link.has_attr("href") else ""

    return {
        "store_id": store_id,  # Just the numeric ID
        "store_number": f"Store #{store_id}",  # Full store number with prefix
        "store_name": store_number_text,  # Original store name text
        "address": address,
        "available_spaces": available_spaces.strip(),
        "google_maps_url": maps_url,
        "spaces": [],
    }


def extract_modal_data(modal_html):
    """Extract spaces information from modal HTML with accurate detection for all square footage sizes."""
    soup = BeautifulSoup(modal_html, "html.parser")
    spaces = []

    # NEW APPROACH: Look for the correct modal structure based on the actual HTML
    # Check if we're seeing the modal content or the navigation bar
    if modal_html and '<div class="MuiToolbar-root MuiToolbar-regular">' in modal_html:
        logger.debug(f"{Fore.YELLOW}Modal HTML appears to be the navigation bar, not the modal content{Style.RESET_ALL}")
        return spaces  # Return empty spaces, let JS extraction handle it
    
    # Since the JavaScript extraction is working well, we'll keep a simplified HTML extraction
    # as a fallback only
    
    # Simple pattern-based extraction for Suite | sqft format
    suite_pattern = re.compile(r'Suite\s+(\w+)\s*\|\s*(\d+)\s*sqft', re.IGNORECASE)
    
    # Apply this pattern to all text content
    text_content = soup.get_text()
    for match in suite_pattern.finditer(text_content):
        try:
            suite = match.group(1)
            sqft = int(match.group(2))
            spaces.append({
                "suite": suite,
                "sqft": sqft,
                "text": f"Suite {suite} | {sqft} sqft",
            })
            logger.info(f"{Fore.GREEN}Found via simple pattern: Suite {suite} | {sqft} sqft{Style.RESET_ALL}")
        except (ValueError, IndexError) as e:
            logger.warning(f"{Fore.YELLOW}Error extracting suite details: {e}{Style.RESET_ALL}")

    # Extra validation and logging - don't show warnings here since we know JS is working
    if spaces:
        logger.info(f"{Fore.GREEN}Found {len(spaces)} spaces via HTML parsing: {[(s['suite'], s['sqft']) for s in spaces]}{Style.RESET_ALL}")
    # Don't log a warning here since this is expected to fail when JS extraction works
    
    # Return spaces sorted by suite number for consistency
    return sorted(spaces, key=lambda x: x.get("suite", ""))


def extract_modal_data_from_html(modal_html):
    """Extract spaces information from modal HTML string (helper for JavaScript extraction)."""
    spaces = []
    
    # Process using BeautifulSoup
    soup = BeautifulSoup(modal_html, "html.parser")
    
    try:
        # Look for suite information in the HTML
        suite_pattern = re.compile(r'Suite\s+(\w+)\s*\|\s*(\d+)\s*sqft', re.IGNORECASE)
        
        # Find all text nodes in the HTML
        for text in soup.stripped_strings:
            match = suite_pattern.search(text)
            if match:
                suite = match.group(1)
                sqft = int(match.group(2))
                spaces.append({
                    "suite": suite,
                    "sqft": sqft,
                    "text": f"Suite {suite} | {sqft} sqft",
                })
                logger.info(f"{Fore.GREEN}Direct HTML extraction: Suite {suite} | {sqft} sqft{Style.RESET_ALL}")
        
        if spaces:
            # Deduplicate spaces
            unique_spaces = {}
            for space in spaces:
                suite = space["suite"]
                if suite not in unique_spaces or space["sqft"] < unique_spaces[suite]["sqft"]:
                    unique_spaces[suite] = space
            
            spaces = list(unique_spaces.values())
        
    except Exception as e:
        logger.error(f"{Fore.RED}Error extracting spaces from direct HTML: {str(e)}{Style.RESET_ALL}")
    
    return spaces


def process_properties_sequentially():
    """
    Process all properties sequentially (non-parallel) to ensure accurate data extraction,
    following a precise workflow and providing detailed colored logs.
    Returns:
        List of property dictionaries with space information
    """
    logger.info(f"{Fore.CYAN}Step 1: Starting sequential Walmart leasing scraper...{Style.RESET_ALL}")
    # Create a browser instance for the entire process
    browser_info = setup_playwright_browser(headless=True, retries=3)
    if not browser_info:
        logger.error(f"{Fore.RED}Failed to create browser instance after multiple attempts{Style.RESET_ALL}")
        return []
    page = browser_info["page"]
    properties = []
    eligible_properties = []
    
    try:
        # Step 1: Load the website
        logger.info(f"{Fore.CYAN}Step 1: Loading Walmart leasing page...{Style.RESET_ALL}")
        page.goto(WALMART_LEASING_URL, wait_until="domcontentloaded")
        try:
            wait_for_element(page, "button.jss56", timeout=60)  # Increased timeout
            logger.info(f"{Fore.GREEN}Page loaded successfully{Style.RESET_ALL}")
        except PlaywrightTimeoutError:
            logger.error(f"{Fore.RED}Timeout waiting for page to load{Style.RESET_ALL}")
            return []
        time.sleep(10)  # Increased from 5 to 10
        logger.info(f"{Fore.CYAN}Step 2: Identifying all available properties...{Style.RESET_ALL}")
        all_buttons = page.query_selector_all("button.jss56")
        button_count = len(all_buttons)
        if (button_count == 0):
            logger.error(f"{Fore.RED}No property buttons found on the page{Style.RESET_ALL}")
            debug_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug")
            os.makedirs(debug_dir, exist_ok=True)
            screenshot_path = os.path.join(debug_dir, "no_buttons_found.png")
            page.screenshot(path=screenshot_path)
            logger.error(f"{Fore.RED}Screenshot saved to {screenshot_path}{Style.RESET_ALL}")
            for alt_selector in ["button.MuiButtonBase-root", "div.jss58 > button", "div[role='button']"]:
                alt_buttons = page.query_selector_all(alt_selector)
                if len(alt_buttons) > 0:
                    logger.info(f"{Fore.YELLOW}Found {len(alt_buttons)} buttons using alternative selector: {alt_selector}{Style.RESET_ALL}")
                    all_buttons = alt_buttons
                    button_count = len(all_buttons)
                    break
            if button_count == 0:
                return []
        logger.info(f"{Fore.GREEN}Found {button_count} property buttons to process{Style.RESET_ALL}")
        store_ids = []
        for idx, button in enumerate(all_buttons):
            try:
                button_html = button.inner_html()
                prop_info = extract_property_info(button_html)
                if prop_info:
                    store_ids.append({
                        "index": idx,
                        "store_id": prop_info["store_id"],
                        "store_number": prop_info["store_number"],
                        "address": prop_info["address"]
                    })
                    logger.info(f"{Fore.BLUE}Property {idx+1}/{button_count}: {prop_info['store_number']} - {prop_info['address']}{Style.RESET_ALL}")
            except Exception as e:
                logger.warning(f"{Fore.YELLOW}Failed to extract info for button {idx}: {str(e)}{Style.RESET_ALL}")
        logger.info(f"{Fore.GREEN}Successfully identified {len(store_ids)} properties with valid store IDs{Style.RESET_ALL}")
        logger.info(f"{Fore.CYAN}Step 3: Processing each property in sequence...{Style.RESET_ALL}")
        for idx, store_info in enumerate(store_ids):
            retry_count = 0
            max_retries = 3
            success = False
            while retry_count < max_retries and not success:
                if retry_count > 0:
                    logger.info(f"{Fore.YELLOW}Retry #{retry_count} for {store_info['store_number']}{Style.RESET_ALL}")
                    delay = 5 * (2 ** retry_count)
                    time.sleep(delay)
                button_idx = store_info["index"]
                store_id = store_info["store_id"]
                store_number = store_info["store_number"]
                logger.info(f"{Fore.MAGENTA}Processing {store_number} ({idx+1}/{len(store_ids)}){Style.RESET_ALL}")
                try:
                    all_buttons = page.query_selector_all("button.jss56")
                    if (button_idx >= len(all_buttons)):
                        logger.warning(f"{Fore.YELLOW}Button index {button_idx} for {store_number} is out of range, refreshing page...{Style.RESET_ALL}")
                        page.reload(wait_until="domcontentloaded")
                        time.sleep(10)
                        all_buttons = page.query_selector_all("button.jss56")
                        if button_idx >= len(all_buttons):
                            logger.error(f"{Fore.RED}Button still out of range after refresh, skipping {store_number}{Style.RESET_ALL}")
                            break
                    button = all_buttons[button_idx]
                    try:
                        page.evaluate("""button => {
                            button.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'center' });
                        }""", button)
                        time.sleep(1)
                    except Exception as e:
                        logger.warning(f"{Fore.YELLOW}Failed to scroll to button for {store_number}, retrying... {str(e)}{Style.RESET_ALL}")
                        retry_count += 1
                        continue
                    debug_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug")
                    os.makedirs(debug_dir, exist_ok=True)
                    pre_click_screenshot = os.path.join(debug_dir, f"pre_click_{store_id}.png")
                    page.screenshot(path=pre_click_screenshot)
                    logger.info(f"{Fore.BLUE}Clicking button for {store_number} to open details{Style.RESET_ALL}")
                    try:
                        button.click(timeout=5000)
                        logger.debug(f"Used standard click for {store_number}")
                        time.sleep(2)
                    except Exception as e:
                        logger.debug(f"Standard click failed: {str(e)}, trying force click")
                        try:
                            button.click(force=True, timeout=8000)
                            logger.debug(f"Used force click for {store_number}")
                            time.sleep(2)
                        except Exception as e2:
                            logger.debug(f"Force click failed: {str(e2)}, trying JavaScript click")
                            try:
                                page.evaluate("button => button.click()", button)
                                logger.debug(f"Used JavaScript click for {store_number}")
                                time.sleep(2)
                            except Exception as e3:
                                logger.warning(f"{Fore.YELLOW}All click methods failed for {store_number}{Style.RESET_ALL}")
                                retry_count += 1
                                continue
                    time.sleep(5)
                    modal_selectors = [
                        # More specific selectors
                        ".MuiDialog-paperScrollPaper", 
                        ".MuiDialog-paper",
                        # Try with content-specific selectors
                        "div[role='dialog'] div:has(p:contains('Store #'))",
                        "div[role='dialog'] div:has(p:contains('Showing'))",
                        # Then try the general selectors with filtering
                        ".MuiDialog-container",
                        ".MuiModal-root",
                        "div[role='dialog']",
                        # Only use .MuiPaper-root as last resort with content verification
                        ".MuiPaper-root"
                    ]
                    
                    modal_element = None
                    modal_found = False
                    navbar_detected = False
                    
                    for selector in modal_selectors:
                        try:
                            elements = page.query_selector_all(selector)
                            for element in elements:
                                # Skip if this is the navbar
                                inner_html = element.inner_html()
                                if ('class="MuiToolbar-root"' in inner_html or 
                                    'Your Shop at Walmart' in inner_html or
                                    'viewspaces' in inner_html):
                                    navbar_detected = True
                                    logger.debug(f"Skipping navbar element found with selector: {selector}")
                                    continue
                                    
                                # Look for indicators this is the modal we want
                                # Try to verify this is actual modal content by checking for likely content
                                modal_content_check = page.evaluate("""
                                    (element) => {
                                        // Check if element contains store information text
                                        const hasStoreInfo = 
                                            element.textContent.includes("Store #") || 
                                            element.textContent.includes("Showing") ||
                                            element.textContent.includes("Suite") ||
                                            element.textContent.includes("sqft");
                                            
                                        // Check if element contains modal-specific UI like a back button
                                        const hasBackButton = element.querySelector('svg') !== null;
                                        
                                        return {
                                            hasStoreInfo,
                                            hasBackButton,
                                            text: element.textContent.slice(0, 100) // Get first 100 chars for logging
                                        };
                                    }
                                """, element)
                                
                                if modal_content_check.get('hasStoreInfo') or modal_content_check.get('hasBackButton'):
                                    logger.info(f"{Fore.GREEN}Found likely modal content: {modal_content_check.get('text', '').strip()}{Style.RESET_ALL}")
                                    modal_element = element
                                    modal_found = True
                                    break
                            
                            if modal_found:
                                logger.info(f"{Fore.GREEN}Modal found with selector: {selector}{Style.RESET_ALL}")
                                break
                                
                        except Exception as e:
                            logger.debug(f"Error checking selector {selector}: {str(e)}")
                            continue
                    
                    if not modal_found and navbar_detected:
                        logger.warning(f"{Fore.YELLOW}Detected navbar but not the actual modal for {store_number}{Style.RESET_ALL}")
                    
                    if not modal_found:
                        logger.warning(f"{Fore.YELLOW}Could not find the modal content{Style.RESET_ALL}")
                        continue
                    
                    modal_html = ""
                    js_spaces = []
                    try:
                        # Get modal HTML for debugging first
                        if modal_element:
                            modal_html = modal_element.inner_html()
                        
                        # Fixed JavaScript extraction code
                        js_extract_result = page.evaluate("""
                            () => {
                                const result = {
                                    storeNumber: null,
                                    spaces: []
                                };
                                
                                // Get store number
                                const storeNumElem = document.querySelector('p[class*="jss"]');
                                if (storeNumElem && storeNumElem.textContent.includes('Store #')) {
                                    result.storeNumber = storeNumElem.textContent.trim();
                                }
                                
                                // Method 1: Look for paragraphs containing Suite | sqft pattern
                                const paragraphs = document.querySelectorAll('p');
                                for (const p of paragraphs) {
                                    if (p.textContent.includes('Suite') && p.textContent.includes('sqft')) {
                                        const match = p.textContent.match(/Suite\\s+(\\w+)\\s*\\|\\s*(\\d+)\\s*sqft/i);
                                        if (match) {
                                            result.spaces.push({
                                                suite: match[1],
                                                sqft: parseInt(match[2], 10)
                                            });
                                        }
                                    }
                                }
                                
                                // Method 2: Find spans with Suite and sqft text if we haven't found any spaces yet
                                if (result.spaces.length === 0) {
                                    // Look for spans with bold formatting
                                    const boldSpans = Array.from(document.querySelectorAll('span[style*="font-weight: bold"]'));
                                    for (const span of boldSpans) {
                                        if (span.textContent.includes('Suite')) {
                                            // Get the containing paragraph
                                            const parent = span.closest('p');
                                            if (parent && parent.textContent.includes('sqft')) {
                                                const suiteMatch = span.textContent.match(/Suite\\s+(\\w+)/i);
                                                const sqftMatch = parent.textContent.match(/(\\d+)\\s*sqft/i);
                                                if (suiteMatch && sqftMatch) {
                                                    result.spaces.push({
                                                        suite: suiteMatch[1],
                                                        sqft: parseInt(sqftMatch[1], 10)
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                                
                                return result;
                            }
                        """)
                        
                        # Process JS extraction results immediately
                        if js_extract_result and js_extract_result.get('spaces'):
                            logger.info(f"{Fore.GREEN}JS extraction found {len(js_extract_result['spaces'])} spaces{Style.RESET_ALL}")
                            for space in js_extract_result['spaces']:
                                logger.info(f"{Fore.GREEN}JS found: Suite {space['suite']} | {space['sqft']} sqft{Style.RESET_ALL}")
                                js_spaces.append({
                                    "suite": space['suite'],
                                    "sqft": space['sqft'],
                                    "text": f"Suite {space['suite']} | {space['sqft']} sqft"
                                })
                        else:
                            logger.warning(f"{Fore.YELLOW}JavaScript extraction found no spaces for {store_number}{Style.RESET_ALL}")
                            
                            # Try fallback direct HTML scanning
                            direct_js_result = page.evaluate("""
                                () => {
                                    const spaces = [];
                                    
                                    // Get entire document HTML and find all matches
                                    const html = document.documentElement.innerHTML;
                                    
                                    // Regular expression to find Suite X | YYY sqft pattern
                                    const pattern = /Suite\\s+([A-Za-z0-9]+)\\s*\\|\\s*(\\d+)\\s*sqft/gi;
                                    let match;
                                    
                                    while ((match = pattern.exec(html)) !== null) {
                                        spaces.push({
                                            suite: match[1],
                                            sqft: parseInt(match[2], 10)
                                        });
                                    }
                                    
                                    return { spaces };
                                }
                            """)
                            
                            if direct_js_result and direct_js_result.get('spaces'):
                                logger.info(f"{Fore.GREEN}Direct HTML scan found {len(direct_js_result['spaces'])} spaces{Style.RESET_ALL}")
                                for space in direct_js_result['spaces']:
                                    logger.info(f"{Fore.GREEN}HTML scan found: Suite {space['suite']} | {space['sqft']} sqft{Style.RESET_ALL}")
                                    js_spaces.append({
                                        "suite": space['suite'],
                                        "sqft": space['sqft'],
                                        "text": f"Suite {space['suite']} | {space['sqft']} sqft"
                                    })
                    except Exception as e:
                        logger.warning(f"{Fore.YELLOW}Error with JS extraction: {str(e)}, will try HTML parsing{Style.RESET_ALL}")
                    
                    if not modal_html:
                        logger.warning(f"{Fore.YELLOW}Empty modal HTML for {store_number}, trying page content{Style.RESET_ALL}")
                        try:
                            modal_html = page.content()
                        except Exception as page_e:
                            logger.error(f"{Fore.RED}Failed to get page content: {str(page_e)}{Style.RESET_ALL}")
                            modal_html = ""
                    
                    # Try HTML parsing approach
                    html_spaces = extract_modal_data(modal_html)
                    
                    # IMPORTANT: If JS results exist, use those; otherwise use HTML parsing results
                    if js_spaces:
                        spaces = js_spaces
                        if not html_spaces:
                            logger.info(f"{Fore.GREEN}Using {len(js_spaces)} spaces found via JavaScript extraction{Style.RESET_ALL}")
                    else:
                        spaces = html_spaces
                        if not spaces:
                            logger.warning(f"{Fore.RED}✗ NO SPACES: {store_number} - Could not extract any space information{Style.RESET_ALL}")
                    
                    # Save modal HTML for debugging
                    try:
                        debug_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_html")
                        os.makedirs(debug_dir, exist_ok=True)
                        timestamp = int(time.time())
                        with open(os.path.join(debug_dir, f"modal_{timestamp}.html"), "w", encoding="utf-8") as f:
                            f.write(modal_html)
                        logger.info(f"{Fore.BLUE}Saved modal HTML to debug_html/modal_{timestamp}.html{Style.RESET_ALL}")
                    except Exception as e:
                        logger.debug(f"Could not save debug HTML: {e}")
                    
                    # Continue with the rest of the processing
                    for space in spaces:
                        space["store_id"] = store_id
                    
                    all_spaces_count = len(spaces)
                    small_spaces = [space for space in spaces if space["sqft"] < MAX_SPACE_SIZE]
                    small_spaces_count = len(small_spaces)
                    if all_spaces_count > 0:
                        if small_spaces_count > 0:
                            logger.info(f"{Fore.GREEN}✓ ELIGIBLE: {store_number} has {small_spaces_count}/{all_spaces_count} spaces under {MAX_SPACE_SIZE} sqft{Style.RESET_ALL}")
                            for space in small_spaces:
                                logger.info(f"{Fore.GREEN}    ► {store_number} - Suite {space['suite']} | {space['sqft']} sqft{Style.RESET_ALL}")
                            prop_data = {
                                "store_id": store_id,
                                "store_number": store_number,
                                "address": store_info["address"],
                                "spaces": small_spaces
                            }
                            properties.append(prop_data)
                            eligible_properties.append(prop_data)
                        else:
                            logger.info(f"{Fore.YELLOW}✗ NOT ELIGIBLE: {store_number} has {all_spaces_count} spaces, but none under {MAX_SPACE_SIZE} sqft{Style.RESET_ALL}")
                            for space in spaces:
                                logger.info(f"{Fore.YELLOW}    ► {store_number} - Suite {space['suite']} | {space['sqft']} sqft{Style.RESET_ALL}")
                    else:
                        logger.warning(f"{Fore.RED}✗ NO SPACES: {store_number} - Could not extract any space information{Style.RESET_ALL}")
                    logger.info(f"{Fore.BLUE}Returning to property list from {store_number}{Style.RESET_ALL}")
                    back_selectors = [
                        'div.jss152',
                        'div.jss125',
                        # Use standard selectors without :contains()
                        'div[class^="jss"] svg + span',
                        'div[class^="jss"]:has(svg)',
                        'div[class^="jss"] svg',
                        # Try using text content evaluation
                        'button[aria-label="Back"]',
                        'div[role="button"]'
                    ]
                    back_clicked = False
                    for selector in back_selectors:
                        try:
                            back_button = page.query_selector(selector)
                            if back_button:
                                back_button.click()
                                time.sleep(2)
                                back_clicked = True
                                logger.info(f"{Fore.GREEN}Successfully returned to property list using selector: {selector}{Style.RESET_ALL}")
                                break
                        except Exception as e:
                            logger.debug(f"Back button click failed with selector {selector}: {str(e)}")
                    
                    # Use JavaScript approach to find by text content if standard selectors failed
                    if not back_clicked:
                        try:
                            page.evaluate("""
                                () => {
                                    // Find by text content instead of CSS selector
                                    const allElements = Array.from(document.querySelectorAll('div, span, button'));
                                    for (const el of allElements) {
                                        if (el.textContent && el.textContent.includes('Back to properties')) {
                                            // Try to find clickable parent or the element itself
                                            let clickTarget = el;
                                            // Look for parent with role="button" or actual button
                                            while (clickTarget && clickTarget.tagName !== 'BODY') {
                                                if (clickTarget.onclick || 
                                                    clickTarget.tagName === 'BUTTON' ||
                                                    clickTarget.getAttribute('role') === 'button' ||
                                                    clickTarget.classList.value.startsWith('jss')) {
                                                    clickTarget.click();
                                                    return true;
                                                };
                                                clickTarget = clickTarget.parentElement;
                                            }
                                            // If no suitable parent found, try clicking the element anyway
                                            el.click();
                                            return true;
                                        }
                                    }
                                    // Try finding an SVG that's likely a back button
                                    const svgs = document.querySelectorAll('svg');
                                    for (const svg of svgs) {
                                        const parent = svg.parentElement;
                                        if (parent && (parent.classList.value.startsWith('jss'))) {
                                            parent.click();
                                            return true;
                                        }
                                    }
                                    const dialogs = document.querySelectorAll('.MuiDialog-root, .MuiModal-root, [role="dialog"]');
                                    for (const dialog of dialogs) {
                                        const buttons = dialog.querySelectorAll('button');
                                        for (const btn of buttons) {
                                            const rect = btn.getBoundingClientRect();
                                            if (rect.width < 50 && rect.height < 50 && rect.top < dialog.getBoundingClientRect().top + 50) {
                                                btn.click();
                                                return true;
                                            }
                                        }
                                    }
                                    return false;
                                }
                            """)
                            time.sleep(2)
                            logger.info(f"{Fore.GREEN}Attempted to return to property list using JavaScript{Style.RESET_ALL}")
                            back_clicked = True
                        except Exception as e:
                            logger.warning(f"{Fore.YELLOW}JavaScript back button approach failed: {str(e)}{Style.RESET_ALL}")
                    success = True
                except Exception as e:
                    logger.error(f"{Fore.RED}Error processing property {store_number}: {str(e)}{Style.RESET_ALL}")
                    try_close_modal(page, ".MuiDialog-container")
                    time.sleep(2)
                    try:
                        error_screenshot_path = os.path.join(debug_dir, f"error_{store_id}.png")
                        page.screenshot(path=error_screenshot_path)
                        logger.error(f"{Fore.RED}Error screenshot saved to {error_screenshot_path}{Style.RESET_ALL}")
                    except:
                        pass
                    retry_count += 1
                if retry_count >= max_retries and not success:
                    logger.error(f"{Fore.RED}Failed to process {store_number} after {max_retries} attempts{Style.RESET_ALL}")
            logger.info(f"{Fore.MAGENTA}Progress: {idx+1}/{len(store_ids)} properties processed, {len(eligible_properties)} eligible so far{Style.RESET_ALL}")
            if idx < len(store_ids) - 1:
                delay = random.uniform(1.0, 3.0)
                time.sleep(delay)
        logger.info(f"{Fore.GREEN}="*80)
        logger.info(f"{Fore.GREEN}SUMMARY: Processed {len(store_ids)} properties")
        logger.info(f"{Fore.GREEN}ELIGIBLE PROPERTIES: {len(eligible_properties)} properties with spaces under {MAX_SPACE_SIZE} sqft")
        logger.info(f"{Fore.GREEN}="*80)        
    except Exception as e:
        logger.error(f"{Fore.RED}Critical error in scraper: {str(e)}{Style.RESET_ALL}")
    finally:
        # Always clean up resources
        close_browser(browser_info)
    return properties


def try_close_modal(page, modal_selector_or_element):
    """Helper function to try multiple methods to close a modal"""
    close_success = False
    try:
        if isinstance(modal_selector_or_element, str):
            modal_exists = page.query_selector(modal_selector_or_element) is not None
        else:
            modal_exists = modal_selector_or_element is not None
        if not modal_exists:
            return True
        close_button_selectors = [
            'button[aria-label="close"]',
            'button svg[data-testid="CloseIcon"]',
            '.MuiDialogTitle-root button',
            '.MuiDialog-root button',
            'button.MuiIconButton-root',
        ]
        for selector in close_button_selectors:
            close_button = page.query_selector(selector)
            if close_button:
                try:
                    close_button.click(force=True)
                    time.sleep(1)
                    if isinstance(modal_selector_or_element, str):
                        close_success = not page.query_selector(modal_selector_or_element)
                    else:
                        close_success = True
                    if close_success:
                        logger.debug(f"{Fore.GREEN}Closed modal with button click using selector: {selector}{Style.RESET_ALL}")
                        return True
                except Exception:
                    pass
    except Exception:
        pass
    try:
        page.keyboard.press("Escape")
        time.sleep(1)
        if isinstance(modal_selector_or_element, str):
            close_success = not page.query_selector(modal_selector_or_element)
        else:
            close_success = True
        if close_success:
            logger.debug(f"{Fore.GREEN}Closed modal with Escape key{Style.RESET_ALL}")
            return True
    except Exception:
        pass
    try:
        page.evaluate("""
            () => {
                const closeButtons = document.querySelectorAll('button[aria-label="close"]');
                if (closeButtons.length) {
                    closeButtons[0].click();
                    return true;
                }
                const svgCloseIcons = document.querySelectorAll('svg[data-testid="CloseIcon"]');
                if (svgCloseIcons.length) {
                    const svg = svgCloseIcons[0];
                    let clickTarget = svg;
                    while (clickTarget && clickTarget.tagName !== 'BODY') {
                        if (clickTarget.onclick || 
                            clickTarget.tagName === 'BUTTON' ||
                            clickTarget.getAttribute('role') === 'button') {
                            clickTarget.click();
                            return true;
                        }
                        clickTarget = clickTarget.parentElement;
                    }
                }
                const dialogs = document.querySelectorAll('.MuiDialog-root, .MuiModal-root, [role="dialog"]');
                for (const dialog of dialogs) {
                    const buttons = dialog.querySelectorAll('button');
                    for (const btn of buttons) {
                        const rect = btn.getBoundingClientRect();
                        if (rect.width < 50 && rect.height < 50 && rect.top < dialog.getBoundingClientRect().top + 50) {
                            btn.click();
                            return true;
                        }
                    }
                }
                return false;
            }
        """)
        time.sleep(1)
        if isinstance(modal_selector_or_element, str):
            close_success = not page.query_selector(modal_selector_or_element)
        else:
            close_success = True
        if close_success:
            logger.debug(f"{Fore.GREEN}Closed modal with JavaScript{Style.RESET_ALL}")
            return True
    except Exception:
        pass
    logger.warning(f"{Fore.YELLOW}All methods to close modal failed{Style.RESET_ALL}")
    return False


def get_walmart_properties_with_small_spaces():
    """Main function to scrape Walmart leasing properties using sequential processing."""
    logger.info(f"{Fore.CYAN}Starting sequential Walmart leasing scraper...{Style.RESET_ALL}")
    all_properties = process_properties_sequentially()
    deduplicated = {}
    for prop in all_properties:
        store_id = prop.get("store_id")
        if store_id and (
            store_id not in deduplicated
            or len(prop.get("spaces", []))
            > len(deduplicated[store_id].get("spaces", []))
        ):
            deduplicated[store_id] = prop
    deduplicated_properties = list(deduplicated.values())
    logger.info(
        f"{Fore.GREEN}Final result: {len(deduplicated_properties)} unique properties with spaces under {MAX_SPACE_SIZE} sqft{Style.RESET_ALL}"
    )
    return deduplicated_properties
