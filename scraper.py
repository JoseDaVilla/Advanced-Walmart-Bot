"""
Core scraper functionality for Walmart leasing properties.
This module handles web scraping of the Walmart leasing site to extract information 
about properties with available small spaces.
"""

import re
import time
import logging
import concurrent.futures
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from config import WALMART_LEASING_URL, MAX_SPACE_SIZE, WEB_WORKERS
from playwright_utils import (
    setup_playwright_browser,
    close_browser,
    wait_for_element,
    scroll_to_element,
    safe_click,
)

# Configure logging
logger = logging.getLogger(__name__)


def extract_property_info(button_html):
    """
    Extract basic property info from button HTML.
    
    Args:
        button_html (str): HTML content of the property button
        
    Returns:
        dict: Property information or None if extraction failed
    """
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

    # Find the modal content
    modal_content = soup.select_one(".MuiDialogContent-root") or soup

    # Log the full HTML for debugging
    logger.debug(f"Processing modal HTML (length: {len(modal_html)})")

    # First try to extract the raw HTML to inspect for suite data
    raw_html = str(modal_content)

    # EXTRACT ACTUAL RAW TEXT FOR LOGS
    raw_text = modal_content.get_text(separator=" | ", strip=True)
    logger.debug(f"Modal content raw text: {raw_text[:200]}...")

    # IMPROVED: First check for specific div structure that contains spaces
    space_divs = modal_content.select(
        "div.MuiBox-root, div.jss133, div.jss134, div.MuiGrid-item"
    )
    for div in space_divs:
        if not div.get_text().strip():
            continue

        text = div.get_text(" | ", strip=True)

        # Look for suite and sqft pattern with more flexible regex
        suite_sqft_match = re.search(
            r"(?:Suite|Ste\.?|Unit)\s+([A-Za-z0-9-]+).*?(\d{2,5})\s*(?:SF|sq\.?ft\.?|sqft)",
            text,
            re.IGNORECASE,
        )
        if suite_sqft_match:
            suite = suite_sqft_match.group(1).strip()
            sqft_str = suite_sqft_match.group(2).replace(",", "").replace(".", "")

            try:
                sqft = int(sqft_str)
                spaces.append(
                    {
                        "suite": suite,
                        "sqft": sqft,
                        "text": f"Suite {suite} | {sqft} sqft",
                    }
                )
                logger.info(f"Found space from div: Suite {suite} = {sqft} sqft")
                continue  # Found a match, no need to further process this div
            except ValueError:
                pass

    # IMPROVED APPROACH: Try to find data in the structured table format if we didn't find anything yet
    if not spaces:
        tables = modal_content.select("table")
        for table in tables:
            rows = table.select("tr")
            for row in rows:
                cells = row.select("td")
                if len(cells) >= 2:
                    # Extract suite number and square footage from table cells
                    suite_text = cells[0].get_text(strip=True)
                    sqft_text = cells[1].get_text(strip=True)

                    # Process suite number more flexibly
                    suite_match = re.search(
                        r"(?:Suite\s+)?([A-Za-z0-9][\w\s-]*)", suite_text, re.IGNORECASE
                    )
                    suite = (
                        suite_match.group(1).strip()
                        if suite_match
                        else suite_text.strip()
                    )

                    # More flexible square footage regex that handles numbers like 1565
                    sqft_match = re.search(
                        r"([\d,\.]+)\s*(?:sq\.?ft\.?|sqft|SF)", sqft_text, re.IGNORECASE
                    )
                    if sqft_match:
                        try:
                            # Remove commas and convert to int
                            sqft_str = (
                                sqft_match.group(1).replace(",", "").replace(".", "")
                            )
                            sqft = int(sqft_str)
                            spaces.append(
                                {
                                    "suite": suite,
                                    "sqft": sqft,
                                    "text": f"Suite {suite} | {sqft} sqft",
                                }
                            )
                            logger.info(
                                f"Found space from table: Suite {suite} = {sqft} sqft"
                            )
                        except ValueError:
                            pass

    # IMPROVED: Direct regular expression search on the raw HTML
    if not spaces:
        # Direct pattern matching in the raw HTML to catch edge cases
        direct_patterns = [
            # Match "Suite X | Y sqft" pattern directly
            r"Suite\s+([A-Za-z0-9-]+)\s*\|\s*(\d{2,5})\s*(?:SF|sq\.?ft\.?|sqft)",
            # Match Suite and SF patterns that might be near each other
            r"Suite\s+([A-Za-z0-9-]+)(?:[\s\S]{1,30}?)(\d{2,5})\s*(?:SF|sq\.?ft\.?|sqft)",
        ]

        for pattern in direct_patterns:
            matches = re.findall(pattern, raw_html, re.IGNORECASE)
            for match in matches:
                try:
                    suite = match[0].strip()
                    sqft_str = match[1].replace(",", "")
                    sqft = int(sqft_str)

                    # Basic validation
                    if (
                        50 <= sqft <= 100000 and suite
                    ):  # Increased upper limit to catch all sizes
                        spaces.append(
                            {
                                "suite": suite,
                                "sqft": sqft,
                                "text": f"Suite {suite} | {sqft} sqft",
                            }
                        )
                        logger.info(
                            f"Found space from raw HTML: Suite {suite} = {sqft} sqft"
                        )
                except (ValueError, IndexError):
                    pass

    # If we still have nothing, try to extract from the raw text
    if not spaces:
        # Extract all text content with better line breaks preservation
        modal_paragraphs = modal_content.select("p, div, span")
        modal_texts = []
        for p in modal_paragraphs:
            text = p.get_text(strip=True)
            if text:  # Only add non-empty text
                modal_texts.append(text)

        modal_text = "\n".join(modal_texts)

        # IMPROVED PATTERNS: More accurately match suite information
        space_patterns = [
            # Pattern 1: Suite X | Y sqft format
            r"Suite\s+([A-Za-z0-9-]+)\s*\|\s*([\d,\.]+)\s*(?:sq\.?ft\.?|SF|sqft)",
            # Pattern 2: Suite and sqft on same line with various separators
            r"Suite\s+([A-Za-z0-9-]+)[\s\:\|\-]+?([\d,\.]+)\s*(?:sq\.?ft\.?|SF|sqft)",
            # Pattern 3: Suite with potentially multi-word identifier
            r"Suite\s+([\w\s-]+?)[\s\:\|\-]+?([\d,\.]+)\s*(?:sq\.?ft\.?|SF|sqft)",
            # Pattern 4: Any format mentioning suite and sqft within reasonable proximity
            r"(?:Suite|Ste\.?|Unit)\s+([A-Za-z0-9-]+)(?:[\s\S]{1,50}?)([\d,\.]+)\s*(?:sq\.?ft\.?|SF|sqft)",
            # Pattern 5: Look for any sqft format
            r"(\w+[-\w]*)[\s\:\|\-]+?([\d,\.]+)\s*(?:sq\.?ft\.?|SF|sqft)",
        ]

        # Process all text blocks line by line for better detection
        for line in modal_text.split("\n"):
            for pattern in space_patterns:
                matches = re.findall(pattern, line, re.IGNORECASE)
                for match in matches:
                    try:
                        suite = match[0].strip()
                        # Clean up the suite number
                        if not suite or suite.lower() in ["suite", "ste", "unit"]:
                            continue

                        # Handle square footage with commas or periods
                        sqft_str = match[1].replace(",", "").replace(".", "")
                        if not sqft_str.isdigit():
                            continue

                        sqft = int(sqft_str)

                        # Expanded validation - sqft should be reasonable (up to 10000)
                        if 50 <= sqft <= 10000 and suite:
                            # Check if we already have this suite
                            if not any(s["suite"] == suite for s in spaces):
                                spaces.append(
                                    {
                                        "suite": suite,
                                        "sqft": sqft,
                                        "text": f"Suite {suite} | {sqft} sqft",
                                    }
                                )
                                logger.info(
                                    f"Found space from regex: Suite {suite} = {sqft} sqft"
                                )
                    except (ValueError, IndexError):
                        pass

    # Deduplicate spaces based on suite numbers
    if spaces:
        unique_spaces = {}
        for space in spaces:
            suite = space["suite"]
            if (
                suite not in unique_spaces
                or space["sqft"] < unique_spaces[suite]["sqft"]
            ):
                unique_spaces[suite] = space
        spaces = list(unique_spaces.values())

    # Extra validation and logging
    if spaces:
        # Log the list of extracted spaces for debugging
        logger.info(
            f"Found {len(spaces)} spaces: {[(s['suite'], s['sqft']) for s in spaces]}"
        )
    else:
        logger.warning("Could not extract any spaces from modal")

    # Return spaces sorted by suite number for consistency
    return sorted(spaces, key=lambda x: x.get("suite", ""))


def process_property_chunk(button_indices, worker_id=0):
    """
    Process a chunk of property indices with a truly independent browser instance.
    Each worker has its own browser to enable real parallelism.
    
    Args:
        button_indices (list): Indices of buttons to process
        worker_id (int): ID of the worker processing this chunk
        
    Returns:
        list: List of properties with extracted information
    """
    logger.info(
        f"Worker {worker_id}: Starting to process {len(button_indices)} buttons"
    )

    # Set up a new browser instance with retry mechanism
    browser_info = setup_playwright_browser(
        headless=True, retries=3, worker_id=worker_id
    )

    if not browser_info:
        logger.error(
            f"Worker {worker_id}: Failed to create browser instance after multiple attempts"
        )
        return []

    page = browser_info["page"]

    properties = []
    retry_count = 0
    max_retries = 3

    while retry_count < max_retries:
        try:
            # Load the Walmart leasing page
            page.goto(WALMART_LEASING_URL, wait_until="domcontentloaded")
            logger.info(
                f"Worker {worker_id}: Loaded Walmart leasing page (attempt {retry_count + 1})"
            )

            # Wait for the page to load - looking specifically for property buttons
            try:
                wait_for_element(page, "button.jss56", timeout=30)
                logger.info(f"Worker {worker_id}: Page loaded successfully")
            except PlaywrightTimeoutError:
                logger.warning(
                    f"Worker {worker_id}: Timeout waiting for page to load, retrying..."
                )
                retry_count += 1
                continue

            # Extra wait to ensure JavaScript is fully loaded
            time.sleep(5)

            # Find all property buttons
            all_buttons = page.query_selector_all("button.jss56")
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
                        logger.info(
                            f"Worker {worker_id}: Processing button {idx} (progress: {processed_button_count}/{len(button_indices)})"
                        )

                    # Get a fresh reference to all buttons to avoid stale elements
                    if (
                        processed_button_count % 20 == 0
                    ):  # Refresh button list periodically
                        all_buttons = page.query_selector_all("button.jss56")
                        if len(all_buttons) == 0:
                            logger.warning(
                                f"Worker {worker_id}: Buttons disappeared, refreshing page..."
                            )
                            page.reload(wait_until="domcontentloaded")
                            time.sleep(3)
                            all_buttons = page.query_selector_all("button.jss56")

                    # Skip if button index is out of range after refresh
                    if idx >= len(all_buttons):
                        continue

                    # Get the button at this index
                    button = all_buttons[idx]

                    # Scroll to make button visible
                    try:
                        button.scroll_into_view_if_needed()
                        time.sleep(0.2)  # Brief pause after scrolling
                    except Exception as e:
                        logger.warning(
                            f"Worker {worker_id}: Failed to scroll to button {idx}, skipping: {str(e)}"
                        )
                        continue

                    # Extract info from button before clicking
                    try:
                        button_html = button.inner_html()
                        prop_info = extract_property_info(button_html)

                        if not prop_info:
                            continue

                        logger.info(
                            f"Worker {worker_id}: Found property {prop_info['store_name']} with {prop_info['available_spaces']}"
                        )

                        # Click the button to open modal with force:true and various fallbacks
                        try:
                            # First attempt: force click with longer timeout
                            try:
                                button.click(force=True, timeout=10000)
                                logger.info(
                                    f"Force-clicked button for property {prop_info['store_name']}"
                                )
                            except Exception as click_error:
                                # Second attempt: Try JavaScript click
                                logger.warning(
                                    f"Force click failed, trying JS click: {str(click_error)}"
                                )
                                try:
                                    page.evaluate("arguments[0].click()", button)
                                    logger.info(
                                        f"JS-clicked button for property {prop_info['store_name']}"
                                    )
                                except Exception as js_error:
                                    # Last attempt: dispatch click event
                                    logger.warning(
                                        f"JS click failed, trying dispatch event: {str(js_error)}"
                                    )
                                    page.evaluate(
                                        """
                                        (element) => {
                                            const event = new MouseEvent('click', {
                                                view: window,
                                                bubbles: true,
                                                cancelable: true
                                            });
                                            element.dispatchEvent(event);
                                        }
                                    """,
                                        button,
                                    )

                            # Wait for modal - more generous timeout
                            time.sleep(2)  # Increased from 1s to 2s

                            # Extract space data from modal
                            page_html = page.content()
                            spaces = extract_modal_data(page_html)

                            # Filter spaces by size more strictly
                            small_spaces = [
                                space
                                for space in spaces
                                if space["sqft"] < MAX_SPACE_SIZE
                            ]

                            # Only add property if it has at least one small space
                            if small_spaces:
                                logger.info(
                                    f"Worker {worker_id}: Found {len(small_spaces)} spaces under {MAX_SPACE_SIZE} sqft"
                                )
                                prop_info["spaces"] = small_spaces
                                properties.append(prop_info)
                            else:
                                logger.info(
                                    f"Worker {worker_id}: No spaces under {MAX_SPACE_SIZE} sqft, skipping property"
                                )

                            # Close the modal with force click and advanced recovery
                            try:
                                # For closing the modal, try multiple approaches in sequence
                                close_modal_success = False

                                # Approach 1: Find close button by aria-label
                                close_button = page.query_selector(
                                    'button[aria-label="close"]'
                                )
                                if close_button:
                                    try:
                                        close_button.click(force=True, timeout=5000)
                                        close_modal_success = True
                                        logger.info(
                                            "Closed modal with direct button click"
                                        )
                                    except:
                                        pass

                                # Approach 2: Use Escape key if the button click didn't work
                                if not close_modal_success:
                                    try:
                                        page.keyboard.press("Escape")
                                        time.sleep(0.5)
                                        close_modal_success = True
                                        logger.info("Closed modal with Escape key")
                                    except:
                                        pass

                                # Approach 3: Try JavaScript to find and click close button
                                if not close_modal_success:
                                    try:
                                        # This JS will try to find any likely close button element and click it
                                        page.evaluate(
                                            """
                                            () => {
                                                // Try various selectors for close buttons
                                                const selectors = [
                                                    'button[aria-label="close"]', 
                                                    'button.MuiButtonBase-root svg',
                                                    '.MuiDialog-root button',
                                                    'button.MuiIconButton-root',
                                                    'svg[data-testid="CloseIcon"]'
                                                ];
                                                
                                                for(const selector of selectors) {
                                                    const elements = document.querySelectorAll(selector);
                                                    if(elements.length) {
                                                        for(const el of elements) {
                                                            // Try to find a close button by examining parent elements
                                                            let current = el;
                                                            for(let i = 0; i < 3; i++) {  // Check up to 3 levels up
                                                                if(current && current.tagName === 'BUTTON') {
                                                                    current.click();
                                                                    return true;
                                                                }
                                                                current = current.parentElement;
                                                            }
                                                            
                                                            // If we found an SVG, try clicking its parent
                                                            if(el.tagName === 'svg' && el.parentElement) {
                                                                el.parentElement.click();
                                                                return true;
                                                            }
                                                        }
                                                    }
                                                }
                                                
                                                // If all else fails, try to find buttons that might be close buttons
                                                const buttons = document.querySelectorAll('button');
                                                for(const btn of buttons) {
                                                    const rect = btn.getBoundingClientRect();
                                                    // Look for small buttons positioned in top-right corner
                                                    if(rect.width < 50 && rect.height < 50 && rect.top < 100) {
                                                        btn.click();
                                                        return true;
                                                    }
                                                }
                                                
                                                return false;
                                            }
                                        """
                                        )
                                        time.sleep(1)
                                        logger.info(
                                            "Attempted to close modal via JavaScript"
                                        )
                                    except:
                                        pass

                                # Final approach: Just continue even if we couldn't close it
                                logger.info("Continuing to next property...")

                            except Exception as close_error:
                                logger.warning(
                                    f"Error handling modal close: {str(close_error)}"
                                )
                                # The most reliable fallback is just to press Escape
                                try:
                                    page.keyboard.press("Escape")
                                except:
                                    pass

                            # Wait a bit before continuing to next property
                            time.sleep(1)  # Increased from 0.5s to 1s

                        except Exception as e:
                            logger.error(
                                f"Worker {worker_id}: Error processing modal: {str(e)}"
                            )
                            # Try to recover by pressing Escape and continuing
                            try:
                                page.keyboard.press("Escape")
                                time.sleep(1)
                            except:
                                pass

                    except Exception as e:
                        logger.error(
                            f"Worker {worker_id}: Error extracting info from button {idx}: {str(e)}"
                        )
                        continue

                    processed_button_count += 1

                except Exception as e:
                    logger.error(
                        f"Worker {worker_id}: Error in button processing loop: {str(e)}"
                    )
                    continue

            # If we processed any buttons successfully, break the retry loop
            if processed_button_count > 0:
                logger.info(
                    f"Worker {worker_id}: Successfully processed {processed_button_count} buttons"
                )
                break

            retry_count += 1

        except Exception as e:
            logger.error(f"Worker {worker_id}: Critical error in worker: {str(e)}")
            retry_count += 1

    # Clean up
    close_browser(browser_info)

    logger.info(
        f"Worker {worker_id}: Completed with {len(properties)} properties found"
    )
    return properties


def get_total_button_count(max_retries=3):
    """
    Get the total number of property buttons from the leasing page.
    
    Args:
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        int: Total number of property buttons found, or 0 if failed
    """
    browser_info = None

    for attempt in range(max_retries):
        try:
            browser_info = setup_playwright_browser(headless=True)
            if not browser_info:
                logger.error(
                    f"Failed to create browser instance for button count (attempt {attempt + 1})"
                )
                time.sleep(5)
                continue

            page = browser_info["page"]

            logger.info(
                f"Loading Walmart leasing page to count buttons (attempt {attempt + 1})"
            )
            page.goto(WALMART_LEASING_URL, wait_until="domcontentloaded")

            # Wait for page to load
            try:
                wait_for_element(page, "button.jss56", timeout=30)
            except PlaywrightTimeoutError:
                logger.warning("Timeout waiting for buttons to load")
                close_browser(browser_info)
                browser_info = None
                time.sleep(5)
                continue

            # Extra wait to ensure all buttons are loaded
            time.sleep(5)

            buttons = page.query_selector_all("button.jss56")
            count = len(buttons)

            if count > 0:
                logger.info(f"Found {count} total property buttons")
                close_browser(browser_info)
                return count

            logger.warning(f"No buttons found on attempt {attempt + 1}")
            close_browser(browser_info)
            browser_info = None
            time.sleep(5)

        except Exception as e:
            logger.error(f"Error counting buttons: {str(e)}")
            if browser_info:
                close_browser(browser_info)
                browser_info = None
            time.sleep(5)

    # If we get here, all attempts failed
    logger.error("Failed to count buttons after all attempts")
    return 0


def get_walmart_properties_with_small_spaces():
    """
    Main function to scrape Walmart leasing properties using true parallel processing.
    
    Returns:
        list: List of properties with small spaces (< 1000 sqft)
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

    # Process all tasks in parallel using ThreadPoolExecutor
    all_properties = []

    # Use concurrent.futures.ThreadPoolExecutor for parallel execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=WEB_WORKERS) as executor:
        # Submit tasks to the executor
        futures = []
        for worker_id, indices in enumerate(worker_tasks):
            futures.append(executor.submit(process_property_chunk, indices, worker_id))

        # Process results as they complete (not waiting for all to finish)
        for future in concurrent.futures.as_completed(futures):
            try:
                properties = future.result()
                all_properties.extend(properties)
                logger.info(
                    f"Received {len(properties)} properties from worker (total so far: {len(all_properties)})"
                )
            except Exception as e:
                logger.error(f"Worker process generated an exception: {str(e)}")

    # Deduplicate properties based on store_id
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
        f"Found {len(deduplicated_properties)} unique properties with spaces under {MAX_SPACE_SIZE} sqft"
    )

    return deduplicated_properties
