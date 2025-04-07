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

from config import (
    MOBILE_STORE_KEYWORDS,
    MIN_REVIEWS,
    GOOGLE_MAPS_URL,
    SEARCH_RADIUS_METERS,
    API_WORKERS,
)
from playwright_utils import setup_playwright_browser, close_browser, wait_for_element

logger = logging.getLogger(__name__)

# * ================================================
# * ========= EXTRACT CITY AND ZIP FROM ADDRESS ====
# * ================================================


def extract_city_zip_from_address(address):
    """Extract city and zip code from a formatted address string with better handling of different formats."""
    try:
        # Handle empty or None addresses
        if not address or address.strip() in ["Unknown", ""]:
            return {"city": "Unknown", "zip_code": "Unknown"}

        # Remove any non-printable characters like \ue0c8 (location pin icon)
        address = "".join(c for c in address if c.isprintable()).strip()

        # First try to extract US format ZIP code (5 digits, sometimes with 4 digit extension)
        zip_match = re.search(r"(\d{5}(?:-\d{4})?)", address)
        zip_code = zip_match.group(1) if zip_match else "Unknown"

        # Try to extract Puerto Rico specific ZIP codes (common in the results)
        if "Puerto Rico" in address and zip_code == "Unknown":
            zip_match = re.search(r"(\d{5})", address)
            zip_code = zip_match.group(1) if zip_match else "Unknown"

        # Try more patterns for city extraction
        city = "Unknown"

        # Pattern 1: Look for "City, STATE ZIP" format
        city_match = re.search(r"([A-Za-z\s\.]+),\s+[A-Z]{2}\s+\d{5}", address)
        if city_match:
            city = city_match.group(1).strip()

        # Pattern 2: Look for "City, STATE" format
        if city == "Unknown":
            city_match = re.search(r"([A-Za-z\s\.]+),\s+[A-Z]{2}", address)
            if city_match:
                city = city_match.group(1).strip()

        # Pattern 3: Look for ", City, " format (common in international addresses)
        if city == "Unknown":
            city_match = re.search(r",\s*([A-Za-z\s\.]+),", address)
            if city_match:
                city = city_match.group(1).strip()

        # Pattern 4: Look for Puerto Rico specific format "City, Puerto Rico"
        if city == "Unknown" and "Puerto Rico" in address:
            city_match = re.search(
                r"([A-Za-z\s\.]+),\s+(?:\d{5},\s+)?Puerto Rico", address
            )
            if city_match:
                city = city_match.group(1).strip()

        return {"city": city, "zip_code": zip_code}
    except Exception as e:
        logger.error(f"Error extracting city/zip: {str(e)}")
        return {"city": "Unknown", "zip_code": "Unknown"}


# * ================================================
# * ========= CHECK ADDRESS SIMILARITY =============
# * ================================================


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

    # Look for indicators that the store is inside Walmart - EXPANDED
    inside_indicators = [
        "inside walmart",
        "inside the walmart",
        "#the fix",
        "# the fix",
        "in walmart",
        "walmart supercenter",
        "in-store",
        "suite",
        "local",
        "ste",
        "at walmart",
        "walmart #",
        "walmart store",
        "walmart location",
        "inside the store",
        "in store",
        "located inside",
        "within walmart",
        "walmart center",
        "walmart plaza",
        "walmart retail",
        "walmart shopping",
    ]

    inside_walmart = any(
        indicator in addr1 or indicator in addr2 for indicator in inside_indicators
    )

    # Extract key address components for comparison
    # Extract ZIP codes (very reliable for matching)
    zip1 = re.search(r"\b(\d{5})\b", addr1)
    zip2 = re.search(r"\b(\d{5})\b", addr2)

    # Extract cities
    city1 = re.search(r"([a-z\s]+),\s+([a-z]{2}|\w+\s+rico)", addr1)
    city2 = re.search(r"([a-z\s]+),\s+([a-z]{2}|\w+\s+rico)", addr2)

    city1_val = city1.group(1).strip() if city1 else ""
    city2_val = city2.group(1).strip() if city2 else ""

    # Extract street numbers (most reliable for matching when present)
    street_nums1 = re.findall(r"\b(\d{1,5})\b", addr1)
    street_nums2 = re.findall(r"\b(\d{1,5})\b", addr2)

    # CRITICAL: If addresses share the same street number and one has inside indicators
    if street_nums1 and street_nums2:
        for num1 in street_nums1:
            if num1 in street_nums2 and inside_walmart:
                logger.warning(
                    f"Found matching street number with inside indicator: {addr1} vs {addr2}"
                )
                return True

    # If both addresses have ZIP codes and they match
    if zip1 and zip2 and zip1.group(1) == zip2.group(1):
        # If one address has inside indicators, it's likely the same location
        if inside_walmart:
            logger.warning(f"Same ZIP with inside indicator: {addr1} vs {addr2}")
            return True

        # Same ZIP code and city, almost certainly the same location
        if (
            city1_val
            and city2_val
            and (
                city1_val == city2_val
                or city1_val in city2_val
                or city2_val in city1_val
            )
        ):
            return True

    # Special handling for addresses with Walmart in them
    if "walmart" in addr1 or "walmart" in addr2:
        # If they share a street number, they're likely the same place
        if street_nums1 and street_nums2:
            for num1 in street_nums1:
                if num1 in street_nums2:
                    logger.warning(
                        f"Walmart address with matching street number: {addr1} vs {addr2}"
                    )
                    return True

        # If they share a ZIP code and one has suite/local, likely same place
        if zip1 and zip2 and zip1.group(1) == zip2.group(1):
            if (
                "suite" in addr1
                or "suite" in addr2
                or "local" in addr1
                or "local" in addr2
            ):
                logger.warning(
                    f"Walmart address with matching ZIP and suite/local: {addr1} vs {addr2}"
                )
                return True

    # Handle cases where street numbers match but addresses are formatted differently
    if street_nums1 and street_nums2:
        matching_numbers = set(street_nums1) & set(street_nums2)
        if matching_numbers and (
            inside_walmart
            or ("walmart" in addr1 and "walmart" in addr2)
            or (zip1 and zip2 and zip1.group(1) == zip2.group(1))
        ):
            logger.warning(
                f"Found matching street numbers in differently formatted addresses: {addr1} vs {addr2}"
            )
            return True

    return False


# * ================================================
# * ========= EXTRACT REVIEW COUNT FROM PAGE ======
# * ================================================


def extract_review_count_from_page(page, store_panel_selector=None):
    """Extract review count from Google Maps page using multiple methods."""
    review_count = 0

    try:
        # If no store panel selector provided, use default
        if not store_panel_selector:
            store_panel_selector = 'div[role="main"], div.section-hero-header, .xtuJJ'

        # REMOVED: Debug screenshot code - no longer taking screenshots

        # Method 1: Direct text extraction from F7nice div (most reliable)
        f7nice_element = page.query_selector(".F7nice")
        if f7nice_element:
            full_text = f7nice_element.inner_text()
            logger.debug(f"F7nice text content: {full_text}")

            # Try to extract review count from parenthesized numbers
            reviews_match = re.search(r"\(([0-9.,]+)\)", full_text)
            if reviews_match:
                review_str = reviews_match.group(1).replace(".", "").replace(",", "")
                try:
                    count = int(review_str)
                    if count > review_count:
                        review_count = count
                        logger.info(
                            f"Found review count from F7nice div: {review_count}"
                        )
                except ValueError:
                    pass

        # Method 2: Look specifically for spans with aria-label containing "reseñas" or "reviews"
        aria_elements = page.query_selector_all(
            'span[aria-label*="reseñas"], span[aria-label*="reviews"], span[aria-label*="reseña"]'
        )

        for elem in aria_elements:
            aria_text = elem.get_attribute("aria-label") or elem.inner_text()
            logger.info(f"Found review element with aria-label: {aria_text}")

            # Extract numeric value from aria-label text
            # Handle both formats: "11.958 reseñas" (Spanish) or "11,958 reviews" (English)
            review_match = re.search(
                r"([\d.,]+)\s*(?:reseñas|reviews|review|reseñas|reseña)",
                aria_text,
                re.IGNORECASE,
            )
            if review_match:
                review_str = review_match.group(1)

                # Normalize number format - both periods and commas could be thousand separators
                # depending on locale
                if "." in review_str and "," not in review_str:
                    if (
                        len(review_str.split(".")[-1]) == 3
                    ):  # If last part has 3 digits, it's a thousand separator
                        review_str = review_str.replace(".", "")
                elif "," in review_str and "." not in review_str:
                    if (
                        len(review_str.split(",")[-1]) == 3
                    ):  # If last part has 3 digits, it's a thousand separator
                        review_str = review_str.replace(",", "")
                else:
                    # Handle more complex cases
                    review_str = review_str.replace(",", "").replace(".", "")

                try:
                    count = int(review_str)
                    if count > review_count:
                        review_count = count
                        logger.info(
                            f"Found review count via aria-label: {review_count}"
                        )
                except ValueError:
                    pass

        # Method 3: Try to find any span with parenthesized numbers
        if review_count == 0:
            all_spans = page.query_selector_all("span")
            for span in all_spans:
                span_text = span.inner_text().strip()
                # Look for text like "(11.958)" or "(11,958)" - common format in Google Maps
                paren_match = re.match(r"^\(([0-9.,]+)\)$", span_text)
                if paren_match:
                    review_str = paren_match.group(1).replace(".", "").replace(",", "")
                    try:
                        count = int(review_str)
                        if count > review_count:
                            review_count = count
                            logger.info(
                                f"Found review count from parentheses: {review_count}"
                            )
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
                        logger.info(
                            f"Found review count via JavaScript: {review_count}"
                        )
                except ValueError:
                    pass
            except Exception as js_error:
                logger.debug(f"JavaScript review extraction failed: {str(js_error)}")

    except Exception as e:
        logger.error(f"Error extracting review count: {str(e)}")

    return review_count


# * ================================================
# * ========= CHECK GOOGLE REVIEWS AND STORES ======
# * ================================================


def check_google_reviews_and_stores(property_info, worker_id=0):
    """
    Check Google Maps for review counts and nearby mobile stores using Playwright.
    Each call has a worker_id to ensure truly independent operation.
    """
    # Use the worker_id to ensure this instance is completely independent
    # Keep original address and store ID
    original_address = property_info["address"]
    store_id = property_info["store_id"]
    store_number = f"Store #{store_id}"

    # Format search query with Walmart prefix exactly as recommended
    search_query = f"Walmart {store_number} {original_address}"
    logger.info(f"Worker {worker_id}: Searching for: {search_query}")

    browser_info = None
    # Add retries for Google Maps access
    max_retries = 3
    for attempt in range(max_retries):
        # Create a completely independent browser instance for this worker
        browser_info = setup_playwright_browser(headless=True, worker_id=worker_id)

        if not browser_info:
            property_info["meets_criteria"] = False
            property_info["fail_reason"] = "Failed to create browser instance"
            return property_info

        page = browser_info["page"]

        try:
            # Add random delay to avoid throttling
            time.sleep(random.uniform(1, 3))

            # On retry attempts, try a more general query
            if attempt > 0:
                fallback_query = f"Walmart {original_address}"
                encoded_query = urllib.parse.quote(fallback_query)
                logger.info(
                    f"Retry {attempt}/{max_retries} with fallback query: {fallback_query}"
                )
            else:
                encoded_query = urllib.parse.quote(search_query)

            # Access Google Maps with the formatted query
            try:
                page.goto(
                    f"{GOOGLE_MAPS_URL}{encoded_query}", wait_until="domcontentloaded"
                )
            except Exception as e:
                logger.warning(f"Error when accessing Google Maps: {str(e)}")
                close_browser(browser_info)
                browser_info = None
                continue

            # Wait for results to load
            try:
                page.wait_for_selector(
                    'div[role="feed"], div.section-result-content, div[role="main"], .F7nice, .DkEaL',
                    timeout=20000,
                )
            except PlaywrightTimeoutError:
                logger.warning(
                    f"Timeout waiting for Google Maps results for {search_query}"
                )

            # Additional wait to make sure page elements are fully loaded
            time.sleep(5)

            try:
                # Try to get the store panel
                store_panel_selector = (
                    'div[role="main"], div.section-hero-header, .xtuJJ'
                )
                store_panel = page.query_selector(store_panel_selector)

                if not store_panel:
                    raise Exception("Store panel not found")

                # Extract the address
                try:
                    formatted_address_elem = page.query_selector(
                        'button[data-item-id="address"], span.section-info-text'
                    )
                    formatted_address = (
                        formatted_address_elem.inner_text().strip()
                        if formatted_address_elem
                        else ""
                    )
                    property_info["full_address"] = formatted_address
                    property_info["google_address"] = formatted_address

                    # Extract city and zip
                    location_details = extract_city_zip_from_address(formatted_address)
                    property_info["city"] = location_details["city"]
                    property_info["zip_code"] = location_details["zip_code"]

                    # Check for address mismatch
                    if not address_similarity_check(
                        original_address, formatted_address
                    ):
                        property_info["address_mismatch_warning"] = True
                        logger.warning(
                            f"Address mismatch: '{original_address}' vs '{formatted_address}'"
                        )
                except Exception:
                    property_info["full_address"] = property_info["address"]
                    property_info["city"] = "Unknown"
                    property_info["zip_code"] = "Unknown"

                # Extract review count using the specialized function
                review_count = extract_review_count_from_page(page)
                property_info["review_count"] = review_count

                logger.info(f"Found {review_count} reviews for {store_number}")

                # Extract phone number
                try:
                    phone_elem = page.query_selector(
                        'button[data-item-id="phone:tel"], span.phone-number'
                    )
                    if phone_elem:
                        property_info["phone_number"] = phone_elem.inner_text().strip()
                except Exception:
                    pass

                # Extract website if available
                try:
                    website_elem = page.query_selector(
                        'a[data-item-id="authority"], button[data-item-id*="website"], a[href*="walmart.com"]'
                    )
                    if website_elem:
                        website = (
                            website_elem.get_attribute("href")
                            or website_elem.inner_text()
                        )
                        property_info["website"] = website

                        # Extract store ID from website URL
                        if "walmart.com/store/" in website:
                            store_url_match = re.search(
                                r"walmart\.com/store/(\d+)", website
                            )
                            if store_url_match:
                                website_store_id = store_url_match.group(1)
                                property_info["website_store_id"] = website_store_id
                                property_info["leasing_id"] = store_id

                                # Flag if IDs don't match
                                if website_store_id != store_id:
                                    property_info["id_mismatch"] = True
                except Exception:
                    pass

                # Skip further checking if it doesn't meet review threshold
                if property_info.get("review_count", 0) < MIN_REVIEWS:
                    property_info["meets_criteria"] = False
                    property_info["fail_reason"] = (
                        f"Only {property_info.get('review_count', 0)} reviews (minimum {MIN_REVIEWS})"
                    )
                    close_browser(browser_info)
                    return property_info

                # Now check for nearby mobile stores
                mobile_store_result = check_nearby_mobile_stores(
                    browser_info, property_info
                )

                # FIXED: Explicitly log the has_mobile value to debug
                logger.info(
                    f"Mobile store check result for {store_number}: has_mobile = {mobile_store_result['has_mobile']}"
                )

                if mobile_store_result["has_mobile"]:
                    property_info["meets_criteria"] = False
                    property_info["fail_reason"] = "Has a mobile phone store"
                    if (
                        "stores" in mobile_store_result
                        and mobile_store_result["stores"]
                    ):
                        store_names = [
                            store["name"] for store in mobile_store_result["stores"]
                        ]
                        property_info["mobile_store_details"] = (
                            f"Found: {', '.join(store_names)}"
                        )
                        logger.warning(
                            f"DISQUALIFIED: {store_number} has mobile stores: {', '.join(store_names)}"
                        )
                else:
                    property_info["meets_criteria"] = True
                    property_info["fail_reason"] = None
                    logger.info(
                        f"QUALIFIED: {store_number} has NO mobile stores within search radius"
                    )

            except Exception as e:
                logger.warning(
                    f"Could not find store panel for {search_query}: {str(e)}"
                )
                property_info["meets_criteria"] = False
                property_info["fail_reason"] = "Could not find store in Google Maps"

        except Exception as e:
            logger.error(
                f"Error checking Google data for {property_info['store_name']}: {str(e)}"
            )
            if attempt < max_retries - 1:
                # Try again with next retry
                logger.info(
                    f"Will retry Google Maps search for {property_info['store_name']}"
                )
                close_browser(browser_info)
                browser_info = None
                continue
            else:
                property_info["error"] = str(e)
                property_info["meets_criteria"] = False
                property_info["fail_reason"] = f"Error checking Google data: {str(e)}"

        finally:
            # Clean up
            if browser_info:
                close_browser(browser_info)
                browser_info = None

        # If we got here without triggering a continue, break out of retry loop
        break

    return property_info


# * ================================================
# * ========= CHECK NEARBY MOBILE STORES ===========
# * ================================================


def check_nearby_mobile_stores(browser_info, property_info):
    """
    Check for nearby mobile phone repair stores by directly searching on Google Maps.
    Enhanced to better detect stores within Walmart address.
    """
    result = {"has_mobile": False, "stores": []}

    found_stores = []  # Initialize found_stores at the top level
    browser = browser_info["browser"]
    context = browser_info["context"]
    page = browser_info["page"]
    walmart_url = page.url
    walmart_address = property_info.get("full_address") or property_info.get("address")

    # Extract store ID for specific searches
    store_id = property_info.get("store_id", "")
    store_number = property_info.get("store_number", "")

    # Get city and zip from property info
    store_city = property_info.get("city", "")
    store_zip = property_info.get("zip_code", "")

    # Define search strategy - we'll do multiple searches with different approaches
    search_strategies = [
        # Strategy 1: Direct search for specific brands at this Walmart (exact address)
        [
            f"iFixandRepair Walmart {store_id}",
            f"The Fix inside Walmart {store_id}",
            f"Boost Mobile inside Walmart {walmart_address}",
            f"Cricket Wireless inside Walmart {store_id}",
            f"T-Mobile inside Walmart {store_id}",
            f"Simple Mobile inside Walmart {store_id}",
            f"Tech repair inside Walmart {store_id}",
            f"Phone repair inside Walmart {walmart_address}",
        ],
        # Strategy 2: General nearby phone service searches
        [
            f"cell phone repair near {walmart_address}",
            f"mobile repair near {walmart_address}",
            f"phone store near {walmart_address}",
            f"wireless store near {walmart_address}",
        ],
        # Strategy 3: Specific searches with city and zip
        [
            f"phone repair {store_city} {store_zip}",
            f"mobile store {store_city} {store_zip}",
            f"iFixandRepair {store_city}",
            f"The Fix {store_city}",
        ],
        # Strategy 4: Extremely specific searches
        [
            f"cell phone repair {store_id}",
            f"mobile phone inside Walmart {store_id}",
            f"tech repair {walmart_address}",
            f"phone service {walmart_address}",
        ],
    ]

    try:
        # CRITICAL ENHANCEMENT: First do a direct high-priority search for iFixandRepair
        logger.info(
            f"Performing highly-targeted search for iFixandRepair at Walmart #{store_id}"
        )

        # FIXED: Handle the import more robustly
        ifixit_results = []
        try:
            # Try to import from external module
            from check_nearby_mobile_stores import check_for_ifixit_in_walmart

            ifixit_results = check_for_ifixit_in_walmart(
                page, walmart_address, store_id, process_result_elements
            )
        except ImportError:
            # Fall back to our local version
            logger.info("Using local implementation for iFix detection")
            # Skip this check since we can't find the function
            pass

        if ifixit_results:
            logger.warning(
                f"HIGH PRIORITY: Found {len(ifixit_results)} potential mobile stores inside Walmart #{store_id}"
            )
            found_stores.extend(ifixit_results)

        # Function to safely execute searches
        def safe_search_execution(search_url, description):
            """Execute a search with session recovery if needed."""
            nonlocal page, context, browser

            for retry in range(3):
                try:
                    # Add error handling to the initial navigation
                    try:
                        page.goto(search_url, wait_until="domcontentloaded")
                    except Exception as e:
                        logger.warning(
                            f"Error navigating to URL during {description}, retry {retry+1}: {str(e)}"
                        )
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
                        if not page.url.startswith("http"):
                            raise Exception("Invalid page state")
                        page.title()
                    except Exception as e:
                        if retry < 2:
                            logger.warning(
                                f"Page became invalid during {description}, recreating... Error: {str(e)}"
                            )
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
                            logger.error(
                                f"Page still invalid after {retry+1} retries, aborting {description}"
                            )
                            return None
                    # Look for results with multiple selector attempts
                    for selector in [
                        'div[role="article"], div.section-result, .Nv2PK',
                        ".Nv2PK",
                        "div.section-result",
                        ".fontHeadlineSmall",
                        'div[role="feed"] > div',
                    ]:
                        try:
                            result_elements = page.query_selector_all(selector)
                            if result_elements and len(result_elements) > 0:
                                logger.info(
                                    f"Found {len(result_elements)} results for {description}"
                                )
                                return result_elements
                        except:
                            continue
                    # If we get here but found no elements, return empty list instead of None
                    logger.info(f"Found 0 results for {description}")
                    return []
                except Exception as e:
                    if "Failed to establish a new connection" in str(
                        e
                    ) or "WebDriver exception" in str(e):
                        # Connection related errors deserve a fresh page
                        if retry < 2:
                            logger.warning(
                                f"Connection error during {description}, retrying with new page: {str(e)}"
                            )
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
                            logger.warning(
                                f"Error with {description} after final retry: {str(e)}"
                            )
                            return []
                    elif retry < 2:  # For non-connection errors, retry with same page
                        logger.warning(
                            f"Error during {description}, retrying: {str(e)}"
                        )
                        time.sleep(5)
                    else:
                        logger.warning(
                            f"Error with {description} after all retries: {str(e)}"
                        )
                        return []
            return []  # Return empty list if we exhaust all retries

        # Execute all search strategies
        for strategy_idx, search_terms in enumerate(search_strategies):
            logger.info(
                f"Executing search strategy {strategy_idx+1}/{len(search_strategies)}..."
            )

            for term in search_terms:
                # Construct search query
                encoded_query = urllib.parse.quote(term)
                search_url = f"{GOOGLE_MAPS_URL}{encoded_query}"
                logger.info(f"Searching for: {term}")

                # Use safe search execution
                result_elements = safe_search_execution(search_url, f"search '{term}'")
                if result_elements:
                    # Process results using the helper function
                    # For first strategy (brand-specific searches), use extra sensitivity
                    extra_sensitive = strategy_idx == 0
                    more_stores = process_result_elements(
                        page,
                        result_elements,
                        [],
                        walmart_address,
                        extra_sensitive=extra_sensitive,
                        store_id=store_id,
                    )
                    # Add to overall found stores, avoiding duplicates
                    for store in more_stores:
                        if not any(
                            s.get("name") == store.get("name") for s in found_stores
                        ):
                            found_stores.append(store)
                # Wait a bit between searches to avoid rate limits
                time.sleep(2)
            # If we've already found mobile stores, we can stop searching
            if found_stores:
                logger.warning(
                    f"Found {len(found_stores)} mobile stores after strategy {strategy_idx+1}, stopping additional searches"
                )
                break
        # Update result with all found stores
        if found_stores:
            # ENHANCED: Validate if these are actual mobile stores and not false positives
            validated_stores = []
            for store in found_stores:
                store_name = store.get("name", "").lower()
                store_address = store.get("address", "").lower()

                # CRITICAL: Skip any results that are Walmart's own services
                walmart_services = [
                    "walmart tech services",
                    "walmart service",
                    "walmart electronics",
                    "walmart connection center",
                    "walmart wireless",
                    "walmart center",
                    "supercenter",
                    "walmart dept",
                    "walmart department",
                ]

                if any(service in store_name for service in walmart_services):
                    logger.info(f"Filtering out Walmart's own service: {store_name}")
                    continue

                # Skip generic Walmart results (these are not separate mobile stores)
                if "walmart" in store_name and not any(
                    brand in store_name
                    for brand in [
                        "boost",
                        "cricket",
                        "the fix",
                        "ifix",
                        "simple mobile",
                        "t-mobile",
                    ]
                ):
                    logger.info(f"Filtering out generic Walmart result: {store_name}")
                    continue

                # Check for connection center or electronics section - these are part of Walmart
                if "connection" in store_name and "center" in store_name:
                    logger.info(f"Filtering out Connection Center: {store_name}")
                    continue

                # Only include actual third-party mobile stores
                if store.get("is_known_brand") or mobile_terms_present(
                    store_name, store_address
                ):
                    validated_stores.append(store)
                else:
                    logger.info(f"Filtering out non-mobile store: {store_name}")

            # Only mark as having mobile stores if we have valid matches
            if validated_stores:
                result["has_mobile"] = True
                result["stores"] = validated_stores
                logger.warning(
                    f"TOTAL: Found {len(validated_stores)} mobile stores nearby or at the same address: {[s['name'] for s in validated_stores]}"
                )
            else:
                logger.info(
                    f"All potential matches were filtered as false positives for Walmart #{store_id}"
                )
        else:
            logger.info(f"No mobile stores found for Walmart #{store_id}")

        # Add info about the search method to the property info
        property_info["mobile_store_search_method"] = (
            "Enhanced Google Maps Search with Multiple Strategies"
        )
        property_info["mobile_store_search_radius"] = f"{SEARCH_RADIUS_METERS} meters"
        property_info["mobile_store_keywords_checked"] = MOBILE_STORE_KEYWORDS
        property_info["has_mobile_store"] = result["has_mobile"]

        # Store any found matches
        if result["stores"]:
            property_info["mobile_stores_found"] = result["stores"]

    except Exception as e:
        logger.error(f"Error checking for mobile stores: {str(e)}")
        result["has_mobile"] = True  # Assume yes for safety
        result["error"] = str(e)
        property_info["has_mobile_store"] = True
        property_info["mobile_store_error"] = str(e)

    # Try to return to the original URL
    try:
        page.goto(walmart_url, wait_until="domcontentloaded")
    except Exception:
        pass

    return result


def mobile_terms_present(name, address):
    """Helper function to check if a store name or address contains mobile-related terms."""
    mobile_terms = [
        "phone repair",
        "cell phone",
        "iphone repair",
        "smartphone",
        "mobile repair",
        "screen repair",
        "battery replace",
        "device repair",
        "fix phone",
        "unlock",
        "ifixandrepair",
        "ubreakifix",
        "the fix",
        "device doctor",
        "cellairis",
        "cellaris",
        "techy",
    ]

    # Look for strong indicators of a mobile store
    for term in mobile_terms:
        if term in name or term in address:
            return True

    # Check for combination of terms that together indicate a mobile store
    combined_terms = [
        ("phone", "repair"),
        ("mobile", "repair"),
        ("cell", "repair"),
        ("device", "fix"),
        ("tech", "repair"),
    ]

    for term1, term2 in combined_terms:
        if (term1 in name or term1 in address) and (term2 in name or term2 in address):
            return True

    return False


def process_result_elements(
    page,
    result_elements,
    found_stores=None,
    walmart_address=None,
    extra_sensitive=False,
    store_id=None,
):
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
        city_match = re.search(
            r"([A-Za-z\s]+),\s+(?:\d{5},\s+)?(?:[A-Z]{2}|Puerto Rico)", walmart_address
        )
        if city_match:
            walmart_addr_components["city"] = city_match.group(1).strip().lower()

        # Extract ZIP code
        zip_match = re.search(r"\b(\d{5})\b", walmart_address)
        if zip_match:
            walmart_addr_components["zip"] = zip_match.group(1)

        # Extract street number
        street_num_match = re.search(r"\b(\d+)\b", clean_walmart_address)
        if street_num_match:
            walmart_addr_components["street_num"] = street_num_match.group(1)

        # Extract street name - critical for matching
        street_match = re.search(
            r"\b(\d+)\s+([A-Za-z\s]+?)(?:,|\s+[A-Z]{2}|\d{5})", clean_walmart_address
        )
        if street_match:
            walmart_addr_components["street_name"] = (
                street_match.group(2).strip().lower()
            )

        # Check if address contains suite/local info
        suite_match = re.search(
            r"(?:suite|local|#)\s*([a-z0-9-]+)", clean_walmart_address, re.IGNORECASE
        )
        if suite_match:
            walmart_addr_components["suite"] = suite_match.group(1)

        # Extract state/territory
        if "puerto rico" in clean_walmart_address:
            walmart_addr_components["state"] = "puerto rico"

    # Define specific high-risk brands that are commonly found inside Walmart - EXPANDED
    HIGH_CONFIDENCE_IN_WALMART_BRANDS = [
        "the fix",
        "thefix",
        "the-fix",
        "the fix by asurion",
        "thefix by asurion",
        "ifix",
        "i-fix",
        "ifixandrepair",
        "i fix and repair",
        "ifix & repair",
        "cellaris",
        "cellairis",
        "cell airis",
        "cell-airis",
        "talk n fix",
        "talknfix",
        "talk-n-fix",
        "talk and fix",
        "techy",
        "tech-y",
        "tech y",
        "techhub",
        "tech hub",
        "tech desk",
        "mobile solution",
        "mobile solutions",
        "experimax",
        "experihub",
        "gadget repair",
        "gadgets repair",
        "gadget x",
        "gadgetx",
        "wireless clinic",
        "wireless repair clinic",
        "phone clinic",
        "phone surgeon",
        "phonesurgeon",
        "phone-surgeon",
        "we fix phones",
        "wefixphones",
        "phone medic",
        "phonemedic",
        "phone-medic",
    ]

    # Official Walmart services that are NOT third-party mobile stores
    WALMART_OFFICIAL_SERVICES = [
        "walmart tech services",
        "walmart services",
        "walmart auto care",
        "walmart vision center",
        "walmart pharmacy",
        "walmart money center",
        "walmart tire center",
        "walmart photo center",
        "walmart bakery",
        "walmart grocery",
        "walmart deli",
        "walmart jewelry center",
        "walmart photo center",
        "walmart jewelry center",
    ]

    # Get all results (increased from 20 to 30 results)
    for idx, elem in enumerate(result_elements[:30]):
        try:
            # Try multiple selectors for store name
            name_selectors = [
                "h3",
                "h1",
                "h2",
                "h3",
                '[role="heading"]',
                "span.section-result-title",
            ]
            store_name = None
            for selector in name_selectors:
                name_elem = elem.query_selector(selector)
                if name_elem:
                    store_name = name_elem.inner_text().strip()
                    break

            if not store_name:
                continue

            store_name_lower = store_name.lower()
            store_name_normalized = (
                store_name_lower.replace(" ", "").replace("-", "").replace("&", "and")
            )

            # Check for distance info with multiple selectors
            distance_text = "Unknown"
            distance_selectors = [
                'span[aria-label*="miles"]',
                "span.fontBodyMedium > span:nth-child(2)",
                'span[aria-label*="mi"]',
                ".UY7F9",
                'div[aria-label*="miles away"]',
                'div[aria-label*="mi"]',
            ]
            for selector in distance_selectors:
                distance_elem = elem.query_selector(selector)
                if distance_elem:
                    distance_text = distance_elem.inner_text().strip()
                    break

            # Extract numerical distance value (e.g., "2.1 mi" -> 2.1)
            distance_value = None
            distance_match = re.search(r"([\d\.]+)\s*mi", distance_text)
            if distance_match:
                try:
                    distance_value = float(distance_match.group(1))
                except ValueError:
                    pass

            # Extract store address
            store_address = None
            address_selectors = [
                '.fontBodySmall[jsan*="address"]',
                '.fontBodyMedium > div[jsan*="address"]',
                'div[class*="address"]',
                "div.W4Efsd > div.fontBodyMedium:nth-child(1)",
                'div[aria-label*="address"]',
            ]
            for selector in address_selectors:
                address_elem = elem.query_selector(selector)
                if address_elem:
                    store_address = address_elem.inner_text().strip()
                    break

            store_addr_lower = store_address.lower() if store_address else ""

            # Ensure we have a meaningful address
            if not store_address or len(store_address) < 5:
                continue

            # Check for official Walmart services that are not third-party mobile stores
            if any(
                service in store_name_lower for service in WALMART_OFFICIAL_SERVICES
            ):
                # Only include if it ALSO contains specific repair keywords
                specific_repair_terms = ["repair", "fix", "phone repair", "cell repair"]
                if not any(
                    term in store_name_lower or term in store_addr_lower
                    for term in specific_repair_terms
                ):
                    logger.info(
                        f"Ignoring official Walmart service: '{store_name}' (not a third-party mobile store)"
                    )
                    continue

            # Check specifically for mobile-related terms in the name
            mobile_terms = [
                "phone",
                "mobile",
                "cell",
                "tech",
                "device",
                "gadget",
                "smart",
                "screen",
                "battery",
                "repair",
                "fix",
                "clinic",
                "medic",
                "accessories",
                "electronic",
                "electronics",
                "computer",
                "tablet",
            ]

            matched_terms = [
                term
                for term in mobile_terms
                if term in store_name_lower or term in store_addr_lower
            ]
            weak_matched_terms = [
                term
                for term in ["service", "shop", "store", "center", "sales"]
                if term in store_name_lower or term in store_addr_lower
            ]

            # Count matched terms with higher weight for strong terms
            mobile_word_count = len(matched_terms)
            if len(weak_matched_terms) > 0 and mobile_word_count == 0:
                # If we only have weak terms, count them as a fraction
                mobile_word_count = 0.5

            # Check for known brands with high confidence
            has_known_brand = any(
                brand in store_name_normalized
                for brand in [
                    b.replace(" ", "").lower()
                    for b in HIGH_CONFIDENCE_IN_WALMART_BRANDS
                ]
            )

            # IMPROVED: Check for "inside Walmart" type phrases
            explicitly_inside_walmart = False
            # Check store name and address for inside indicators
            INSIDE_WALMART_INDICATORS = [
                "inside walmart",
                "in walmart",
                "walmart #",
                "walmart store",
                "walmart supercenter",
                "inside the walmart",
                "inside the store",
                "the fix at walmart",
                "the fix walmart",
                "fix walmart",
                "techy walmart",
                "inside",
                "at walmart",
                "walmart location",
                "ste",
                "suite",
                "located inside",
                "located in",
                "located at",
                "within walmart",
                "walmart centerpoint",
                "walmart center",
                "in-store",
                "in store",
                "inside supercenter",
                "walmart express",
                "smart style",
                "smartstyle",
                "store within store",
                "kiosk",
                "booth",
                "counter",
                "local",
                "in-walmart",
                "@ walmart",
                "walmart mall",
                "leased space",
            ]
            for indicator in INSIDE_WALMART_INDICATORS:
                if indicator in store_name_lower or indicator in store_addr_lower:
                    explicitly_inside_walmart = True
                    logger.info(
                        f"Inside Walmart indicator found: '{indicator}' in store name/address"
                    )
                    break

            # IMPROVED: Extract address components from store address
            store_addr_components = {}

            # Extract street number and name - critical for exact matching
            street_match = re.search(
                r"\b(\d+)\s+([A-Za-z\s]+?)(?:,|\s+[A-Z]{2}|\d{5})", store_addr_lower
            )
            if street_match:
                store_addr_components["street_num"] = street_match.group(1)
                store_addr_components["street_name"] = street_match.group(2).strip()

            # Extract ZIP code
            zip_match = re.search(r"\b(\d{5})\b", store_addr_lower)
            if zip_match:
                store_addr_components["zip"] = zip_match.group(1)

            # Extract city
            city_match = re.search(
                r"([A-Za-z\s]+),\s+(?:[A-Z]{2}|\d{5})", store_addr_lower
            )
            if city_match:
                store_addr_components["city"] = city_match.group(1).strip()

            # CRITICAL IMPROVEMENT: Direct street address matching
            # If the store has the EXACT same street number and ZIP code as the Walmart, it's inside
            is_same_building = False

            if (
                "street_num" in walmart_addr_components
                and "street_num" in store_addr_components
                and walmart_addr_components["street_num"]
                == store_addr_components["street_num"]
            ):
                # If street numbers match, check ZIP or street name too
                if (
                    "zip" in walmart_addr_components
                    and "zip" in store_addr_components
                    and walmart_addr_components["zip"] == store_addr_components["zip"]
                ) or (
                    "street_name" in walmart_addr_components
                    and "street_name" in store_addr_components
                    and (
                        walmart_addr_components["street_name"]
                        in store_addr_components["street_name"]
                        or store_addr_components["street_name"]
                        in walmart_addr_components["street_name"]
                    )
                ):
                    is_same_building = True
                    logger.warning(
                        f"CRITICAL MATCH: Found store with EXACT SAME STREET ADDRESS: {store_name} at {store_address}"
                    )

            # Now make detection decisions
            # Case 1: It's at the same physical address as Walmart AND has mobile keywords or is a known brand
            if is_same_building and (mobile_word_count >= 1 or has_known_brand):
                # Extra check: Make sure it's not just "Walmart Tech Services" without repair focus
                if store_name_lower == "walmart tech services" and not any(
                    repair_term in store_name_lower or repair_term in store_addr_lower
                    for repair_term in ["repair", "fix", "phone repair"]
                ):
                    logger.info(
                        f"Ignoring generic 'Walmart Tech Services' as it's not specifically a repair shop: {store_address}"
                    )
                    continue

                store_entry = {
                    "name": store_name,
                    "address": store_address,
                    "distance": "SAME BUILDING - Inside Walmart",
                    "keywords_matched": matched_terms + weak_matched_terms,
                    "location_match": "exact_address",
                    "location_confidence": "very_high",
                    "is_same_address": True,
                    "is_known_brand": has_known_brand,
                }
                found_stores.append(store_entry)
                logger.warning(
                    f"FOUND MOBILE STORE AT EXACT SAME ADDRESS: '{store_name}' at '{store_address}'"
                )

            # Case 2: It explicitly says it's inside Walmart AND has mobile keywords
            elif explicitly_inside_walmart and mobile_word_count >= 1:
                store_entry = {
                    "name": store_name,
                    "address": store_address,
                    "distance": "Inside Walmart (explicit)",
                    "keywords_matched": matched_terms,
                    "location_match": "explicit_mention",
                    "location_confidence": "very_high",
                    "is_known_brand": has_known_brand,
                }
                found_stores.append(store_entry)
                logger.warning(
                    f"FOUND MOBILE STORE EXPLICITLY INSIDE WALMART: '{store_name}'"
                )

            # Case 3: It's a known brand of mobile store (anywhere within search radius)
            elif has_known_brand:
                store_entry = {
                    "name": store_name,
                    "address": store_address,
                    "distance": (
                        distance_text if distance_text != "Unknown" else "Nearby"
                    ),
                    "keywords_matched": matched_terms,
                    "location_match": "known_brand",
                    "location_confidence": "high",
                    "is_known_brand": has_known_brand,
                }
                found_stores.append(store_entry)
                logger.warning(
                    f"FOUND KNOWN MOBILE BRAND NEARBY: '{store_name}' at distance: {distance_text}"
                )

            # Case 4: It has multiple mobile keywords and is relatively close
            elif mobile_word_count >= 2 and (
                distance_value is not None and distance_value <= 0.2
            ):
                store_entry = {
                    "name": store_name,
                    "address": store_address,
                    "distance": (
                        distance_text if distance_text != "Unknown" else "Very Close"
                    ),
                    "keywords_matched": matched_terms,
                    "location_match": "multiple_keywords_close",
                    "location_confidence": "medium",
                    "distance_value": distance_value,
                }
                found_stores.append(store_entry)
                logger.warning(
                    f"FOUND LIKELY MOBILE STORE VERY CLOSE: '{store_name}' with keywords: {matched_terms}"
                )

            # For extra sensitive searches (direct searches), be more aggressive
            elif extra_sensitive and mobile_word_count >= 1:
                store_entry = {
                    "name": store_name,
                    "address": store_address,
                    "distance": (
                        distance_text
                        if distance_text != "Unknown"
                        else "Within search radius"
                    ),
                    "keywords_matched": matched_terms,
                    "location_match": "sensitive_search",
                    "location_confidence": "low",
                    "distance_value": distance_value,
                }
                found_stores.append(store_entry)
                logger.warning(
                    f"[Sensitive search] Found potential mobile store: '{store_name}' with keyword: {matched_terms}"
                )

        except Exception as e:
            logger.error(f"Error processing result element: {str(e)}")

    return found_stores


# * ================================================
# * ========= RUN PARALLEL LOCATION CHECKS =========
# * ================================================


def check_locations_in_parallel(small_space_properties):
    """Check Google Maps data for properties in parallel with true independence."""
    logger.info(
        f"Checking Google Maps data for {len(small_space_properties)} properties in parallel"
    )

    # Determine effective number of workers based on workload size
    effective_workers = min(API_WORKERS, max(1, len(small_space_properties) // 10 + 1))

    if effective_workers < API_WORKERS:
        logger.info(
            f"Reducing number of workers to {effective_workers} due to workload size"
        )

    checked_properties = []

    # Create a function that each worker will run
    def process_property_independently(property_info, worker_id):
        try:
            logger.info(
                f"Worker {worker_id}: Processing {property_info.get('store_number', 'Unknown')}"
            )
            result = check_google_reviews_and_stores(property_info, worker_id)
            return result
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {str(e)}")
            property_info["meets_criteria"] = False
            property_info["fail_reason"] = f"Processing error: {str(e)}"
            return property_info

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=effective_workers
    ) as executor:
        # Create a more efficient distribution - one property at a time to each worker
        futures = []
        for idx, prop in enumerate(small_space_properties):
            worker_id = (
                idx % effective_workers
            )  # Distribute properties evenly across workers
            futures.append(
                executor.submit(process_property_independently, prop, worker_id)
            )

        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    checked_properties.append(result)
                    status = "MATCH" if result.get("meets_criteria") else "NO MATCH"
                    if result.get("meets_criteria"):
                        match_count += 1
                        logger.info(
                            f"Property {result.get('store_number', 'Unknown')}: {status} - FOUND MATCH! ({match_count} matches so far)"
                        )
                    else:
                        reason = result.get("fail_reason", "Unknown")
                        logger.info(
                            f"Property {result.get('store_number', 'Unknown')}: {status} - Reason: {reason}"
                        )
            except Exception as e:
                logger.error(f"Error processing property result: {str(e)}")

    return checked_properties
