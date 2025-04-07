"""
Debug script to check a specific Walmart store for mobile stores
"""

import sys
import logging
import time
import urllib.parse  # Added import at the top level
from config import GOOGLE_MAPS_URL
from playwright_utils import setup_playwright_browser, close_browser
from location_checker import process_result_elements, extract_city_zip_from_address

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def debug_store(store_id, address=None):
    """Debug a specific Walmart store to see if mobile stores are detected."""
    logger.info(f"Debugging Walmart Store #{store_id}")

    browser_info = setup_playwright_browser(headless=True)
    if not browser_info:
        logger.error("Failed to create browser")
        return

    page = browser_info["page"]

    try:
        # Search for the Walmart
        search_query = f"Walmart Store #{store_id}"
        if address:
            search_query += f" {address}"

        logger.info(f"Searching for: {search_query}")
        encoded_query = urllib.parse.quote(search_query)
        search_url = f"{GOOGLE_MAPS_URL}{encoded_query}"

        page.goto(search_url, wait_until="domcontentloaded")
        time.sleep(5)

        # Extract address
        try:
            formatted_address_elem = page.query_selector(
                'button[data-item-id="address"], span.section-info-text'
            )
            if formatted_address_elem:
                walmart_address = formatted_address_elem.inner_text().strip()
                logger.info(f"Found Walmart at address: {walmart_address}")

                # Extract city and zip
                location = extract_city_zip_from_address(walmart_address)
                logger.info(f"City: {location['city']}, ZIP: {location['zip_code']}")
            else:
                walmart_address = address or "Unknown"
                logger.warning(
                    f"Could not find address element, using provided address: {walmart_address}"
                )
        except Exception as e:
            walmart_address = address or "Unknown"
            logger.error(f"Error extracting address: {e}")

        # Now search for mobile stores near this Walmart
        search_terms = [
            f"mobile phone repair near Walmart {store_id}",
            f"cell phone repair near {walmart_address}",
            f"iFixandRepair near Walmart {store_id}",
            f"The Fix inside Walmart {store_id}",
        ]

        for term in search_terms:
            logger.info(f"Searching for: {term}")
            encoded_query = urllib.parse.quote(term)
            search_url = f"{GOOGLE_MAPS_URL}{encoded_query}"

            page.goto(search_url, wait_until="domcontentloaded")
            time.sleep(5)

            # Look for results
            found_results = False
            for selector in [
                'div[role="article"]',
                "div.section-result",
                ".Nv2PK",
                ".fontHeadlineSmall",
                'div[role="feed"] > div',
            ]:
                result_elements = page.query_selector_all(selector)
                if result_elements and len(result_elements) > 0:
                    logger.info(f"Found {len(result_elements)} results")
                    found_results = True

                    # Process the results
                    stores = process_result_elements(
                        page,
                        result_elements,
                        [],
                        walmart_address,
                        extra_sensitive=True,
                        store_id=store_id,
                    )

                    if stores:
                        logger.info(f"Found {len(stores)} potential mobile stores:")
                        for s in stores:
                            logger.info(
                                f"- {s['name']} ({s.get('distance', 'Unknown distance')})"
                            )
                            logger.info(f"  Keywords: {s.get('keywords_matched', [])}")
                            logger.info(
                                f"  Location match: {s.get('location_match', 'unknown')}"
                            )
                    else:
                        logger.info("No mobile stores found in these results")

                    break

            if not found_results:
                logger.info("No results found for this search")

    except Exception as e:
        logger.error(f"Error debugging store: {e}")

    finally:
        close_browser(browser_info)


if __name__ == "__main__":
    # No need to import urllib.parse here since it's imported at the top

    if len(sys.argv) < 2:
        print("Usage: python debug_walmart.py <store_id> [address]")
        sys.exit(1)

    store_id = sys.argv[1]
    address = sys.argv[2] if len(sys.argv) > 2 else None

    debug_store(store_id, address)
