import re
import time
import urllib.parse
import logging
from config import GOOGLE_MAPS_URL

logger = logging.getLogger(__name__)


def check_for_ifixit_in_walmart(
    page, walmart_address, store_id, process_result_elements
):
    """Special high-priority function to detect iFixandRepair inside Walmart."""
    found_stores = []

    # Extract street number and address components
    street_num = None
    street_match = re.search(r"(\d+)\s+([A-Za-z\s]+)", walmart_address)
    if street_match:
        street_num = street_match.group(1)

    # Extract city and state
    city_match = re.search(r"([A-Za-z\s]+),\s+([A-Z]{2})", walmart_address)
    city = city_match.group(1) if city_match else ""
    state = city_match.group(2) if city_match else ""

    # Create direct search queries specifically for iFixandRepair
    direct_searches = [
        # Specific iFixandRepair searches
        f"iFixandRepair Walmart {street_num}",
        f"iFixandRepair Walmart {store_id}",
        f"iFix inside Walmart {street_num}",
        f"iFix and Repair Walmart {store_id}",
        f"iFixandRepair {city} {state}",
        # The Fix (Asurion) searches
        f"The Fix Walmart {store_id}",
        f"The Fix by Asurion Walmart {store_id}",
        f"The Fix {walmart_address}",
        # Other common in-store brands
        f"Boost Mobile inside Walmart {store_id}",
        f"Cricket Wireless inside Walmart {store_id}",
        f"Simple Mobile Walmart {store_id}",
        # Generic searches with high specificity
        f"phone repair inside Walmart {street_num}",
        f"cell phone inside Walmart {store_id}",
        f"mobile store Walmart {store_id}",
        f"phone service inside Walmart {walmart_address}",
        # Most specific search possible
        f"iFixandRepair {walmart_address}",
    ]

    logger.warning(f"Running CRITICAL iFixandRepair detection for Walmart #{store_id}")

    # Execute each search and process results
    for search_query in direct_searches:
        encoded_query = urllib.parse.quote(search_query)
        search_url = f"{GOOGLE_MAPS_URL}{encoded_query}"

        logger.info(f"Direct iFixandRepair search: {search_query}")

        try:
            # Navigate to search URL
            page.goto(search_url, wait_until="domcontentloaded")
            time.sleep(3)  # Give time for results to load

            # Look for results with multiple selector patterns
            for selector in [
                'div[role="article"]',
                "div.section-result",
                ".Nv2PK",
                ".fontHeadlineSmall",
                'div[role="feed"] > div',
            ]:
                try:
                    result_elements = page.query_selector_all(selector)
                    if result_elements and len(result_elements) > 0:
                        # Process with extra sensitivity for matching
                        matches = process_result_elements(
                            page,
                            result_elements,
                            [],
                            walmart_address,
                            extra_sensitive=True,
                            store_id=store_id,
                        )

                        # Add to overall found stores
                        for store in matches:
                            if not any(
                                s.get("name") == store.get("name") for s in found_stores
                            ):
                                found_stores.append(store)

                        if matches:
                            logger.warning(
                                f"HIGH PRIORITY CHECK: Found potential in-Walmart mobile stores: {[s['name'] for s in matches]}"
                            )
                        break
                except Exception as e:
                    logger.error(f"Error processing search results: {str(e)}")
        except Exception as e:
            logger.error(f"Error navigating to iFixandRepair search URL: {str(e)}")

        # Avoid getting rate limited
        time.sleep(1.5)

    # Deduplicate results one more time
    unique_stores = []
    for store in found_stores:
        if not any(s.get("name") == store.get("name") for s in unique_stores):
            unique_stores.append(store)

    return unique_stores
