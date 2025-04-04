import re
import time
import urllib.parse
import logging
from config import GOOGLE_MAPS_URL

logger = logging.getLogger(__name__)

def check_for_ifixit_in_walmart(page, walmart_address, store_id, process_result_elements):
    """Special high-priority function to detect iFixandRepair inside Walmart."""
    found_stores = []
    
    # Extract street number and address components
    street_num = None
    street_match = re.search(r'(\d+)\s+([A-Za-z\s]+)', walmart_address)
    if street_match:
        street_num = street_match.group(1)
    
    # Create direct search queries specifically for iFixandRepair
    direct_searches = [
        f"iFixandRepair Walmart {street_num}",
        f"iFixandRepair Walmart {store_id}",
        f"iFix inside Walmart {street_num}",
        f"phone repair inside Walmart {street_num}",
        f"The Fix at Walmart {street_num}",
        f"Cellaris at Walmart {street_num}",
        # Add the most specific search possible
        f"iFixandRepair {walmart_address}"
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
                'div.section-result', 
                '.Nv2PK',
                '.fontHeadlineSmall'
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
                            store_id=store_id
                        )
                        
                        # Add to overall found stores
                        for store in matches:
                            if not any(s.get('name') == store.get('name') for s in found_stores):
                                found_stores.append(store)
                        
                        if matches:
                            logger.warning(f"HIGH PRIORITY CHECK: Found potential in-Walmart mobile stores: {[s['name'] for s in matches]}")
                        break
                except Exception as e:
                    logger.error(f"Error processing search results: {str(e)}")
        except Exception as e:
            logger.error(f"Error navigating to iFixandRepair search URL: {str(e)}")
    
    return found_stores
