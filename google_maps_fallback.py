"""
Google Maps direct search fallback for when DataForSEO API doesn't have enough data
"""

import time
import logging
import urllib.parse
import random
from playwright_utils import setup_playwright_browser, close_browser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

GOOGLE_MAPS_URL = "https://www.google.com/maps/search/"

def search_nearby_mobile_stores(location_coordinate, walmart_address=None, store_id=None):
    """
    Search Google Maps directly for mobile stores near a location
    
    Args:
        location_coordinate: String with lat,lng format
        walmart_address: Optional Walmart address
        store_id: Optional Walmart store ID
        
    Returns:
        Dictionary with search results
    """
    logger.info(f"Starting Google Maps search for mobile stores near {location_coordinate}")
    
    # Generate search queries
    search_queries = [
        f"mobile phone repair near {location_coordinate}",
        f"cell phone store near {location_coordinate}",
        f"smartphone repair near {location_coordinate}"
    ]
    
    if walmart_address:
        search_queries.extend([
            f"mobile store near {walmart_address}",
            f"cell phone repair near {walmart_address}"
        ])
    
    if store_id:
        search_queries.extend([
            f"mobile store inside walmart {store_id}",
            f"the fix inside walmart {store_id}",
            f"boost mobile walmart {store_id}"
        ])
    
    # Set up browser
    browser_info = setup_playwright_browser(headless=True)
    if not browser_info:
        logger.error("Failed to set up browser")
        return {"has_mobile_store": False, "error": "Browser setup failed"}
    
    try:
        page = browser_info["page"]
        found_stores = []
        
        # Process each search query
        for query in search_queries[:5]:  # Limit to 5 searches
            logger.info(f"Searching Google Maps for: {query}")
            encoded_query = urllib.parse.quote(query)
            search_url = f"{GOOGLE_MAPS_URL}{encoded_query}"
            
            try:
                page.goto(search_url, wait_until="domcontentloaded")
                time.sleep(5)  # Wait for results to load
                
                # Look for business results
                for selector in ['div[role="article"]', ".Nv2PK", 'div[role="feed"] > div']:
                    results = page.query_selector_all(selector)
                    
                    if results:
                        logger.info(f"Found {len(results)} results for query: {query}")
                        
                        for idx, result in enumerate(results[:10]):  # Limit to first 10 results
                            try:
                                # Extract business name
                                name_elem = result.query_selector('h3, [role="heading"]')
                                if not name_elem:
                                    continue
                                    
                                name = name_elem.inner_text().strip()
                                
                                # Extract address if available
                                address_elem = result.query_selector('.fontBodyMedium div:nth-child(1)')
                                address = address_elem.inner_text().strip() if address_elem else "Unknown"
                                
                                # Extract distance if available
                                distance_elem = result.query_selector('span[aria-label*="miles"], span[aria-label*="mi"]')
                                distance = distance_elem.inner_text().strip() if distance_elem else "Unknown"
                                
                                # Check if it's a mobile store
                                mobile_keywords = [
                                    "phone", "mobile", "cell", "repair", "fix", "wireless", 
                                    "iphone", "screen", "battery", "the fix", "ifixandrepair",
                                    "boost", "cricket", "tech repair"
                                ]
                                
                                is_mobile = any(kw in name.lower() for kw in mobile_keywords)
                                
                                if is_mobile:
                                    logger.info(f"Found mobile store: {name}")
                                    store_data = {
                                        "name": name,
                                        "address": address,
                                        "distance": distance,
                                        "found_by_query": query
                                    }
                                    
                                    # Only add if not a duplicate
                                    if not any(s["name"] == name for s in found_stores):
                                        found_stores.append(store_data)
                            except Exception as e:
                                logger.error(f"Error processing result {idx}: {e}")
                        
                        # Break out if we found at least one result
                        if found_stores:
                            break
            except Exception as e:
                logger.error(f"Error searching for {query}: {e}")
            
            # Random delay between searches
            time.sleep(random.uniform(2, 5))
        
        return {
            "has_mobile_store": len(found_stores) > 0,
            "mobile_stores": found_stores,
            "search_method": "direct_google_maps"
        }
    
    finally:
        close_browser(browser_info)

if __name__ == "__main__":
    # Example usage
    import sys
    coords = sys.argv[1] if len(sys.argv) > 1 else "35.4635026,-97.6226054"
    results = search_nearby_mobile_stores(coords)
    
    print("\nSearch Results:")
    print(f"Has mobile stores: {results['has_mobile_store']}")
    print(f"Found {len(results['mobile_stores'])} mobile stores")
    
    for store in results['mobile_stores']:
        print(f"\n  - {store['name']}")
        print(f"    Address: {store.get('address', 'N/A')}")
        print(f"    Distance: {store['distance']}")
        print(f"    Found by query: {store['found_by_query']}")
