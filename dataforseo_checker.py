"""
Mobile store checker using DataForSEO API
Checks if Walmart stores have mobile stores inside them
"""

import logging
import json
import base64
import time
import random
import requests
from concurrent.futures import ThreadPoolExecutor

from config import DATASEO_LOGIN, DATASEO_PASSWORD, DATASEO_NEARBY_RADIUS, API_WORKERS

# Configure logging
logger = logging.getLogger(__name__)

# DataForSEO API endpoint - using the working endpoint found in testing
DATASEO_ENDPOINT = "https://api.dataforseo.com/v3/business_data/google/my_business_info/live"

# Mobile-related keywords for detection
MOBILE_KEYWORDS = [
    "mobile repair",
    "phone repair",
    "cell phone",
    "ifixandrepair",
    "the fix",
    "boost mobile",
    "cricket wireless",
    "cell repair",
    "smartphone repair",
    "tech repair",
    "device repair",
    "screen repair",
    "battery replacement",
]


def check_mobile_stores(store_property):
    """
    Check if a store has mobile stores nearby or inside using DataForSEO API.
    
    Args:
        store_property: Dictionary with store information including coordinates
        
    Returns:
        dict: Updated store property with mobile store check results
    """
    store_id = store_property.get("store_id", "Unknown")
    coordinates = store_property.get("location_coordinate")
    address = store_property.get("full_address", store_property.get("address", ""))
    
    if not coordinates:
        logger.warning(f"No coordinates available for Store #{store_id}")
        store_property["meets_criteria"] = False
        store_property["fail_reason"] = "Missing coordinates for mobile store check"
        return store_property
    
    logger.info(f"Checking mobile stores for #{store_id} at {coordinates}")
    
    # Build auth credentials for API
    auth_string = base64.b64encode(f"{DATASEO_LOGIN}:{DATASEO_PASSWORD}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_string}",
        "Content-Type": "application/json"
    }
    
    # Build search keywords
    search_keywords = []
    
    # Add general mobile store searches
    for keyword in ["mobile store", "phone repair", "cell phone repair"]:
        search_keywords.append(f"{keyword} near {coordinates}")
    
    # Add specific in-store searches with Walmart address or ID
    search_keywords.append(f"phone repair inside Walmart {store_id}")
    search_keywords.append(f"mobile repair in Walmart {address}")
    
    # Get specific locations for well-known mobile store brands
    known_brands = ["iFixandRepair", "The Fix", "Boost Mobile", "Cricket Wireless", "Asurion"]
    for brand in known_brands:
        search_keywords.append(f"{brand} Walmart {store_id}")
        search_keywords.append(f"{brand} near {coordinates}")
    
    # Randomize searches to avoid patterns
    random.shuffle(search_keywords) 
    
    # Only use first 5 searches to reduce API usage
    search_keywords = search_keywords[:5]
    
    all_results = []
    mobile_stores_found = []
    
    # Process each search keyword
    for keyword in search_keywords:
        try:
            # Build the request payload
            payload = [{
                "keyword": keyword,
                "language_code": "en"
            }]
            
            # Make the API request
            response = requests.post(
                DATASEO_ENDPOINT,
                headers=headers,
                data=json.dumps(payload),
                timeout=30
            )
            
            if response.status_code != 200:
                logger.warning(f"API request failed with status code: {response.status_code}")
                continue
            
            data = response.json()
            
            if data.get("status_code") != 20000:
                logger.warning(f"API error: {data.get('status_code')} - {data.get('status_message')}")
                continue
            
            # Process results
            tasks = data.get("tasks", [])
            for task in tasks:
                if task.get("status_code") != 20000 or not task.get("result"):
                    continue
                
                results = task.get("result", [])
                
                for result in results:
                    # Check for items in result
                    items = []
                    
                    if "items" in result and result["items"]:
                        items = result["items"]
                    
                    # Process each business
                    for item in items:
                        business_name = item.get("name", item.get("title", ""))
                        if not business_name:
                            continue
                            
                        business_address = item.get("address", "")
                        
                        # Check if this is a mobile store
                        is_mobile = False
                        
                        # Method 1: Check for mobile keywords in name
                        name_lower = business_name.lower()
                        for mobile_keyword in MOBILE_KEYWORDS:
                            if mobile_keyword.lower() in name_lower:
                                is_mobile = True
                                break
                        
                        # Method 2: Check for specific brand names
                        for brand in known_brands:
                            if brand.lower() in name_lower:
                                is_mobile = True
                                break
                        
                        # Method 3: Check if it's explicitly inside Walmart
                        address_lower = business_address.lower()
                        inside_walmart = (
                            "inside walmart" in address_lower or 
                            "at walmart" in address_lower or
                            "walmart" in address_lower and (
                                "ste" in address_lower or 
                                "suite" in address_lower or
                                "kiosk" in address_lower
                            )
                        )
                        
                        # If it's a mobile store, add to results
                        if is_mobile:
                            mobile_store = {
                                "name": business_name,
                                "address": business_address,
                                "is_inside_walmart": inside_walmart,
                                "search_keyword": keyword
                            }
                            
                            # Check for duplicates before adding
                            duplicate = False
                            for existing in mobile_stores_found:
                                if existing["name"] == business_name:
                                    duplicate = True
                                    break
                            
                            if not duplicate:
                                mobile_stores_found.append(mobile_store)
                                all_results.append(mobile_store)
            
            # Add small delay between API calls to avoid rate limiting
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error processing keyword '{keyword}': {str(e)}")
    
    # Update the store property with results
    if mobile_stores_found:
        store_property["meets_criteria"] = False
        store_property["fail_reason"] = f"Has {len(mobile_stores_found)} mobile stores"
        store_property["mobile_stores"] = mobile_stores_found
        logger.info(f"✗ Store #{store_id} has {len(mobile_stores_found)} mobile stores")
    else:
        store_property["meets_criteria"] = True
        logger.info(f"✓ Store #{store_id} has NO mobile stores")
    
    return store_property


def check_stores_for_mobile(store_properties, max_workers=None):
    """
    Check multiple stores for mobile stores using DataForSEO API in parallel.
    
    Args:
        store_properties: List of store property dictionaries
        max_workers: Maximum number of parallel workers (defaults to API_WORKERS from config)
        
    Returns:
        list: Updated store properties with mobile store check results
    """
    if max_workers is None:
        max_workers = API_WORKERS
    
    logger.info(f"Checking {len(store_properties)} stores for mobile stores with {max_workers} workers")
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(check_mobile_stores, store): store for store in store_properties}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(futures):
            store = futures[future]
            try:
                result = future.result()
                results.append(result)
                
                # Add progress logging
                if len(results) % 5 == 0 or len(results) == len(store_properties):
                    logger.info(f"Progress: {len(results)}/{len(store_properties)} stores checked")
            except Exception as e:
                logger.error(f"Error processing store {store.get('store_id', 'Unknown')}: {str(e)}")
                # Add failed store with error info
                store["meets_criteria"] = False
                store["fail_reason"] = f"Mobile store check error: {str(e)}"
                results.append(store)
    
    # Count how many meet criteria
    meeting_criteria = sum(1 for p in results if p.get("meets_criteria", False))
    logger.info(f"Finished checking {len(results)} stores: {meeting_criteria} have no mobile stores")
    
    return results


# For direct testing
if __name__ == "__main__":
    import sys
    import os
    from config import OUTPUT_DIR
    
    # Configure root logger for direct execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    # Input file path (stores that passed review check)
    input_file = os.path.join(OUTPUT_DIR, "stores_with_reviews.json")
    
    if os.path.exists(input_file):
        with open(input_file, 'r', encoding='utf-8') as f:
            stores = json.load(f)
            
        # Filter for stores that meet the review criteria
        qualifying_stores = [s for s in stores if s.get("meets_criteria", False)]
        logger.info(f"Loaded {len(qualifying_stores)} stores that meet review criteria")
        
        # Process first 3 stores if in test mode
        test_stores = qualifying_stores[:3] if len(sys.argv) > 1 and sys.argv[1] == "--test" else qualifying_stores
        processed_stores = check_stores_for_mobile(test_stores)
        
        # Save results
        output_file = os.path.join(OUTPUT_DIR, "final_checked_stores.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(processed_stores, f, indent=2)
            
        logger.info(f"Results saved to {output_file}")
        
        # Print summary
        meeting_criteria = [s for s in processed_stores if s.get("meets_criteria", False)]
        logger.info(f"Summary: {len(meeting_criteria)}/{len(processed_stores)} stores have NO mobile stores")
    else:
        logger.error(f"Input file not found: {input_file}")
        logger.info("Please run the review scraper first")
