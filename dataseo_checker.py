import requests
import json
import time
import logging
import concurrent.futures
import base64
import os
from urllib.parse import quote
from colorama import Fore, Style, init

from config import DATASEO_LOGIN, DATASEO_PASSWORD, DATASEO_NEARBY_RADIUS, API_WORKERS
from data_manager import save_intermediate_results

init(autoreset=True)
logger = logging.getLogger(__name__)
DATASEO_API_URL = "https://api.dataforseo.com/v3/maps/google/search/task_post"
DATASEO_RESULT_URL = "https://api.dataforseo.com/v3/maps/google/search/task_get"

MOBILE_KEYWORDS = [
    "cell phone repair",
    "mobile repair",
    "phone repair",
    "boost mobile",
    "cricket wireless",
    "the fix",
    "ifixandrepair",
    "the fix by asurion",
    "tech repair",
    "cellaris"
]

def make_dataseo_request(url, data=None):

    try:
        
        auth_string = base64.b64encode(
            f"{DATASEO_LOGIN}:{DATASEO_PASSWORD}".encode()
        ).decode()
        
        headers = {
            "Authorization": f"Basic {auth_string}",
            "Content-Type": "application/json"
        }
        
        if data:
            response = requests.post(url, headers=headers, data=json.dumps(data))
        else:
            response = requests.get(url, headers=headers)
        
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"{Fore.RED}DataForSEO API error: {response.status_code} - {response.text}{Style.RESET_ALL}")
            return None
    
    except Exception as e:
        logger.error(f"{Fore.RED}Error making DataForSEO request: {str(e)}{Style.RESET_ALL}")
        return None


def start_nearby_search(store, keyword):

    store_address = store.get("full_address", store.get("address", ""))
    store_id = store.get("store_id", "Unknown")
    
    
    location = f"Walmart {store_id} {store_address}"
    
    
    data = [
        {
            "language_name": "English",
            "keyword": f"{keyword} near {location}",
            "location_name": location,
            "depth": 30,
            "priority": 2,
            "se_domain": "google.com",
            "search_radius": DATASEO_NEARBY_RADIUS
        }
    ]
    
    try:
        response = make_dataseo_request(DATASEO_API_URL, data)
        
        if response and response.get("status_code") == 20000:
            tasks = response.get("tasks", [])
            if tasks and len(tasks) > 0:
                task_id = tasks[0].get("id")
                logger.info(f"{Fore.GREEN}Started search for '{keyword}' near Store #{store_id}: Task ID {task_id}{Style.RESET_ALL}")
                return task_id
        
        logger.warning(f"{Fore.YELLOW}Failed to start search for '{keyword}' near Store #{store_id}{Style.RESET_ALL}")
        return None
    
    except Exception as e:
        logger.error(f"{Fore.RED}Error starting search: {str(e)}{Style.RESET_ALL}")
        return None


def check_search_results(task_id, store_id, keyword, attempts=3):

    for attempt in range(attempts):
        try:
            
            if attempt > 0:
                wait_time = 5 * (2 ** attempt)
                logger.info(f"{Fore.CYAN}Waiting {wait_time}s before checking results for Store #{store_id} ({keyword}){Style.RESET_ALL}")
                time.sleep(wait_time)
            
            
            response = make_dataseo_request(f"{DATASEO_RESULT_URL}/{task_id}")
            
            if not response or response.get("status_code") != 20000:
                continue
            
            tasks = response.get("tasks", [])
            if not tasks or len(tasks) == 0:
                continue
            
            
            status = tasks[0].get("status_code")
            if status == 20000:  
                results = tasks[0].get("result", [])
                if results and len(results) > 0:
                    items = results[0].get("items", [])
                    logger.info(f"{Fore.GREEN}Found {len(items)} results for '{keyword}' near Store #{store_id}{Style.RESET_ALL}")
                    return items
                return []
            elif status in [40000, 40100]:  
                logger.info(f"{Fore.CYAN}Task still in progress for Store #{store_id} ({keyword}), status: {status}{Style.RESET_ALL}")
            else:
                logger.warning(f"{Fore.YELLOW}Task failed for Store #{store_id} ({keyword}), status: {status}{Style.RESET_ALL}")
                return None
        
        except Exception as e:
            logger.error(f"{Fore.RED}Error checking results: {str(e)}{Style.RESET_ALL}")
    
    logger.warning(f"{Fore.YELLOW}Failed to get results for Store #{store_id} ({keyword}) after {attempts} attempts{Style.RESET_ALL}")
    return None


def process_store_nearby_search(store):

    store_id = store.get("store_id", "Unknown")
    logger.info(f"{Fore.CYAN}Checking for nearby mobile stores for Store #{store_id}{Style.RESET_ALL}")
    
    
    if not store.get("meets_criteria", False):
        logger.info(f"{Fore.YELLOW}Skipping Store #{store_id} as it doesn't meet review criteria{Style.RESET_ALL}")
        return store
    
    
    store["has_mobile_store"] = False
    store["mobile_stores_found"] = []
    store["mobile_store_search_method"] = "DataForSEO API"
    
    
    tasks = []
    
    
    for keyword in MOBILE_KEYWORDS:
        task_id = start_nearby_search(store, keyword)
        if task_id:
            tasks.append({
                "task_id": task_id,
                "keyword": keyword,
                "completed": False
            })
    
    
    time.sleep(5)
    
    
    found_mobile_stores = []
    
    for task in tasks:
        results = check_search_results(task["task_id"], store_id, task["keyword"])
        task["completed"] = True
        
        if results:
            
            for result in results:
                
                title = result.get("title", "").lower()
                snippet = result.get("snippet", "").lower() if result.get("snippet") else ""
                
                
                is_inside_walmart = any(term in snippet for term in ["inside walmart", "walmart", "in walmart", "at walmart"])
                is_mobile_store = any(term in title.lower() for term in [
                    "repair", "fix", "phone", "mobile", "cell", "wireless", "boost", "cricket", 
                    "the fix", "ifixandrepair", "tech"
                ])
                
                
                if (is_inside_walmart and is_mobile_store) or (is_mobile_store and result.get("distance_in_meters", 1000) < 200):
                    mobile_store = {
                        "name": result.get("title"),
                        "address": result.get("address"),
                        "distance": f"{result.get('distance_in_meters', 'Unknown')} meters",
                        "rating": result.get("rating", {}).get("value", "Unknown"),
                        "reviews": result.get("rating", {}).get("count", 0),
                        "found_via": task["keyword"]
                    }
                    
                    
                    if not any(store["name"] == mobile_store["name"] for store in found_mobile_stores):
                        found_mobile_stores.append(mobile_store)
                        logger.warning(f"{Fore.YELLOW}Found mobile store: {mobile_store['name']} near Store #{store_id}{Style.RESET_ALL}")
    
    
    if found_mobile_stores:
        store["has_mobile_store"] = True
        store["mobile_stores_found"] = found_mobile_stores
        store["meets_criteria"] = False
        store["fail_reason"] = f"Found {len(found_mobile_stores)} mobile stores nearby"
        logger.warning(f"{Fore.YELLOW}Store #{store_id} has {len(found_mobile_stores)} mobile stores nearby{Style.RESET_ALL}")
    else:
        store["mobile_stores_found"] = []
        store["has_mobile_store"] = False
        logger.info(f"{Fore.GREEN}No mobile stores found near Store #{store_id}{Style.RESET_ALL}")
    
    return store


def check_stores_for_mobile(stores):
    logger.info(f"{Fore.CYAN}Checking {len(stores)} stores for nearby mobile stores{Style.RESET_ALL}")
    
    
    eligible_stores = [store for store in stores if store.get("meets_criteria", False)]
    logger.info(f"{Fore.CYAN}{len(eligible_stores)} stores meet review criteria and will be checked{Style.RESET_ALL}")
    
    if not eligible_stores:
        return stores
    
    workers = min(API_WORKERS // 2, max(1, len(eligible_stores)))
    logger.info(f"{Fore.CYAN}Using {workers} parallel workers for DataForSEO API{Style.RESET_ALL}")
    
    results = []
    processed_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        
        future_to_store = {
            executor.submit(process_store_nearby_search, store): store
            for store in eligible_stores
        }
        
        for future in concurrent.futures.as_completed(future_to_store):
            try:
                result = future.result()
                results.append(result)
                processed_count += 1
                
                
                logger.info(f"{Fore.CYAN}Progress: {processed_count}/{len(eligible_stores)} stores processed{Style.RESET_ALL}")
                
                
                if processed_count % 5 == 0 or processed_count == len(eligible_stores):
                    save_intermediate_results(results, f"mobile_check_progress_{processed_count}.json")
                
                
                time.sleep(1)
            
            except Exception as e:
                logger.error(f"{Fore.RED}Error processing store: {str(e)}{Style.RESET_ALL}")
    
    final_results = []
    for store in stores:
        
        matched_result = next((s for s in results if s.get("store_id") == store.get("store_id")), None)
        
        if matched_result:
            final_results.append(matched_result)
        else:
            final_results.append(store)
    
    
    logger.info(f"{Fore.GREEN}Completed mobile store check for {len(results)} stores.{Style.RESET_ALL}")
    save_intermediate_results(final_results, "mobile_checked_stores.json")
    
    return final_results
