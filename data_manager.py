"""
Data management for Walmart Leasing Checker
Handles loading, saving and versioning of property data
"""

import os
import json
import logging
from datetime import datetime
from config import OUTPUT_DIR

# Configure logging
logger = logging.getLogger(__name__)

def load_previous_results():
    """Load previously identified matching properties from file."""
    file_path = os.path.join(OUTPUT_DIR, "matching_properties.json")
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(f"Loaded {len(data)} properties from previous results")
                return data
        except Exception as e:
            logger.error(f"Failed to load previous matching properties: {str(e)}")
    return []

def is_duplicate_property(new_prop, existing_props):
    """Check if a property is a duplicate of one already in our results."""
    for prop in existing_props:
        # Compare store ID and address for uniqueness
        store_match = new_prop.get('store_id') == prop.get('store_id')
        website_match = (
            new_prop.get('website_store_id') == prop.get('website_store_id') 
            and new_prop.get('website_store_id') is not None
        )
        
        # If either store ID or website store ID match, consider it the same store
        if store_match or website_match:
            return True
    return False

def improve_property_data(merged_properties):
    """
    Improve property data by filling in missing information from other properties
    with the same store ID or address.
    """
    # Create dictionaries to look up properties by ID and address
    by_store_id = {}
    by_website_id = {}
    by_address = {}
    
    # First pass - build lookup dictionaries
    for prop in merged_properties:
        store_id = prop.get('store_id')
        website_id = prop.get('website_store_id')
        address = prop.get('address', '').lower()
        
        if store_id and store_id not in by_store_id:
            by_store_id[store_id] = prop
        
        if website_id and website_id not in by_website_id:
            by_website_id[website_id] = prop
            
        if address and address not in by_address:
            by_address[address] = prop
    
    # Second pass - fill in missing information
    for prop in merged_properties:
        # Try to fill in city and zip if unknown
        if prop.get('city') == 'Unknown' or prop.get('zip_code') == 'Unknown':
            store_id = prop.get('store_id')
            website_id = prop.get('website_store_id')
            address = prop.get('address', '').lower()
            
            # Try to find another property with the same identifiers
            match = None
            if store_id and store_id in by_store_id and by_store_id[store_id] != prop:
                match = by_store_id[store_id]
            elif website_id and website_id in by_website_id and by_website_id[website_id] != prop:
                match = by_website_id[website_id]
            elif address and address in by_address and by_address[address] != prop:
                match = by_address[address]
            
            if match:
                if prop.get('city') == 'Unknown' and match.get('city') != 'Unknown':
                    prop['city'] = match.get('city')
                    prop['city_source'] = 'matched_property'
                    
                if prop.get('zip_code') == 'Unknown' and match.get('zip_code') != 'Unknown':
                    prop['zip_code'] = match.get('zip_code')
                    prop['zip_code_source'] = 'matched_property'
    
    return merged_properties

def save_results_with_versioning(properties):
    """Save results with versioning to avoid overwriting previous data."""
    # Create base filename
    base_filename = "matching_properties"
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # First, save current version with timestamp
    versioned_filename = f"{base_filename}_{current_time}.json"
    versioned_path = os.path.join(OUTPUT_DIR, versioned_filename)
    
    # Always save the current results with timestamp
    with open(versioned_path, "w", encoding="utf-8") as f:
        json.dump(properties, f, indent=2)
    logger.info(f"Saved current results to {versioned_filename}")
    
    # Now update the main file (merged with previous if applicable)
    main_path = os.path.join(OUTPUT_DIR, f"{base_filename}.json")
    
    # Load previous results
    previous_results = load_previous_results()
    
    # If we have previous results, merge with current results
    if previous_results:
        # Add new properties that aren't duplicates
        merged_properties = previous_results.copy()
        new_count = 0
        
        for prop in properties:
            if not is_duplicate_property(prop, previous_results):
                merged_properties.append(prop)
                new_count += 1
        
        # Improve the merged data
        merged_properties = improve_property_data(merged_properties)
        
        # Save merged results
        with open(main_path, "w", encoding="utf-8") as f:
            json.dump(merged_properties, f, indent=2)
        
        logger.info(f"Updated matching_properties.json with {new_count} new properties (total: {len(merged_properties)})")
        return merged_properties
    else:
        # If no previous results, just save current results as the main file
        with open(main_path, "w", encoding="utf-8") as f:
            json.dump(properties, f, indent=2)
        logger.info(f"Created new matching_properties.json with {len(properties)} properties")
        return properties

def save_intermediate_results(properties, filename):
    """Save intermediate results for debugging or analysis."""
    file_path = os.path.join(OUTPUT_DIR, filename)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(properties, f, indent=2)
    logger.info(f"Saved {len(properties)} properties to {filename}")
