"""
Script to geocode Walmart store locations and add coordinates for DataForSEO API
"""

import os
import json
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Check for required modules
try:
    from geopy.geocoders import Nominatim, ArcGIS
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError
except ImportError:
    logger.error("Required module 'geopy' is not installed.")
    logger.info("Please install it using: pip install geopy")
    logger.info("Or run: python install_requirements.py geopy")
    import sys
    sys.exit(1)

# Path to the matching properties JSON file
JSON_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 
    "json_data", 
    "matching_properties_20250423_200331.json"
)

# Path for the output file with coordinates
OUTPUT_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), 
    "json_data", 
    "properties_with_coordinates.json"
)

def load_properties():
    """
    Load properties from matching_properties JSON file
    
    Returns:
        List of property dictionaries
    """
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            properties = json.load(f)
        logger.info(f"Loaded {len(properties)} properties from {JSON_FILE_PATH}")
        return properties
    except Exception as e:
        logger.error(f"Error loading properties from JSON: {e}")
        return []

def geocode_address(address, city=None, zip_code=None, retries=3, delay=1):
    """
    Geocode an address string to get coordinates with fallback providers
    
    Args:
        address: Address string
        city: City name (optional)
        zip_code: ZIP code (optional)
        retries: Number of retry attempts
        delay: Delay between retries in seconds
    
    Returns:
        Tuple of (latitude, longitude) or None if geocoding failed
    """
    # Create a full address if components are provided
    if city and zip_code:
        full_address = f"{address}, {city}, {zip_code}"
    else:
        full_address = address
        
    # Clean control characters from the address
    full_address = ''.join(c for c in full_address if ord(c) >= 32)
    
    # Remove Unicode control characters and emojis
    full_address = full_address.replace("\ue0c8", "").strip()  # Remove the map icon
    
    # Try Nominatim first
    nominatim = Nominatim(user_agent="WalmartLocationChecker")
    
    for attempt in range(retries):
        try:
            location = nominatim.geocode(full_address, timeout=10)
            if location:
                return (location.latitude, location.longitude)
                
            # If we didn't get a result, try with just city and zip
            if city and zip_code and attempt == 0:
                logger.info(f"Trying with city and zip only: {city}, {zip_code}")
                location = nominatim.geocode(f"{city}, {zip_code}", timeout=10)
                if location:
                    return (location.latitude, location.longitude)
            
            logger.info(f"Nominatim didn't find results for: {full_address}")
            break  # Break out to try the next provider
            
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logger.warning(f"Nominatim error on attempt {attempt+1}: {str(e)}")
            
        # Add delay between retries
        if attempt < retries - 1:
            time.sleep(delay)
    
    # Fallback to ArcGIS
    arcgis = ArcGIS(timeout=10)
    logger.info(f"Trying ArcGIS for: {full_address}")
    
    for attempt in range(retries):
        try:
            location = arcgis.geocode(full_address)
            if location:
                return (location.latitude, location.longitude)
                
            # If we didn't get a result, try with just city and zip
            if city and zip_code and attempt == 0:
                logger.info(f"Trying ArcGIS with city and zip only: {city}, {zip_code}")
                location = arcgis.geocode(f"{city}, {zip_code}")
                if location:
                    return (location.latitude, location.longitude)
            
            logger.warning(f"ArcGIS didn't find results for: {full_address}")
            
        except Exception as e:
            logger.warning(f"ArcGIS error on attempt {attempt+1}: {str(e)}")
            
        # Add delay between retries
        if attempt < retries - 1:
            time.sleep(delay)
    
    # If all providers failed
    return None

def extract_coordinates_from_maps_url(maps_url):
    """
    Extract coordinates from a Google Maps URL if available
    
    Args:
        maps_url: Google Maps URL string
        
    Returns:
        String with "lat,lng" format or None if extraction failed
    """
    if not maps_url:
        return None
        
    # Try to extract coordinates from URL
    import re
    
    # Pattern for coordinates in Google Maps URLs
    patterns = [
        r"@(-?\d+\.\d+),(-?\d+\.\d+)",  # Standard format: @lat,lng
        r"q=(-?\d+\.\d+),(-?\d+\.\d+)",  # Query format: q=lat,lng
        r"ll=(-?\d+\.\d+),(-?\d+\.\d+)"  # LL format: ll=lat,lng
    ]
    
    for pattern in patterns:
        matches = re.search(pattern, maps_url)
        if matches:
            lat, lng = matches.groups()
            return f"{lat},{lng}"
    
    return None

def geocode_properties(properties):
    """
    Add coordinates to all properties
    
    Args:
        properties: List of property dictionaries
    
    Returns:
        Updated list of property dictionaries with coordinates
    """
    for prop in properties:
        store_id = prop.get("store_id", "Unknown")
        
        # Get address fields
        address = prop.get("full_address") or prop.get("address")
        city = prop.get("city")
        zip_code = prop.get("zip_code")
        
        logger.info(f"Processing: Store #{store_id}, {address}")
        
        # Check if we already have coordinates in the property
        if "location_coordinate" in prop:
            logger.info(f"Store #{store_id} already has coordinates: {prop['location_coordinate']}")
            continue
        
        # First try to extract coordinates from Google Maps URL if available
        maps_url = prop.get("google_maps_url")
        coordinates_str = extract_coordinates_from_maps_url(maps_url)
        
        if coordinates_str:
            prop["location_coordinate"] = coordinates_str
            logger.info(f"✓ Got coordinates from Maps URL for Store #{store_id}: {coordinates_str}")
            continue
            
        # If no coordinates from URL, geocode the address
        coordinates = geocode_address(address, city, zip_code)
        
        if coordinates:
            # Format coordinates as "lat,lng" string for DataForSEO
            prop["location_coordinate"] = f"{coordinates[0]},{coordinates[1]}"
            logger.info(f"✓ Got coordinates for Store #{store_id}: {prop['location_coordinate']}")
        else:
            logger.error(f"✗ Failed to geocode Store #{store_id}")
            
        # Add a short delay to avoid rate limiting
        time.sleep(1)
    
    return properties

def save_properties_with_coordinates(properties):
    """
    Save properties with coordinates to a new JSON file
    
    Args:
        properties: List of property dictionaries with coordinates
    """
    try:
        with open(OUTPUT_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(properties, f, indent=2)
        
        # Count how many properties have coordinates
        with_coords = sum(1 for p in properties if "location_coordinate" in p)
        logger.info(f"Saved {len(properties)} properties to {OUTPUT_FILE_PATH}")
        logger.info(f"{with_coords}/{len(properties)} properties have coordinates")
    except Exception as e:
        logger.error(f"Error saving properties to JSON: {e}")

def main():
    """Main function to process all properties"""
    logger.info("Starting geocoding of Walmart properties")
    
    # Load properties from JSON
    properties = load_properties()
    
    if not properties:
        logger.error("No properties loaded, exiting")
        return
    
    # Add coordinates to properties
    properties_with_coords = geocode_properties(properties)
    
    # Save updated properties
    save_properties_with_coordinates(properties_with_coords)
    
    logger.info("Geocoding complete")

if __name__ == "__main__":
    main()
