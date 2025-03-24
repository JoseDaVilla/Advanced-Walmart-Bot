"""
Configuration settings for the Walmart Leasing Checker
"""

import os

# URLs and search settings
WALMART_LEASING_URL = "https://leasing.walmart.com/viewspaces"
MAX_SPACE_SIZE = 1000  
MIN_REVIEWS = 10000

# Keywords for mobile store detection
MOBILE_STORE_KEYWORDS = ["CPR", "TalknFix", "iTalkandRepair", "mobile repair", "phone repair", 
                        "cell phone", "cellular", "smartphone repair", "iphone repair", "wireless repair",
                        "Cell Phone Repair", "Ifixandrepair", "Cellaris", "Thefix", "Casemate", "Techy",
                        "iFixandRepair", "IFixAndRepair", "The Fix", "Case Mate", "CaseMate"]

# Email configuration
EMAIL_SENDER = "testproject815@gmail.com"
EMAIL_PASSWORD = "bhkf idoc twdj hidb"
EMAIL_RECEIVER = "josedvilla18@gmail.com"

# Output directory for JSON data
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Concurrency settings
WEB_WORKERS = 15  # Number of parallel browser workers for Walmart scraping
API_WORKERS = 8   # Number of parallel browser workers for location checking

# Google Maps search settings
SEARCH_RADIUS_METERS = 100  # Search radius for nearby stores
GOOGLE_MAPS_URL = "https://www.google.com/maps/search/"

# Timeout settings
PAGE_LOAD_TIMEOUT = 45      # Seconds to wait for page load
SCRIPT_TIMEOUT = 30         # Seconds to wait for scripts to execute
ELEMENT_TIMEOUT = 20        # Seconds to wait for elements to appear
MAPS_LOAD_TIMEOUT = 20      # Seconds to wait for Google Maps to load
