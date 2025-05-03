"""
Configuration settings for the Walmart Leasing Checker
"""

import os

# URLs and search settings
WALMART_LEASING_URL = "https://leasing.walmart.com/viewspaces"
MAX_SPACE_SIZE = 1000
MIN_REVIEWS = 10000

# Keywords for mobile store detection - EXPANDED with specific brands
MOBILE_STORE_KEYWORDS = [
    "TalknFix",
    "iTalkandRepair",
    "mobile repair",
    "phone repair",
    "cell phone",
    "cellular",
    "smartphone repair",
    "iphone repair",
    "wireless repair",
    "Cell Phone Repair",
    "Ifixandrepair",
    "Cellaris",
    "Thefix",
    "Casemate",
    "Techy",
    "iFixandRepair",
    "IFixAndRepair",
    "The Fix",
    "Case Mate",
    "CaseMate",
    "The fix",
    "IFix and repair",
    "Cellaris",
    "Talk N fix",
    "Techy",
    "PhoneFix",
    "iDoctor",
    "uBreakiFix",
    "iRepair",
    "Experimax",
    "Gadget",
    "Experimac",
    "Device Pitstop",
    "Wireless Clinic",
    "Mobile Solutions",
    "Cell Doc",
    "mobilerepair",
    "phonerepair",
    "devicerepair",
    "smartphonerepair",
    # Added variations for iFixandRepair that might be missed
    "iFixRepair",
    "iFix and Repair",
    "i Fix",
    "i-Fix",
    "iFix Orlando",
    # Additional variations for commonly missed stores
    "Asurion",
    "asurion tech repair",
    "asurion repair",
    "CPR Cell Phone Repair",
    "CPR Phone Repair",
    "iCare",
    "i Care",
    "iCare Repair",
    "Simply Mac",
    "Simply Fix",
    "The Fix by Asurion",
    "Fix by Asurion",
    "Tech Solutions",
    "Tech Bar",
    "Tech Corner",
    "Walmart Tech Services",
    "Tech Services",
    "ImmedaTech",
    "TechXpress",
    "Tech Xpress",
    "iParts",
    "i Parts",
    "iFix Solutions",
]

# Email configuration
EMAIL_SENDER = "testproject815@gmail.com"
EMAIL_PASSWORD = "bhkf idoc twdj hidb"
EMAIL_RECEIVER = "josedvilla18@gmail.com"

# Output directory for JSON data
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Concurrency settings
WEB_WORKERS = 1  # Changed to 1 for sequential processing
API_WORKERS = 8  # Number of parallel browser workers for location checking

# Google Maps search settings
SEARCH_RADIUS_METERS = (
    200  # Search radius for nearby stores (increased from 100 to 200)
)
GOOGLE_MAPS_URL = "https://www.google.com/maps/search/"

# Timeout settings
PAGE_LOAD_TIMEOUT = 60  # Increased from 45 to 60 seconds
SCRIPT_TIMEOUT = 45  # Increased from 30 to 45 seconds
ELEMENT_TIMEOUT = 30  # Increased from 20 to 30 seconds
MAPS_LOAD_TIMEOUT = 30  # Increased from 20 to 30 seconds

# DataForSEO API credentials
DATASEO_LOGIN = "josevilla@geeks5g.com"  # Your account email
DATASEO_PASSWORD = "81a8a7a078bfa37c"    # Your API key
DATASEO_NEARBY_RADIUS = 200  # Search radius in meters
