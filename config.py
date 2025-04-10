"""
Configuration settings for the Walmart Leasing Checker.
This module defines all configurable parameters used throughout the application,
including URLs, search settings, mobile store detection keywords, and more.
"""

import os

# URLs and search settings
WALMART_LEASING_URL = "https://leasing.walmart.com/viewspaces"
MAX_SPACE_SIZE = 1000  # Maximum square footage for small spaces
MIN_REVIEWS = 10000    # Minimum Google reviews required to qualify

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
EMAIL_PASSWORD = "bhkf idoc twdj hidb"  # App-specific password, not actual account password
EMAIL_RECEIVER = "josedvilla18@gmail.com"

# Output directory for JSON data
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Concurrency settings
WEB_WORKERS = 15  # Number of parallel browser workers for Walmart scraping
API_WORKERS = 8   # Number of parallel browser workers for location checking

# Google Maps search settings
SEARCH_RADIUS_METERS = 200  # Search radius for nearby stores in meters
GOOGLE_MAPS_URL = "https://www.google.com/maps/search/"

# Timeout settings
PAGE_LOAD_TIMEOUT = 45  # Seconds to wait for page load
SCRIPT_TIMEOUT = 30     # Seconds to wait for scripts to execute
ELEMENT_TIMEOUT = 20    # Seconds to wait for elements to appear
MAPS_LOAD_TIMEOUT = 20  # Seconds to wait for Google Maps to load
