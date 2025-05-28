

import os

WALMART_LEASING_URL = "https://leasing.walmart.com/viewspaces"
MAX_SPACE_SIZE = 1000
MIN_REVIEWS = 10000

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
    "iFixRepair",
    "iFix and Repair",
    "i Fix",
    "i-Fix",
    "iFix Orlando",    
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

EMAIL_SENDER = "testproject815@gmail.com"
EMAIL_PASSWORD = "bhkf idoc twdj hidb"
EMAIL_RECEIVER = "josedvilla18@gmail.com"

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

WEB_WORKERS = 1  
API_WORKERS = 8  

SEARCH_RADIUS_METERS = (
    200  
)
GOOGLE_MAPS_URL = "https://www.google.com/maps/search/"

PAGE_LOAD_TIMEOUT = 60  
SCRIPT_TIMEOUT = 45  
ELEMENT_TIMEOUT = 30  
MAPS_LOAD_TIMEOUT = 30  

DATASEO_LOGIN = "josevilla@geeks5g.com"  
DATASEO_PASSWORD = "81a8a7a078bfa37c"    
DATASEO_NEARBY_RADIUS = 200  
