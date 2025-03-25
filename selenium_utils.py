"""
Selenium utility functions for browser automation
"""

import time
import logging
import os
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException, TimeoutException, SessionNotCreatedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from config import PAGE_LOAD_TIMEOUT, SCRIPT_TIMEOUT

# Configure logging
logger = logging.getLogger(__name__)

def setup_selenium_driver(headless=True, retries=3):
    """Set up and return a Selenium WebDriver with retry mechanism."""
    for attempt in range(retries):
        try:
            chrome_options = Options()
            if headless:
                chrome_options.add_argument('--headless=new')
            
            # Other useful options
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--window-size=1920,1080')
            
            # Fix WebGL warnings
            chrome_options.add_argument('--disable-webgl')
            chrome_options.add_argument('--enable-unsafe-swiftshader')
            chrome_options.add_argument('--ignore-gpu-blocklist')
            
            # Connection reliability options
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-browser-side-navigation')
            chrome_options.add_argument('--dns-prefetch-disable')
            
            # Suppress console output
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_argument('--silent')
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Randomize user agent to avoid blocking
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
            ]
            chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
            
            # Suppress browser warning messages
            chrome_options.add_argument('--log-level=3')  # Only show fatal errors
            chrome_options.add_argument('--silent')
            
            # Set page load strategy to eager for faster loading
            chrome_options.page_load_strategy = 'eager'
            
            # Increase timeout settings
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            
            # Try to use webdriver-manager for ChromeDriver installation
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
                # Set timeout values from config
                driver.set_script_timeout(SCRIPT_TIMEOUT)
                driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
                
                # Test that browser is responsive
                driver.get("data:text/html,<html><body>Test Page</body></html>")
                
                logger.info(f"Chrome WebDriver created successfully on attempt {attempt+1}")
                return driver
            except ImportError:
                # Fallback if webdriver-manager is not available
                driver = webdriver.Chrome(options=chrome_options)
                driver.set_script_timeout(SCRIPT_TIMEOUT)
                driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
                return driver
                
        except (WebDriverException, SessionNotCreatedException) as e:
            logger.warning(f"Attempt {attempt+1} failed: {str(e)}")
            if attempt < retries - 1:
                # Wait before retry
                time.sleep(5 * (attempt + 1))
            else:
                logger.error(f"Failed to set up Selenium driver after {retries} attempts: {str(e)}")
                logger.error("Make sure you have Chrome and ChromeDriver installed and updated.")
    
    return None

def wait_for_element(driver, selector, timeout=10, by=By.CSS_SELECTOR):
    """Wait for an element to appear on the page."""
    try:
        wait = WebDriverWait(driver, timeout)
        element = wait.until(EC.presence_of_element_located((by, selector)))
        return element
    except TimeoutException:
        logger.warning(f"Timeout waiting for element: {selector}")
        return None

def safe_click(driver, element):
    """Safely click an element with multiple fallback methods."""
    try:
        element.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as e:
            logger.error(f"Failed to click element: {str(e)}")
            return False

def scroll_to_element(driver, element):
    """Scroll to make an element visible."""
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", element)
        time.sleep(0.3)  # Short pause for scroll to complete
        return True
    except Exception as e:
        logger.error(f"Failed to scroll to element: {str(e)}")
        return False
