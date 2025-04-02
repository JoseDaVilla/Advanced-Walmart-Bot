"""
Playwright utility functions for browser automation
"""

import time
import logging
import os
import random
import tempfile
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from config import PAGE_LOAD_TIMEOUT, SCRIPT_TIMEOUT

# Configure logging
logger = logging.getLogger(__name__)

def setup_playwright_browser(headless=True, retries=3, worker_id=0):
    """Set up and return a Playwright browser instance with retry mechanism for true parallelism."""
    for attempt in range(retries):
        playwright = None
        try:
            # Start Playwright
            playwright = sync_playwright().start()
            
            # Create a unique user data directory for this worker
            user_data_dir = os.path.join(tempfile.gettempdir(), f'playwright-profile-{worker_id}-{random.randint(1000, 9999)}')
            os.makedirs(user_data_dir, exist_ok=True)
            
            # Configure browser options - NOTE: Playwright doesn't use the same options as Selenium
            browser_args = [
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                f'--window-name=worker-{worker_id}',
                '--disable-webgl',
                '--disable-extensions',
                '--disable-browser-side-navigation',
                '--dns-prefetch-disable',
                '--log-level=3',
                '--silent',
            ]
            
            # Set up browser - FIXED: removed user_data_dir from launch() parameters
            browser = playwright.chromium.launch(
                headless=headless,
                args=browser_args,
                timeout=30000,  # 30 seconds in ms
            )
            
            # Create context with the user data directory - FIXED: user_data_dir goes here
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=get_random_user_agent(worker_id),
                # Note: Playwright doesn't support setting user_data_dir in new_context
                # so we'll rely on isolated browser contexts instead
            )
            
            # Create page with appropriate timeouts
            page = context.new_page()
            page.set_default_timeout(PAGE_LOAD_TIMEOUT * 1000)  # Convert to ms
            page.set_default_navigation_timeout(PAGE_LOAD_TIMEOUT * 1000)
            
            # Test that browser is responsive with a simple data URL (avoids network)
            page.goto("data:text/html,<html><body>Test Page</body></html>")
            
            logger.info(f"Playwright browser created successfully on attempt {attempt+1}")
            
            # Return everything needed to clean up properly
            return {
                "playwright": playwright,
                "browser": browser,
                "context": context,
                "page": page
            }
            
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed to create Playwright browser: {str(e)}")
            # Clean up resources if initialization failed
            try:
                if playwright:
                    playwright.stop()
            except:
                pass
                
            if attempt < retries - 1:
                # Wait before retry
                time.sleep(5 * (attempt + 1))
            else:
                logger.error(f"Failed to set up Playwright browser after {retries} attempts: {str(e)}")
    
    return None

def get_random_user_agent(worker_id=0):
    """Get a random user agent with worker ID to ensure uniqueness."""
    user_agents = [
        f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Worker/{worker_id}',
        f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Worker/{worker_id}',
        f'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Worker/{worker_id}'
    ]
    return random.choice(user_agents)

def wait_for_element(page, selector, timeout=10):
    """Wait for an element to appear on the page and return it."""
    try:
        return page.wait_for_selector(selector, timeout=timeout*1000, state="visible")
    except PlaywrightTimeoutError:
        logger.warning(f"Timeout waiting for element: {selector}")
        return None

def safe_click(page, selector, timeout=5):
    """Safely click an element with multiple fallback methods."""
    try:
        element = wait_for_element(page, selector, timeout=timeout)
        if element:
            element.click()
            return True
        return False
    except Exception as e:
        try:
            # Try JavaScript click as fallback
            page.evaluate(f"document.querySelector('{selector}').click()")
            return True
        except Exception as js_e:
            logger.error(f"Failed to click element {selector}: {str(e)}, JS error: {str(js_e)}")
            return False

def scroll_to_element(page, selector):
    """Scroll to make an element visible."""
    try:
        element = page.query_selector(selector)
        if element:
            element.scroll_into_view_if_needed()
            time.sleep(0.3)  # Short pause for scroll to complete
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to scroll to element {selector}: {str(e)}")
        return False
        
def close_browser(browser_info):
    """Properly close the browser and all associated resources."""
    if not browser_info:
        return
        
    try:
        if "page" in browser_info:
            browser_info["page"].close()
    except:
        pass
        
    try:
        if "context" in browser_info:
            browser_info["context"].close()
    except:
        pass
        
    try:
        if "browser" in browser_info:
            browser_info["browser"].close()
    except:
        pass
        
    try:
        if "playwright" in browser_info:
            browser_info["playwright"].stop()
    except:
        pass

def force_click(page, element_or_selector, timeout=5, retries=3):
    """
    Forcefully click an element using multiple strategies with retries.
    Works around intercepted clicks and other click-related issues.
    
    Args:
        page: Playwright page object
        element_or_selector: Either a Playwright ElementHandle or a CSS selector string
        timeout: Timeout in seconds for click operations
        retries: Number of times to retry if click fails
    
    Returns:
        bool: True if click succeeded, False otherwise
    """
    for attempt in range(retries):
        try:
            # Get the element if a selector was passed
            element = element_or_selector
            if isinstance(element_or_selector, str):
                element = page.query_selector(element_or_selector)
                if not element:
                    logger.warning(f"Element not found with selector: {element_or_selector}")
                    if attempt < retries - 1:
                        time.sleep(1)
                        continue
                    return False
            
            # Strategy 1: Force click with Playwright
            try:
                element.click(force=True, timeout=timeout*1000)
                return True
            except Exception as e:
                logger.info(f"Force click failed: {str(e)}")
                
            # Strategy 2: JavaScript click
            try:
                page.evaluate("arguments[0].click()", element)
                return True
            except Exception as e:
                logger.info(f"JS click failed: {str(e)}")
                
            # Strategy 3: Dispatch mouse event
            try:
                page.evaluate("""
                    (element) => {
                        const event = new MouseEvent('click', {
                            view: window,
                            bubbles: true,
                            cancelable: true
                        });
                        element.dispatchEvent(event);
                    }
                """, element)
                time.sleep(0.5)
                return True
            except Exception as e:
                logger.info(f"Event dispatch failed: {str(e)}")
                
            # Strategy 4: Try clicking the center of the element using page.mouse
            try:
                # Get element position and dimensions
                box = element.bounding_box()
                if box:
                    center_x = box['x'] + box['width'] / 2
                    center_y = box['y'] + box['height'] / 2
                    
                    # Scroll to ensure element is in view
                    element.scroll_into_view_if_needed()
                    time.sleep(0.3)
                    
                    # Click at the center coordinates
                    page.mouse.click(center_x, center_y)
                    return True
            except Exception as e:
                logger.info(f"Mouse position click failed: {str(e)}")
            
            if attempt < retries - 1:
                logger.info(f"All click strategies failed, retrying ({attempt+1}/{retries})")
                time.sleep(1)
            else:
                logger.warning(f"Failed to click element after {retries} attempts")
                
        except Exception as e:
            logger.error(f"Error in force_click: {str(e)}")
            if attempt < retries - 1:
                time.sleep(1)
            
    return False
