"""
Setup script for installing Playwright and its dependencies
"""

import subprocess
import sys
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def install_playwright():
    """Install Playwright and its dependencies."""
    try:
        logger.info("Installing Playwright...")

        # Step 1: Install the playwright package
        subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright"])
        logger.info("Playwright package installed successfully")

        # Step 2: Install browser binaries
        subprocess.check_call(
            [sys.executable, "-m", "playwright", "install", "chromium"]
        )
        logger.info("Chromium browser installed successfully")

        logger.info("Playwright setup completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error during installation: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


def test_playwright():
    """Test that Playwright is working properly."""
    try:
        logger.info("Testing Playwright installation...")

        # Create a simple test script
        test_script = """
from playwright.sync_api import sync_playwright

def test():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto('https://www.example.com')
        title = page.title()
        print(f"Page title: {title}")
        browser.close()
        return True
    return False

if test():
    print("PLAYWRIGHT_TEST_SUCCESS")
else:
    print("PLAYWRIGHT_TEST_FAILURE")
"""

        # Write the test script to a file
        with open("test_playwright.py", "w") as f:
            f.write(test_script)

        # Run the test script
        result = subprocess.check_output(
            [sys.executable, "test_playwright.py"], text=True
        )

        # Remove the test script
        os.remove("test_playwright.py")

        if "PLAYWRIGHT_TEST_SUCCESS" in result:
            logger.info("Playwright test successful!")
            return True
        else:
            logger.error("Playwright test failed")
            return False
    except Exception as e:
        logger.error(f"Error testing Playwright: {e}")
        return False


if __name__ == "__main__":
    if install_playwright():
        test_playwright()
    else:
        sys.exit(1)
