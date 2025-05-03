"""
Script to install required dependencies for the Walmart Leasing Bot
"""

import subprocess
import sys
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def install_requirements():
    """Install required packages from requirements.txt"""
    try:
        requirements_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "requirements.txt")
        
        if not os.path.exists(requirements_file):
            logger.error(f"Requirements file not found at: {requirements_file}")
            return False
            
        logger.info("Installing required packages...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_file])
        logger.info("All requirements installed successfully!")
        return True
    except Exception as e:
        logger.error(f"Error installing requirements: {e}")
        return False

def install_single_package(package):
    """Install a single package using pip"""
    try:
        logger.info(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        logger.info(f"{package} installed successfully!")
        return True
    except Exception as e:
        logger.error(f"Error installing {package}: {e}")
        return False

if __name__ == "__main__":
    # Install all requirements if no arguments provided
    if len(sys.argv) == 1:
        install_requirements()
    else:
        # Install specific packages from command line arguments
        for package in sys.argv[1:]:
            install_single_package(package)
