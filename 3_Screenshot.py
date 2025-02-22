import os
import time
import urllib.parse
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import requests
from requests.auth import HTTPBasicAuth

from utils import (
    logger, 
    execute_query, 
    get_wp_config,
    insert_task,
    update_task_status
)

SCREENSHOTS_DIR = "screenshots"
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

def get_screenshot_filename(url: str) -> str:
    """Generate screenshot filename from URL."""
    # Parse the URL and get the domain
    parsed_url = urllib.parse.urlparse(url)
    domain = parsed_url.netloc.replace('www.', '')
    # Remove .com, .au, etc and convert to lowercase
    base_name = domain.split('.')[0].lower()
    return f"{base_name}-homepage.jpg"

def upload_to_media_library(file_path: str, country: str) -> Optional[str]:
    """Upload screenshot to the WordPress media library for the specified country."""
    try:
        # Get WordPress configuration for the country
        wp_config = get_wp_config(country)
        if not wp_config:
            return None

        # Prepare the API endpoint
        api_url = f"{wp_config['site']}/wp-json/wp/v2/media"
        
        # Prepare the file
        filename = os.path.basename(file_path)
        with open(file_path, 'rb') as f:
            files = {
                'file': (filename, f, 'image/jpeg')
            }
            
            # Upload to WordPress
            response = requests.post(
                api_url,
                files=files,
                auth=HTTPBasicAuth(wp_config['username'], wp_config['password']),
                headers={
                    'Content-Disposition': f'attachment; filename="{filename}"'
                }
            )
            
            response.raise_for_status()
            
            # Get the URL of the uploaded media
            media_data = response.json()
            if 'source_url' in media_data:
                logger.info(f"Successfully uploaded {filename} to {country} WordPress site")
                return media_data['source_url']
            else:
                logger.error(f"No source URL in WordPress response for {filename}")
                return None
                
    except requests.exceptions.RequestException as e:
        logger.error(f"Error uploading to WordPress: {str(e)}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error during upload: {str(e)}", exc_info=True)
        return None

def take_screenshot(url: str) -> Optional[str]:
    """Take a screenshot of the website."""
    driver = None
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1400,800')
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        
        # Set viewport size
        driver.set_window_size(1400, 800)
        
        # Load the page
        driver.get(url)
        
        # Wait for page to load (adjust timeout as needed)
        time.sleep(5)  # Allow time for dynamic content to load
        
        # Generate filename
        filename = get_screenshot_filename(url)
        filepath = os.path.join(SCREENSHOTS_DIR, filename)
        
        # Take screenshot
        driver.save_screenshot(filepath)
        
        return filepath
        
    except Exception as e:
        logger.error(f"Error taking screenshot of {url}: {str(e)}", exc_info=True)
        return None
    finally:
        if driver:
            driver.quit()

def main():
    """Process websites and take screenshots."""
    try:
        while True:
            # Get listings that need screenshots
            listings = execute_query("""
                SELECT l.listing_id, l.website_url, l.country 
                FROM listings l
                WHERE l.website_url IS NOT NULL 
                    AND l.website_url != '' 
                    AND l.post_images IS NULL
                LIMIT 5
            """)
            
            if not listings:
                logger.info("No more listings need screenshots")
                break
            
            for listing_id, website_url, country in listings:
                logger.info(f"Processing screenshot for: {website_url}")
                
                # Create task for screenshot
                task_id = insert_task(
                    "capture_screenshot",
                    {
                        "listing_id": listing_id,
                        "website_url": website_url,
                        "country": country
                    }
                )
                
                if not task_id:
                    logger.error(f"Failed to create task for website {website_url}")
                    continue
                
                update_task_status(task_id, "processing")
                
                # Take screenshot
                screenshot_path = take_screenshot(website_url)
                if not screenshot_path:
                    update_task_status(task_id, "failed", "Failed to capture screenshot")
                    continue
                
                # Upload to media library
                media_url = upload_to_media_library(screenshot_path, country)
                if not media_url:
                    update_task_status(task_id, "failed", "Failed to upload to media library")
                    continue
                
                # Update database
                execute_query(
                    """
                    UPDATE listings 
                    SET post_images = %s,
                        updated_at = NOW()
                    WHERE listing_id = %s
                    """,
                    (media_url, listing_id)
                )
                
                logger.info(f"Updated listing {listing_id} with screenshot: {media_url}")
                
                # Clean up local file
                os.remove(screenshot_path)
                
                update_task_status(task_id, "completed")
                
    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
