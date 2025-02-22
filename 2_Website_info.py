import os
import re
from typing import Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from utils import (
    logger, 
    execute_query, 
    insert_task,
    update_task_status
)

CHATGPT_API_KEY = os.getenv('CHATGPT_API_KEY')
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_website_content(url: str) -> Optional[str]:
    """Get website content with retry logic."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"Error fetching website {url}: {str(e)}", exc_info=True)
        return None

def find_about_page_url(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """Find the about page URL from navigation links."""
    about_keywords = ['about', 'about us', 'about-us', 'our story', 'who we are']
    
    for link in soup.find_all('a', href=True):
        text = link.get_text().lower().strip()
        href = link['href'].lower()
        
        if any(keyword in text or keyword in href for keyword in about_keywords):
            return urljoin(base_url, link['href'])
    
    return None

def extract_about_content(soup: BeautifulSoup) -> str:
    """Extract relevant content from the about page."""
    # Common content container classes/IDs
    content_selectors = [
        'main', 'article', '.content', '#content',
        '.about-content', '#about-content'
    ]
    
    for selector in content_selectors:
        content = soup.select_one(selector)
        if content:
            # Remove script, style, and nav elements
            for element in content.find_all(['script', 'style', 'nav']):
                element.decompose()
            
            # Get text and clean it
            text = ' '.join(content.get_text().split())
            return text
    
    # Fallback: get all paragraph text
    paragraphs = soup.find_all('p')
    return ' '.join(p.get_text().strip() for p in paragraphs if p.get_text().strip())

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=4, max=10))
def generate_blurb(about_text: str) -> Optional[str]:
    """Generate a blurb using ChatGPT API."""
    if not CHATGPT_API_KEY:
        return None
        
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {CHATGPT_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "gpt-3.5-turbo",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a professional content writer specializing in jewelry store descriptions."
                    },
                    {
                        "role": "user",
                        "content": f"Create a concise, engaging 2-3 sentence description for a jewelry store based on this information: {about_text}"
                    }
                ],
                "max_tokens": 150,
                "temperature": 0.7
            }
        )
        response.raise_for_status()
        return response.json()['choices'][0]['message']['content'].strip()
        
    except Exception as e:
        logger.error(f"Error generating blurb: {str(e)}", exc_info=True)
        return None

def process_website(url: str, task_id: int) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Process a website to extract about page content and generate blurb."""
    try:
        update_task_status(task_id, "processing")
        
        # Get main page content
        main_content = get_website_content(url)
        if not main_content:
            update_task_status(task_id, "failed", "Failed to fetch website content")
            return None, None, None, None
            
        soup = BeautifulSoup(main_content, 'html.parser')
        
        # Find about page
        about_url = find_about_page_url(soup, url)
        if about_url:
            about_content = get_website_content(about_url)
            if about_content:
                soup = BeautifulSoup(about_content, 'html.parser')
        
        # Extract content from website
        website_content = extract_about_content(soup)
        if not website_content:
            update_task_status(task_id, "failed", "No content found on website")
            return None, None, None, None
            
        # Generate content using extracted website information
        blurb_prompt = f"""
You are a professional content writer for jewelry stores. Using only the following content from the jeweler's website, 
write a natural, engaging 100-word description of the business. Focus on their history, expertise, and specialties.
Maintain their authentic voice while making the content concise and compelling. Do not invent or assume any details 
not present in the source material.

Source content:
{website_content}
"""

        differentiator_prompt = f"""
You are a professional content writer for jewelry stores. Using only the following content from the jeweler's website, 
write a focused 100-word passage about what makes this jeweler unique compared to others. Highlight specific services, 
approaches, or philosophies that set them apart. Focus only on concrete differentiators mentioned in their content, 
avoiding generic claims or assumptions.

Source content:
{website_content}
"""

        post_content = generate_blurb(blurb_prompt)
        what_makes_us_different = generate_blurb(differentiator_prompt)
        
        if not post_content or not what_makes_us_different:
            update_task_status(task_id, "failed", "Failed to generate content")
            return None, None, None, None

        about_text = f"{post_content}\n\nWhat Makes Us Different:\n{what_makes_us_different}"
        blurb = post_content

        update_task_status(task_id, "completed")
        return about_text, blurb, post_content, what_makes_us_different
        
    except Exception as e:
        error_msg = f"Error processing website {url}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        update_task_status(task_id, "failed", error_msg)
        return None, None, None, None

def main():
    """Main function to process websites and extract information."""
    try:
        while True:
            # Get next batch of listings to process
            listings = execute_query("""
                SELECT listing_id, website_url 
                FROM listings 
                WHERE website_url IS NOT NULL 
                    AND website_url != '' 
                    AND (post_content IS NULL OR what_makes_us_different IS NULL)
                LIMIT 10
            """)
            
            if not listings:
                logger.info("No more listings to process")
                break
            
            for listing_id, website_url in listings:
                logger.info(f"Processing website: {website_url}")
                
                # Create a task for this website
                task_id = insert_task(
                    "process_website",
                    {
                        "listing_id": listing_id,
                        "website_url": website_url
                    }
                )
                
                if not task_id:
                    logger.error(f"Failed to create task for website {website_url}")
                    continue
                
                about_text, blurb, post_content, what_makes_us_different = process_website(website_url, task_id)
                
                if about_text:
                    execute_query(
                        """
                        UPDATE listings 
                        SET post_content = %s,
                            what_makes_us_different = %s,
                            about_page_text = %s, 
                            blurb = %s,
                            updated_at = NOW()
                        WHERE listing_id = %s
                        """,
                        (post_content, what_makes_us_different, about_text, blurb, listing_id)
                    )
                    logger.info(f"Updated listing {listing_id} with website content")
                else:
                    logger.warning(f"No content found for listing {listing_id}")
                
    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
