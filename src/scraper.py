"""
Pinterest Scraper using Playwright
Scrapes pins from Pinterest search results with dynamic content loading
"""

import json
import logging
import time
from datetime import datetime

from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def scrape_pinterest(search_query: str = "data science", max_pins: int = 150) -> list:
    """
    Scrape Pinterest pins from search results
    
    Args:
        search_query: Search term to query on Pinterest
        max_pins: Maximum number of pins to collect
    
    Returns:
        List of dictionaries containing pin data
    """
    pins_data = []
    
    try:
        with sync_playwright() as p:
            logger.info(f"Starting browser for Pinterest scraping - Query: {search_query}")
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()
            
            # Navigate to Pinterest search
            search_url = f"https://www.pinterest.com/search/pins/?q={search_query.replace(' ', '%20')}"
            logger.info(f"Navigating to: {search_url}")
            page.goto(search_url, wait_until='networkidle', timeout=60000)
            
            # Wait for initial content to load
            time.sleep(3)
            
            # Scroll and collect pins
            scroll_count = 0
            max_scrolls = 20
            last_height = 0
            consecutive_no_change = 0
            
            logger.info("Starting to scroll and collect pins...")
            
            while len(pins_data) < max_pins and scroll_count < max_scrolls:
                # Get current pins on page - try multiple selectors
                pins = page.query_selector_all('[data-test-id="pin"]')
                if not pins or len(pins) == 0:
                    # Try alternative selectors
                    pins = page.query_selector_all('div[role="listitem"]')
                if not pins or len(pins) == 0:
                    pins = page.query_selector_all('[class*="pin"]')
                
                current_pin_count = len(pins_data)
                
                for pin in pins:
                    if len(pins_data) >= max_pins:
                        break
                    
                    try:
                        pin_data = extract_pin_data(pin, page)
                        if pin_data:
                            # Check for duplicates based on pin_link or image_url
                            is_duplicate = False
                            for existing in pins_data:
                                if (pin_data.get('pin_link') and 
                                    existing.get('pin_link') == pin_data.get('pin_link')):
                                    is_duplicate = True
                                    break
                                if (pin_data.get('image_url') and 
                                    existing.get('image_url') == pin_data.get('image_url')):
                                    is_duplicate = True
                                    break
                            
                            if not is_duplicate:
                                pins_data.append(pin_data)
                                logger.info(f"Collected pin {len(pins_data)}: {pin_data.get('title', 'N/A')[:50]}")
                    except Exception as e:
                        logger.warning(f"Error extracting pin data: {e}")
                        continue
                
                # If no new pins were collected, increment no-change counter
                if len(pins_data) == current_pin_count:
                    consecutive_no_change += 1
                else:
                    consecutive_no_change = 0
                
                # Scroll down
                current_height = page.evaluate("document.body.scrollHeight")
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)  # Wait for new content to load
                
                new_height = page.evaluate("document.body.scrollHeight")
                
                if new_height == last_height:
                    consecutive_no_change += 1
                    if consecutive_no_change >= 3:
                        logger.info("No more content loading, stopping scroll")
                        break
                else:
                    consecutive_no_change = 0
                
                last_height = new_height
                scroll_count += 1
                
                logger.info(f"Scroll {scroll_count}: Collected {len(pins_data)} pins so far")
            
            browser.close()
            logger.info(f"Scraping completed. Total pins collected: {len(pins_data)}")
            
    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        raise
    
    return pins_data


def extract_pin_data(pin_element, page) -> dict:
    """
    Extract data from a single pin element
    
    Args:
        pin_element: Playwright element handle
        page: Playwright page object
    
    Returns:
        Dictionary with pin data
    """
    try:
        # Try multiple selector strategies for title
        title = ""
        title_selectors = [
            '[data-test-id="pinrep-title"]',
            'h3',
            '[class*="title"]',
            '[class*="Title"]'
        ]
        for selector in title_selectors:
            title_elem = pin_element.query_selector(selector)
            if title_elem:
                title = title_elem.inner_text().strip()
                if title:
                    break
        
        # Try multiple selector strategies for description
        description = ""
        desc_selectors = [
            '[data-test-id="pinrep-description"]',
            '[class*="description"]',
            '[class*="Description"]',
            'p'
        ]
        for selector in desc_selectors:
            desc_elem = pin_element.query_selector(selector)
            if desc_elem:
                description = desc_elem.inner_text().strip()
                if description and description != title:
                    break
        
        # Extract image URL - try multiple strategies
        image_url = ""
        img_elem = pin_element.query_selector('img')
        if img_elem:
            image_url = img_elem.get_attribute('src') or img_elem.get_attribute('data-src') or ""
            # Clean up image URL (remove size parameters if present)
            if image_url and '?' in image_url:
                image_url = image_url.split('?')[0]
        
        # Extract link - try multiple strategies
        pin_link = ""
        link_elem = pin_element.query_selector('a[href*="/pin/"]')
        if not link_elem:
            link_elem = pin_element.query_selector('a')
        if link_elem:
            pin_link = link_elem.get_attribute('href') or ""
            if pin_link and not pin_link.startswith('http'):
                pin_link = f"https://www.pinterest.com{pin_link}"
        
        # Extract board name
        board_name = ""
        board_selectors = [
            '[data-test-id="board-name"]',
            '[class*="board"]',
            '[class*="Board"]'
        ]
        for selector in board_selectors:
            board_elem = pin_element.query_selector(selector)
            if board_elem:
                board_name = board_elem.inner_text().strip()
                if board_name:
                    break
        
        # Extract author/username
        author = ""
        author_selectors = [
            '[data-test-id="username"]',
            '[class*="username"]',
            '[class*="Username"]',
            '[class*="user"]',
            'a[href*="/"]'
        ]
        for selector in author_selectors:
            author_elem = pin_element.query_selector(selector)
            if author_elem:
                author_text = author_elem.inner_text().strip()
                href = author_elem.get_attribute('href') or ""
                if author_text and '/@' in href:
                    author = author_text
                    break
        
        # Extract save count
        save_count = 0
        save_selectors = [
            '[data-test-id="save-count"]',
            '[class*="save"]',
            '[class*="Save"]'
        ]
        for selector in save_selectors:
            save_elem = pin_element.query_selector(selector)
            if save_elem:
                save_count_text = save_elem.inner_text().strip()
                save_count = parse_save_count(save_count_text)
                if save_count > 0:
                    break
        
        # Only return if we have at least a title or image
        if title or image_url:
            return {
                'title': title,
                'description': description,
                'image_url': image_url,
                'pin_link': pin_link,
                'board_name': board_name,
                'author': author,
                'save_count': save_count,
                'scraped_at': datetime.now().isoformat()
            }
    except Exception as e:
        logger.warning(f"Error extracting pin data: {e}")
    
    return None


def parse_save_count(text: str) -> int:
    """
    Parse save count text (e.g., "1.2K", "500", "5M") to integer
    
    Args:
        text: Save count text string
    
    Returns:
        Integer value of saves
    """
    if not text or text == "0":
        return 0
    
    text = text.upper().replace(',', '').strip()
    
    try:
        if 'K' in text:
            return int(float(text.replace('K', '')) * 1000)
        elif 'M' in text:
            return int(float(text.replace('M', '')) * 1000000)
        else:
            return int(text)
    except:
        return 0


def save_raw_data(data: list, output_file: str = "data/raw_pins.json"):
    """
    Save raw scraped data to JSON file
    
    Args:
        data: List of pin dictionaries
        output_file: Output file path
    """
    import os
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Raw data saved to {output_file}")


if __name__ == "__main__":
    # Test scraping
    pins = scrape_pinterest(search_query="data science", max_pins=150)
    save_raw_data(pins, "data/raw_pins.json")
    print(f"\nScraped {len(pins)} pins successfully!")

