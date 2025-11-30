"""
Data Cleaning and Preprocessing Module
Cleans and normalizes Pinterest pin data
"""

import json
import logging
import re
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_raw_data(input_file: str = "data/raw_pins.json") -> list:
    """
    Load raw scraped data from JSON file
    
    Args:
        input_file: Path to raw data JSON file
    
    Returns:
        List of raw pin dictionaries
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} records from {input_file}")
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {input_file}")
        return []
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return []


def remove_duplicates(data: list) -> list:
    """
    Remove duplicate pins based on pin_link or image_url
    
    Args:
        data: List of pin dictionaries
    
    Returns:
        List with duplicates removed
    """
    seen_links = set()
    seen_images = set()
    unique_data = []
    duplicates_count = 0
    
    for pin in data:
        pin_link = pin.get('pin_link', '')
        image_url = pin.get('image_url', '')
        
        # Check if we've seen this link or image before
        if pin_link and pin_link in seen_links:
            duplicates_count += 1
            continue
        if image_url and image_url in seen_images:
            duplicates_count += 1
            continue
        
        if pin_link:
            seen_links.add(pin_link)
        if image_url:
            seen_images.add(image_url)
        
        unique_data.append(pin)
    
    logger.info(f"Removed {duplicates_count} duplicate records")
    return unique_data


def normalize_text(text: str) -> str:
    """
    Normalize text by removing extra whitespace and special characters
    
    Args:
        text: Input text string
    
    Returns:
        Normalized text string
    """
    if not text:
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    # Remove leading/trailing whitespace
    text = text.strip()
    # Remove non-printable characters
    text = ''.join(char for char in text if char.isprintable() or char.isspace())
    
    return text


def handle_missing_values(pin: dict) -> dict:
    """
    Handle missing values by providing defaults
    
    Args:
        pin: Pin dictionary
    
    Returns:
        Pin dictionary with missing values handled
    """
    cleaned_pin = pin.copy()
    
    # Set defaults for missing values
    cleaned_pin['title'] = cleaned_pin.get('title') or "Untitled"
    cleaned_pin['description'] = cleaned_pin.get('description') or ""
    cleaned_pin['image_url'] = cleaned_pin.get('image_url') or ""
    cleaned_pin['pin_link'] = cleaned_pin.get('pin_link') or ""
    cleaned_pin['board_name'] = cleaned_pin.get('board_name') or "Unknown"
    cleaned_pin['author'] = cleaned_pin.get('author') or "Unknown"
    cleaned_pin['save_count'] = cleaned_pin.get('save_count', 0)
    
    return cleaned_pin


def convert_types(pin: dict) -> dict:
    """
    Convert data types to appropriate formats
    
    Args:
        pin: Pin dictionary
    
    Returns:
        Pin dictionary with converted types
    """
    cleaned_pin = pin.copy()
    
    # Ensure save_count is integer
    try:
        cleaned_pin['save_count'] = int(cleaned_pin.get('save_count', 0))
    except (ValueError, TypeError):
        cleaned_pin['save_count'] = 0
    
    # Ensure all string fields are strings
    string_fields = ['title', 'description', 'image_url', 'pin_link', 'board_name', 'author']
    for field in string_fields:
        if field in cleaned_pin:
            cleaned_pin[field] = str(cleaned_pin[field])
    
    # Validate and format scraped_at timestamp
    if 'scraped_at' in cleaned_pin:
        try:
            # Validate ISO format
            datetime.fromisoformat(cleaned_pin['scraped_at'].replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            cleaned_pin['scraped_at'] = datetime.now().isoformat()
    
    return cleaned_pin


def clean_data(data: list) -> list:
    """
    Main cleaning function that applies all cleaning steps
    
    Args:
        data: List of raw pin dictionaries
    
    Returns:
        List of cleaned pin dictionaries
    """
    logger.info(f"Starting data cleaning for {len(data)} records")
    
    if not data:
        logger.warning("No data to clean")
        return []
    
    # Step 1: Remove duplicates
    data = remove_duplicates(data)
    
    # Step 2: Handle missing values and normalize
    cleaned_data = []
    for pin in data:
        # Handle missing values
        pin = handle_missing_values(pin)
        
        # Normalize text fields
        pin['title'] = normalize_text(pin['title'])
        pin['description'] = normalize_text(pin['description'])
        pin['board_name'] = normalize_text(pin['board_name'])
        pin['author'] = normalize_text(pin['author'])
        
        # Convert types
        pin = convert_types(pin)
        
        # Only keep pins with at least title or image_url
        if pin.get('title') and pin['title'] != "Untitled" or pin.get('image_url'):
            cleaned_data.append(pin)
    
    logger.info(f"Data cleaning completed. {len(cleaned_data)} records after cleaning")
    
    # Validate minimum record count
    if len(cleaned_data) < 100:
        logger.warning(f"Only {len(cleaned_data)} records after cleaning. Minimum required: 100")
    
    return cleaned_data


def save_cleaned_data(data: list, output_file: str = "data/cleaned_pins.json"):
    """
    Save cleaned data to JSON file
    
    Args:
        data: List of cleaned pin dictionaries
        output_file: Output file path
    """
    import os
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Cleaned data saved to {output_file}")


if __name__ == "__main__":
    # Test cleaning
    raw_data = load_raw_data("data/raw_pins.json")
    cleaned_data = clean_data(raw_data)
    save_cleaned_data(cleaned_data, "data/cleaned_pins.json")
    print(f"\nCleaned {len(cleaned_data)} records successfully!")

