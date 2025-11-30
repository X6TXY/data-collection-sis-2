"""
Complete Pipeline Runner
Runs the entire pipeline: scraping -> cleaning -> loading
Useful for testing before setting up Airflow
"""

import os
import sys

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import logging

from cleaner import clean_data, load_raw_data, save_cleaned_data
from loader import (create_database_schema, insert_data_to_db,
                    load_cleaned_data, verify_database)
from scraper import save_raw_data, scrape_pinterest

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_pipeline(search_query: str = "data science", max_pins: int = 150):
    """
    Run the complete data pipeline
    
    Args:
        search_query: Pinterest search query
        max_pins: Maximum number of pins to collect
    """
    logger.info("=" * 60)
    logger.info("Starting Pinterest Data Pipeline")
    logger.info("=" * 60)
    
    try:
        # Step 1: Scraping
        logger.info("\n[STEP 1] Scraping Pinterest data...")
        pins = scrape_pinterest(search_query=search_query, max_pins=max_pins)
        save_raw_data(pins, "data/raw_pins.json")
        logger.info(f"✓ Scraped {len(pins)} pins")
        
        # Step 2: Cleaning
        logger.info("\n[STEP 2] Cleaning data...")
        raw_data = load_raw_data("data/raw_pins.json")
        cleaned_data = clean_data(raw_data)
        save_cleaned_data(cleaned_data, "data/cleaned_pins.json")
        logger.info(f"✓ Cleaned {len(cleaned_data)} records")
        
        # Step 3: Loading
        logger.info("\n[STEP 3] Loading data into database...")
        create_database_schema("data/output.db")
        cleaned_data = load_cleaned_data("data/cleaned_pins.json")
        insert_data_to_db(cleaned_data, "data/output.db")
        stats = verify_database("data/output.db")
        logger.info(f"✓ Loaded {stats.get('total_records', 0)} records into database")
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 60)
        logger.info(f"Raw pins collected: {len(pins)}")
        logger.info(f"Cleaned records: {len(cleaned_data)}")
        logger.info(f"Database records: {stats.get('total_records', 0)}")
        logger.info(f"Average save count: {stats.get('average_save_count', 0)}")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"\nPipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Pinterest data pipeline')
    parser.add_argument('--query', type=str, default='data science',
                        help='Pinterest search query (default: data science)')
    parser.add_argument('--max-pins', type=int, default=150,
                        help='Maximum number of pins to collect (default: 150)')
    
    args = parser.parse_args()
    
    success = run_pipeline(search_query=args.query, max_pins=args.max_pins)
    sys.exit(0 if success else 1)

