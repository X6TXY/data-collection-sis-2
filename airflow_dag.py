"""
Apache Airflow DAG for Pinterest Data Pipeline
Scrapes, cleans, and loads Pinterest data into SQLite database
Runs no more than once per day
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from cleaner import clean_data, load_raw_data, save_cleaned_data
from loader import (create_database_schema, insert_data_to_db,
                    load_cleaned_data, verify_database)
from scraper import save_raw_data, scrape_pinterest

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Define the DAG
dag = DAG(
    'pinterest_data_pipeline',
    default_args=default_args,
    description='Pinterest scraping, cleaning, and loading pipeline',
    schedule_interval=timedelta(days=1),  # Run once per day (24 hours)
    catchup=False,
    tags=['pinterest', 'scraping', 'etl'],
)


def scrape_task(**context):
    """Task to scrape Pinterest data"""
    import logging
    logging.info("Starting Pinterest scraping task")
    
    try:
        # Scrape Pinterest pins
        pins = scrape_pinterest(search_query="data science", max_pins=150)
        
        # Save raw data
        save_raw_data(pins, "data/raw_pins.json")
        
        logging.info(f"Scraping completed successfully. Collected {len(pins)} pins.")
        return len(pins)
    except Exception as e:
        logging.error(f"Scraping task failed: {e}")
        raise


def clean_task(**context):
    """Task to clean and preprocess data"""
    import logging
    logging.info("Starting data cleaning task")
    
    try:
        # Load raw data
        raw_data = load_raw_data("data/raw_pins.json")
        
        # Clean data
        cleaned_data = clean_data(raw_data)
        
        # Save cleaned data
        save_cleaned_data(cleaned_data, "data/cleaned_pins.json")
        
        logging.info(f"Cleaning completed successfully. {len(cleaned_data)} records after cleaning.")
        return len(cleaned_data)
    except Exception as e:
        logging.error(f"Cleaning task failed: {e}")
        raise


def load_task(**context):
    """Task to load data into SQLite database"""
    import logging
    logging.info("Starting data loading task")
    
    try:
        # Ensure schema exists
        create_database_schema("data/output.db")
        
        # Load cleaned data
        cleaned_data = load_cleaned_data("data/cleaned_pins.json")
        
        # Insert into database
        insert_data_to_db(cleaned_data, "data/output.db")
        
        # Verify database
        stats = verify_database("data/output.db")
        
        logging.info(f"Loading completed successfully. {stats.get('total_records', 0)} records in database.")
        return stats
    except Exception as e:
        logging.error(f"Loading task failed: {e}")
        raise


# Define tasks
scrape = PythonOperator(
    task_id='scrape_pinterest',
    python_callable=scrape_task,
    dag=dag,
)

clean = PythonOperator(
    task_id='clean_data',
    python_callable=clean_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load_to_database',
    python_callable=load_task,
    dag=dag,
)

# Set task dependencies: scrape -> clean -> load
scrape >> clean >> load

