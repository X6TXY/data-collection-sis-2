"""
SQLite Database Loader Module
Loads cleaned data into SQLite database
"""

import json
import logging
import os
import sqlite3
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_database_schema(db_path: str = "data/output.db"):
    """
    Create database table schema
    
    Args:
        db_path: Path to SQLite database file
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS pinterest_pins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        description TEXT,
        image_url TEXT,
        pin_link TEXT UNIQUE,
        board_name TEXT,
        author TEXT,
        save_count INTEGER DEFAULT 0,
        scraped_at TEXT NOT NULL,
        loaded_at TEXT NOT NULL
    );
    """
    
    cursor.execute(create_table_sql)
    
    # Create index on pin_link for faster lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pin_link ON pinterest_pins(pin_link);
    """)
    
    conn.commit()
    conn.close()
    
    logger.info(f"Database schema created at {db_path}")


def load_cleaned_data(input_file: str = "data/cleaned_pins.json") -> list:
    """
    Load cleaned data from JSON file
    
    Args:
        input_file: Path to cleaned data JSON file
    
    Returns:
        List of cleaned pin dictionaries
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


def insert_data_to_db(data: list, db_path: str = "data/output.db", replace_existing: bool = True):
    """
    Insert cleaned data into SQLite database
    
    Args:
        data: List of cleaned pin dictionaries
        db_path: Path to SQLite database file
        replace_existing: If True, replace existing records with same pin_link
    """
    if not data:
        logger.warning("No data to insert")
        return
    
    # Ensure database schema exists
    create_database_schema(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    loaded_at = datetime.now().isoformat()
    inserted_count = 0
    updated_count = 0
    error_count = 0
    
    insert_sql = """
    INSERT OR REPLACE INTO pinterest_pins 
    (title, description, image_url, pin_link, board_name, author, save_count, scraped_at, loaded_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    for pin in data:
        try:
            values = (
                pin.get('title', ''),
                pin.get('description', ''),
                pin.get('image_url', ''),
                pin.get('pin_link', ''),
                pin.get('board_name', ''),
                pin.get('author', ''),
                pin.get('save_count', 0),
                pin.get('scraped_at', ''),
                loaded_at
            )
            
            # Check if record exists
            cursor.execute("SELECT id FROM pinterest_pins WHERE pin_link = ?", (pin.get('pin_link', ''),))
            exists = cursor.fetchone()
            
            cursor.execute(insert_sql, values)
            
            if exists:
                updated_count += 1
            else:
                inserted_count += 1
                
        except sqlite3.IntegrityError as e:
            logger.warning(f"Integrity error for pin {pin.get('pin_link', 'N/A')}: {e}")
            error_count += 1
        except Exception as e:
            logger.error(f"Error inserting pin: {e}")
            error_count += 1
    
    conn.commit()
    conn.close()
    
    logger.info(f"Data loading completed: {inserted_count} inserted, {updated_count} updated, {error_count} errors")


def get_record_count(db_path: str = "data/output.db") -> int:
    """
    Get total number of records in database
    
    Args:
        db_path: Path to SQLite database file
    
    Returns:
        Number of records
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM pinterest_pins")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        logger.error(f"Error getting record count: {e}")
        return 0


def verify_database(db_path: str = "data/output.db") -> dict:
    """
    Verify database contents and return statistics
    
    Args:
        db_path: Path to SQLite database file
    
    Returns:
        Dictionary with database statistics
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM pinterest_pins")
        total_count = cursor.fetchone()[0]
        
        # Get count with images
        cursor.execute("SELECT COUNT(*) FROM pinterest_pins WHERE image_url != ''")
        with_images = cursor.fetchone()[0]
        
        # Get average save count
        cursor.execute("SELECT AVG(save_count) FROM pinterest_pins")
        avg_saves = cursor.fetchone()[0] or 0
        
        # Get sample records
        cursor.execute("SELECT title, author, save_count FROM pinterest_pins LIMIT 5")
        samples = cursor.fetchall()
        
        conn.close()
        
        stats = {
            'total_records': total_count,
            'records_with_images': with_images,
            'average_save_count': round(avg_saves, 2),
            'sample_records': samples
        }
        
        logger.info(f"Database verification: {total_count} total records")
        return stats
        
    except Exception as e:
        logger.error(f"Error verifying database: {e}")
        return {}


if __name__ == "__main__":
    # Test loading
    cleaned_data = load_cleaned_data("data/cleaned_pins.json")
    insert_data_to_db(cleaned_data, "data/output.db")
    stats = verify_database("data/output.db")
    print(f"\nDatabase loaded successfully!")
    print(f"Total records: {stats.get('total_records', 0)}")

