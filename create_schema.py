"""
SQL Schema Creation Script
Creates the database schema for Pinterest pins
"""

import os
import sqlite3


def create_schema(db_path: str = "data/output.db"):
    """
    Create the database schema
    
    Args:
        db_path: Path to SQLite database file
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Drop table if exists (for clean start)
    cursor.execute("DROP TABLE IF EXISTS pinterest_pins;")
    
    # Create table
    create_table_sql = """
    CREATE TABLE pinterest_pins (
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
    
    # Create indexes
    cursor.execute("""
        CREATE INDEX idx_pin_link ON pinterest_pins(pin_link);
    """)
    
    cursor.execute("""
        CREATE INDEX idx_author ON pinterest_pins(author);
    """)
    
    cursor.execute("""
        CREATE INDEX idx_scraped_at ON pinterest_pins(scraped_at);
    """)
    
    conn.commit()
    
    # Verify table creation
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print(f"Tables created: {[table[0] for table in tables]}")
    
    cursor.execute("PRAGMA table_info(pinterest_pins);")
    columns = cursor.fetchall()
    print("\nTable schema:")
    print("Column Name | Type | Not Null | Default")
    print("-" * 50)
    for col in columns:
        print(f"{col[1]:<12} | {col[2]:<10} | {bool(col[3]):<8} | {col[4] or 'None'}")
    
    conn.close()
    print(f"\nSchema created successfully at {db_path}")


if __name__ == "__main__":
    create_schema("data/output.db")

