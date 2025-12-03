# Pinterest Data Pipeline

## Project Overview
This project implements a complete mini data pipeline for scraping, cleaning, and storing Pinterest data into a SQLite database. The pipeline is automated using **Apache Airflow** and runs at most once per day. It demonstrates a full workflow “from website to database,” including handling dynamic, JavaScript-rendered content using **Playwright**.

**Key Features:**
- Scrapes Pinterest pins for a given search query
- Handles dynamic content (infinite scroll, lazy loading)
- Cleans and normalizes data
- Stores cleaned data in a SQLite database with a clear schema
- Automated with Airflow DAG including logging and retries

**Chosen Website:** [Pinterest](https://www.pinterest.com)

---

## Project Structure

```
project/
│   README.md
│   .gitignore
│   requirements.txt
│   SIS2_Assignment.docx
│   airflow_dag.py
│   create_schema.py
│   run_pipeline.py
├── src/
│   ├── scraper.py
│   ├── cleaner.py
│   └── loader.py
└── data/
    └── output.db
```


## Dependencies

Install required Python packages:
```bash
pip install -r requirements.txt
```

Requirements include:
```
playwright==1.40.0 — for web scraping dynamic content
pandas==2.1.3 — for data processing
apache-airflow==2.7.3 — for task orchestration
python-dateutil==2.8.2 — utility functions
sqlite3 — included in Python standard library
Note: After installing Playwright, run playwright install to download browser binaries.
```


## Running the Pipeline
## 1. Run the complete pipeline manually
```
You can test the pipeline end-to-end before setting up Airflow:
python run_pipeline.py --query "data science" --max-pins 150
--query specifies the Pinterest search query
--max-pins sets the maximum number of pins to scrape
The pipeline will:
Scrape pins from Pinterest
Clean and preprocess data
Save cleaned data to data/cleaned_pins.json
Load data into data/output.db
## 2. Airflow DAG
The Airflow DAG airflow_dag.py automates the workflow:
Steps included in DAG:
Scraping (scrape_pinterest)
Cleaning (clean_data)
Loading to SQLite (load_to_database)
```


##Run DAG:
airflow db init
airflow scheduler
airflow webserver
DAG name: pinterest_data_pipeline
Schedule: once per day (schedule_interval=timedelta(days=1))
Logs can be accessed in the Airflow web UI to verify execution
SQLite Database
Database file: data/output.db
Table: pinterest_pins

## SQLite Database Schema

| Column Name | Type    | Not Null | Default       |
|------------|---------|----------|---------------|
| id         | INTEGER | Yes      | AUTOINCREMENT |
| title      | TEXT    | Yes      | -             |
| description| TEXT    | No       | -             |
| image_url  | TEXT    | No       | -             |
| pin_link   | TEXT    | No       | UNIQUE        |
| board_name | TEXT    | No       | -             |
| author     | TEXT    | No       | -             |
| save_count | INTEGER | No       | 0             |
| scraped_at | TEXT    | Yes      | -             |
| loaded_at  | TEXT    | Yes      | -             |

  
##  Indexes:
idx_pin_link on pin_link
idx_author on author
idx_scraped_at on scraped_at
You can verify database contents using the verify_database function in loader.py.


## Data Cleaning
Key cleaning steps in cleaner.py:
Remove duplicate pins based on pin_link or image_url
Handle missing values with defaults
Normalize text fields (remove whitespace, special characters)
Convert types (save_count → integer)
Ensure at least 100 records after cleaning

## Expected Output
After running the pipeline:
data/raw_pins.json — raw scraped pins
data/cleaned_pins.json — cleaned and normalized pins
data/output.db — SQLite database with all cleaned pins

Sample stats after successful run:
Total pins collected: 150
Cleaned records: 145
Database records: 145
Average save count: 120
Notes
Make sure both team members understand the pipeline for the oral defense
The Airflow DAG demonstrates retries, logging, and scheduled execution
You must not use AI tools for generating this code according to the assignment rules


## References
```
Pinterest — website used for scraping
Playwright Python
Apache Airflow
Python sqlite3 module
```

