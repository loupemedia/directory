import logging
import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from tenacity import retry, stop_after_attempt, wait_exponential
import json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection pool
db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)

# Google Places to GeoDirectory category mapping
GOOGLE_TO_DIRECTORY_CATEGORY = {
    "Bangle Shop": ("Jewellery Gifts", 106),
    "Bead Store": ("Handmade Jewellery", 102),
    "Diamond Buyer": ("Diamond Jewellery", 93),
    "Diamond Dealer": ("Diamond Jewellery", 93),
    "Gemologist": ("Jewellery Appraiser", 153),
    "Goldsmith": ("Custom Rings", 94),
    "Jeweler": ("Independent Jeweller", 99),
    "Jewelry Appraiser": ("Jewellery Appraiser", 153),
    "Jewelry Buyer": ("Pawnbroker", 108),
    "Jewelry Designer": ("Designer Jewellery", 105),
    "Jewelry Engraver": ("Jewellery Gifts", 106),
    "Jewelry Equipment Supplier": ("Jewellery Manufacturers", 140),
    "Jewelry Exporter": ("Jewellery Manufacturers", 140),
    "Jewelry Manufacturer": ("Jewellery Manufacturers", 140),
    "Jewelry Repair Service": ("Jewellery Repair", 152),
    "Jewelry Store": ("Independent Jeweller", 99),
    "Silversmith": ("Handmade Jewellery", 102),
    "Watch Manufacturer": ("Watches", 96),
    "Watch Repair Service": ("Jewellery Repair", 152),
    "Watch Store": ("Watches", 96),
    "Wholesale Jeweler": ("Jewellery Manufacturers", 140),
}

# Database table definitions
TABLE_DEFINITIONS = {
    'postcodes': {
        'table_name': 'postcodes',
        'columns': {
            'id': 'SERIAL PRIMARY KEY',
            'postcode': 'VARCHAR(50)',
            'city': 'VARCHAR(100)',
            'region': 'VARCHAR(100)',
            'country': 'VARCHAR(50)',
            'last_scraped': 'TIMESTAMP',
            'created_at': 'TIMESTAMP DEFAULT NOW()',
            'updated_at': 'TIMESTAMP DEFAULT NOW()'
        }
    },
    'listings': {
        'table_name': 'listings',
        'columns': {
            'listing_id': 'SERIAL PRIMARY KEY',
            'google_place_id': 'VARCHAR(100)',
            'post_title': 'VARCHAR(255)',
            'post_content': 'TEXT',
            'post_status': 'VARCHAR(50)',
            'post_author': 'INT DEFAULT 1',
            'post_type': 'VARCHAR(50) DEFAULT \'gd_place\'',
            'post_date': 'TIMESTAMP DEFAULT NOW()',
            'post_modified': 'TIMESTAMP DEFAULT NOW()',
            'post_tags': 'TEXT',
            'post_category': 'TEXT',
            'default_category': 'INT',
            'featured': 'BOOLEAN DEFAULT FALSE',
            'street': 'VARCHAR(255)',
            'street2': 'VARCHAR(255)',
            'city': 'VARCHAR(100)',
            'region': 'VARCHAR(100)',
            'country': 'VARCHAR(50)',
            'zip': 'VARCHAR(50)',
            'latitude': 'DECIMAL(10,7)',
            'longitude': 'DECIMAL(10,7)',
            'claimed': 'BOOLEAN DEFAULT FALSE',
            'package_id': 'INT',
            'expire_date': 'TIMESTAMP',
            'ratings': 'DECIMAL(3,2)',
            'show_additional_information': 'BOOLEAN DEFAULT TRUE',
            'established': 'VARCHAR(50)',
            'video': 'TEXT',
            'business_hours': 'TEXT',
            'website': 'TEXT',
            'phone': 'VARCHAR(50)',
            'email': 'VARCHAR(255)',
            'facebook': 'TEXT',
            'instagram': 'TEXT',
            'what_makes_us_different': 'TEXT',
            'please_check_this_checkbox_if_category_is_not_list': 'BOOLEAN DEFAULT FALSE',
            'new_category': 'VARCHAR(255)',
            'neighbourhood': 'VARCHAR(255)',
            'post_images': 'TEXT',
            'created_at': 'TIMESTAMP DEFAULT NOW()',
            'updated_at': 'TIMESTAMP DEFAULT NOW()',
            'postcode_id': 'INT REFERENCES postcodes(id)',
            'name': 'VARCHAR(255)',
            'address': 'TEXT',
            'website_url': 'TEXT',
            'about_page_text': 'TEXT',
            'blurb': 'TEXT',
            'screenshot_url': 'TEXT',
            'wp_post_id': 'INT'
        }
    }
}

# Add after existing TABLE_DEFINITIONS dictionary
TASKS_TABLE_DEFINITION = {
    'table_name': 'tasks',
    'columns': {
        'task_id': 'SERIAL PRIMARY KEY',
        'task_type': 'VARCHAR(50)',
        'payload': 'JSONB',
        'status': 'VARCHAR(20)',
        'attempts': 'INT DEFAULT 0',
        'error_message': 'TEXT',
        'created_at': 'TIMESTAMP DEFAULT NOW()',
        'updated_at': 'TIMESTAMP DEFAULT NOW()',
        'started_at': 'TIMESTAMP',
        'completed_at': 'TIMESTAMP'
    }
}

SUBMISSIONS_TABLE_DEFINITION = {
    'table_name': 'submissions',
    'columns': {
        'submission_id': 'SERIAL PRIMARY KEY',
        'listing_id': 'INT REFERENCES listings(listing_id)',
        'submission_url': 'TEXT',
        'api_response': 'JSONB',
        'submission_status': 'VARCHAR(20)',
        'attempts': 'INT DEFAULT 0',
        'submitted_at': 'TIMESTAMP DEFAULT NOW()',
        'updated_at': 'TIMESTAMP DEFAULT NOW()',
        'error_message': 'TEXT'
    }
}

# WordPress configurations by country
WP_CONFIGS = {
    'AU': {
        'username': os.getenv('WP_AU_USERNAME'),
        'password': os.getenv('WP_AU_PASS'),
        'site': os.getenv('WP_AU_SITE'),
    },
    'CA': {
        'username': os.getenv('WP_CA_USERNAME'),
        'password': os.getenv('WP_CA_PASS'),
        'site': os.getenv('WP_CA_SITE'),
    },
    'IE': {
        'username': os.getenv('WP_IE_USERNAME'),
        'password': os.getenv('WP_IE_PASS'),
        'site': os.getenv('WP_IE_SITE'),
    },
    'NZ': {
        'username': os.getenv('WP_NZ_USERNAME'),
        'password': os.getenv('WP_NZ_PASS'),
        'site': os.getenv('WP_NZ_SITE'),
    },
    'IN': {
        'username': os.getenv('WP_IN_USERNAME'),
        'password': os.getenv('WP_IN_PASS'),
        'site': os.getenv('WP_IN_SITE'),
    },
    'UK': {
        'username': os.getenv('WP_UK_USERNAME'),
        'password': os.getenv('WP_UK_PASS'),
        'site': os.getenv('WP_UK_SITE'),
    },
    'US': {
        'username': os.getenv('WP_US_USERNAME'),
        'password': os.getenv('WP_US_PASS'),
        'site': os.getenv('WP_US_SITE'),
    },
    'SG': {
        'username': os.getenv('WP_SG_USERNAME'),
        'password': os.getenv('WP_SG_PASS'),
        'site': os.getenv('WP_SG_SITE'),
    },
    'ZA': {
        'username': os.getenv('WP_ZA_USERNAME'),
        'password': os.getenv('WP_ZA_PASS'),
        'site': os.getenv('WP_ZA_SITE'),
    }
}

def get_wp_config(country: str) -> Optional[Dict[str, str]]:
    """Get WordPress configuration for a specific country."""
    country_code = country.upper()
    if country_code not in WP_CONFIGS:
        logger.error(f"No WordPress configuration found for country: {country}")
        return None
    
    config = WP_CONFIGS[country_code]
    if not all(config.values()):
        logger.error(f"Incomplete WordPress configuration for country: {country}")
        return None
    
    return config

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_db_connection():
    """Get a database connection from the pool with retry logic."""
    try:
        conn = db_pool.getconn()
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}", exc_info=True)
        raise

def return_db_connection(conn):
    """Return a connection to the pool."""
    db_pool.putconn(conn)

def execute_query(query: str, params: tuple = None) -> Any:
    """Execute a database query with proper connection handling."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        return cur.fetchall()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Query execution error: {str(e)}", exc_info=True)
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            return_db_connection(conn)

def insert_task(task_type: str, payload: Optional[Dict] = None) -> Optional[int]:
    """Insert a new task and return its ID."""
    try:
        result = execute_query(
            """
            INSERT INTO tasks (task_type, payload, status)
            VALUES (%s, %s, 'queued')
            RETURNING task_id
            """,
            (task_type, json.dumps(payload) if payload else None)
        )
        return result[0][0] if result else None
    except Exception as e:
        logger.error(f"Error inserting task: {str(e)}", exc_info=True)
        return None

def update_task_status(task_id: int, status: str, error_message: Optional[str] = None) -> bool:
    """Update task status and error message."""
    try:
        execute_query(
            """
            UPDATE tasks 
            SET status = %s,
                error_message = %s,
                updated_at = NOW(),
                completed_at = CASE WHEN %s IN ('completed', 'failed') THEN NOW() ELSE completed_at END
            WHERE task_id = %s
            """,
            (status, error_message, status, task_id)
        )
        return True
    except Exception as e:
        logger.error(f"Error updating task status: {str(e)}", exc_info=True)
        return False

def insert_submission(listing_id: int, submission_url: str) -> Optional[int]:
    """Insert a new submission record and return its ID."""
    try:
        result = execute_query(
            """
            INSERT INTO submissions (listing_id, submission_url, submission_status)
            VALUES (%s, %s, 'pending')
            RETURNING submission_id
            """,
            (listing_id, submission_url)
        )
        return result[0][0] if result else None
    except Exception as e:
        logger.error(f"Error inserting submission: {str(e)}", exc_info=True)
        return None

def update_submission_status(
    submission_id: int, 
    status: str, 
    api_response: Optional[Dict] = None, 
    error_message: Optional[str] = None
) -> bool:
    """Update submission status, API response, and error message."""
    try:
        execute_query(
            """
            UPDATE submissions 
            SET submission_status = %s,
                api_response = %s,
                error_message = %s,
                updated_at = NOW(),
                attempts = attempts + 1
            WHERE submission_id = %s
            """,
            (status, json.dumps(api_response) if api_response else None, error_message, submission_id)
        )
        return True
    except Exception as e:
        logger.error(f"Error updating submission status: {str(e)}", exc_info=True)
        return False 