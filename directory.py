import json
import logging
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class ValidationError(Exception):
    pass

def setup_logging():
    """Setup basic logging to file and console"""
    # Create logs directory
    log_dir = Path.cwd() / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"directory_process_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logging.info(f"Logging initialized. Log file: {log_file}")
    return log_file

class NetworkHandler:
    """Simple network handler with retry logic"""
    def __init__(self):
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,  # number of retries
            backoff_factor=0.5,  # wait 0.5, 1, 2 seconds between retries
            status_forcelist=[408, 429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

def validate_url(url):
    """Validate URL format"""
    logging.debug(f"Validating URL: {url}")
    
    if not url or pd.isna(url):
        logging.warning("Empty or NaN URL provided")
        return False
    
    try:
        parsed = urlparse(url)
        valid = all([parsed.scheme, parsed.netloc])
        if not valid:
            logging.warning(f"Invalid URL structure: {url}")
        return valid
    except Exception as e:
        logging.error(f"URL validation error for {url}: {str(e)}")
        return False

def validate_required_columns(df, required_columns, file_name):
    """Validate that all required columns exist in dataframe (case-insensitive)"""
    logging.info(f"Validating columns for {file_name}")
    logging.debug(f"Required columns: {required_columns}")
    logging.debug(f"Available columns: {df.columns.tolist()}")
    
    # Create case-insensitive column mapping
    column_mapping = {col.lower(): col for col in df.columns}
    
    # Check for missing columns (case-insensitive)
    missing_columns = []
    for required_col in required_columns:
        if required_col.lower() not in column_mapping:
            missing_columns.append(required_col)
    
    if missing_columns:
        error_msg = f"Missing required columns in {file_name}: {', '.join(missing_columns)}"
        logging.error(error_msg)
        raise ValidationError(error_msg)
    
    logging.info(f"Column validation successful for {file_name}")
    return {req_col: column_mapping[req_col.lower()] for req_col in required_columns}

def clean_domain(url):
    """Clean and extract domain from URL"""
    if not url or pd.isna(url):
        return ""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.", "").split("?")[0]
        return domain.split(".")[0]
    except:
        return ""

def parse_business_hours(hours_str):
    """Parse business hours with error handling"""
    if not hours_str or pd.isna(hours_str):
        return None
        
    try:
        hours_dict = json.loads(hours_str.replace("'", '"'))
        hours_list = []
        
        for day, time in hours_dict.items():
            if "Closed" in time:
                continue
                
            time = time.lower()
            time = time.replace("am", ":00").replace("pm", ":00")
            
            if not re.match(r'\d{1,2}:\d{2}', time):
                logging.warning(f"Invalid time format for {day}: {time}")
                continue
                
            hours_list.append(f"{day[:2]} {time}")
            
        return json.dumps(hours_list) if hours_list else None
    except Exception as e:
        logging.error(f"Error parsing business hours: {str(e)}")
        return None

def download_image(session, url, filename, max_size_kb=250):
    """Download image with size validation"""
    logging.info(f"Downloading image from {url}")
    
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        
        # Check file size
        content_length = len(response.content)
        size_kb = content_length / 1024
        
        if content_length > max_size_kb * 1024:
            logging.error(f"Image too large ({size_kb:.2f}KB > {max_size_kb}KB): {filename}")
            return False
            
        with open(filename, 'wb') as f:
            f.write(response.content)
            
        logging.info(f"Successfully downloaded image: {filename}")
        return True
        
    except Exception as e:
        logging.error(f"Error downloading image {filename}: {str(e)}")
        return False

def main():
    # Setup logging
    log_file = setup_logging()
    logging.info("Starting directory processing script")
    
    # Create output directory
    output_dir = Path.cwd() / "output"
    output_dir.mkdir(exist_ok=True)
    
    # Initialize network handler
    network_handler = NetworkHandler()
    
    try:
        # Load and validate data files
        logging.info("Loading data files...")
        
        # Required columns (case-sensitive as they appear in files)
        gd_required_columns = ["ID", "post_title", "website", "post_category"]
        scrape_required_columns = ["ID", "working_hours", "site.company_insights.description", "city", "subtypes"]
        images_required_columns = ["query", "screenshot"]
        
        # Load files
        logging.info("Attempting to read gd.csv with ISO-8859-1 encoding...")
        gd_df = pd.read_csv("gd.csv", encoding='iso-8859-1')
        logging.info(f"Successfully read gd.csv: {len(gd_df)} rows")
        validate_required_columns(gd_df, gd_required_columns, "gd.csv")
        logging.info(f"Loaded {len(gd_df)} rows from gd.csv")
        
        scrape_df = pd.read_excel("scrape.xlsx")
        validate_required_columns(scrape_df, scrape_required_columns, "scrape.xlsx")
        logging.info(f"Loaded {len(scrape_df)} rows from scrape.xlsx")
        
        images_df = pd.read_excel("images.xlsx")
        validate_required_columns(images_df, images_required_columns, "images.xlsx")
        logging.info(f"Loaded {len(images_df)} rows from images.xlsx")
        
        # Initialize counters
        processed_count = 0
        error_count = 0
        image_download_count = 0
        missing_subtypes = set()
        
        # Process each row
        for index, row in gd_df.iterrows():
            jeweller_id = row["ID"]
            logging.info(f"\nProcessing row {index + 1}/{len(gd_df)} - ID: {jeweller_id}")
            
            try:
                # Process website URL
                if pd.notna(row["website"]):
                    if validate_url(row["website"]):
                        gd_df.at[index, "website"] = row["website"].split("?")[0]
                    else:
                        gd_df.at[index, "website"] = ""
                
                # Match with scraped data
                scrape_match = scrape_df[scrape_df["ID"] == jeweller_id]
                if scrape_match.empty:
                    logging.warning(f"No matching scrape data for ID {jeweller_id}")
                    continue
                
                scrape_row = scrape_match.iloc[0]
                
                # Process business hours
                if pd.notna(scrape_row["working_hours"]):
                    parsed_hours = parse_business_hours(scrape_row["working_hours"])
                    if parsed_hours:
                        gd_df.at[index, "business_hours"] = parsed_hours
                
                # Process description
                if pd.notna(scrape_row["site.company_insights.description"]):
                    content = scrape_row["site.company_insights.description"]
                    sentences = [s for s in content.split('. ') if '...' not in s]
                    gd_df.at[index, "post_content"] = '. '.join(sentences)
                
                # Process title
                city = scrape_row["city"]
                title = row["post_title"]
                if city and city.lower() in title.lower():
                    parts = title.lower().split(city.lower())
                    if parts[0].strip():
                        gd_df.at[index, "post_title"] = parts[0].strip()
                
                # Process images
                if pd.notna(row["website"]):
                    domain_gd = clean_domain(row["website"])
                    image_match = images_df[images_df["query"].apply(clean_domain) == domain_gd]
                    if not image_match.empty:
                        image_url = image_match.iloc[0]["screenshot"]
                        image_filename = output_dir / f"{gd_df.at[index, 'post_title']}.jpg"
                        if download_image(network_handler.session, image_url, image_filename):
                            image_download_count += 1
                
                # Process categories
                subtypes = scrape_row["subtypes"].split(",")
                category_ids = []
                for subtype in subtypes:
                    if subtype in google_to_directory_category:
                        category_ids.append(str(google_to_directory_category[subtype][1]))
                    else:
                        missing_subtypes.add(subtype)
                gd_df.at[index, "post_category"] = "," + ",".join(category_ids) + ","
                
                processed_count += 1
                
            except Exception as e:
                logging.error(f"Error processing row {index + 1} (ID: {jeweller_id}): {str(e)}")
                error_count += 1
        
        # Save output
        output_file = output_dir / "gd_upload.csv"
        gd_df.to_csv(output_file, index=False, encoding="utf-8")
        
        # Log summary
        logging.info("\nProcessing Summary:")
        logging.info(f"Total rows processed: {processed_count}")
        logging.info(f"Successful image downloads: {image_download_count}")
        logging.info(f"Errors encountered: {error_count}")
        if missing_subtypes:
            logging.info(f"Missing subtypes: {missing_subtypes}")
        logging.info(f"Output file saved as: {output_file}")
        logging.info(f"Log file location: {log_file}")
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
