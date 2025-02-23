import os
import time
from typing import Dict, List, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from utils import (
    get_db_connection,
    logger,
    GOOGLE_TO_DIRECTORY_CATEGORY,
    execute_query,
    insert_task,
    update_task_status,
    log_progress,
    get_testing_clause
)

PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
SEARCH_RADIUS_METERS = 5000  # 5km radius
JEWELRY_KEYWORDS = ['jeweler', 'jeweller', 'jewelry store']

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def search_places(latitude: float, longitude: float, keyword: str) -> List[Dict]:
    """Search Google Places API for jewelry stores."""
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    
    params = {
        'location': f"{latitude},{longitude}",
        'radius': SEARCH_RADIUS_METERS,
        'keyword': keyword,
        'key': PLACES_API_KEY
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get('results', [])

def get_place_details(place_id: str) -> Optional[Dict]:
    """Get detailed information about a place."""
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    
    params = {
        'place_id': place_id,
        'fields': 'name,formatted_address,geometry,website,formatted_phone_number,types',
        'key': PLACES_API_KEY
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get('result')

def save_place_to_db(place_data: Dict, postcode_id: int) -> bool:
    """Save place data to listings table."""
    try:
        query = """
            INSERT INTO listings (
                google_place_id, name, address, latitude, longitude,
                postcode_id, website_url, phone, post_title, post_type,
                street, city, region, country, zip,
                post_category, default_category
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, 'gd_place',
                %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (google_place_id) DO UPDATE SET
                name = EXCLUDED.name,
                address = EXCLUDED.address,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                website_url = EXCLUDED.website_url,
                phone = EXCLUDED.phone,
                post_title = EXCLUDED.post_title,
                street = EXCLUDED.street,
                city = EXCLUDED.city,
                region = EXCLUDED.region,
                country = EXCLUDED.country,
                zip = EXCLUDED.zip,
                post_category = EXCLUDED.post_category,
                default_category = EXCLUDED.default_category,
                updated_at = NOW()
            RETURNING listing_id
        """
        
        # Parse address components
        address_components = place_data.get('address_components', [])
        street_number = next((comp['long_name'] for comp in address_components 
                            if 'street_number' in comp['types']), '')
        route = next((comp['long_name'] for comp in address_components 
                     if 'route' in comp['types']), '')
        street = f"{street_number} {route}".strip()
        city = next((comp['long_name'] for comp in address_components 
                    if 'locality' in comp['types']), '')
        region = next((comp['long_name'] for comp in address_components 
                      if 'administrative_area_level_1' in comp['types']), '')
        country = next((comp['long_name'] for comp in address_components 
                       if 'country' in comp['types']), '')
        postal_code = next((comp['long_name'] for comp in address_components 
                          if 'postal_code' in comp['types']), '')

        # Get category mapping
        place_types = place_data.get('types', [])
        category_info = None
        for place_type in place_types:
            if place_type in GOOGLE_TO_DIRECTORY_CATEGORY:
                category_info = GOOGLE_TO_DIRECTORY_CATEGORY[place_type]
                break
        
        default_category = category_info[1] if category_info else None
        post_category = f",{default_category}," if default_category else None
        
        params = (
            place_data['place_id'],
            place_data['name'],
            place_data['formatted_address'],
            place_data['geometry']['location']['lat'],
            place_data['geometry']['location']['lng'],
            postcode_id,
            place_data.get('website', ''),
            place_data.get('formatted_phone_number', ''),
            place_data['name'],
            street,
            city,
            region,
            country,
            postal_code,
            post_category,
            default_category
        )
        
        result = execute_query(query, params)
        return bool(result)
        
    except Exception as e:
        logger.error(f"Error saving place to database: {str(e)}", exc_info=True)
        return False

def process_postcode(postcode_id: int, latitude: float, longitude: float, task_id: int):
    """Process a single postcode location."""
    places_found = 0
    
    try:
        # Update task status to processing
        update_task_status(task_id, "processing")
        
        for keyword in JEWELRY_KEYWORDS:
            try:
                places = search_places(latitude, longitude, keyword)
                
                for place in places:
                    try:
                        details = get_place_details(place['place_id'])
                        if details and save_place_to_db(details, postcode_id):
                            places_found += 1
                        
                        # Rate limiting
                        time.sleep(2)  # 2 second delay between API calls
                        
                    except Exception as e:
                        logger.error(f"Error processing place {place.get('name')}: {str(e)}")
                        continue
                
            except Exception as e:
                logger.error(f"Error searching places with keyword {keyword}: {str(e)}")
                continue
        
        # Update task status to completed
        update_task_status(task_id, "completed")
        return places_found
        
    except Exception as e:
        error_msg = f"Error processing postcode {postcode_id}: {str(e)}"
        logger.error(error_msg)
        update_task_status(task_id, "failed", error_msg)
        return places_found

def get_search_clusters(radius_km: float = 5) -> List[Dict]:
    """Group postcodes into clusters based on proximity to avoid duplicate searches."""
    # Get total clusters for progress tracking
    total_clusters = execute_query(f"""
        SELECT COUNT(*) 
        FROM postcodes 
        WHERE is_cluster_center = TRUE
        {get_testing_clause()}
    """)[0][0]
    
    logger.info(f"Processing {total_clusters} clusters")
    
    return execute_query(f"""
        WITH numbered_locations AS (
            SELECT 
                id,
                postcode,
                locality,
                latitude,
                longitude
            FROM postcodes
            WHERE latitude IS NOT NULL 
            AND longitude IS NOT NULL
            AND (last_scraped IS NULL OR last_scraped < NOW() - INTERVAL '7 days')
            {get_testing_clause()}
        ),
        clusters AS (
            SELECT 
                a.id,
                a.postcode,
                a.locality,
                a.latitude,
                a.longitude,
                ARRAY_AGG(
                    b.id ORDER BY b.id
                ) as covered_postcodes
            FROM numbered_locations a
            LEFT JOIN numbered_locations b ON 
                -- Calculate distance using Haversine formula
                2 * 6371 * asin(sqrt(
                    sin(radians(b.latitude - a.latitude)/2)^2 +
                    cos(radians(a.latitude)) * cos(radians(b.latitude)) *
                    sin(radians(b.longitude - a.longitude)/2)^2
                )) <= %s
            GROUP BY a.id, a.postcode, a.locality, a.latitude, a.longitude
        )
        SELECT DISTINCT ON (c.covered_postcodes)
            c.id,
            c.postcode,
            c.locality,
            c.latitude,
            c.longitude,
            c.covered_postcodes
        FROM clusters c
        ORDER BY c.covered_postcodes, 
            ARRAY_LENGTH(c.covered_postcodes, 1) DESC
    """, (radius_km,))

def get_next_cluster():
    """Get the next cluster that needs to be scraped."""
    return execute_query("""
        SELECT 
            id,
            center_postcode_id,
            center_postcode,
            latitude,
            longitude,
            covered_postcodes
        FROM postcode_clusters
        WHERE last_scraped IS NULL 
            OR last_scraped < NOW() - INTERVAL '7 days'
        ORDER BY last_scraped NULLS FIRST 
        LIMIT 1
    """)

def main():
    """Main function to find jewelry stores efficiently."""
    try:
        while True:
            # Get next cluster to process
            cluster = get_next_cluster()
            if not cluster:
                logger.info("No more areas to process")
                break

            postcode_id = cluster['center_postcode_id']
            lat = cluster['latitude']
            lng = cluster['longitude']
            covered_postcodes = cluster['covered_postcodes']

            logger.info(f"Processing cluster centered on {cluster['center_postcode']} ({cluster['center_postcode']}) "
                      f"covering {len(covered_postcodes)} postcodes")

            # Create a single task for the cluster
            task_id = insert_task(
                "scrape_area",
                {
                    "center_postcode_id": postcode_id,
                    "postcode": cluster['center_postcode'],
                    "locality": cluster['locality'],
                    "latitude": lat,
                    "longitude": lng,
                    "covered_postcodes": covered_postcodes
                }
            )

            if not task_id:
                logger.error(f"Failed to create task for cluster {cluster['center_postcode']}")
                continue

            update_task_status(task_id, "processing")
            
            # Single search for the entire cluster
            places_found = process_postcode(postcode_id, lat, lng, task_id)

            # Update last_scraped for all covered postcodes
            execute_query(
                """
                UPDATE postcodes 
                SET last_scraped = NOW() 
                WHERE id = ANY(%s)
                """,
                (covered_postcodes,)
            )

            logger.info(f"Found {places_found} places in cluster. "
                      f"Updated {len(covered_postcodes)} postcodes.")
            
            update_task_status(task_id, "completed")

            # Rate limiting between clusters
            time.sleep(2)

    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
