import os
from typing import Dict, Optional, List
import requests
from requests.auth import HTTPBasicAuth

from utils import (
    logger, 
    execute_query, 
    get_wp_config,
    insert_task,
    update_task_status,
    log_progress,
    get_testing_clause
)

def create_wp_post(listing_data: Dict, country: str, task_id: int) -> Optional[int]:
    """Create a WordPress post using the GeoDirectory REST API."""
    try:
        wp_config = get_wp_config(country)
        if not wp_config:
            update_task_status(task_id, "failed", "WordPress configuration not found")
            return None

        # Use GeoDirectory v2 API endpoint for places
        api_url = f"{wp_config['site']}/wp-json/geodir/v2/places"
        
        # Prepare post data according to GeoDirectory API structure
        post_data = {
            'title': listing_data['post_title'],
            'content': listing_data['post_content'],
            'status': 'publish',
            'default_category': listing_data.get('default_category'),
            'street': listing_data.get('street'),
            'city': listing_data.get('city'),
            'region': listing_data.get('region'),
            'country': listing_data.get('country'),
            'zip': listing_data.get('zip'),
            'latitude': listing_data.get('latitude'),
            'longitude': listing_data.get('longitude'),
            'phone': listing_data.get('phone'),
            'email': listing_data.get('email'),
            'website': listing_data.get('website'),
            'facebook': listing_data.get('facebook'),
            'instagram': listing_data.get('instagram'),
            'timing': listing_data.get('business_hours'),
            'what_makes_us_different': listing_data.get('what_makes_us_different'),
        }

        # Add categories if present
        if listing_data.get('post_category'):
            post_data['post_category'] = [
                int(cat) for cat in listing_data['post_category'].strip(',').split(',') 
                if cat
            ]

        # Add featured image if present
        if listing_data.get('post_images'):
            post_data['featured_media'] = listing_data['post_images']

        response = requests.post(
            api_url,
            json=post_data,
            auth=HTTPBasicAuth(wp_config['username'], wp_config['password'])
        )
        
        response.raise_for_status()
        return response.json().get('id')

    except Exception as e:
        error_msg = f"Error creating GeoDirectory post: {str(e)}"
        logger.error(error_msg, exc_info=True)
        update_task_status(task_id, "failed", error_msg)
        return None

def process_listings():
    """Process listings and create WordPress posts."""
    listings = execute_query(f"""
        SELECT l.*, p.country 
        FROM listings l
        JOIN postcodes p ON l.postcode_id = p.id
        WHERE l.wp_post_id IS NULL 
        AND l.post_content IS NOT NULL
        {get_testing_clause()}
        LIMIT 50
    """)
    
    total = len(listings)
    for idx, listing in enumerate(listings, 1):
        log_progress(idx, total, "Creating WordPress posts")
        listing_dict = {
            'listing_id': listing[0],
            'post_title': listing[1],
            'post_content': listing[2],
            'post_category': listing[3],
            'business_hours': listing[4],
            'website': listing[5],
            'phone': listing[6],
            'email': listing[7],
            'facebook': listing[8],
            'instagram': listing[9],
            'what_makes_us_different': listing[10],
            'street': listing[11],
            'city': listing[12],
            'region': listing[13],
            'country': listing[14],
            'zip': listing[15],
            'latitude': listing[16],
            'longitude': listing[17],
            'post_images': listing[18]
        }

        # Create task for WordPress post creation
        task_id = insert_task(
            "create_wp_post",
            {
                "listing_id": listing_dict['listing_id'],
                "country": listing_dict['country']
            }
        )

        if not task_id:
            logger.error(f"Failed to create task for listing {listing_dict['listing_id']}")
            continue

        update_task_status(task_id, "processing")

        # Create WordPress post
        wp_post_id = create_wp_post(listing_dict, listing_dict['country'], task_id)
        
        if wp_post_id:
            # Update listing with WordPress post ID
            execute_query(
                """
                UPDATE listings 
                SET wp_post_id = %s,
                    updated_at = NOW()
                WHERE listing_id = %s
                """,
                (wp_post_id, listing_dict['listing_id'])
            )
            logger.info(f"Created WordPress post {wp_post_id} for listing {listing_dict['listing_id']}")
            update_task_status(task_id, "completed")
        else:
            logger.warning(f"Failed to create WordPress post for listing {listing_dict['listing_id']}")

def main():
    """Main function to create WordPress listings."""
    try:
        logger.info("Starting WordPress listing creation process")
        process_listings()
        logger.info("Completed WordPress listing creation process")
    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
