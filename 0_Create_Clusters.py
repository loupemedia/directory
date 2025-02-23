import os
from typing import Dict, List

from utils import (
    logger,
    execute_query
)

def ensure_columns_exist():
    """Ensure all required columns exist in postcodes table."""
    # Execute ALTER TABLE without expecting results
    execute_query("""
        ALTER TABLE postcodes
            ADD COLUMN IF NOT EXISTS latitude DECIMAL(10, 8),
            ADD COLUMN IF NOT EXISTS longitude DECIMAL(11, 8),
            ADD COLUMN IF NOT EXISTS is_cluster_center BOOLEAN DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS cluster_id INTEGER,
            ADD COLUMN IF NOT EXISTS cluster_postcodes INTEGER[],
            ADD COLUMN IF NOT EXISTS last_scraped TIMESTAMP,
            ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN IF NOT EXISTS scrape_status VARCHAR(20) DEFAULT 'pending',
            ADD COLUMN IF NOT EXISTS error_message TEXT;
        COMMIT;
    """)

    # Create indexes in separate statements
    execute_query("COMMIT;")  # Ensure previous ALTER TABLE is committed
    
    execute_query("""
        CREATE INDEX IF NOT EXISTS idx_postcodes_coords 
        ON postcodes(latitude, longitude) 
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
    """)
    
    execute_query("""
        CREATE INDEX IF NOT EXISTS idx_postcodes_cluster 
        ON postcodes(cluster_id) 
        WHERE cluster_id IS NOT NULL;
    """)
    
    execute_query("""
        CREATE INDEX IF NOT EXISTS idx_postcodes_scrape 
        ON postcodes(last_scraped) 
        WHERE is_cluster_center = TRUE;
    """)

def generate_clusters(radius_km: float = 5) -> int:
    """Generate postcode clusters and update the postcodes table."""
    try:
        # Reset existing cluster data
        execute_query("""
            UPDATE postcodes 
            SET is_cluster_center = FALSE,
                cluster_id = NULL,
                cluster_postcodes = NULL,
                scrape_status = 'pending',
                error_message = NULL
            WHERE is_cluster_center = TRUE 
               OR cluster_id IS NOT NULL;
        """)
        
        # Generate new clusters
        clusters_added = execute_query("""
            WITH numbered_locations AS (
                SELECT 
                    id,
                    postcode,
                    latitude,
                    longitude
                FROM postcodes
                WHERE latitude IS NOT NULL 
                AND longitude IS NOT NULL
            ),
            potential_clusters AS (
                SELECT 
                    a.id as center_id,
                    a.postcode as center_postcode,
                    a.latitude,
                    a.longitude,
                    ARRAY_AGG(DISTINCT b.id ORDER BY b.id) as covered_postcodes,
                    COUNT(DISTINCT b.id) as coverage_count
                FROM numbered_locations a
                LEFT JOIN numbered_locations b ON 
                    2 * 6371 * asin(sqrt(
                        sin(radians(b.latitude - a.latitude)/2)^2 +
                        cos(radians(a.latitude)) * cos(radians(b.latitude)) *
                        sin(radians(b.longitude - a.longitude)/2)^2
                    )) <= %s
                GROUP BY a.id, a.postcode, a.latitude, a.longitude
            ),
            optimized_clusters AS (
                SELECT DISTINCT ON (pc.covered_postcodes)
                    pc.*,
                    ROW_NUMBER() OVER () as cluster_id
                FROM potential_clusters pc
                ORDER BY pc.covered_postcodes,
                    pc.coverage_count DESC
            )
            UPDATE postcodes p
            SET 
                is_cluster_center = TRUE,
                cluster_id = oc.cluster_id,
                cluster_postcodes = oc.covered_postcodes,
                last_updated = NOW(),
                scrape_status = 'pending'
            FROM optimized_clusters oc
            WHERE p.id = oc.center_id
            RETURNING p.id
        """, (radius_km,))

        # Update cluster_id for all postcodes in clusters
        execute_query("""
            UPDATE postcodes p
            SET 
                cluster_id = c.cluster_id,
                last_updated = NOW()
            FROM postcodes c
            WHERE c.is_cluster_center = TRUE
            AND p.id = ANY(c.cluster_postcodes)
            AND p.id != c.id;
        """)
        
        return len(clusters_added)
    except Exception as e:
        logger.error(f"Error in generate_clusters: {str(e)}", exc_info=True)
        raise

def main():
    """Main function to create postcode clusters."""
    try:
        logger.info("Starting cluster generation")
        
        ensure_columns_exist()
        clusters_count = generate_clusters()
        
        logger.info(f"Successfully generated {clusters_count} clusters")
        
        # Print statistics about the clusters
        stats = execute_query("""
            SELECT 
                COUNT(*) as total_clusters,
                AVG(ARRAY_LENGTH(cluster_postcodes, 1)) as avg_postcodes_per_cluster,
                MAX(ARRAY_LENGTH(cluster_postcodes, 1)) as max_postcodes_per_cluster,
                MIN(ARRAY_LENGTH(cluster_postcodes, 1)) as min_postcodes_per_cluster,
                COUNT(DISTINCT UNNEST(cluster_postcodes)) as total_postcodes_covered
            FROM postcodes
            WHERE is_cluster_center = TRUE
        """)[0]
        
        logger.info("Cluster Statistics:")
        logger.info(f"Total Clusters: {stats['total_clusters']}")
        logger.info(f"Average Postcodes per Cluster: {stats['avg_postcodes_per_cluster']:.2f}")
        logger.info(f"Max Postcodes per Cluster: {stats['max_postcodes_per_cluster']}")
        logger.info(f"Min Postcodes per Cluster: {stats['min_postcodes_per_cluster']}")
        logger.info(f"Total Postcodes Covered: {stats['total_postcodes_covered']}")
        
    except Exception as e:
        logger.error(f"Error generating clusters: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 