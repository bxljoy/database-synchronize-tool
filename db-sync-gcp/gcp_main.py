from gcp_utils import logger
from gcp_sync_utils import sync_table, load_table_config
from gcs_sync import sync_gcs_buckets
import os

def run_gcs_syncs():
    """Run all GCS bucket syncs"""
    logger.info("\nStarting GCS bucket syncs...")
    
    try:
        # Get bucket pairs from environment variables
        bucket_pairs = []
        pair_index = 1
        
        while True:
            source = os.getenv(f"SOURCE_GCS_BUCKET_{pair_index}")
            dest = os.getenv(f"DEST_GCS_BUCKET_{pair_index}")
            
            if not source or not dest:
                break
                
            bucket_pairs.append((source, dest))
            pair_index += 1
        
        if not bucket_pairs:
            logger.info("No GCS bucket pairs configured, skipping GCS sync")
            return True
            
        # Run the sync
        stats = sync_gcs_buckets(bucket_pairs, dry_run=False)
        
        # Check for errors
        failed_pairs = [pair for pair, pair_stats in stats.items() if "error" in pair_stats]
        
        if failed_pairs:
            logger.error(f"Some GCS syncs failed: {', '.join(failed_pairs)}")
            return False
        
        logger.info("All GCS syncs completed successfully")
        return True
            
    except Exception as e:
        logger.error(f"GCS sync process failed: {str(e)}")
        return False

def run_all_syncs():
    """Run all database and GCS syncs"""
    logger.info("Starting all syncs...")
    
    try:
        # Load all table configurations
        tables = load_table_config()
        success_status = {table_name: False for table_name in tables.keys()}
        logger.debug(f"Tables: {tables}")
        
        # Group tables by service for better logging
        service_tables = {}
        for table_name, config in tables.items():
            service = config['service']
            if service not in service_tables:
                service_tables[service] = []
            service_tables[service].append(table_name)
        
        # Sync tables service by service
        for service, table_list in service_tables.items():
            logger.info(f"Starting sync for {service} service...")
            
            for table_name in table_list:
                try:
                    logger.info(f"Starting sync for {table_name}...")
                    sync_table(table_name)
                    success_status[table_name] = True
                    logger.info(f"{table_name} sync completed successfully")
                except Exception as e:
                    logger.error(f"{table_name} sync failed: {str(e)}")
                    if table_name != table_list[-1]:  # If not the last table in this service
                        logger.error("Continuing with next table...")
            
            logger.info(f"Completed syncs for {service} service")
        
        # Run GCS syncs after database syncs
        gcs_success = run_gcs_syncs()
        
        # Report final status
        db_success = all(success_status.values())
        if db_success and gcs_success:
            logger.info("\nAll syncs completed successfully")
        else:
            failed_db_syncs = [name for name, success in success_status.items() if not success]
            error_msg = []
            if failed_db_syncs:
                error_msg.append(f"Database syncs failed: {', '.join(failed_db_syncs)}")
            if not gcs_success:
                error_msg.append("GCS sync failed")
            
            logger.error(f"\nSync failures: {'; '.join(error_msg)}")
            exit(1)  # Exit with error code if any sync failed
            
    except Exception as e:
        logger.error(f"Fatal error in sync process: {str(e)}")
        exit(1)

if __name__ == "__main__":
    run_all_syncs() 