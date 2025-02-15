from utils import logger
from sync_utils import sync_table, load_table_config

def run_all_syncs():
    """Run all database syncs"""
    logger.info("Starting database syncs...")
    
    # Load table configurations
    tables = load_table_config()
    success_status = {table_name: False for table_name in tables.keys()}
    
    # Sync each table
    for table_name in tables.keys():
        try:
            logger.info(f"Starting sync for {table_name}...")
            sync_table(table_name)
            success_status[table_name] = True
            logger.info(f"{table_name} sync completed successfully")
        except Exception as e:
            logger.error(f"{table_name} sync failed: {str(e)}")
            if table_name != list(tables.keys())[-1]:  # If not the last table
                logger.error("Continuing with next sync...")
    
    # Report final status
    if all(success_status.values()):
        logger.info("All syncs completed successfully")
    else:
        failed_syncs = [name for name, success in success_status.items() if not success]
        logger.error(f"Some syncs failed: {', '.join(failed_syncs)}")
        exit(1)  # Exit with error code if any sync failed

if __name__ == "__main__":
    run_all_syncs() 