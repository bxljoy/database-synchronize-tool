from sqlalchemy import create_engine
import logging
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection configurations
DB_CONFIGS = {
    os.getenv('DB_PROD_NAME'): {
        'connection_string': os.getenv('DB_PROD_URL'),
    },
    os.getenv('DB_STAGE_NAME'): {
        'connection_string': os.getenv('DB_STAGE_URL'),
    }
}

def create_db_connections():
    """Create database engine connections"""
    engines = {}
    try:
        for db_name, config in DB_CONFIGS.items():
            engines[db_name] = create_engine(config['connection_string'])
            logger.info(f"Successfully connected to {db_name} database")
    except Exception as e:
        logger.error(f"Error creating database connections: {str(e)}")
        raise
    return engines

def batch_insert_with_progress(engine, df, insert_query, prepare_record_func, batch_size=1000):
    """Generic function to insert records in batches with progress tracking"""
    try:
        if df.empty:
            logger.info("No records to insert")
            return
            
        total_records = len(df)
        total_batches = (total_records + batch_size - 1) // batch_size
        logger.info(f"Starting insert of {total_records} records (in {total_batches} batches)")
        
        # Prepare data for insertion
        insert_data = [prepare_record_func(row) for _, row in df.iterrows()]
        
        with engine.connect() as connection:
            with connection.begin():
                cursor = connection.connection.cursor()
                
                # Process in batches and show progress every 10%
                processed_records = 0
                last_progress_report = 0
                for i in range(0, len(insert_data), batch_size):
                    batch = insert_data[i:i + batch_size]
                    execute_values(cursor, insert_query, batch)
                    processed_records += len(batch)
                    progress = (processed_records / total_records) * 100
                    
                    # Only log every 10% progress
                    if int(progress) // 10 > last_progress_report // 10:
                        last_progress_report = int(progress)
                        logger.info(f"Progress: {progress:.1f}% - Processed {processed_records}/{total_records} records")
                
                logger.info(f"Successfully inserted all {total_records} records into staging database")
                
    except Exception as e:
        logger.error(f"Error inserting records: {str(e)}")
        logger.error(f"Error details - Type: {type(e).__name__}")
        raise 