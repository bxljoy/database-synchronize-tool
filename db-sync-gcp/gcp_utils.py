from sqlalchemy import create_engine
import logging
import os
from google.cloud.sql.connector import Connector
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Parse DB_SECRET_INFO
def parse_db_config():
    db_secret_info = os.getenv('DB_SECRET_INFO')
    if not db_secret_info:
        raise ValueError("DB_SECRET_INFO environment variable is not set")
    
    try:
        config = yaml.safe_load(db_secret_info)
        logger.debug(f"Config: {config}")
        # Initialize connection parameters dictionary
        connections = {}
        table_config = {}
        
        # Parse each service (inventory, merchant, order)
        for service, service_config in config.items():
            logger.debug(f"Service: {service}")
            logger.debug(f"Service config: {service_config}")
            if 'db' not in service_config:
                continue
                
            db_config = service_config['db']
            
            # Store tables_config path
            if 'table_config' in service_config:
                table_config[service] = service_config['table_config']
            
            # Add prod connection info
            if 'prod' in db_config:
                logger.debug(f"Adding prod connection info for {service}")
                prod_key = f"{service}_prod"
                connections[prod_key] = {
                    'instance_connection_name': db_config['prod']['instance-connection-name'],
                    'database_name': db_config['prod']['database-name'],
                    'username': db_config['prod']['username'],
                    'password': db_config['prod']['password']
                }
            
            # Add stage connection info
            if 'stage' in db_config:
                stage_key = f"{service}_stage"
                logger.debug(f"Adding stage connection info for {service}")
                connections[stage_key] = {
                    'instance_connection_name': db_config['stage']['instance-connection-name'],
                    'database_name': db_config['stage']['database-name'],
                    'username': db_config['stage']['username'],
                    'password': db_config['stage']['password']
                }
        
        logger.debug(f"Connections: {connections}")
        logger.debug(f"Table config: {table_config}")
        return connections, table_config
        
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML from DB_SECRET_INFO: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing DB_SECRET_INFO: {str(e)}")
        raise

def create_db_connections():
    engines = {}
    """Initialize Cloud SQL Python Connector object"""
    connector = Connector()
    
    # Get database configurations
    db_configs, _ = parse_db_config()
    logger.debug(f"DB configs: {db_configs}")
    
    # Create connection functions for each database
    def create_connection_func(config):
        def get_conn():
            return connector.connect(
                config['instance_connection_name'],
                "pg8000",
                user=config['username'],
                password=config['password'],
                db=config['database_name']
            )
        return get_conn
    
    # Create engines for each database
    for db_key, config in db_configs.items():
        logger.debug(f"Creating engine for {db_key}")
        engines[db_key] = create_engine(
            "postgresql+pg8000://",
            creator=create_connection_func(config),
            pool_size=5,
            max_overflow=2,
            pool_timeout=30,
            pool_recycle=1800,
        )
        
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
                    # Replace execute_values with pg8000's executemany
                    placeholders = '(' + ','.join(['%s'] * len(batch[0])) + ')'
                    formatted_query = insert_query % placeholders
                    cursor.executemany(formatted_query, batch)
                    
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
