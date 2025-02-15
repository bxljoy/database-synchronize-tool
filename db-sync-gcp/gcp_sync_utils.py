from gcp_utils import create_db_connections, batch_insert_with_progress, logger, parse_db_config
import pandas as pd
import yaml
import json

def load_table_config():
    """Load table configurations from YAML file"""
    try:
        # Get the tables_config paths from DB_SECRET_INFO
        _, table_config = parse_db_config()

        logger.debug(f"Table config: {table_config}")
        
        all_tables = {}
        # Load each service's table config
        for service, config_path in table_config.items():
            with open(config_path, 'r') as f:
                service_tables = yaml.safe_load(f)['tables']
                # Optionally prefix table configs with service name
                for table_name, table_config in service_tables.items():
                    table_config['service'] = service  # Add service info to config
                    all_tables[table_name] = table_config
                
        logger.debug(f"Loaded {len(all_tables)} tables")
        return all_tables
    except Exception as e:
        logger.error(f"Error loading table config: {str(e)}")
        raise

def generate_column_list(columns):
    """Generate a comma-separated list of column names"""
    return ', '.join(col['name'] for col in columns)

def get_check_value(engine, table_name, config):
    """Get the latest value to check for new records"""
    check_column = config['sync_config']['check_column']
    check_type = config['sync_config']['check_type']
    
    query = f"""
    SELECT MAX({check_column}) as check_value 
    FROM {table_name}
    """
    
    try:
        with engine.connect() as connection:
            result = pd.read_sql(query, connection)
            check_value = result['check_value'].iloc[0]
            
            if check_type == 'id':
                check_value = int(check_value) if pd.notnull(check_value) else 0
            
            logger.debug(f"Latest {check_column} in staging: {check_value}")
            return check_value
    except Exception as e:
        logger.error(f"Error getting check value: {str(e)}")
        raise

def extract_all_data(engine, table_name, columns):
    """Extract all data from the specified table"""
    column_list = generate_column_list(columns)
    query = f"""
    SELECT {column_list}
    FROM {table_name}
    """
    
    try:
        df = pd.read_sql(query, engine)
        logger.info(f"Extracted {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error extracting from {table_name}: {str(e)}")
        raise

def extract_new_data(engine, table_name, columns, config, check_value):
    """Extract new data from the specified table"""
    column_list = generate_column_list(columns)
    check_column = config['sync_config']['check_column']
    check_type = config['sync_config']['check_type']
    
    operator = '>' if check_type in ['id', 'timestamp'] else '>='
    
    query = f"""
    SELECT {column_list}
    FROM {table_name}
    WHERE {check_column} {operator} %s
    """
    
    try:
        df = pd.read_sql(query, engine, params=(check_value,))
        logger.info(f"Extracted {len(df)} new rows from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error extracting new data from {table_name}: {str(e)}")
        raise

def prepare_record(row, columns):
    """Prepare a single record based on column configurations"""
    values = []
    for col in columns:
        value = row[col['name']]
        
        # Handle null/empty checks based on type
        if col['type'].startswith('ARRAY') or col['type'].endswith('[]'):
            # Handle arrays - they should already be in list format
            if value is None or (isinstance(value, list) and len(value) == 0):
                values.append([])
            elif isinstance(value, list):
                values.append(value)
            else:
                logger.warning(f"Unexpected array value type for {col['name']}: {type(value)}")
                values.append([])
        elif pd.isnull(value):
            values.append(None)
        elif col['type'].startswith('jsonb'):
            try:
                if isinstance(value, str):
                    # Fix single quotes to double quotes for JSON
                    if value.startswith("'{") and value.endswith("}'"):
                        # Remove outer single quotes if present
                        value = value[1:-1]
                    # Replace escaped single quotes with double quotes
                    value = value.replace("''", '"')
                    # Verify it's valid JSON
                    json.loads(value)
                    values.append(value)
                else:
                    # Convert dict/list to JSON string
                    values.append(json.dumps(value))
                    
            except Exception as e:
                logger.error(f"Error processing JSONB field: {str(e)}")
                try:
                    # Second attempt: try to fix common JSON formatting issues
                    if isinstance(value, str):
                        # Replace single quotes with double quotes, but preserve escaped ones
                        fixed_value = value.replace("'", '"').replace('""', "'")
                        # Verify and use the fixed JSON
                        json.loads(fixed_value)
                        values.append(fixed_value)
                    else:
                        values.append(None)
                except Exception:
                    values.append(None)
        elif 'int' in col['type'] or col['type'] == 'bigserial':
            try:
                values.append(int(float(value)) if pd.notnull(value) else None)
            except Exception as e:
                logger.error(f"Error processing integer field: {str(e)}")
                values.append(None)
        else:
            try:
                values.append(str(value).strip() if pd.notnull(value) else None)
            except Exception as e:
                logger.error(f"Error processing string field: {str(e)}")
                values.append(None)
    return tuple(values)

def get_primary_keys(engine, table_name):
    """Get primary key columns from the database"""
    query = """
    SELECT a.attname as column_name
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                        AND a.attnum = ANY(i.indkey)
    WHERE  i.indrelid = %s::regclass
    AND    i.indisprimary;
    """
    
    try:
        df = pd.read_sql(query, engine, params=(table_name,))
        primary_keys = df['column_name'].tolist()
        
        if not primary_keys:
            logger.warning(f"No primary key found for table {table_name}")
            # Use all columns as conflict key if no primary key defined
            query_all_cols = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
            """
            df_all = pd.read_sql(query_all_cols, engine, params=(table_name,))
            primary_keys = df_all['column_name'].tolist()
            
        logger.debug(f"Using columns as conflict key for {table_name}: {', '.join(primary_keys)}")
        return primary_keys
    except Exception as e:
        logger.error(f"Error getting primary keys for table {table_name}: {str(e)}")
        raise

def generate_upsert_query(table_name, columns, primary_keys):
    """Generate INSERT or INSERT ON CONFLICT query based on configuration"""
    column_list = generate_column_list(columns)

    if len(primary_keys) > 0:
        # Generate UPDATE SET clause excluding primary key columns
        update_columns = [col['name'] for col in columns if col['name'] not in primary_keys]
        update_clause = ', '.join(f"{col} = EXCLUDED.{col}" for col in update_columns)
        
        return f"""
        INSERT INTO {table_name} ({column_list})
        VALUES %s
        ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE SET
            {update_clause}
        """
    else:
        return f"""
        INSERT INTO {table_name} ({column_list})
        VALUES %s
        """

def get_table_schema(engine, table_name, config):
    """Get table schema information from the database"""
    query = """
    SELECT column_name, data_type, is_nullable, 
           character_maximum_length, numeric_precision, numeric_scale,
           udt_name
    FROM information_schema.columns 
    WHERE table_name = %s
    ORDER BY ordinal_position;
    """
    
    try:
        df = pd.read_sql(query, engine, params=(table_name,))
        
        # Get ignore columns from config
        ignore_columns = config.get('sync_config', {}).get('ignore_columns', [])
        
        columns = []
        for _, row in df.iterrows():
            # Skip ignored columns
            if row['column_name'] in ignore_columns and row['is_nullable'] == 'YES':
                logger.info(f"Ignoring nullable column: {row['column_name']}")
                continue
                
            # Rest of your existing column processing code
            data_type = row['data_type']
            if row['udt_name'].endswith('[]'):
                data_type = f"{row['udt_name'][:-2]}[]"
            elif row['character_maximum_length'] is not None:
                data_type = f"{data_type}({row['character_maximum_length']})"
            elif row['numeric_precision'] is not None and row['numeric_scale'] is not None:
                data_type = f"{data_type}({row['numeric_precision']},{row['numeric_scale']})"
            
            columns.append({
                'name': row['column_name'],
                'type': data_type,
                'nullable': row['is_nullable'] == 'YES'
            })
        
        return columns
    except Exception as e:
        logger.error(f"Error getting schema for table {table_name}: {str(e)}")
        raise

def sync_table(table_name):
    """Sync a single table based on its configuration"""
    try:
        # Load configuration
        config = load_table_config()[table_name]
        service = config['service']  # Get the service name

        logger.debug(f"Config: {config}")
        # Create database connections
        engines = create_db_connections()
        
        # Use service-specific connection names
        prod_engine = engines[f"{service}_prod"]
        stage_engine = engines[f"{service}_stage"]

        logger.debug(f"Prod engine: {prod_engine}")
        logger.debug(f"Stage engine: {stage_engine}")
        
        # Pass config to get_table_schema
        columns = get_table_schema(prod_engine, table_name, config)
        primary_keys = get_primary_keys(prod_engine, table_name)
        
        # Get check value from staging
        check_value = get_check_value(stage_engine, table_name, config)
        logger.debug(f"Check value: {check_value}")
        
        # Extract data based on check_value
        if check_value is None:
            logger.info(f"No existing data found in {table_name}. Will copy all data from production...")
            df = extract_all_data(prod_engine, table_name, columns)
        else:
            logger.info(f"Found existing data in {table_name}, latest {config['sync_config']['check_column']} is {check_value}, extracting new data from {table_name}...")
            df = extract_new_data(prod_engine, table_name, columns, config, check_value)
        
        # Insert data into staging
        if not df.empty:
            insert_query = generate_upsert_query(table_name, columns, primary_keys)
            
            batch_insert_with_progress(
                engine=stage_engine,
                df=df,
                insert_query=insert_query,
                prepare_record_func=lambda row: prepare_record(row, columns)
            )
            logger.info(f"Sync completed successfully for {table_name}")
        else:
            logger.info(f"No data to sync for {table_name}")
        
    except Exception as e:
        logger.error(f"Sync failed for {table_name}: {str(e)}")
        raise
    finally:
        # Close all database connections
        for engine in engines.values():
            engine.dispose() 