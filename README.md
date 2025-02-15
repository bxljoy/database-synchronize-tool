# Database Sync Service

A Python service that synchronizes data between production and staging databases and Google Cloud Storage buckets, with support for both local development and Google Cloud Platform (GCP) environments.

## Features

- Synchronizes multiple database tables with configurable sync strategies
- Synchronizes files between Google Cloud Storage buckets (using MD5 hash comparison)
- Supports both local PostgreSQL databases and GCP Cloud SQL
- Batch processing with progress tracking
- Configurable through YAML files
- Environment-based configuration (local/production)
- Abstract database connection interface for different environments

## Prerequisites

for local development:

- Python 3.11+
- Docker
- pip or pip3
- Pipenv

for GCP:

- GCP Cloud SQL (for database)
- GCP Cloud Run (for deployment)
- GCP Storage (for bucket sync)

## Installation

1. Clone the repository:

```bash
git clone https://github.com/bxljoy/database-synchronize-tool.git
cd database-synchronize-tool
```

2. set up databases by docker compose:

```bash
docker compose up -d
```

3. Install pipenv and dependencies:

```bash
brew install pipenv

cd db-sync-local

pipenv install
```

## Configuration

### Environment Variables

Create a `.env` file for local development:(just change .env.example to .env)

```env
# Local Development(only for local development)
DB_PROD_URL=postgresql://user:password@localhost:5432/prod_db
DB_STAGE_URL=postgresql://user:password@localhost:5432/stage_db
DB_PROD_NAME=production
DB_STAGE_NAME=staging

CONFIG_PATH="table_config.yaml"
```

### Table Configuration

Configure tables for synchronization in YAML files:(netflix.yaml is an example, just use it)

```yaml
tables:
  table_name:
    sync_config:
      check_column: column_name
      check_type: timestamp|id
      ignore_columns:
        - nullable_column
```

Available example configuration files:

- `netflix.yaml`: Netflix-related tables

## Usage

### Local Development

1. Activate the virtual environment:

```bash
pipenv shell
```

2. Run the database sync service:

```bash
python main.py
```

3. check the result in the database

### GCP Production Deployment (Cloud Run)

1. Build, Push and Deploy to Cloud Run Jobs:

```bash
make deploy
```

2. Execute the sync job manually:

```bash
make exec
```

## GCS Bucket Sync Features

The `gcs_sync.py` module provides a simple and efficient way to sync files between GCS buckets:

- Automatically syncs all files from source to destination bucket
- Uses MD5 hash comparison to identify different files
- Only copies files that are new or have changed
- Provides detailed statistics about the sync operation
- Includes comprehensive logging

Example usage in Python:

```python
from gcs_sync import sync_gcs_buckets

# Sync all files between buckets
stats = sync_gcs_buckets('source-bucket', 'dest-bucket')
```

The sync process will:

1. Compare files using MD5 hashes
2. Skip identical files
3. Copy only new or modified files
4. Provide statistics about total, synced, and skipped files

## Adding New Database Pairs

1. Add a new PROD_INSTANCE_CONNECTION_NAME_EXAMPLE="project:region:instance" to the .env file.
2. Create a new `table_config.yaml` file for the new database pair (example: `order.yaml`).

## License

[MIT License](LICENSE)
