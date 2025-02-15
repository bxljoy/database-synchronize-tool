from google.cloud import storage
import logging
import os
from typing import List, Tuple, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GCSBucketSync:
    def __init__(self, source_bucket_name: str, dest_bucket_name: str, dry_run: bool = False):
        """Initialize GCS client and buckets"""
        self.client = storage.Client()
        self.source_bucket = self.client.bucket(source_bucket_name)
        self.dest_bucket = self.client.bucket(dest_bucket_name)
        self.dry_run = dry_run
        logger.info(f"Initialized GCS sync between {source_bucket_name} and {dest_bucket_name}")
        if self.dry_run:
            logger.info("Running in DRY RUN mode - no files will be copied")

    def get_existing_files(self, bucket):
        """Get set of file paths in a bucket"""
        return {blob.name for blob in bucket.list_blobs()}

    def sync_bucket(self) -> dict:
        """Sync files from source to destination bucket based on file existence"""
        stats = {
            'total_files': 0,
            'new_files': 0,
            'existing_files': 0
        }

        try:
            # Get lists of files in both buckets
            source_files = self.get_existing_files(self.source_bucket)
            dest_files = self.get_existing_files(self.dest_bucket)
            
            stats['total_files'] = len(source_files)
            logger.info(f"Found {stats['total_files']} files in source bucket")
            
            # Find files that need to be copied
            files_to_copy = source_files - dest_files
            stats['new_files'] = len(files_to_copy)
            stats['existing_files'] = len(source_files & dest_files)
            
            # Copy new files
            for file_path in sorted(files_to_copy):
                if not self.dry_run:
                    source_blob = self.source_bucket.blob(file_path)
                    self.source_bucket.copy_blob(
                        source_blob,
                        self.dest_bucket,
                        file_path
                    )
                    action = "Copied"
                else:
                    action = "Would copy"
                
                logger.info(f"{action}: {file_path}")

            # Log summary
            action_prefix = "Would copy" if self.dry_run else "Copied"
            logger.info(
                f"Sync completed - Total files: {stats['total_files']}, "
                f"{action_prefix}: {stats['new_files']}, "
                f"Already existed: {stats['existing_files']}"
            )
            return stats

        except Exception as e:
            logger.error(f"Error during bucket sync: {str(e)}")
            raise

def sync_gcs_buckets(bucket_pairs: List[Tuple[str, str]], dry_run: bool = False) -> Dict[str, dict]:
    """
    Sync files for multiple bucket pairs
    
    Args:
        bucket_pairs: List of tuples containing (source_bucket, dest_bucket) pairs
        dry_run: If True, only show what would be copied without actually copying
    
    Returns:
        dict: Statistics about the sync operation for each bucket pair
    """
    all_stats = {}
    
    for source_bucket, dest_bucket in bucket_pairs:
        pair_name = f"{source_bucket} → {dest_bucket}"
        logger.info(f"\nProcessing bucket pair: {pair_name}")
        
        try:
            sync_handler = GCSBucketSync(source_bucket, dest_bucket, dry_run)
            all_stats[pair_name] = sync_handler.sync_bucket()
        except Exception as e:
            logger.error(f"Failed to sync bucket pair {pair_name}: {str(e)}")
            all_stats[pair_name] = {"error": str(e)}
    
    return all_stats

if __name__ == "__main__":
    # Example usage
    source_bucket = os.getenv("SOURCE_GCS_BUCKET_1")
    dest_bucket = os.getenv("DEST_GCS_BUCKET_1")
    
    if not source_bucket or not dest_bucket:
        logger.error("Please set SOURCE_GCS_BUCKET_1 and DEST_GCS_BUCKET_1 environment variables")
        exit(1)
        
    try:
        # First do a dry run to see what would be copied
        logger.info("Performing dry run first...")
        sync_gcs_buckets([(source_bucket, dest_bucket)], dry_run=True)
        
        # Ask for confirmation
        response = input("\nDo you want to proceed with the actual sync? (y/N): ")
        if response.lower() == 'y':
            stats = sync_gcs_buckets([(source_bucket, dest_bucket)], dry_run=False)
            logger.info("Sync completed successfully")
        else:
            logger.info("Sync cancelled")
            exit(0)
    except Exception as e:
        logger.error(f"Sync failed: {str(e)}")
        exit(1) 