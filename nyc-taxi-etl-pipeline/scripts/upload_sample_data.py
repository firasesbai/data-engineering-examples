import boto3
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def upload_sample_data(bucket_name: str, year: str = "2024", month: str = "01"):
    """
    Upload sample NYC taxi data to S3 landing bucket.
    
    Args:
        bucket_name: Name of the S3 bucket
        year: Year for the data (default: 2024)
        month: Month for the data (default: 01)
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Define source and destination paths
        sample_file = Path(__file__).parent.parent / "data" / "sample" / "nyc_taxi_sample.csv"
        s3_key = f"landing/{year}/{month}/nyc_taxi_sample.csv"
        
        # Check if sample file exists
        if not sample_file.exists():
            logger.error(f"Error: Sample file not found at {sample_file}")
            sys.exit(1)
        
        # Upload file to S3
        logger.info(f"Uploading {sample_file} to s3://{bucket_name}/{s3_key}")
        s3_client.upload_file(
            str(sample_file),
            bucket_name,
            s3_key
        )
        
        logger.info(f"Successfully uploaded sample data to s3://{bucket_name}/{s3_key}")
        
    except Exception as e:
        logger.error(f"Error uploading sample data: {str(e)}")
        sys.exit(1)

def main():
    if len(sys.argv) < 2:
        logger.error("Usage: python scripts/upload_sample_data.py <bucket_name> [year] [month]")
        sys.exit(1)
    
    bucket_name = sys.argv[1]
    year = sys.argv[2] if len(sys.argv) > 2 else "2024"
    month = sys.argv[3] if len(sys.argv) > 3 else "01"
    
    upload_sample_data(bucket_name, year, month)

if __name__ == "__main__":
    main() 