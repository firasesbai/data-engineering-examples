# NYC Taxi Trip Data ETL Solution

## Overview

This solution demonstrates a **simplified, production-ready ETL pipeline** for processing NYC Taxi Trip data using AWS services. This version focuses on **core data engineering concepts** while maintaining essential functionality.

## Project Structure

```
nyc-taxi-etl-pipeline/
├── app.py                          # CDK application entry point
├── cdk.json                        # CDK configuration
├── pyproject.toml                  # Python project dependencies
├── README.md                       # Project documentation
│
├── data/                           # Data files
│   └── sample/                     # Sample datasets
│       └── nyc_taxi_sample.csv     # Sample NYC taxi data
│
├── infrastructure/                 # AWS CDK infrastructure code
│   ├── s3_stack.py                # S3 bucket definitions
│   └── glue_stack.py              # Glue ETL job and crawler
│
├── glue_script/                    # AWS Glue ETL processing code
│   ├── data_transformation_job.py # Main ETL transformation logic
│   └── utils.py                   # Utility functions for data processing
│
├── scripts/                        # Utility scripts
│   └── upload_sample_data.py      # Script to upload sample data to S3
│
├── tests/                          # Test suite
│   └── unit/                       # Unit tests
│       └── test_taxi_transformations.py
```

## Workflow Architecture


```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw CSV Data  │───▶│   S3 Data       │───▶│  Glue ETL Job   │
│                 │    │     Bucket      │    │  (Scheduled)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                │                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   AWS Athena    │◀───│  Glue Catalog   │◀───│  S3 Processed   │
│   (Querying)    │    │                 │    │     Data        │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow

```
1. Data Ingestion
   ┌─────────────────┐
   │ Raw CSV Files   │───▶ S3 Data Bucket (s3://bucket/landing/YYYY/MM/)
   │                 │
   └─────────────────┘

2. Automated Processing
   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
   │ EventBridge     │───▶│ Lambda Trigger  │───▶│ Glue ETL Job    │
   │ (Daily 4 AM)    │    │ (Handler)       │    │ (Transform)     │
   └─────────────────┘    └─────────────────┘    └─────────────────┘

3. Data Storage
   ┌─────────────────┐    ┌─────────────────┐
   │ Glue ETL Job    │───▶│ S3 Processed    │
   │ (Transform)     │    │ Zone (Parquet)  │
   └─────────────────┘    └─────────────────┘

4. Data Catalog & Querying
   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
   │ Glue Crawler    │───▶│ Glue Catalog    │───▶│ AWS Athena      │
   │ (Schema Update) │    │ (Metadata)      │    │ (SQL Queries)   │
   └─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Components Description

- **Storage**
  - S3 Data Bucket: Stores both raw CSV files and processed data
    - `landing/YYYY/MM/` - Raw CSV files
    - `processed/` - Transformed data in Parquet format, partitioned by year, month, and day

- **ETL Processing**
  - AWS Glue Job: Transforms raw CSV data into a normalized format
  - Data transformation includes:
    - Converting pickup/dropoff timestamps to UTC and local time
    - Calculating trip duration and distance
    - Adding time-based features (hour, day of week, season)
    - Basic data validation

- **Data Catalog**
  - AWS Glue Data Catalog: Central metadata repository for table definitions and schemas
  - Enables SQL querying through Athena

- **Orchestration**
  - AWS EventBridge: Triggers Lambda function on schedule (daily 4 AM UTC)
  - AWS Lambda: Handles Glue job triggering with proper error handling

- **Query Interface**
  - AWS Athena: SQL-based querying of processed data

## Data Structure

### Input Data Format
The solution expects NYC Taxi Trip data in CSV format with the following columns:
- `pickup_datetime`: Pickup timestamp (YYYY-MM-DD HH:MM:SS)
- `dropoff_datetime`: Dropoff timestamp (YYYY-MM-DD HH:MM:SS)
- `pickup_longitude`: Pickup location longitude
- `pickup_latitude`: Pickup location latitude
- `dropoff_longitude`: Dropoff location longitude
- `dropoff_latitude`: Dropoff location latitude
- `passenger_count`: Number of passengers
- `trip_distance`: Trip distance in miles
- `fare_amount`: Fare amount in USD
- `tip_amount`: Tip amount in USD
- `total_amount`: Total amount in USD

### Output Data Format
The transformed data includes:
- `trip_id`: Unique trip identifier
- `pickup_time_utc`: Pickup time in UTC
- `pickup_time_local`: Pickup time in local timezone (America/New_York)
- `dropoff_time_utc`: Dropoff time in UTC
- `dropoff_time_local`: Dropoff time in local timezone
- `trip_duration_minutes`: Calculated trip duration
- `pickup_latitude`, `pickup_longitude`: Pickup coordinates
- `dropoff_latitude`, `dropoff_longitude`: Dropoff coordinates
- `passenger_count`: Number of passengers
- `trip_distance_miles`: Trip distance
- `fare_amount`, `tip_amount`, `total_amount`: Financial data
- `pickup_hour`, `pickup_day_of_week`: Time-based features
- `season`: Seasonal classification
- `year`, `month`, `date`: Partitioning columns
- `is_valid_coordinates`, `is_valid_fare`: Basic validation flags

## Implementation Details

### Key Transformations

1. **Time Zone Handling**: 
   - Converts timestamps to both UTC and local time (America/New_York)
   - Handles daylight saving time transitions

2. **Trip Duration Calculation**:
   - Calculates trip duration in minutes
   - Handles edge cases and invalid data

3. **Seasonal Classification**:
   - Winter: Dec, Jan, Feb
   - Spring: Mar, Apr, May
   - Summer: Jun, Jul, Aug
   - Fall: Sep, Oct, Nov

4. **Data Quality**:
   - Rounds monetary values to 2 decimal places
   - Validates coordinate ranges for NYC area
   - Basic fare amount validation

### Partitioning Strategy

Data is partitioned by:
- `year`: For annual analysis
- `month`: For monthly trends
- `date`: For daily granularity

This enables efficient querying by time periods and reduces costs.

## Getting Started

### Prerequisites Checklist
- [ ] AWS CLI installed and configured with appropriate permissions
- [ ] Python 3.11+ installed
- [ ] AWS CDK CLI installed (`npm install -g aws-cdk`)
- [ ] Node.js 18+ installed (required for CDK)
- [ ] Basic knowledge of AWS services (S3, Glue, Lambda, EventBridge)
- [ ] AWS account with appropriate service quotas

### Step-by-Step Setup

#### 1. Environment Setup
```bash
# Clone the repository
git clone <repository-url>
cd nyc-taxi-etl-pipeline

# Install Python dependencies
uv sync

# Activate virtual environment
source .venv/bin/activate
```

#### 2. AWS Configuration
```bash
# Configure AWS credentials
aws configure

# Or set environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=eu-central-1
```

#### 3. CDK Bootstrap (First-time setup)
```bash
# Bootstrap CDK in your AWS account
cdk bootstrap
```

#### 4. Deploy Infrastructure
```bash
# Deploy all stacks
cdk deploy --all

# Or deploy individual stacks
cdk deploy S3Stack
cdk deploy GlueStack
```

#### 5. Upload Sample Data
```bash
# Get your data bucket name from CDK outputs
cdk list-exports

# Upload sample data
python scripts/upload_sample_data.py your-data-bucket-name 2024 01
```

#### 6. Verify Deployment
- Check AWS Glue Console for the ETL job
- Verify S3 buckets are created
- Check EventBridge rule is scheduled
- Test Lambda function manually if needed

### Quick Commands Reference
```bash
# Deploy infrastructure
cdk deploy --all

# Destroy infrastructure
cdk destroy --all

# View CloudFormation templates
cdk synth

# List deployed stacks
cdk list

# Run tests
pytest tests/ -v

# Upload sample data
python scripts/upload_sample_data.py <bucket-name> <year> <month>
```

## Configuration Reference

### Environment Variables
| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `AWS_DEFAULT_REGION` | AWS region for deployment | `eu-central-1` | Yes |
| `AWS_PROFILE` | AWS profile to use | `default` | No |

### CDK Parameters
| Parameter | Description | Default | Stack |
|-----------|-------------|---------|-------|
| `DataBucketName` | S3 bucket for data storage | Auto-generated | S3Stack |
| `ScriptsBucketName` | S3 bucket for Glue scripts | Auto-generated | S3Stack |
| `GlueJobName` | Name of the Glue ETL job | `nyc-taxi-transformation-job` | GlueStack |
| `GlueScriptPath` | S3 path to Glue script | `scripts/data_transformation_job.py` | GlueStack |
| `GlueTempDir` | S3 path for Glue temp files | `temp/` | GlueStack |

### Lambda Function Configuration
| Setting | Value | Description |
|---------|-------|-------------|
| Runtime | Python 3.9 | Lambda runtime environment |
| Handler | `trigger_data_transformation.lambda_handler` | Function entry point |
| Timeout | 60 seconds | Maximum execution time |
| Memory | 128 MB | Allocated memory |

### Glue Job Configuration
| Setting | Value | Description |
|---------|-------|-------------|
| Workers | 10 | Number of Glue workers |
| Worker Type | G.1X | Worker instance type |
| Glue Version | 5.0 | Glue runtime version |
| Python Version | 3 | Python runtime version |

### EventBridge Schedule
| Setting | Value | Description |
|---------|-------|-------------|
| Schedule | `cron(0 4 * * *)` | Daily at 4 AM UTC |
| Timezone | UTC | Schedule timezone |

### Custom Deployment Examples
```bash
# Deploy with custom bucket names
cdk deploy S3Stack \
  --parameters DataBucketName=my-nyc-taxi-data \
  --parameters ScriptsBucketName=my-nyc-taxi-scripts

# Deploy with custom Glue job name
cdk deploy GlueStack \
  --parameters GlueJobName=my-custom-etl-job

# Deploy to specific region
AWS_DEFAULT_REGION=us-west-2 cdk deploy --all
```

## Testing

### Unit Tests
```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=glue_script --cov-report=html
```

## Sample Data

A sample NYC taxi dataset is included in `data/sample/nyc_taxi_sample.csv` for testing and development purposes. This file contains a small subset of taxi trip data that can be used to:

- Test the ETL pipeline locally
- Understand the expected data format
- Develop and debug transformations
- Validate the output schema

### Using the Sample Data

A utility script is provided to automate the upload process:

```bash
# Upload sample data to your S3 bucket
python scripts/upload_sample_data.py your-bucket-name 2024 01

# The script will upload the sample data to:
# s3://your-bucket-name/landing/2024/01/nyc_taxi_sample.csv
```

## Monitoring

### CloudWatch Logs
The solution generates logs in the following CloudWatch log groups:
- **Lambda Function**: `/aws/lambda/GlueStack-GlueTriggerLambda-*`
- **Glue Job**: `/aws-glue/jobs/logs-v2/nyc-taxi-transformation-job`

### Key Metrics to Monitor
| Metric | Description | Threshold | Action |
|--------|-------------|-----------|--------|
| Lambda Invocations | Number of Lambda function calls | Monitor for failures | Check EventBridge rule |
| Lambda Errors | Lambda function errors | > 0 | Review Lambda logs |
| Glue Job Success Rate | Successful job runs | < 95% | Check Glue job logs |
| Glue Job Duration | Job execution time | > 30 minutes | Optimize job performance |
| S3 Object Count | Number of processed files | Monitor trends | Verify data pipeline |


### Performance Monitoring
- **Glue Job Performance**: Monitor worker utilization and job duration
- **S3 Storage**: Track data growth and storage costs
- **Lambda Performance**: Monitor cold starts and execution time
- **EventBridge**: Verify scheduled executions are running

## Cost Estimation

**⚠️ Important**: Running this ETL pipeline will incur AWS costs. The solution uses multiple AWS services including S3, Glue, Lambda, EventBridge, and Athena, which will generate charges based on your usage. Please review AWS pricing for these services in your region and monitor your costs regularly.

## Contributing

Contributions are welcome! Please feel free to submit a pull request to the repository.

