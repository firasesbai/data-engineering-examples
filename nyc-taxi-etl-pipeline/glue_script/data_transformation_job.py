import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, udf, when, date_format, year, month, 
    hour, dayofweek, unix_timestamp, from_unixtime, round as spark_round,
    isnan, isnull, count
)
from pyspark.sql.types import StringType, TimestampType, DoubleType, IntegerType, BooleanType
from utils import (
    calculate_trip_duration, get_season, generate_trip_id, 
    validate_coordinates, validate_monetary_amount
)
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATA_BUCKET', 'YEAR', 'MONTH'])

# Construct S3 paths dynamically
s3_input_path = f"s3://{args['DATA_BUCKET']}/landing/{args['YEAR']}/{args['MONTH']}/"
s3_output_path = f"s3://{args['DATA_BUCKET']}/processed"

logger.info(f"Processing data from: {s3_input_path}")
logger.info(f"Output path: {s3_output_path}")

# Initialize Glue & Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read CSV file
logger.info("Reading input CSV data...")
df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("mode", "PERMISSIVE")\
    .option("columnNameOfCorruptRecord", "_corrupt_record")\
    .load(s3_input_path)

logger.info(f"Initial record count: {df.count()}")
logger.info(f"Initial schema: {df.schema}")
logger.info(f"Available columns: {df.columns}")
logger.info("Sample data preview:")
df.show(5, truncate=False)

# Register UDFs
calculate_duration_udf = udf(calculate_trip_duration, DoubleType())
get_season_udf = udf(get_season, StringType())
generate_trip_id_udf = udf(generate_trip_id, StringType())
validate_coords_udf = udf(lambda lat, lon: validate_coordinates(float(lat), float(lon)), BooleanType())
validate_amount_udf = udf(validate_monetary_amount, BooleanType())

logger.info("Performing data quality checks...")

# Check for null values (only use isnull for all columns, isnan only for numeric columns)
null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
logger.info("Null value counts:")
null_counts.show()

# Check for NaN values only in numeric columns
numeric_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (DoubleType, IntegerType))]
if numeric_columns:
    nan_counts = df.select([count(when(isnan(c), c)).alias(f"{c}_nan") for c in numeric_columns])
    logger.info("NaN value counts in numeric columns:")
    nan_counts.show()

# Check for corrupt records (only if the column exists)
if "_corrupt_record" in df.columns:
    corrupt_records = df.filter(col("_corrupt_record").isNotNull())
    corrupt_count = corrupt_records.count()
    if corrupt_count > 0:
        logger.warning(f"Found {corrupt_count} corrupt records")
        corrupt_records.show()
    else:
        logger.info("No corrupt records found")
else:
    logger.info("No corrupt records column found - data parsed successfully")

# Data transformation pipeline
logger.info("Starting data transformation...")

# Verify required columns exist
required_columns = [
    "pickup_datetime", "dropoff_datetime", "pickup_longitude", "pickup_latitude",
    "dropoff_longitude", "dropoff_latitude", "passenger_count", "trip_distance",
    "fare_amount", "tip_amount", "total_amount"
]

missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    logger.error(f"Missing required columns: {missing_columns}")
    logger.error(f"Available columns: {df.columns}")
    raise ValueError(f"Missing required columns: {missing_columns}")

logger.info("All required columns present, proceeding with transformation...")

transformed_df = df.withColumn(
    "trip_id", generate_trip_id_udf(col("pickup_datetime"), col("dropoff_datetime"))
).withColumn(
    "pickup_time_utc", col("pickup_datetime").cast(TimestampType())
).withColumn(
    "dropoff_time_utc", col("dropoff_datetime").cast(TimestampType())
).withColumn(
    "pickup_time_local", from_unixtime(unix_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "dropoff_time_local", from_unixtime(unix_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "trip_duration_minutes", calculate_duration_udf(col("pickup_datetime"), col("dropoff_datetime"))
).withColumn(
    "pickup_hour", hour(col("pickup_datetime"))
).withColumn(
    "pickup_day_of_week", dayofweek(col("pickup_datetime"))
).withColumn(
    "season", get_season_udf(col("pickup_datetime"))
).withColumn(
    "year", year(col("pickup_datetime"))
).withColumn(
    "month", month(col("pickup_datetime"))
).withColumn(
    "date", date_format(col("pickup_datetime"), "yyyy-MM-dd")
).withColumn(
    "trip_distance_miles", spark_round(col("trip_distance"), 2)
).withColumn(
    "fare_amount", spark_round(col("fare_amount"), 2)
).withColumn(
    "tip_amount", spark_round(col("tip_amount"), 2)
).withColumn(
    "total_amount", spark_round(col("total_amount"), 2)
).withColumn(
    "passenger_count", col("passenger_count").cast(IntegerType())
).withColumn(
    "is_valid_coordinates", validate_coords_udf(col("pickup_latitude"), col("pickup_longitude")) & 
                           validate_coords_udf(col("dropoff_latitude"), col("dropoff_longitude"))
).withColumn(
    "is_valid_fare", validate_amount_udf(col("fare_amount"))
).select(
    "trip_id",
    "pickup_time_utc",
    "pickup_time_local", 
    "dropoff_time_utc",
    "dropoff_time_local",
    "trip_duration_minutes",
    "pickup_latitude",
    "pickup_longitude",
    "dropoff_latitude", 
    "dropoff_longitude",
    "passenger_count",
    "trip_distance_miles",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "pickup_hour",
    "pickup_day_of_week",
    "season",
    "year",
    "month",
    "date",
    "is_valid_coordinates",
    "is_valid_fare"
).orderBy("pickup_time_utc")

# Log transformation statistics
logger.info(f"Transformed record count: {transformed_df.count()}")

# Save output in Parquet format to S3 partitioned by year, month, and date
logger.info("Writing transformed data to S3...")
transformed_df.write.mode("overwrite").partitionBy("year", "month", "date").parquet(s3_output_path)

logger.info("Data transformation completed successfully!")

# Commit Glue Job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit() 