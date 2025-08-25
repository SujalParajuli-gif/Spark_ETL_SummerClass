import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

# Explicitly add utility path to sys.path
sys.path.append('/home/sujal/Workspace/etl/utility')

# Import utility functions
from utility import setup_logger, format_time

# Set up logger
logger = setup_logger('transform.log')

def create_spark_session():
    """Initialize a Spark session"""
    try:
        spark = SparkSession.builder \
            .appName("ETL Transform") \
            .getOrCreate()
        logger.info("Spark session created successfully.")
        time.sleep(1)  # Slow down the log progression slightly
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def load_and_clean(spark, input_dir, output_dir):
    """Stage 1: Load data, drop duplicates, remove nulls, save cleaned data."""
    start_time = time.time()
    logger.info("Stage 1: Loading and cleaning data...")
    time.sleep(1)  # Slow down the log progression slightly

    # Define schema for shopping_behavior_updated.csv
    shopping_behavior_schema = T.StructType([
        T.StructField("user_id", T.StringType(), False),
        T.StructField("age", T.IntegerType(), True),
        T.StructField("gender", T.StringType(), True),
        T.StructField("income", T.FloatType(), True),
        T.StructField("purchased_items", T.StringType(), True)
    ])

    # Define schema for shopping_trends.csv
    shopping_trends_schema = T.StructType([
        T.StructField("item_id", T.StringType(), False),
        T.StructField("category", T.StringType(), True),
        T.StructField("trend_score", T.FloatType(), True),
        T.StructField("popularity", T.IntegerType(), True)
    ])

    try:
        # Load the data
        shopping_behavior_df = spark.read.schema(shopping_behavior_schema).csv(os.path.join(input_dir, "shopping_behavior_updated.csv"), header=True)
        shopping_trends_df = spark.read.schema(shopping_trends_schema).csv(os.path.join(input_dir, "shopping_trends.csv"), header=True)

        # Clean the data: Drop duplicates and null values
        shopping_behavior_df = shopping_behavior_df.dropDuplicates(["user_id"]).filter(F.col("user_id").isNotNull())
        shopping_trends_df = shopping_trends_df.dropDuplicates(["item_id"]).filter(F.col("item_id").isNotNull())

        # Write cleaned data to output directory (stage 1)
        shopping_behavior_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "shopping_behavior"))
        shopping_trends_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "shopping_trends"))

        logger.info(f"Stage 1: Data cleaned and saved to {os.path.join(output_dir, 'stage1')}.")
        time.sleep(1)  # Slow down the log progression slightly
    except Exception as e:
        logger.error(f"Error in Stage 1: {e}")
        raise

    end_time = time.time()
    elapsed_time = end_time - start_time
    formatted_time = format_time(elapsed_time)
    logger.info(f"Stage 1 completed in {formatted_time}.")
    time.sleep(1)  # Slow down the log progression slightly

    return shopping_behavior_df, shopping_trends_df

def create_master_table(output_dir, shopping_behavior_df, shopping_trends_df):
    """Stage 2: Create master table by joining shopping behavior and shopping trends."""
    start_time = time.time()
    logger.info("Stage 2: Creating master table by joining datasets...")
    time.sleep(1)  # Slow down the log progression slightly

    try:
        # Example join: Assuming user purchases are related to item categories from trends
        master_df = shopping_behavior_df.join(shopping_trends_df, shopping_behavior_df.purchased_items == shopping_trends_df.item_id, "left") \
            .select(
                shopping_behavior_df.user_id,
                shopping_behavior_df.age,
                shopping_behavior_df.gender,
                shopping_behavior_df.income,
                shopping_behavior_df.purchased_items,
                shopping_trends_df.category,
                shopping_trends_df.trend_score,
                shopping_trends_df.popularity
            )

        # Save the master table
        master_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage2", "master_table"))
        logger.info(f"Stage 2: Master table created and saved to {os.path.join(output_dir, 'stage2')}.")
        time.sleep(1)  # Slow down the log progression slightly
    except Exception as e:
        logger.error(f"Error in Stage 2: {e}")
        raise

    end_time = time.time()
    elapsed_time = end_time - start_time
    formatted_time = format_time(elapsed_time)
    logger.info(f"Stage 2 completed in {formatted_time}.")
    time.sleep(1)  # Slow down the log progression slightly

def create_query_tables(output_dir, shopping_behavior_df, shopping_trends_df):
    """Stage 3: Create query-optimized tables."""
    start_time = time.time()
    logger.info("Stage 3: Creating query-optimized tables...")
    time.sleep(1)  # Slow down the log progression slightly

    try:
        # Example: Prepare data for queries, explode trends
        trends_exploded = shopping_trends_df.withColumn("trend_info", F.explode(F.array("category", "trend_score", "popularity"))) \
            .select("item_id", "trend_info")

        trends_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "trends_exploded"))

        # Prepare metadata tables
        shopping_behavior_metadata = shopping_behavior_df.select(
            "user_id", "age", "gender", "income"
        )
        shopping_behavior_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "shopping_behavior_metadata"))

        shopping_trends_metadata = shopping_trends_df.select(
            "item_id", "category", "trend_score", "popularity"
        )
        shopping_trends_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "shopping_trends_metadata"))

        logger.info(f"Stage 3: Query-optimized tables saved to {os.path.join(output_dir, 'stage3')}.")
        time.sleep(1)  # Slow down the log progression slightly
    except Exception as e:
        logger.error(f"Error in Stage 3: {e}")
        raise

    end_time = time.time()
    elapsed_time = end_time - start_time
    formatted_time = format_time(elapsed_time)
    logger.info(f"Stage 3 completed in {formatted_time}.")
    time.sleep(1)  # Slow down the log progression slightly

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logger.error("Usage: python execute.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()

    shopping_behavior_df, shopping_trends_df = load_and_clean(spark, input_dir, output_dir)
    create_master_table(output_dir, shopping_behavior_df, shopping_trends_df)
    create_query_tables(output_dir, shopping_behavior_df, shopping_trends_df)

    logger.info("ETL Transformation pipeline completed successfully.")
    time.sleep(1)  # Final pause before completion
