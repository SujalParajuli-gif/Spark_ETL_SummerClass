import sys
import os
import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession
import time

# Add the 'utility' folder to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utility')))

from utility import setup_logger, format_time

# Set up logger
logger = setup_logger('load.log')

def create_spark_session():
    """Initialize a Spark session."""
    return SparkSession.builder \
        .appName("Load and Execute") \
        .config("spark.jars", "/home/sujal/Workspace/pyspark/venv/lib/python3.10/site-packages/pyspark/jars/")\
        .getOrCreate()

def create_postgres_tables(pg_un, pg_pw):
    """Create PostgreSQL tables if they don't exist using psycopg2."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=pg_un,
            password=pg_pw,
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        create_table_queries = [
            """ CREATE TABLE IF NOT EXISTS master_table (track_id VARCHAR(50), track_name TEXT, track_popularity INTEGER, artist_id VARCHAR(50), artist_name TEXT, followers FLOAT, genres TEXT, artist_popularity INTEGER, danceability FLOAT, energy FLOAT, tempo FLOAT, related_ids TEXT[]); """,
            """ CREATE TABLE IF NOT EXISTS recommendations_exploded (id VARCHAR(50), related_id VARCHAR(50)); """,
            """ CREATE TABLE IF NOT EXISTS artist_track (id VARCHAR(50), artist_id VARCHAR(50)); """,
            """ CREATE TABLE IF NOT EXISTS tracks_metadata (id VARCHAR(50) PRIMARY KEY, name TEXT, popularity INTEGER, duration_ms INTEGER, danceability FLOAT, energy FLOAT, tempo FLOAT); """,
            """ CREATE TABLE IF NOT EXISTS artists_metadata (id VARCHAR(50) PRIMARY KEY, name TEXT, followers FLOAT, popularity INTEGER); """
        ]

        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        logger.info("PostgreSQL tables created successfully.")

    except Exception as e:
        logger.error(f"Error creating tables: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_to_postgres(spark, input_dir, pg_un, pg_pw):
    """Load Parquet files to PostgreSQL."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver"
    }

    tables = [
        ("stage2/master_table", "master_table"),
        ("stage3/recommendations_exploded", "recommendations_exploded"),
        ("stage3/artist_track", "artist_track"),
        ("stage3/tracks_metadata", "tracks_metadata"),
        ("stage3/artists_metadata", "artists_metadata")
    ]

    for parquet_path, table_name in tables:
        start_time = time.time()  # Start the timer
        try:
            df = spark.read.parquet(os.path.join(input_dir, parquet_path))
            mode = "append" if 'master' in parquet_path else "overwrite"
            df.write \
                .mode(mode) \
                .jdbc(jdbc_url, table_name, properties=connection_properties)
            logger.info(f"Loaded {table_name} to PostgreSQL")
        except Exception as e:
            logger.error(f"Error loading {table_name}: {e}")
        
        end_time = time.time()  # End the timer
        elapsed_time = end_time - start_time  # Calculate the elapsed time
        formatted_time = format_time(elapsed_time)  # Format the time
        logger.info(f"Loading {table_name} took {formatted_time}.")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python execute.py <input_directory> <pg_username> <pg_password>")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    pg_un = sys.argv[2]
    pg_pw = sys.argv[3]

    if not os.path.exists(input_dir):
        logger.error(f"Error: Input directory {input_dir} does not exist.")
        sys.exit(1)
    
    spark = create_spark_session()
    create_postgres_tables(pg_un, pg_pw)  # Pass credentials here
    load_to_postgres(spark, input_dir, pg_un, pg_pw)  # Pass credentials here
    
    logger.info("Load stage completed")
