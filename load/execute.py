
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
        .config("spark.jars", "/home/sujal/Workspace/pyspark/venv/lib/python3.10/site-packages/pyspark/jars/") \
        .getOrCreate()

def unlock_tables(pg_un, pg_pw):
    """Unlock all tables to ensure they are accessible."""
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
        
        # Disable row-level security if it's enabled
        unlock_table_query = """
        ALTER TABLE public.master_table DISABLE ROW LEVEL SECURITY;
        ALTER TABLE public.shopping_behavior DISABLE ROW LEVEL SECURITY;
        ALTER TABLE public.shopping_trends DISABLE ROW LEVEL SECURITY;
        """
        cursor.execute(unlock_table_query)
        conn.commit()
        logger.info("Tables have been unlocked successfully.")

    except Exception as e:
        logger.error(f"Error unlocking tables: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def create_master_table(pg_un, pg_pw):
    """Create the master table in PostgreSQL."""
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

        # Create the master_table schema in PostgreSQL
        create_table_query = """
        CREATE TABLE IF NOT EXISTS master_table (
            user_id VARCHAR(50),
            age INT,
            gender VARCHAR(10),
            income FLOAT,
            purchased_items TEXT,
            item_id VARCHAR(50),
            category VARCHAR(50),
            trend_score FLOAT,
            popularity INT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Master table created successfully.")

    except Exception as e:
        logger.error(f"Error creating master table: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

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
            """ CREATE TABLE IF NOT EXISTS shopping_behavior (
                    user_id VARCHAR(50) PRIMARY KEY,
                    age INT,
                    gender VARCHAR(10),
                    income FLOAT,
                    purchased_items TEXT
                ); """,
            """ CREATE TABLE IF NOT EXISTS shopping_trends (
                    item_id VARCHAR(50) PRIMARY KEY,
                    category VARCHAR(50),
                    trend_score FLOAT,
                    popularity INT
                ); """
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

    # Updated paths to your correct Parquet files
    tables = [
        ("/home/sujal/Data/Transform/stage2/master_table", "master_table"),
        ("/home/sujal/Data/Transform/stage3/shopping_behavior_metadata", "shopping_behavior"),
        ("/home/sujal/Data/Transform/stage3/shopping_trends_metadata", "shopping_trends")
    ]

    for parquet_path, table_name in tables:
        start_time = time.time()  # Start the timer
        try:
            # Check if the path exists
            if not os.path.exists(parquet_path):
                logger.error(f"Path does not exist: {parquet_path}")
                continue

            # Read parquet file into Spark DataFrame
            df = spark.read.parquet(parquet_path)

            # Handle missing data (Replace column names based on your dataset)
            if table_name == 'shopping_behavior':
                df = df.fillna({'income': 0, 'purchased_items': 'Unknown'})  # Adjust column names
            if table_name == 'shopping_trends':
                df = df.fillna({'trend_score': 0, 'popularity': 0, 'category': 'Unknown'})  # Adjust for missing columns

            # Write to PostgreSQL with either append or overwrite mode
            mode = "append" if 'shopping_behavior' in parquet_path else "overwrite"
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
    
    create_master_table(pg_un, pg_pw)
    unlock_tables(pg_un, pg_pw)

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
