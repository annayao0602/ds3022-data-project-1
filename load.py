import duckdb
import os
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)


def load_parquet_files():

    con = None
   

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        con.execute("""
            CREATE OR REPLACE TABLE yellow_trip_data (
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                trip_distance DOUBLE
            );
        """)
        columns_to_load = [
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "trip_distance"
        ]
        columns_str = ", ".join(columns_to_load)
        year = 2024

        for month in range(1, 13):
            month_str = f"{month:02d}"
            
            # Dynamically create the file URL for the current month
            file_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month_str}.parquet"
            
            logger.info(f"Loading data from {file_url}...")
            
            # The INSERT statement is now inside the loop
            con.execute(f"""
                INSERT INTO yellow_trip_data ({columns_str})
                SELECT {columns_str} 
                FROM read_parquet('{file_url}');
                """)

        logger.info("Finished loading all 12 months of data.")
        
        row_count = con.execute("SELECT COUNT(*) FROM yellow_trip_data").fetchone()[0]
        print(f"Successfully loaded data. Table 'yellow_trip_data' now contains {row_count:,} rows.")
        logger.info(f"Final row count for 'yellow_trip_data': {row_count:,}")

        con.execute("""
            CREATE OR REPLACE TABLE green_trip_data (
                lpep_pickup_datetime TIMESTAMP,
                lpep_dropoff_datetime TIMESTAMP,
                trip_distance DOUBLE
            );
        """)
        columns_to_load2 = [
            "lpep_pickup_datetime", "lpep_dropoff_datetime",
            "trip_distance"
        ]
        columns_str2 = ", ".join(columns_to_load2)

        for month in range(1, 13):
            month_str = f"{month:02d}"
            
            # Dynamically create the file URL for the current month
            file_url2 = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month_str}.parquet"
            
            logger.info(f"Loading data from {file_url2}...")
            
            # The INSERT statement is now inside the loop
            con.execute(f"""
                INSERT INTO green_trip_data ({columns_str2})
                SELECT {columns_str2} 
                FROM read_parquet('{file_url2}');
                """)

        logger.info("Finished loading all 12 months of data.")
        
        row_count2 = con.execute("SELECT COUNT(*) FROM green_trip_data").fetchone()[0]
        print(f"Successfully loaded data. Table 'green_trip_data' now contains {row_count2:,} rows.")
        logger.info(f"Final row count for 'green_trip_data': {row_count2:,}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load_parquet_files()

