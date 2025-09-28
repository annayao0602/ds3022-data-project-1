import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)


def clean_parquet():

    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        #before cleaning
        print("Before cleaning:")
        logger.info("Before cleaning:")

        #Yellow trip data to be cleaned
        duplicate_yellow = con.execute("SELECT COUNT(*) - COUNT(DISTINCT *) FROM yellow_trip_data").fetchone()[0]
        print(f"Number of duplicate rows in yellow_trip_data before cleaning: {duplicate_yellow}")
        logger.info(f"Number of duplicate rows in yellow_trip_data before cleaning: {duplicate_yellow}")

        passenger0_count_yellow = con.execute("SELECT COUNT(*) FROM yellow_trip_data WHERE passenger_count = 0").fetchone()[0]
        print(f"Number of rows with passenger_count = 0 in yellow_trip_data before cleaning: {passenger0_count_yellow}")
        logger.info(f"Number of rows with passenger_count = 0 in yellow_trip_data before cleaning: {passenger0_count_yellow}")

        invalid_trip_distance_yellow = con.execute("SELECT COUNT(*) FROM yellow_trip_data WHERE trip_distance <= 0 OR trip_distance > 100").fetchone()[0]
        print(f"Number of rows with invalid trip_distance in yellow_trip_data before cleaning: {invalid_trip_distance_yellow}")
        logger.info(f"Number of rows with invalid trip_distance in yellow_trip_data before cleaning: {invalid_trip_distance_yellow}")

        invalid_time_yellow = con.execute("SELECT COUNT(*) FROM yellow_trip_data WHERE epoch(tpep_dropoff_datetime - tpep_pickup_datetime) < 0 OR epoch(tpep_dropoff_datetime - tpep_pickup_datetime) > 86400").fetchone()[0]
        print(f"Number of rows with invalid trip time in yellow_trip_data before cleaning: {invalid_time_yellow}")
        logger.info(f"Number of rows with invalid trip time in yellow_trip_data before cleaning: {invalid_time_yellow}")

        #Green trip data to be cleaned
        duplicate_green = con.execute("SELECT COUNT(*) - COUNT(DISTINCT *) FROM green_trip_data").fetchone()[0]
        print(f"Number of duplicate rows in green_trip_data before cleaning: {duplicate_green}")
        logger.info(f"Number of duplicate rows in green_trip_data before cleaning: {duplicate_green}")

        passenger0_count_green = con.execute("SELECT COUNT(*) FROM green_trip_data WHERE passenger_count = 0").fetchone()[0]
        print(f"Number of rows with passenger_count = 0 in green_trip_data before cleaning: {passenger0_count_green}")
        logger.info(f"Number of rows with passenger_count = 0 in green_trip_data before cleaning: {passenger0_count_green}")

        invalid_trip_distance_green = con.execute("SELECT COUNT(*) FROM green_trip_data WHERE trip_distance <= 0 OR trip_distance > 100").fetchone()[0]
        print(f"Number of rows with invalid trip_distance in green_trip_data before cleaning: {invalid_trip_distance_green}")
        logger.info(f"Number of rows with invalid trip_distance in green_trip_data before cleaning: {invalid_trip_distance_green}")

        invalid_time_green = con.execute("SELECT COUNT(*) FROM green_trip_data WHERE epoch(lpep_dropoff_datetime - lpep_pickup_datetime) < 0 OR epoch(lpep_dropoff_datetime - lpep_pickup_datetime) > 86400").fetchone()[0]
        print(f"Number of rows with invalid trip time in green_trip_data before cleaning: {invalid_time_green}")
        logger.info(f"Number of rows with invalid trip time in green_trip_data before cleaning: {invalid_time_green}")

        # Clean yellow trip data
        print("Cleaning data...")
        con.execute(f"""
                    CREATE OR REPLACE TABLE yellow_trip_data AS 
                    SELECT DISTINCT * 
                    FROM yellow_trip_data
                    WHERE
                        passenger_count > 0
                    AND
                        trip_distance > 0 AND trip_distance <= 100
                    AND  
                        epoch(tpep_dropoff_datetime - tpep_pickup_datetime) BETWEEN 0 AND 86400;

                    """)
        logger.info("Removed duplicates and invalid rows from yellow trip data")
        print("Removed duplicates and invalid rows from yellow trip data")
        row_count = con.execute("SELECT COUNT(*) FROM yellow_trip_data").fetchone()[0]
        print(f"Table 'yellow_trip_data' now contains {row_count:,} rows.")

        # Remove duplicates in green trip data
        con.execute(f"""
                    CREATE OR REPLACE TABLE green_trip_data AS 
                    SELECT DISTINCT * 
                    FROM green_trip_data
                    WHERE
                        passenger_count > 0
                    AND
                        trip_distance > 0 AND trip_distance <= 100
                    AND  
                        epoch(lpep_dropoff_datetime - lpep_pickup_datetime) BETWEEN 0 AND 86400;
                    """)
        

        logger.info("Removed duplicates and invalid rows from green trip data")
        print("Removed duplicates and invalid rows from green trip data")
        row_count = con.execute("SELECT COUNT(*) FROM green_trip_data").fetchone()[0]
        print(f"Table 'green_trip_data' now contains {row_count:,} rows.")

        #after cleaning
        print("After cleaning:")
        logger.info("After cleaning:")
        #check if there are any rows that still need to be cleaned for yellow trip data
        cleaned_rows = con.execute("""SELECT COUNT(*) 
                                       FROM yellow_trip_data 
                                       WHERE 
                                        passenger_count = 0 
                                       OR
                                        trip_distance <= 0 OR trip_distance > 100
                                       OR   
                                        epoch(tpep_dropoff_datetime - tpep_pickup_datetime) < 0 OR epoch(tpep_dropoff_datetime - tpep_pickup_datetime) > 86400;
                                       """).fetchone()[0]
        print(f"Number of rows to be cleaned in yellow_trip_data after cleaning: {cleaned_rows}")
        logger.info(f"Number of rows to be cleaned in yellow_trip_data after cleaning: {cleaned_rows}") 

        #check if there are any rows that still need to be cleaned for green trip data
        cleaned_rows2 = con.execute("""SELECT COUNT(*)    
                                        FROM green_trip_data 
                                        WHERE 
                                         passenger_count = 0 
                                        OR
                                         trip_distance <= 0 OR trip_distance > 100
                                        OR   
                                         epoch(lpep_dropoff_datetime - lpep_pickup_datetime) < 0 OR epoch(lpep_dropoff_datetime - lpep_pickup_datetime) > 86400;
                                        """).fetchone()[0]
        print(f"Number of rows to be cleaned in green_trip_data after cleaning: {cleaned_rows2}")
        logger.info(f"Number of rows to be cleaned in green_trip_data after cleaning: {cleaned_rows2}")  

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    clean_parquet()

