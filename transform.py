import duckdb
import logging

#USING DBT FOR TRANSFORMATIONS, log in dbt.log

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)


def transform():

    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        #calculate new column trip_co2_kgs for yellow_trip_data
        print("Transforming yellow_trip_data...")
        logger.info("Transforming yellow_trip_data...")
        con.execute("""
                    CREATE OR REPLACE TABLE yellow_trip_data AS
                    SELECT *,
                    (trip_distance * 
                    (SELECT co2_grams_per_mile FROM vehicle_emissions WHERE vehicle_type = 'yellow_taxi') / 1000) 
                    AS trip_co2_kgs
                    FROM yellow_trip_data;
                    """)
        logger.info("Transformed yellow_trip_data with new column trip_co2_kgs")
        print("Transformed yellow_trip_data with new column trip_co2_kgs")

        #calculate avg_mph for yellow_trip_data
        con.execute("""
                    CREATE OR REPLACE TABLE yellow_trip_data AS
                    SELECT *,
                    trip_distance / ((epoch(tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0) AS avg_mph
                    FROM yellow_trip_data;
                    """)
        logger.info("Transformed yellow_trip_data with new column avg_mph")
        print("Transformed yellow_trip_data with new column avg_mph")
        #extract new columns hour_of_day, day_of_week, week_of_year, month_of_year for yellow_trip_data
        con.execute("""
                    CREATE OR REPLACE TABLE yellow_trip_data AS
                    SELECT *,
                    EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour_of_day,
                    EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
                    EXTRACT(WEEK FROM tpep_pickup_datetime) AS week_of_year,
                    EXTRACT(MONTH FROM tpep_pickup_datetime) AS month_of_year
                    FROM yellow_trip_data;
                    """)
        print("Transformed yellow_trip_data with new datetime columns")
        logger.info("Transformed yellow_trip_data with new datetime columns")
        

        #___GREEN TAXI DATA TRANSFORMATIONS___#
        #calculate new column trip_co2_kgs for green_trip_data
        print("Transforming green_trip_data...")
        con.execute("""
                    CREATE OR REPLACE TABLE green_trip_data AS
                    SELECT *,
                    (trip_distance * 
                    (SELECT co2_grams_per_mile FROM vehicle_emissions WHERE vehicle_type = 'green_taxi') / 1000) 
                    AS trip_co2_kgs,
                    trip_distance / ((epoch(lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600.0) AS avg_mph
                    FROM green_trip_data;
                    """)
        logger.info("Transformed green_trip_data with new columns trip_co2_kgs and avg_mph")
        print("Transformed green_trip_data with new columns trip_co2_kgs and avg_mph")
        #extract new columns hour_of_day, day_of_week, week_of_year, month_of_year for green_trip_data
        con.execute("""
                    CREATE OR REPLACE TABLE green_trip_data AS
                    SELECT *,
                    EXTRACT(HOUR FROM lpep_pickup_datetime) AS hour_of_day,
                    EXTRACT(DOW FROM lpep_pickup_datetime) AS day_of_week,
                    EXTRACT(WEEK FROM lpep_pickup_datetime) AS week_of_year,
                    EXTRACT(MONTH FROM lpep_pickup_datetime) AS month_of_year
                    FROM green_trip_data;
                    """)
        logger.info("Transformed green_trip_data with new datetime columns")
        

        
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    transform()