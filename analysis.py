import duckdb
import logging
import matplotlib.pyplot as plt
import pandas as pd


logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)


def transform():

    con = None

    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        #single largest carbon producing trip of the year for yellow taxi
        #----NOTE: Using stg_tables created in dbt (saving source data for best practices)----
        print("Analyzing data...")
        logger.info("Analyzing data...")
        largest_trip = con.execute("""
                    SELECT *
                    FROM stg_yellow_trip_data
                    ORDER BY trip_co2_kgs DESC
                    LIMIT 1;
                    """).fetchone()
        print(f"The largest carbon producing trip of the year for yellow taxis on {largest_trip[4]} and produced {largest_trip[6]} kgs of CO2")
        logger.info(f"The largest carbon producing trip of the year for yellow taxis on {largest_trip[4]} and produced {largest_trip[6]} kgs of CO2")

        #single largest carbon producing trip of the year for green taxi
        largest_trip_green = con.execute("""
                     SELECT *
                    FROM stg_green_trip_data
                    ORDER BY trip_co2_kgs DESC
                    LIMIT 1;
                    """).fetchone()
        print(f"The largest carbon producing trip of the year for green taxis was on {largest_trip_green[4]} and produced {largest_trip_green[6]} kgs of CO2")
        logger.info(f"The largest carbon producing trip of the year for green taxis on {largest_trip_green[4]} and produced {largest_trip_green[6]} kgs of CO2")

        #----average trip CO2 by time period----
        time = ['hour_of_day', 'day_of_week', 'week_of_year', 'month_of_year']
        time_labels = ['Hour of Day', 'Day of Week', 'Week of Year', 'Month of Year']

        #for loop to iterate through each time period
        for t, label in zip(time, time_labels):
            #highest average trip CO2 by time period
            highest_avg_co2 = con.execute(f"""
                        SELECT {t}, AVG(trip_co2_kgs) AS avg_co2
                        FROM stg_yellow_trip_data
                        GROUP BY {t}
                        ORDER BY avg_co2 DESC
                        LIMIT 1;
                        """).fetchone()
            print(f"Highest average trip CO2 by {label} (yellow taxi): {label}: {highest_avg_co2[0]}, Avg CO2: {highest_avg_co2[1]}")
            logger.info(f"Highest average trip CO2 by {label} (yellow taxi): {label}: {highest_avg_co2[0]}, Avg CO2: {highest_avg_co2[1]}")

            highest_avg_co2_green = con.execute(f"""
                        SELECT {t}, AVG(trip_co2_kgs) AS avg_co2
                        FROM stg_green_trip_data
                        GROUP BY {t}
                        ORDER BY avg_co2 DESC
                        LIMIT 1;
                        """).fetchone()
            print(f"Highest average trip CO2 by {label} (green taxi): {label}: {highest_avg_co2_green[0]}, Avg CO2: {highest_avg_co2_green[1]}")
            logger.info(f"Highest average trip CO2 by {label} (green taxi): {label}: {highest_avg_co2_green[0]}, Avg CO2: {highest_avg_co2_green[1]}")

            #lowest average trip CO2 by time period
            lowest_avg_co2 = con.execute(f"""
                        SELECT {t}, AVG(trip_co2_kgs) AS avg_co2
                        FROM stg_yellow_trip_data
                        GROUP BY {t}
                        ORDER BY avg_co2 ASC
                        LIMIT 1;
                        """).fetchone()
            print(f"Lowest average trip CO2 by {label} (yellow taxi): {label}: {lowest_avg_co2[0]}, Avg CO2: {lowest_avg_co2[1]}")
            logger.info(f"Lowest average trip CO2 by {label} (yellow taxi): {label}: {lowest_avg_co2[0]}, Avg CO2: {lowest_avg_co2[1]}")

            lowest_avg_co2_green = con.execute(f"""
                        SELECT {t}, AVG(trip_co2_kgs) AS avg_co2
                        FROM stg_green_trip_data
                        GROUP BY {t}
                        ORDER BY avg_co2 ASC
                        LIMIT 1;
                        """).fetchone()
            print(f"Lowest average trip CO2 by {label} (green taxi): {label}: {lowest_avg_co2_green[0]}, Avg CO2: {lowest_avg_co2_green[1]}")
            logger.info(f"Lowest average trip CO2 by {label} (green taxi): {label}: {lowest_avg_co2_green[0]}, Avg CO2: {lowest_avg_co2_green[1]}")
        

        #time-series plot or histogram with MONTH along the X-axis and CO2 totals along the Y-axis
        logger.info("Creating time-series plot for monthly CO2 emissions...")
        print("Creating time-series plot for monthly CO2 emissions...")
        monthly_co2 = con.execute("""
                    SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
                    FROM stg_yellow_trip_data
                    GROUP BY month_of_year
                    ORDER BY month_of_year;
                    """).fetchall()
        #make dataframe from query result
        df_monthly = pd.DataFrame(monthly_co2, columns=['month_of_year', 'total_co2'])
        #plot using matplotlib
        plt.figure(figsize=(10, 6))
        plt.plot(df_monthly['month_of_year'], df_monthly['total_co2'], marker='o')
        plt.title('Monthly CO2 Emissions from Yellow Taxi Trips')
        plt.xlabel('Month of Year')
        plt.ylabel('Total CO2 Emissions (kgs)')
        plt.xticks(range(1, 13))
        plt.grid()
        plt.savefig('monthly_co2_emissions_yellow_taxi.png')
        plt.close()
        print("Saved time-series plot as 'monthly_co2_emissions_yellow_taxi.png'")
        logger.info("Saved time-series plot as 'monthly_co2_emissions_yellow_taxi.png'")

        #time-series plot for green taxi
        logger.info("Creating time-series plot for monthly CO2 emissions (green taxi)...")
        print("Creating time-series plot for monthly CO2 emissions (green taxi)...")
        #sql query to get monthly CO2 totals for green taxi
        monthly_co2_green = con.execute("""
                    SELECT month_of_year, SUM(trip_co2_kgs) AS total_co2
                    FROM stg_green_trip_data 
                    GROUP BY month_of_year
                    ORDER BY month_of_year;
                    """).fetchall()
        #make dataframe from query result
        df_monthly_green = pd.DataFrame(monthly_co2_green, columns=['month_of_year', 'total_co2'])
        #plot using matplotlib
        plt.figure(figsize=(10, 6))
        plt.plot(df_monthly_green['month_of_year'], df_monthly_green['total_co2'], marker='o', color='green')
        plt.title('Monthly CO2 Emissions from Green Taxi Trips')
        plt.xlabel('Month of Year')
        plt.ylabel('Total CO2 Emissions (kgs)')
        plt.xticks(range(1, 13))
        plt.grid()
        plt.savefig('monthly_co2_emissions_green_taxi.png')
        plt.close()
        print("Saved time-series plot as 'monthly_co2_emissions_green_taxi.png'")
        logger.info("Saved time-series plot as 'monthly_co2_emissions_green_taxi.png'")

        print("Analysis complete.")
        logger.info("Analysis complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    transform()