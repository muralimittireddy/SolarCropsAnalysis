import requests
import pandas as pd
# import pyarrow.parquet as pq
# import pyarrow as pa
# from google.cloud import storage
# import os
from sqlalchemy import create_engine
# import yaml

import aiohttp  # Async HTTP library
import asyncio  # Async programming library

POSTGRES_CONN = "postgresql://app_user:app_password@postgres_app:5432/app_db"


async def fetch_data(session, year):
    
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude=17.68&longitude=83.21&start_date={year}-01-01&end_date={year}-01-31&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,precipitation_probability,precipitation,rain,pressure_msl,surface_pressure,cloud_cover,wind_speed_10m,wind_direction_10m,soil_temperature_6cm,soil_temperature_18cm,soil_moisture_0_to_1cm,soil_moisture_3_to_9cm,is_day,sunshine_duration,direct_radiation,diffuse_radiation&daily=sunrise,sunset,daylight_duration&timezone=Asia%2FBangkok"
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Failed to fetch data for {year}: {response.status}")
                return None
    except Exception as e:
        print(f"Error fetching data for {year}: {e}")
        return None


# Main function to fetch all years
async def main():
    async with aiohttp.ClientSession() as session:  # Creates a session for all requests
        tasks = [fetch_data(session, year) for year in range(2013, 2014 + 1)]
        print("In Asynchronus function")
        results = await asyncio.gather(*tasks)  # Runs all tasks at the same time
    return results


def fetch_all_data():
    """Fetch data from API"""
    # Run the async function
    data = asyncio.run(main())  
    print("Done fetching all years!")
    return data

def save_to_postgres(df,table_name):
    """Save extracted data to PostgreSQL running in Docker"""
    try:
        engine = create_engine(POSTGRES_CONN)
        with engine.connect() as conn:
            df.to_sql(table_name, con=conn, if_exists="replace", index=False)
        print("Data saved to PostgreSQL table: solar_crops_raw")
    except Exception as e:
        print(f"Error saving data to PostgreSQL: {e}")

def extract_data():
    print("Extracting data from API...")
    data = fetch_all_data()
    print("Extraction completed")
    # Convert JSON to DataFrames
    df_hourly_list = []
    df_daily_list = []

    for entry in data:
        hourly_units = entry["hourly_units"]
        hourly_data = entry["hourly"]
        daily_units = entry["daily_units"]
        daily_data = entry["daily"]

        # Hourly DataFrame
        df_hourly = pd.DataFrame(hourly_data)
        df_hourly["Time_Units"] = hourly_units["time"]
        for key, unit in hourly_units.items():
            if key != "time":
                df_hourly[f"{key}_unit"] = unit  # Add unit suffix to differentiate
        df_hourly_list.append(df_hourly)

        # Daily DataFrame
        df_daily = pd.DataFrame(daily_data)
        df_daily["Time_Units"] = daily_units["time"]
        for key, unit in daily_units.items():
            if key != "time":
                df_daily[f"{key}_unit"] = unit  # Add unit suffix
        df_daily_list.append(df_daily)

    # Combine into single DataFrame
    df_hourly = pd.concat(df_hourly_list, ignore_index=True)
    df_daily = pd.concat(df_daily_list, ignore_index=True)

    save_to_postgres(df_hourly,"hourly_data")
    save_to_postgres(df_daily,"daily_data")

    # Display results
    # print("Hourly Data:")
    # print(df_hourly)
    # print("\nDaily Data:")
    # print(df_daily)

