"""
NASA POWER Climate Data Collector
Author: [Mohamed Elsayed]
Description: This script collects historical climate data from NASA's POWER API for regions 
             vulnerable to temperature extremes and heat waves. The data includes solar radiation,
             temperature, humidity, and other meteorological parameters useful for climate studies.
"""

import requests
import pandas as pd
import time
import concurrent.futures
from collections import defaultdict

# Simulated locations in regions facing climate challenges
# Coordinates represent real vulnerable areas but with fictional names
climate_vulnerable_regions = {
    "Sunblaze Valley": (33.7, -116.2),  # Similar to California's Coachella Valley (heat waves)
    "Mirage Plains": (24.6, 77.3),  # Similar to Rajasthan, India (extreme temperatures)
    "Ember Delta": (-16.5, -68.1),  # Similar to Bolivia Altiplano (high solar radiation)
    "Scorch Basin": (35.3, 115.6),  # Similar to Shandong, China (heat waves)
    "Droughtford": (-30.0, 136.0),  # Similar to South Australia (arid climate)
    "Heatburg": (32.6, -114.6),  # Similar to Arizona desert (extreme heat)
    "Flame Oasis": (28.2, 33.6)  # Similar to Egypt's Eastern Desert (high temperatures)
}

# Climate parameters of interest
climate_parameters = [
    "CLRSKY_SFC_SW_DWN",  # Clear-sky surface shortwave downward irradiance
    "ALLSKY_SFC_SW_DWN",  # All-sky surface shortwave downward irradiance
    "ALLSKY_SFC_SW_DIFF",  # Diffuse solar radiation at surface
    "ALLSKY_SFC_SW_DNI",  # Direct normal irradiance
    "SZA",  # Solar zenith angle
    "T2M",  # Temperature at 2 meters
    "T2MDEW",  # Dew/frost point at 2 meters
    "RH2M",  # Relative humidity at 2 meters
    "WS10M",  # Wind speed at 10 meters
    "WD10M",  # Wind direction at 10 meters
    "ALLSKY_SFC_UV_INDEX"  # UV Index
]

# Configuration
BATCH_SIZE = 5  # Number of parameters per API request
START_YEAR = 2001
END_YEAR = 2024
MAX_WORKERS = 6  # For concurrent requests


def fetch_climate_data(url):
    """Fetch data from NASA POWER API with retry logic"""
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=60)
            if response.status_code == 200:
                return response.json()
            print(f"Attempt {attempt + 1} failed - Status {response.status_code}")
            time.sleep(5)
        except Exception as e:
            print(f"Connection error: {e}")
            time.sleep(10)
    return None


def process_region_data(region_name, lat, lon, parameters, year):
    """Process climate data for a single region and year"""
    try:
        url = (f"https://power.larc.nasa.gov/api/temporal/hourly/point?"
               f"parameters={','.join(parameters)}&community=RE&"
               f"longitude={lon}&latitude={lat}&"
               f"start={year}0101&end={year}1231&format=JSON")

        response = fetch_climate_data(url)
        if not response:
            return None

        parameters_data = response.get("properties", {}).get("parameter", {})
        results = []

        # Get timestamps from the first parameter (all should have same timestamps)
        if parameters_data:
            timestamps = next(iter(parameters_data.values())).keys()

            for ts in timestamps:
                if len(ts) == 10 and ts.isdigit():
                    record = {
                        "YEAR": int(ts[:4]),
                        "MONTH": int(ts[4:6]),
                        "DAY": int(ts[6:8]),
                        "HOUR": int(ts[8:10]),
                        "LATITUDE": lat,
                        "LONGITUDE": lon,
                        "REGION": region_name
                    }

                    for param, values in parameters_data.items():
                        record[param] = values.get(ts)

                    results.append(record)

        print(f"Processed {year} data for {region_name} ({len(results)} records)")
        return results

    except Exception as e:
        print(f"Error processing {region_name} {year}: {e}")
        return None


def main():
    """Main execution function"""
    # Split parameters into batches for API requests
    param_batches = [climate_parameters[i:i + BATCH_SIZE]
                     for i in range(0, len(climate_parameters), BATCH_SIZE)]

    all_data = []

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []

            # Create tasks for each year, region, and parameter batch
            for year in range(START_YEAR, END_YEAR + 1):
                for region, (lat, lon) in climate_vulnerable_regions.items():
                    for batch in param_batches:
                        futures.append(
                            executor.submit(
                                process_region_data,
                                region, lat, lon, batch, year
                            )
                        )

            # Collect results as they complete
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    all_data.extend(result)

    except Exception as e:
        print(f"Execution error: {e}")

    # Save collected data
    if all_data:
        output_file = f"climate_data_{START_YEAR}_{END_YEAR}.csv"
        pd.DataFrame(all_data).to_csv(output_file, index=False)
        print(f"Successfully saved {len(all_data)} records to {output_file}")
    else:
        print("No data was collected")


if __name__ == "__main__":
    print("Starting climate data collection...")
    start_time = time.time()
    main()
    print(f"Completed in {(time.time() - start_time) / 60:.2f} minutes")