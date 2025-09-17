import logging
from datetime import datetime
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from google.cloud import storage
from io import BytesIO
import os


# Configuration
GCS_BUCKET_NAME = 'maxisales_bucket'

logger = logging.getLogger(__name__)

# Set the Google Cloud credentials file path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service.json"

def weather_data():
    """Fetch weather data from Open-Meteo API for multiple locations and combine into one file"""
    try:
        # Initialize clients ONCE outside the loop
        cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)
        
        # Initialize storage client ONCE
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        
        url = "https://archive-api.open-meteo.com/v1/archive"
        
        LOCATIONS = [
            {'latitude': 52.52, 'longitude': 13.41, 'name': 'berlin'},
            {'latitude': 40.41, 'longitude': -3.70, 'name': 'madrid'},
            {'latitude': 52.36, 'longitude': 4.40, 'name': 'amsterdam'},
            {'latitude': 48.85, 'longitude': 2.35, 'name': 'paris'},
            {'latitude': 51.51, 'longitude': -0.13, 'name': 'london'}
        ]
        
        # List to store DataFrames from all locations
        all_dataframes = []
        
        # Generate timestamp ONCE for the combined file
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        for location in LOCATIONS:
            logger.info(f"Starting weather data fetch for {location['name']} from Open-Meteo API")
            
            params = {
                "latitude": location['latitude'],
                "longitude": location['longitude'],
                "start_date": "2023-01-01",
                "end_date": "2024-12-01",
                "daily": [
                "temperature_2m_mean", "weather_code", "sunshine_duration", 
                "temperature_2m_max", "temperature_2m_min", "apparent_temperature_mean", 
                "apparent_temperature_max", "apparent_temperature_min", "sunrise", 
                "sunset", "rain_sum", "snowfall_sum"
                ],
                "timezone": "Europe/London"
            }
            
            # Make API request
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]
            
            # Log the response coordinates
            logger.info(f"Fetched weather data for {location['name']} at coordinates {response.Latitude()}°N {response.Longitude()}°E")
            
            # Process daily data
            daily = response.Daily()
        
            # Create date range
            date_range = pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            ).tz_convert(None) 
        
            # Extract variables
            daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
            daily_weather_code = daily.Variables(1).ValuesAsNumpy()
            daily_sunshine_duration = daily.Variables(2).ValuesAsNumpy()
            daily_temperature_2m_max = daily.Variables(3).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(4).ValuesAsNumpy()
            daily_sunrise = daily.Variables(8).ValuesInt64AsNumpy()
            daily_sunset = daily.Variables(9).ValuesInt64AsNumpy()
            
            # Create DataFrame for each location
            daily_data = {
                "date": date_range,
                "location": location['name'],
                "latitude": location['latitude'],
                "longitude": location['longitude'],
                "temperature_2m_mean": daily_temperature_2m_mean,
                "weather_code": daily_weather_code,
                "sunshine_duration": daily_sunshine_duration,
                "temperature_2m_max": daily_temperature_2m_max,
                "temperature_2m_min": daily_temperature_2m_min,
                "sunrise": daily_sunrise,
                "sunset": daily_sunset,
            }
            
            df = pd.DataFrame(data=daily_data)
            all_dataframes.append(df)  # Add to list instead of uploading immediately
        
            logger.info(f"Successfully created DataFrame with {len(df)} rows for {location['name']}")
        
        # Combine all DataFrames into one
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Combined all location data into single DataFrame with {len(combined_df)} rows")
        
        # Upload the combined DataFrame to Google Cloud Storage as ONE file
        buffer = BytesIO()
        combined_df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        # Single file name for all weather data
        file_name = f"all_weather_data_{timestamp}.parquet"
        blob = bucket.blob(f'weathers_data/{file_name}') 
        blob.upload_from_string(buffer.getvalue(), content_type='application/octet-stream')
        
        logger.info(f"Combined weather data for all locations saved to GCS as {file_name}")
        logger.info(f"File contains data for locations: {[loc['name'] for loc in LOCATIONS]}")
        
        return file_name
        
    except Exception as e:
        logger.error(f"Error fetching weather data: {str(e)}")
        raise

print("WEATHER DATA FETCHED")
weather_data()