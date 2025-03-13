import requests
import csv
import pandas as pd
import numpy as np

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)
    if response.status_code == 200:
        return response.json()
    
    else:
        print('error fetching data', response.status_code)

    ## TODO: complete the code, the output should be data in json format
    return none


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    headers = ["time", "temperature_2m", "wind_speed_10m", "relative_humidity_2m"]

    with open(filename, "a", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write header only if file is empty
        if file.tell() == 0:
            writer.writerow(headers)
        
        # Write hourly data
        for i in range(len(data["hourly"]["time"])):
            writer.writerow([
                data["hourly"]["time"][i],
                data["hourly"]["temperature_2m"][i],
                data["hourly"]["wind_speed_10m"][i],
                data["hourly"].get("relative_humidity_2m", [None] * len(data["hourly"]["time"]))[i]  # Handle missing key
            ])

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """

    ### TODO: complete rest of the code
    
    df = pd.read_csv(input_file) 
    print('my_data', df)   
    
    ## this will drop the null values
    new_df = df.dropna()    
    
    for i in new_df.index:
        if (new_df.loc[i, 'temperature_2m'] < 0) & (new_df.loc[i, 'temperature_2m'] > 60):
            new_df.drop(i, inplace = True)
            
        if (new_df.loc[i, 'relative_humidity_2m'] < 0) & (new_df.loc[i, 'relative_humidity_2m'] > 80):
            df.drop(i, inplace = True)
            
        if (new_df.loc[i, 'wind_speed_10m'] < 3) & (new_df.loc[i, 'wind_speed_10m'] > 150):
            df.drop(i, inplace = True)    
    
    
    ## save to output file
    
    new_df.to_csv(output_file)

    print("Cleaned data saved to", output_file)



### Part 4. Aggregation Operation 
def summarize_data(filename):
    """Summarizes weather data including averages and extremes."""
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read header row
        data = list(reader)  # Convert CSV data to list
        
        # Ensure we have data
        if not data:
            print("No data available to summarize.")
            return

        # Extract values from columns
        temperatures = [float(row[2]) for row in data if row[2]]
        humidity_values = [float(row[4]) for row in data if row[4]]
        wind_speeds = [float(row[3]) for row in data if row[3]]

        # Compute statistics
        ### TODO: complete rest of the code by computing the below mentioned metrics
        avg_temp = np.mean(temperatures)
        max_temp = np.max(temperatures)
        min_temp = np.min(temperatures)
        avg_humidity = np.mean(humidity_values)
        avg_wind_speed = np.mean(wind_speeds)
        
        total_records = len(data)
        

        # Print summary
        print("📊 Weather Data Summary 📊")
        print(f"Total Records: {total_records}")
        print(f"🌡️ Average Temperature: {avg_temp:.2f}°C")
        print(f"🔥 Max Temperature: {max_temp:.2f}°C")
        print(f"❄️ Min Temperature: {min_temp:.2f}°C")
        print(f"💧 Average Humidity: {avg_humidity:.1f}%")
        print(f"💨 Average Wind Speed: {avg_wind_speed:.2f} m/s")


if __name__ == "__main__":
    weather_data = fetch_weather_data()
    if weather_data:
        save_to_csv(weather_data, "weather_data.csv")
        print("Weather data saved to weather_data.csv")
        clean_data("weather_data.csv", "cleaned_data.csv")
        print("Weather data clean saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")
        

