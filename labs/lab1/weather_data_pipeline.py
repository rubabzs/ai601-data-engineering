import requests
import numpy as np
import csv
import pandas as pd

#URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m"
URL = 'https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m'
### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)

    ## TODO: complete the code, the output should be data in json format
    response_json = response.json()
    #print(response_json)
    return response_json


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:
        ### TODO: complete rest of the code, HINT: write the header row and body separately
        writer = csv.writer(file, delimiter=",")
        writer.writerow(['DateTime', 'Temp', 'Humid', 'Wind Speed'])

        t = data['hourly']['time']
        temp = data['hourly']['temperature_2m']
        h = data['hourly']['relative_humidity_2m']
        w = data['hourly']['wind_speed_10m']

        for i in range(len(t)):
            writer.writerow([t[i], temp[i], h[i], w[i]])
    #with open('PeshVsQuetta.json', encoding='utf-8-sig') as f_input:
    #df = pd.DataFrame(data)
    #print(df.head())

    #df.to_csv(filename, encoding='utf-8', index=False)
    return None

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """

    ### TODO: complete rest of the code
    data = []
    with open(input_file, mode='r') as f:
        csvFile = csv.reader(f)
        for rows in csvFile:
            #print(rows)
            try:
                if float(rows[1])<=60 and float(rows[1])>=0 and int(rows[2])>=0 and int(rows[2])<=80 and float(rows[3])>=3 and float(rows[3])<=150:
                    data.append(rows)
                    
            except:
                print("Skipping due to Mismatch")

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(['DateTime', 'Temp', 'Humid', 'Wind Speed'])
        writer.writerows(data)

            
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
        temperatures = [float(row[1]) for row in data if row[1]]
        humidity_values = [float(row[2]) for row in data if row[2]]
        wind_speeds = [float(row[3]) for row in data if row[3]]

        # Compute statistics
        ### TODO: complete rest of the code by computing the below mentioned metrics
        total_records = len(data)
        avg_temp = np.average(temperatures)
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = np.average(humidity_values)
        avg_wind_speed = np.average(wind_speeds)
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
        

