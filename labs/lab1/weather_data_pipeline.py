import requests
from datetime import datetime, timedelta
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&start_date=2025-01-24&end_date=2025-02-01"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)

    ## TODO: complete the code, the output should be data in json format
    #Checking if the request was successful
    if response.status_code == 200:
        return response.json()  #Returning the JSON data
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    if not data:
        print("No data to save.")
        return
    
    try:
        # Extract hourly weather data
        hourly_data = data.get("hourly", {})
        times = hourly_data.get("time", [])
        temperatures = hourly_data.get("temperature_2m", [])
        humidities = hourly_data.get("relative_humidity_2m", ["N/A"] * len(times))
        wind_speeds = hourly_data.get("wind_speed_10m", ["N/A"] * len(times))

        # Convert time strings to datetime objects
        parsed_times = [datetime.strptime(t, "%Y-%m-%dT%H:%M") for t in times]

        # Get the date 10 days ago
        ten_days_ago = datetime.now() - timedelta(days=10)

        # Filter the last 10 days of data
        filtered_data = [
            (times[i], temperatures[i], humidities[i], wind_speeds[i])
            for i in range(len(parsed_times))
            if parsed_times[i] >= ten_days_ago
        ]

        if not filtered_data:
            print("No data available for the last 10 days.")
            return

        # Save filtered data to CSV
        with open(filename, "w", newline='', encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Date", "Temperature (°C)", "Humidity (%)", "Wind Speed (m/s)"])
            writer.writerows(filtered_data)

        print(f"Data for the last 10 days saved to {filename}")

    except Exception as e:
        print("Error processing data:", e)
        print("Check the structure of the API response.")

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        #Writing header
        headers = next(reader)
        writer.writerow(headers)
        
        for row in reader:
            try:
                date = row[0]
                temp = float(row[1])
                humidity = float(row[2]) if row[2] != "N/A" else None
                wind_speed = float(row[3]) if row[3] != "N/A" else None
                
                #Apply cleaning rules
                if 0 <= temp <= 60 and (humidity is None or 0 <= humidity <= 80) and (wind_speed is None or 3 <= wind_speed <= 150):
                    writer.writerow(row)
            except ValueError:
                continue  #Skipping data for invalid values
    
    print("Cleaned data saved to", output_file)


### Part 4. Aggregation Operation 
def summarize_data(filename):
    """Summarizes weather data including averages and extremes."""
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  #reading data
        data = list(reader)  #Converting data to list

        #check in case of empty dataset
        if not data:
            print("No data available to summarize.")
            return

        #Extracting values from columns
        temperatures = [float(row[1]) for row in data if row[1]]
        humidity_values = [float(row[2]) for row in data if row[2] != "N/A"]
        wind_speeds = [float(row[3]) for row in data if row[3] != "N/A"]

        #Computing weather stats
        total_records = len(temperatures)
        avg_temp = sum(temperatures) / total_records if total_records else 0
        max_temp = max(temperatures) if temperatures else 0
        min_temp = min(temperatures) if temperatures else 0
        avg_humidity = sum(humidity_values) / len(humidity_values) if humidity_values else 0
        avg_wind_speed = sum(wind_speeds) / len(wind_speeds) if wind_speeds else 0


        #Printing results
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
        

