import requests
import csv
import pandas as pd

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"



### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    responses = requests.get(URL)

    if responses.status_code == 200:
        return responses.json()
    else:
        print("Failed to fetch weather data!")
        return None

    ## TODO: complete the code, the output should be data in json format
    # fetched_data = (responses.json())
    # save_to_csv(fetched_data, 'weather_data.csv')
    

### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    """Saves weather data to a CSV file."""
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        writer.writerow(["Date", "Temperature", "Humidity", "Wind Speed"])
        
        times = data["hourly"]["time"]
        temperature = data["hourly"]["temperature_2m"]
        humidity = data["hourly"]["relative_humidity_2m"]
        wind_speed = data["hourly"]["wind_speed_10m"]

        for i in range(len(times)):
            writer.writerow([times[i], temperature[i], humidity[i], wind_speed[i]])
    

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """

    with open(input_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read header row
        with open(output_file, 'w', newline='', encoding='utf-8') as output_file:
            writer = csv.writer(output_file)
            writer.writerow(headers)  # Write header row
            for row in reader:
                # Extract values from row
                date, temperature, humidity, wind_speed = row
                # Check if values are within acceptable range
                if 0 <= float(temperature) <= 60 and 0 <= float(humidity) <= 80 and 3 <= float(wind_speed) <= 150:
                    writer.writerow(row)
                else:
                    print(f"Skipping row with invalid data: {row}")     
            
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
        total_records = len(data)
        avg_temp = sum(temperatures) / len(temperatures)
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values) / len(humidity_values)
        avg_wind_speed = sum(wind_speeds) / len(wind_speeds)

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
        