from logging import exception

import requests
import csv

from sympy.physics.units import temperature

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)
    print(response.json())
    data = response.json()

    times=data['hourly']['time']
    temps=data['hourly']['temperature_2m']
    humidity=data['hourly']['relative_humidity_2m']
    wind_speed=data['hourly']['wind_speed_10m']

    ### hourly_values = list(r1['hourly'].values())

    hourly_values =  [
        {"time" : t, "temperature_2m" : temp, "relative_humidity_2m" : hum, "wind_speed_10m" : ws}
        for t, temp, hum, ws in zip(times, temps, humidity, wind_speed)
    ]

    return hourly_values

    ## TODO: complete the code, the output should be data in json format


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:
        fieldnames=data[0].keys()
        writer=csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(data)

        ### TODO: complete rest of the code, HINT: write the header row and body separately
    print(f"Weather data saved to {filename}")

    return None


### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    cleaned_data=[]

    try:
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                try:
                    temp = float(row['temperature_2m']) if row['temperature_2m'] else None
                    humidity = float(row['relative_humidity_2m']) if row['relative_humidity_2m'] else None
                    wind_speed = row['wind_speed_10m']

                    # Example cleaning rules (ADAPT THESE AFTER INSPECTING weather_data.csv):
                    if temp is not None and 0 <= temp <= 60 and humidity is not None and 0 <= humidity <= 100 and wind_speed != "N/A" and wind_speed != "" and 3 <= float(
                            wind_speed) <= 150:
                        cleaned_data.append(row)
                    elif temp is None:
                        print(f"Skipping row due to missing Temperature: {row}")
                    elif humidity is None:
                        print(f"Skipping row due to missing Humidity: {row}")
                    elif humidity > 100:
                        print(f"Skipping row due to invalid humidity: {row}")
                    elif wind_speed == "N/A" or wind_speed == "":
                        print(f"Skipping row due to missing wind_speed: {row}")
                    elif not 3 <= float(wind_speed) <= 150:
                        print(f"Skipping row due to invalid wind_speed: {row}")
                except ValueError:
                    print(f"Skipping row due to invalid data in: {row}")
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                if cleaned_data:
                    fieldnames = cleaned_data[0].keys()
                    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(cleaned_data)
        except Exception as e:  # Handle any error during the file writing.
            print(f"An error occurred while writing to the output file: {e}")
            return False


    except FileNotFoundError:
        print(f"Input file '{input_file}' not found.")
        return False




    ### TODO: complete rest of the code
            
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
        avg_temp = sum(temperatures) / total_records if temperatures else 0
        max_temp = max(temperatures) if temperatures else 0
        min_temp = min(temperatures) if temperatures else 0
        avg_humidity = sum(humidity_values) / total_records if humidity_values else 0
        avg_wind_speed = sum(wind_speeds) / total_records if wind_speeds else 0

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
        

