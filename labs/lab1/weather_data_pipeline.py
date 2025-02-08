import requests
import csv
import json

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)


    ## TODO: complete the code, the output should be data in json format

    if response.status_code == 200:
        return response.json()
        
    else:
        print(f"Error: {response.status_code}")


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:
    

        ### TODO: complete rest of the code, HINT: write the header row and body separately
        header = ['time',
                'temperature_2m',
                'relative_humidity_2m',
                'wind_speed_10m',]
        writer = csv.writer(file)
        writer.writerow(header)


        time = data['hourly']['time']
        temperature = data['hourly']['temperature_2m']
        humidity = data['hourly']['relative_humidity_2m']
        wind_speed = data['hourly']['wind_apeed_10m']
        for i in range(len(time)):
            writer.writerow([time[i], temperature[i],humidity[i],wind_speed[i]])
        

    
        

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """

    ### TODO: complete rest of the code
    with open(input_file, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = []

        for row in reader:
            try:
                temp = float(row["temperature_2m"])
                humidity = float(row["relative_humidity_2m"])
                wind_speed = float(row["wind_speed_10m"])

                if 0 <= temp <= 60 and 0 <= humidity <= 80 and 3 <= wind_speed <= 150:
                    rows.append(row)  # Keep only valid rows

            except ValueError:
                print("Skipping invalid row:", row)  # Handle missing/invalid values

    # Write cleaned data to output CSV file
    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)
            
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
        avg_temp = sum(temperatures) / len(temperatures) if temperatures else 0
        max_temp = max(temperatures) if temperatures else 0
        min_temp = min(temperatures) if temperatures else 0
        avg_humidity = (
            sum(humidity_values) / len(humidity_values) if humidity_values else 0
        )
        avg_wind_speed = sum(wind_speeds) / len(wind_speeds) if wind_speeds else 0

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
        

