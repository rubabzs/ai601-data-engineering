import requests
import csv
import datetime
from collections import defaultdict

URL = "https://api.open-meteo.com/v1/forecast"
today = datetime.date.today()
start_date = today - datetime.timedelta(days=10)

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    params = {
        "latitude": 31.590133,
        "longitude": 74.434513,
        "start_date": start_date.isoformat(),
        "end_date": today.isoformat(),
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "Asia/Karachi"
    }
    
    response = requests.get(URL, params=params)
    if response.status_code == 200:
        data = response.json()
        if "hourly" in data:
            hourly = data["hourly"]
            times = hourly.get("time", [])
            temperatures = hourly.get("temperature_2m", [])
            humidities = hourly.get("relative_humidity_2m", [])
            wind_speeds = hourly.get("wind_speed_10m", [])
            
            # Group hourly data by date (YYYY-MM-DD)
            daily_data = defaultdict(lambda: {"temperatures": [], "humidities": [], "wind_speeds": []})
            for i, time_str in enumerate(times):
                date = time_str.split("T")[0]  # Extract the date part
                daily_data[date]["temperatures"].append(temperatures[i])
                daily_data[date]["humidities"].append(humidities[i])
                daily_data[date]["wind_speeds"].append(wind_speeds[i])
            
            # Compute daily averages from the grouped hourly data
            weather_records = []
            for date, metrics in daily_data.items():
                avg_temp = sum(metrics["temperatures"]) / len(metrics["temperatures"])
                avg_humid = sum(metrics["humidities"]) / len(metrics["humidities"])
                avg_w_speed = sum(metrics["wind_speeds"]) / len(metrics["wind_speeds"])
                weather_records.append({
                    "date": date,
                    "temperature": avg_temp,
                    "humidity": avg_humid,      
                    "wind_speed": avg_w_speed
                })
            return weather_records
        else:
            print("No hourly data found in the API response.")
            return None
    else:
        print("Error fetching data:", response.status_code)
        return None


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:
        fieldnames = ["date", "temperature", "humidity", "wind_speed"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)
    return None

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """

    cleaned_rows = []
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            try:
                temperature = float(row["temperature"])
                humidity = float(row["humidity"])
                wind_speed = float(row["wind_speed"])
                if 0 <= temperature <= 60 and 0 <= humidity <= 80 and 3 <= wind_speed <= 150:
                    cleaned_rows.append(row)
            except ValueError:
                # Skip rows with invalid numeric values
                continue

    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        fieldnames = ["date", "temperature", "humidity", "wind_speed"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in cleaned_rows:
            writer.writerow(row)
            
    print("Cleaned data saved to", output_file)

### Part 4. Aggregation Operation 
def summarize_data(filename):
    """Summarizes weather data including averages and extremes."""
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        data = list(reader)

        if not data:
            print("No data available to summarize.")
            return

        # Extract numeric values
        temperatures = [float(row["temperature"]) for row in data if row["temperature"]]
        humidity_values = [float(row["humidity"]) for row in data if row["humidity"]]
        wind_speeds = [float(row["wind_speed"]) for row in data if row["wind_speed"]]

        total_records = len(data)
        avg_temp = sum(temperatures) / len(temperatures) if temperatures else 0
        max_temp = max(temperatures) if temperatures else 0
        min_temp = min(temperatures) if temperatures else 0
        avg_humidity = sum(humidity_values) / len(humidity_values) if humidity_values else 0
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
        

