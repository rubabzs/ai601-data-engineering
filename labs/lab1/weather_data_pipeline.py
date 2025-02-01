import json
import requests
import csv


URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response : requests.Response = requests.get(URL)
    return response.json()


### Part 2. Write Operation (Load)
def save_to_csv(data: dict, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:
        ### TODO: complete rest of the code, HINT: write the header row and body separately
        header = ["Time", "Temperature", "Humidity", "Wind Speed" ]
        writer = csv.writer(file)
        writer.writerow(header)

        time = data["hourly"]["time"]
        temperatures = data["hourly"]["temperature_2m"]
        humidity_values = data["hourly"]["relative_humidity_2m"]
        wind_speeds = data["hourly"]["wind_speed_10m"]

        for i in range(len(temperatures)):
            writer.writerow([ time[i], temperatures[i], humidity_values[i], wind_speeds[i]])

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    with open(input_file, 'r', encoding='utf-8') as file:
        file = csv.reader(file)

        keys = next(file) # Header row
        values = list(file)

        with open(output_file, "w",newline='',encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(keys)
            for line in values:
                temperature = float(line[1])
                humidity = float(line[2])
                wind_speed = float(line[3])

                temperatureBetween0And60 = temperature >= 0 and temperature <= 60
                humidityBetween0And80 = humidity >= 0 and humidity <= 80
                windSpeedBetween3And150 = wind_speed >= 3.0 and wind_speed <= 150.0

                if temperatureBetween0And60 and humidityBetween0And80 and windSpeedBetween3And150:
                    writer.writerow(line)
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

        total_records = len(data)
        avg_temp = sum(temperatures) / total_records
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values) / total_records
        avg_wind_speed = sum(wind_speeds) / total_records

        # Print summary
        print("\n\n📊 Weather Data Summary 📊\n\n")
        print(f"🔖 Total Records: {total_records}")
        print(f"🌡️  Average Temperature: {avg_temp:.2f}°C")
        print(f"🔥 Max Temperature: {max_temp:.2f}°C")
        print(f"❄️  Min Temperature: {min_temp:.2f}°C")
        print(f"💧 Average Humidity: {avg_humidity:.1f}%")
        print(f"💨 Average Wind Speed: {avg_wind_speed:.2f} m/s")


if __name__ == "__main__":
    weather_data = fetch_weather_data()
    if weather_data:
        save_to_csv(weather_data, "weather_data.csv")
        print("📝 Weather data saved to weather_data.csv")
        clean_data("weather_data.csv", "cleaned_data.csv")
        print("🧹 Weather data cleaned and saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")
        print("\n\n✨ Weather data summarized successfully!")