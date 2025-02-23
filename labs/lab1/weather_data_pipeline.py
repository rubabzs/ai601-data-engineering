import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("ERROR: unable to fetch weather data.")
        return None


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, 'w' , newline='', encoding='utf-8') as file:  
        writer = csv.writer(file)
        headers = ['time', 'temperature_2m', 'relative_humidity_2m', 'wind_speed_10m']
        writer.writerow(headers)
        for i in range(len(data['hourly']['time'])):
            time = data['hourly']['time'][i]
            temperature = data['hourly']['temperature_2m'][i]
            humidity = data['hourly']['relative_humidity_2m'][i]
            wind_speed = data['hourly']['wind_speed_10m'][i]
            writer.writerow([time, temperature, humidity, wind_speed])
        
        return None

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    with open(input_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        data = list(reader)

        if not data:
            print("No data available to clean.")
            return

        with open(output_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)

            for row in data:
                time = row[0]
                temperature = float(row[1]) if row[1] else None
                humidity = float(row[2]) if row[2] else None
                wind_speed = float(row[3]) if row[3] else None

                if temperature and 0 <= temperature <= 60 and humidity and 0 <= humidity <= 80 and wind_speed and 3 <= wind_speed <= 150:
                    writer.writerow(row)
                else:
                    print(f"Invalid data found at time {time}. Skipping record.")
                    continue

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
        

