import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m&hourly=relative_humidity_2m&hourly=wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)
    return response.json() if response.status_code == 200 else "Error"

### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    # Extract the hourly data
    times = data['hourly']['time']
    temperatures = data['hourly'].get('temperature_2m', [None] * len(times))
    humidities = data['hourly'].get('relative_humidity_2m', [None] * len(times))
    windspeeds = data['hourly'].get('wind_speed_10m', [None] * len(times))

    with open(filename, "w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write header
        writer.writerow(["time", "temperature_2m", "relative_humidity_2m", "wind_speed_10m"])

        # Write data rows
        for i in range(len(times)):
            writer.writerow([
                times[i], 
                temperatures[i] if i < len(temperatures) else None, 
                humidities[i] if i < len(humidities) else None, 
                windspeeds[i] if i < len(windspeeds) else None
            ])
        return None

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
        headers = next(reader)
        writer.writerow(headers)
        n=0
        for row in reader:
            if row[1] and row[2] and row[3]:
                temp = float(row[1])
                humidity = float(row[2])
                wind_speed = float(row[3])
                if 0 <= temp <= 60 and 0 <= humidity <= 80 and 3 <= wind_speed <= 150:
                    writer.writerow(row)
                else:
                    n+=1
        print(f"Skipping {n} rows due to invalid values.")
        #print("Cleaned data saved to", output_file)  
            
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
        avg_temp = sum(temperatures) / total_records
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values) / total_records
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
        print("Weather Data saved to weather_data.csv")
        clean_data("weather_data.csv", "cleaned_data.csv")
        print("Cleaned Data saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")
        