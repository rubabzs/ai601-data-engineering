import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)

    ## TODO: complete the code, the output should be data in json format
    return response.json()


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:
        header = ["time", "wind_speed_10m", "temperature_2m", "relative_humidity_2m"]
        writer = csv.DictWriter(file, fieldnames=header)
        writer.writeheader()
        print(len(data['hourly']))
        dict = {}
        print(len(data['hourly']['time']), len(data['hourly']['wind_speed_10m']), len(data['hourly']['temperature_2m']), len(data['hourly']['relative_humidity_2m']))
        # dict['time'] = data['hourly']['time']
        # dict['wind_speed_10m'] = data['hourly']['wind_speed_10m']
        # dict['temperature_2m'] =  data['hourly']['temperature_2m']
        # dict['relative_humidity_2m'] =  data['hourly']['relative_humidity_2m']
        # writer.writerow(dict)
        for i in range(len(data['hourly']['time'])):
            writer.writerow({
                "time": data['hourly']['time'][i],
                "wind_speed_10m": data['hourly']['wind_speed_10m'][i],
                "temperature_2m": data['hourly']['temperature_2m'][i],
                "relative_humidity_2m": data['hourly']['relative_humidity_2m'][i]
            })

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150

    Example Source
    time,wind_speed_10m,temperature_2m,relative_humidity_2m
    2025-01-22T00:00,4.7,-2.1,100
    2025-01-22T01:00,3.5,-2.4,100
    2025-01-22T02:00,3.6,-2.7,100
    2025-01-22T03:00,5.4,-2.7,100
    """
    with open(input_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        data = list(reader)
        data_cleaned = []
        for row in data:
            time, wind_speed, temperature, humidity = row
            if 0 <= float(temperature) <= 60 and 0 <= float(humidity) <= 80 and 3 <= float(wind_speed) <= 150:
                data_cleaned.append(row)
        with open(output_file, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(headers)
            writer.writerows(data_cleaned)


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
        

