import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
# $ curl "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

# {
#   "hourly": {
#     "time": ["2022-06-19T00:00","2022-06-19T01:00", ...]
#     "wind_speed_10m": [3.16,3.02,3.3,3.14,3.2,2.95, ...],
#     "temperature_2m": [13.7,13.3,12.8,12.3,11.8, ...],
#     "relative_humidity_2m": [82,83,86,85,88,88,84,76, ...],
#   }
# }
### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)

    ## TODO: complete the code, the output should be data in json format
    print(response.status_code)

    if response.status_code == 200:
        return response.json();
    return -1



### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    hourly_data = data.get('hourly')

    columns = ['time', 'temperature_2m', 'relative_humidity_2m', 'wind_speed_2m']

    with open(filename, "w", newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        
        for i in range(len(hourly_data['time'])):
            row = {
                'time': hourly_data['time'][i],
                'temperature_2m': hourly_data['temperature_2m'][i],
                'relative_humidity_2m': hourly_data['relative_humidity_2m'][i],
                'wind_speed_2m': hourly_data['wind_speed_10m'][i]
            }
            writer.writerow(row)
        return None

    
    
### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    ### TODO: complete rest of the code

    with open(input_file, "r", newline='', encoding='utf-8') as file:
        with open(output_file, "w", newline='', encoding='utf-8') as file_output:
            reader = csv.DictReader(file)
            writer = csv.DictWriter(file_output, fieldnames=['time', 'temperature_2m', 'relative_humidity_2m', 'wind_speed_2m'])
            writer.writeheader()

            for row in reader:
                temp = float(row['temperature_2m'])
                humidity = float(row['relative_humidity_2m'])
                wind_speed = float(row['wind_speed_2m'])
                if (temp >= 0 and temp <= 60) and (humidity >= 0 and humidity <= 80) and (wind_speed >= 3 and wind_speed <= 150):
                    writer.writerow(row)
                else:
                    print("Invalid data")
        return None


            
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
        avg_temp = sum(temperatures) / len(data)
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values) / len(data)
        avg_wind_speed = sum(wind_speeds) / len(data)

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
        

