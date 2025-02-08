import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"



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
    return response.json()

    ## TODO: complete the code, the output should be data in json format


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:

        ### TODO: complete rest of the code, HINT: write the header row and body separately

        writer = csv.writer(file)
        writer.writerow(data["hourly"].keys())

        required_data = [
        data["hourly"]["time"],
        data["hourly"]["temperature_2m"],
        data["hourly"]["relative_humidity_2m"],
        data["hourly"]["wind_speed_10m"],
        ]

        for row in zip(*required_data):
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
    required_criteria = [
        ("temperature_2m", 0, 60),
        ("relative_humidity_2m", 0, 80),
        ("wind_speed_10m", 3, 150)
    ]
    with open(input_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        headers = reader.fieldnames
        cleaned_data = []
        
        for item in reader:
            if all(lower <= float(item[key]) <= upper for key, lower, upper in required_criteria):
                cleaned_data.append(item)
    
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(cleaned_data)
            
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
        

