import requests
import csv
import json


URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&start_date=2025-01-22&end_date=2025-01-31"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)
    data = response.json()
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    # print (response.json())
    return data

    ## TODO: complete the code, the output should be data in json format


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:
        # file.write("Date, Temperature, Humidity, Wind Speed\n")
        hourly_data = data['hourly']
        time = hourly_data['time']
        temperature = hourly_data['temperature_2m']
        humidity = hourly_data['relative_humidity_2m']
        wind_speed = hourly_data['wind_speed_10m']

        writer = csv.writer(file)
        writer.writerow(["Date", "Temperature", "Humidity", "Wind Speed"])
        for i in range(0,len(time)):
            writer.writerow([time[i], temperature[i], humidity[i], wind_speed[i]])

        ### TODO: complete rest of the code, HINT: write the header row and body separately

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

    with open(output_file, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for datum in data:
            temp = float (datum[1])
            humidity = float (datum[2])
            wind_speed = float (datum[3])
            if temp < 0 or temp > 60:
                continue
            elif humidity < 0 or humidity < 80:
                continue
            elif wind_speed < 3 or wind_speed > 150:
                continue
            else:
                writer.writerow(datum)


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

        total_records = len(data)
        avg_temp = sum(temperatures) / len(temperatures)
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values) / len(humidity_values)
        avg_wind_speed = sum(wind_speeds) / len(wind_speeds)

        # Compute statistics
        ### TODO: complete rest of the code by computing the below mentioned metrics

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
        

