import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)

    return response.json()['hourly']
    ## TODO: complete the code, the output should be data in json format


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w+", newline='', encoding='utf-8') as file:

        ### TODO: complete rest of the code, HINT: write the header row and body separately
        writer = csv.writer(file, delimiter=',')
        writer.writerow(['time', 'temperature_2m', 'relative_humidity_2m', 'wind_speed_10m'])
        for idx, i in enumerate(data['time']):
            writer.writerow([data['time'][idx], data['temperature_2m'][idx],  data['relative_humidity_2m'][idx],  data['wind_speed_10m'][idx]])
        return file

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    time, temperatures, humidity_values, wind_speeds = None, None,None,None
    ### TODO: complete rest of the code
    with open(input_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read header row
        data = list(reader)  # Convert CSV data to list

        # Ensure we have data
        if not data:
            print("No data available to summarize.")
            return

        # Extract values from columns
        time = [row[0] for row in data if row[0]]
        temperatures = [float(row[1]) for row in data if row[1]]
        humidity_values = [float(row[2]) for row in data if row[2]]
        wind_speeds = [float(row[3]) for row in data if row[3]]
        with open('cleaned_data.csv', "w+", newline='', encoding='utf-8') as file:

        ### TODO: complete rest of the code, HINT: write the header row and body separately
            writer = csv.writer(file, delimiter=',')
            writer.writerow(['time', 'temperature_2m', 'relative_humidity_2m', 'wind_speed_10m'])
            for idx, i in enumerate(time):
                if temperatures[idx]<0 or temperatures[idx]>60:
                    continue
                if humidity_values[idx] < 0 or humidity_values[idx]>80:
                    continue
                if wind_speeds[idx]<3 or wind_speeds[idx]>150:
                    continue
                writer.writerow([time[idx], temperatures[idx],  humidity_values[idx],  wind_speeds[idx]])
        return file
            

        

            
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
        total_records = len(temperatures)
        avg_temp = sum(temperatures)/len(temperatures)
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values)/len(humidity_values)
        avg_wind_speed = sum(wind_speeds)/len(wind_speeds)
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
        

