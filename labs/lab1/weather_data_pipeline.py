import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    ## TODO: complete the code, the output should be data in json format
    response = requests.get(URL)
    if response.status_code == 200:
        return response.json()
    
    print("Failed to fetch weather data")
    return None 

### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:

        ### TODO: complete rest of the code, HINT: write the header row and body separately
        csvwriter = csv.writer(file)
        csvwriter.writerow(data['hourly'].keys())
        time = data['hourly']['time']
        wind_speed = data['hourly']['wind_speed_10m']
        temperature = data['hourly']['temperature_2m']
        humidity = data['hourly']['relative_humidity_2m']

        for i in range(len(time)): 
            csvwriter.writerow([time[i], temperature[i], humidity[i], wind_speed[i]])   

        return None

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    ### TODO: complete rest of the code
    with open(input_file, 'r', encoding='utf-8') as in_file:
        reader = csv.reader(in_file)
        headers = next(reader)

        with open(output_file, 'w', newline='', encoding='utf-8') as out_file:
            writer = csv.writer(out_file)
            writer.writerow(headers)

            for row in reader:
                time, temperature, humidity, wind_speed = row
                if 0 <= float(temperature) <= 60 and 0 <= float(humidity) <= 80 and 3 <= float(wind_speed) <= 150:
                    writer.writerow(row)    
            
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

        # Print summary
        data_len = len(data)    
        print("📊 Weather Data Summary 📊")
        print(f"Total Records: {data_len}")
        print(f"🌡️ Average Temperature: {sum(temperatures)/data_len:.2f}°C")
        print(f"🔥 Max Temperature: {max(temperatures):.2f}°C")
        print(f"❄️ Min Temperature: {min(temperatures):.2f}°C")
        print(f"💧 Average Humidity: {sum(humidity_values)/data_len:.1f}%")
        print(f"💨 Average Wind Speed: {sum(wind_speeds)/data_len:.2f} m/s")


if __name__ == "__main__":
    weather_data = fetch_weather_data()
    if weather_data:
        save_to_csv(weather_data, "weather_data.csv")
        print("Weather data saved to weather_data.csv")
        clean_data("weather_data.csv", "cleaned_data.csv")
        print("Weather data clean saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")
        

