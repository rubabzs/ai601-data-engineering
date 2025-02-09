import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)
    json=response.json()
    return json

    ## TODO: complete the code, the output should be data in json format


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    # with open(filename, 'w', newline='', encoding='utf-8') as file:
    #      writer = csv.DictWriter(file, fieldnames=["time","wind_speed_10m","temperature_2m", "relative_humidity_2m"])
    #      fieldnames = ['name', 'branch', 'year', 'cgpa']
    
    #      writer.writeheader()
    #      writer.writerows(data)
    #     ### TODO: complete rest of the code, HINT: write the header row and body separately
    # Open CSV file and write data
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        
        # Write headers
        writer.writerow(["time", "temperature_2m", "relative_humidity_2m", "wind_speed_10m"])
        
        # Write data rows
        for i in range(len(data["hourly"]["time"])):
            writer.writerow([
                data["hourly"]["time"][i],
                data["hourly"]["temperature_2m"][i],
                data["hourly"]["relative_humidity_2m"][i],
                data["hourly"]["wind_speed_10m"][i]
            ])

    return None

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    with open(input_file, mode = 'r') as input_file, open(output_file, mode='w', newline='') as output_file:
        reader = csv.reader(input_file)
        writer = csv.writer(output_file)
        
        # Read and write header
        headers = next(reader)
        writer.writerow(headers)
        
        # Filter and write valid rows
        for row in reader:
            time, temp, humidity, wind_speed = row
            
            try:
                temp = float(temp)
                humidity = float(humidity)
                wind_speed = float(wind_speed)
                
                if 0 <= temp <= 60 and 0 <= humidity <= 80 and 3 <= wind_speed <= 150:
                    writer.writerow([time, temp, humidity, wind_speed])
            except ValueError:
                continue  # Skip rows with invalid data
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
        print(len(temperatures))
        print(len(humidity_values))

        total_records= len(temperatures)
        avg_temp= sum(temperatures)/len(temperatures)
        max_temp=max(temperatures)
        min_temp= min(temperatures)
        avg_humidity=sum(humidity_values)/len(humidity_values)
        avg_wind_speed=sum(wind_speeds)/len(wind_speeds)

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
    print(weather_data)
    if weather_data:
        save_to_csv(weather_data, "weather_data.csv") #getting data-> json and  saving into weather_data
        print("Weather data saved to weather_data.csv")
        clean_data("weather_data.csv", "cleaned_data.csv")
        print("Weather data clean saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")

        

