import requests
import csv
import os
URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)
    #i added this line

    ## TODO: complete the code, the output should be data in json format
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None
           


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    
    hourly_data = data.get("hourly", {})
    keys = ["time", "wind_speed_10m",  "temperature_2m", "relative_humidity_2m"]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(keys)
        
        for i in range(len(hourly_data["time"])):
            row = [hourly_data[key][i] for key in keys]
            writer.writerow(row)

    print(f"Data saved successfully to {filename}")

        ### TODO: complete rest of the code, HINT: write the header row and body separately
#     weather_data = fetch_weather_data()

#     if weather_data:
#         save_to_csv(weather_data, "weather_data.csv")    

#     return None

# import os
# print("weather_data.csv" in os.listdir())


### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """

    ### TODO: complete rest of the code
    
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        header = next(reader) # Read header
        writer.writerow(header) # Write header to output
        for row in reader:
            try:
                time, wind_speed, temperature, humidity = row
                
                wind_speed = float(wind_speed)
                temperature = float(temperature)
                humidity = float(humidity)
                
                if (0 <= temperature <= 60) and (0 <= humidity <= 80) and (3 <= wind_speed <= 150):
                            writer.writerow(row)
            except ValueError:
                continue


    print(f"Cleaned data saved to {output_file}")

    #   cleaned_row = [cell.strip() if isinstance(cell, str) else cell for cell in row] # Trim whitespace
    #   cleaned_row = ['N/A' if not cell else cell for cell in cleaned_row] # Handle missing values
    #   writer.writerow(cleaned_row)

    # clean_data('weather_data.csv', 'cleaned_weather_data.csv')
    # print("Cleaned data saved to", output_file)

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
        #print("Weather data clean saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")
      
 