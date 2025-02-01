import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)
    ## TODO: complete the code, the output should be data in json format
    return response.json()



### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    columns = ""
    for k in data['hourly'].keys():
        columns = columns + k +","
    
    with open(filename,"a+", newline='', encoding='utf-8') as file:
        textwriter = csv.writer(file, delimiter=' ',)
        textwriter.writerow(columns)
    
    with open(filename,"a+", newline='', encoding='utf-8') as file:
        textwriter = csv.writer(file, delimiter=' ',)
        for i in range(len(data['hourly']['time'])):
            data_sample = ""
            for k in data['hourly'].keys():
                data_sample = data_sample + str(data['hourly'][k][i]) + ","
            textwriter.writerow(data_sample)


        ### TODO: complete rest of the code, HINT: write the header row and body separately

        return None

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """
    cleaned_data = []
    with open(input_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read header row
        data = list(reader)
        cleaned_data.append(headers)
        for data_sample in data:
            time, temp, humid, wind,_ = data_sample
            temp, humid, wind = float(''.join(temp.split())), float(''.join(humid.split())), float(''.join(wind.split()))
            # print("Data::", time, temp, humid, wind)
            if temp < 0.0 or temp > 60.0:
                continue
            if humid < 0.0 or humid > 80.0:
                continue
            if wind < 3.0 or wind > 150.0:
                continue
            else:
                cleaned_data.append(data_sample)
                # print(data_sample)
    

    # print(len(cleaned_data), cleaned_data)
    with open(output_file,"a+", newline='', encoding='utf-8') as file:
        cleanedwriter = csv.writer(file, delimiter=' ',)
        for row in cleaned_data:
            data_sample = ""
            for elem in row:
                data_sample = data_sample + elem +","
        
            data_sample = ''.join(data_sample.split())
            cleanedwriter.writerow(data_sample)


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
        temperatures = [float(''.join(row[1].split())) for row in data if row[1]]
        humidity_values = [float(''.join(row[2].split())) for row in data if row[2]]
        wind_speeds = [float(''.join(row[3].split())) for row in data if row[3]]

        total_records = len(temperatures)
        avg_temp = sum(temperatures)/len(temperatures)
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values)/len(humidity_values)
        avg_wind_speed = sum(wind_speeds)/len(wind_speeds)

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
        

