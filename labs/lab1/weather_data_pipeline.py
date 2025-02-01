import requests
import csv
import json

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    # params = {
    #     "latitude": 52.52,
    #     "longitude": 13.41,
    #     "current": ["temperature_2m", "relative_humidity_2m", "is_day", "weather_code", "wind_speed_10m"],
    #     "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
    #     "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "uv_index_max", "wind_speed_10m_max"]
    # }
    response = requests.get(URL)
    if response.status_code == 200:
        # print(response.content)
        return response.json()
    ## TODO: complete the code, the output should be data in json format


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:

        ### TODO: complete rest of the code, HINT: write the header row and body separately
        column_names = list(data["hourly"].keys())

        all_data = [column_names]
        for i in range(len(data["hourly"][column_names[0]])):
            data_row = []
            for j in range(len(column_names)):
                data_row.append(data["hourly"][column_names[j]][i])
            all_data.append(data_row)

        writer = csv.writer(file)
        writer.writerows(all_data)
    return None

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """

    ### TODO: complete rest of the code

    # Read data from CSV file
    with open(input_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        updated_data = []
        for i, row in enumerate(reader):
            if i == 0:
                headers = row
                updated_data.append(headers)
            else:
                # process
                for j, column_header in enumerate(headers):
                    if column_header == "temperature_2m":
                        if float(row[j]) < 0:
                            updated_temp_value = float("0")
                        elif float(row[j]) > 60:
                            updated_temp_value = float("60")
                        else:
                            updated_temp_value = row[j]
                    elif column_header == "relative_humidity_2m":
                        if int(row[j]) > 80:
                            updated_humidity_value = 80
                        else:
                            updated_humidity_value = row[j]
                    elif column_header == "wind_speed_10m":
                        if float(row[j]) < 3:
                            updated_windspeed_vlaue = 3
                        elif float(row[j]) > 150:
                            updated_windspeed_vlaue = 150
                        else:
                            updated_windspeed_vlaue = row[j]
                updated_data.append([row[0], str(updated_temp_value), str(updated_humidity_value), str(updated_windspeed_vlaue)])
    with open(output_file, "w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(updated_data)
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
    # weather_data = fetch_weather_data()
    # if weather_data:
    #     weather_data = fetch_weather_data()
    #     save_to_csv(weather_data, "weather_data.csv")
    #     print("Weather data saved to weather_data.csv")
    #     clean_data("weather_data.csv", "cleaned_data.csv")
    #     print("Weather data clean saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")
        

