import os
import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"


### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)

    ## TODO: complete the code, the output should be data in json format
    if response.status_code == 200:
        data = response.json()
    return data


### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    # os.remove(filename)
    with open(filename, "w", newline="", encoding="utf-8") as file:

        ### TODO: complete rest of the code, HINT: write the header row and body separately
        writer = csv.writer(file)
        columns = data["hourly"].keys()
        rows = []
        writer.writerow(columns)

        (time, temperature_2m, relative_humidity_2m, wind_speed_10m) = (
            data["hourly"]["time"],
            data["hourly"]["temperature_2m"],
            data["hourly"]["relative_humidity_2m"],
            data["hourly"]["wind_speed_10m"],
        )

        for i in range(len(time)):
            d = [time[i], temperature_2m[i], relative_humidity_2m[i], wind_speed_10m[i]]
            rows.append(d)

        writer.writerows(rows)
        file.close()
    return


### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """clean the data based on the following rules:
    1. Temperature should be between 0 and 60°C
    2. Humidity should be between 0% and 80%
    3. Wind speed in a betweeen 3 and 150
    """

    ### TODO: complete rest of the code

    print("Cleaned data saved to", output_file)
    with open(input_file, "r", encoding="utf-8") as file:
        with open(output_file, "w", newline="", encoding="utf-8") as cleaned_file:
            reader = csv.reader(file)
            writer = csv.writer(cleaned_file)
            data = list(reader)
            cleaned_data = []
            for idx, line in enumerate(data):
                if idx == 0:
                    cleaned_data.append(line)
                    continue
                is_line_validated = True

                # Temperature
                if float(line[1]) < 0 or float(line[1]) > 60:
                    is_line_validated = False
                # Humidity
                if float(line[2]) < 0 or float(line[2]) > 80:
                    is_line_validated = False
                # Wind Speed
                if float(line[3]) < 3 or float(line[3]) > 150:
                    is_line_validated = False

                if is_line_validated:
                    cleaned_data.append(line)

            writer.writerows(cleaned_data)
            file.close()
            cleaned_file.close()


### Part 4. Aggregation Operation
def summarize_data(filename):
    """Summarizes weather data including averages and extremes."""
    with open(filename, "r", encoding="utf-8") as file:
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
        avg_temp = sum(temperatures) / total_records
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_humidity = sum(humidity_values) / total_records
        avg_wind_speed = sum(wind_speeds) / total_records

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
