import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching data")
        return None
    

### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    print(data.keys())
    print(len(data['hourly']))
    with open(filename, "w", newline='', encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        header = data.keys()
        csv_writer.writerow(['Datatime', "Temperature", "Humidity", "Wind Speed"])
        for stamps in range(len(data["hourly"]["time"])):
            csv_writer.writerow([data["hourly"]["time"][stamps], data["hourly"]["temperature_2m"][stamps] , data["hourly"]["relative_humidity_2m"][stamps],data["hourly"]["wind_speed_10m"][stamps]])

        return None

### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    with open(input_file) as file:
        content = file.readlines()
        header = content[:1]
        rows = content[1:]

        # print(rows[0])
        # row = rows[0].split(',')
        # print(f"This is the fist row : {row}")
        # print(row)
        # print(len)
        # print(row[1])
        # for element in row:
        #     print(element)
        # for row in rows:
        # print(type(row[1]))
        print(len(rows))
        c = 0
    with open(output_file, "w", newline='', encoding='utf-8') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(['Datatime', "Temperature", "Humidity", "Wind Speed"])
        for row in rows:
            row = row.split(',')
            if (float(row[1]) > 0 and float(row[1]) < 60 ) and  int(row[2]) > 0 and int(row[2])< 80 and (float(row[3]) > 3 and float(row[3]) < 150):
                    csv_writer = csv.writer(file)
                    print(row[0],row[1],row[2],row[3])
                    csv_writer.writerow([row[0],row[1],row[2],row[3]])
    
    """ clean the data based on the following rules:
        1. Temperature should be between 0 and 60°C
        2. Humidity should be between 0% and 80%
        3. Wind speed in a betweeen 3 and 150
    """

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

        # Compute statistics
        ### TODO: complete rest of the code by computing the below mentioned metrics
        total_records = len(temperatures)
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
        # save_to_csv(weather_data, "weather_data.csv")
        # print("Weather data saved to weather_data.csv")
        # clean_data("weather_data.csv", "cleaned_data.csv")
        print("Weather data clean saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")
        

