import requests


def get_data():
    response = requests.get('https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m')
    return response.json()

def write_data(data, filename):
    with open('weather_data.csv', 'w') as file:
        file.write(data)

def main():
    data = get_data()
    

if __name__ == '__main__':
    main()
