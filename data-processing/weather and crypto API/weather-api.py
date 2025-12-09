import requests
import csv
from datetime import datetime

# 1. Set Nürnberg's coordinates
latitude = 49.45
longitude = 11.07

# 2. Fetch the current weather data for Mürnberg
url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
response = requests.get(url)
data = response.json()

# 3. Parse temperature and weather code (condition)
current_weather = data['current_weather']
temperature = current_weather['temperature']
weather_code = current_weather['weathercode']
observation_time = current_weather['time']  # Format: '2025-08-15T18:00'

# 4. Map weather_code to a description
weather_conditions = {
    0: 'Clear sky',
    1: 'Mainly clear',
    2: 'Partly cloudy',
    3: 'Overcast',
    45: 'Fog',
    48: 'Depositing rime fog',
    51: 'Light drizzle',
    53: 'Drizzle',
    55: 'Dense drizzle',
    61: 'Slight rain',
    63: 'Rain',
    65: 'Heavy rain',
    80: 'Rain showers',
    95: 'Thunderstorm'
}
condition = weather_conditions.get(weather_code, "Unknown")

# 5. Parse date and time
date_time = datetime.strptime(observation_time, "%Y-%m-%dT%H:%M")
date = date_time.date().isoformat()
time = date_time.time().isoformat(timespec="minutes")

# 6. Save to CSV (append mode, include header only if the file is empty)
csv_file = 'weather_log.csv'
header = ['date', 'time', 'temperature', 'condition']
# 6. Save to CSV (append mode, include header only if the file is empty)
csv_file = 'weather_log.csv'
header = ['date', 'time', 'temperature', 'condition']

try:
    with open(csv_file, 'x', newline='') as f:  # Create file if it doesn't exist
        writer = csv.writer(f)
        writer.writerow(header)
except FileExistsError:
    pass  # File already exists; do nothing

with open(csv_file, 'a', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([date, time, temperature, condition])

print(f"Appended Nürnberg weather ({date} {time}): {temperature}°C, {condition}")
