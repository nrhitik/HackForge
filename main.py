import csv
import json
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
def scrape_aqi_data():
    url = "https://www.aqi.in/dashboard/india/maharashtra/mumbai"

    response = requests.get(url)
    aqi_data_weather = {}

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        for i in range(1, 23):  # 1 to 22
            row = soup.find('tr', class_=f"city-list AQI_toggle-{i}")
            if row:
                location_element = row.find('a')
                if location_element:
                    location = location_element.text.strip()
                else:
                    continue

                aqi_data_weather[location] = {}
                status_element = row.find('td', class_=lambda x: x and x.startswith('AQI_text-'))
                aqi_data_weather[location]['Status'] = status_element.text.strip() if status_element else "N/A"

                data_points = ['AQI (USA)', 'AQI (India)', 'PM2.5', 'PM10', 'NO2']
                td_elements = row.find_all('td')

                for index, data_point in enumerate(data_points, start=2):
                    if index < len(td_elements):
                        value = td_elements[index].text.strip()
                        aqi_data_weather[location][data_point] = value
                    else:
                        aqi_data_weather[location][data_point] = "N/A"
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")

    return aqi_data_weather

def scrape_weather_data():
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    aqi_data_weather = {}

    try:
        url = "https://www.aqi.in/weather/india/maharashtra/mumbai"
        driver.get(url)

        time.sleep(5)

        locations = driver.find_elements(By.CSS_SELECTOR, 'th a')
        location_names = [location.text.strip() for location in locations]

        tbody = driver.find_element(By.TAG_NAME, 'tbody')

        if tbody:
            rows = tbody.find_elements(By.TAG_NAME, 'tr')

            for i, row in enumerate(rows):
                columns = row.find_elements(By.TAG_NAME, 'td')
                column_data = [col.text.strip() for col in columns]

                if i < len(location_names):
                    location = location_names[i]
                    data_dict = {
                        "Status": column_data[0] if len(column_data) > 0 else "N/A",
                        "Temp": column_data[1] if len(column_data) > 1 else "N/A",
                        "Wind": column_data[2] if len(column_data) > 2 else "N/A",
                        "Pressure": column_data[3] if len(column_data) > 3 else "N/A",
                        "Humidity": column_data[4] if len(column_data) > 4 else "N/A"
                    }
                    aqi_data_weather[location] = data_dict
        else:
            print("No <tbody> found in the page.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()
    return aqi_data_weather


def get_lat_long(location, api_key):
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": location,
        "key": api_key
    }
    response = requests.get(base_url, params=params)
    data = response.json()

    if data['status'] == 'OK':
        result = data['results'][0]
        latitude = result['geometry']['location']['lat']
        longitude = result['geometry']['location']['lng']
        return latitude, longitude
    else:
        return None, None

def add_lat_long_to_data(data, api_key):
    for location in data:
        lat, lng = get_lat_long(location, api_key)
        data[location]['Latitude'] = lat
        data[location]['Longitude'] = lng
    return data
def combine_data(aqi_data, weather_data):
    combined_data = {}
    for location in aqi_data:
        combined_data[location] = {**aqi_data.get(location, {}), **weather_data.get(location, {})}
    return combined_data

def save_to_csv(data, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        
        header = ['Location', 'Status (AQI)', 'AQI (USA)', 'AQI (India)', 'PM2.5', 'PM10', 'NO2',
                  'Status (Weather)', 'Temp', 'Wind', 'Pressure', 'Humidity', 'Latitude', 'Longitude']
        writer.writerow(header)
       
        for location, values in data.items():
            row = [
                location,
                values.get('Status', 'N/A'),
                values.get('AQI (USA)', 'N/A'),
                values.get('AQI (India)', 'N/A'),
                values.get('PM2.5', 'N/A'),
                values.get('PM10', 'N/A'),
                values.get('NO2', 'N/A'),
                values.get('Status', 'N/A'),
                values.get('Temp', 'N/A'),
                values.get('Wind', 'N/A'),
                values.get('Pressure', 'N/A'),
                values.get('Humidity', 'N/A'),
                values.get('Latitude', 'N/A'),
                values.get('Longitude', 'N/A')
            ]
            writer.writerow(row)

def load_device_keys(filename):
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return json.load(file)
    return {}

def save_device_keys(filename, keys):
    with open(filename, 'w') as file:
        json.dump(keys, file, indent=4)


def provision_device(thingsboard_url, provision_device_key, provision_device_secret, device_name, keys_file):
    keys = load_device_keys(keys_file)

    if device_name in keys:
        print(f"Device '{device_name}' already provisioned.")
        return keys[device_name]

    provision_url = f"{thingsboard_url}/api/v1/provision"
    provision_request = {
        "deviceName": device_name,
        "provisionDeviceKey": provision_device_key,
        "provisionDeviceSecret": provision_device_secret
    }

    response = requests.post(provision_url, json=provision_request)

    if response.status_code == 200:
        access_token = response.json().get('credentialsValue')
        keys[device_name] = access_token
        save_device_keys(keys_file, keys)
        return access_token
    else:
        print(f"Failed to provision device {device_name}. Status code: {response.status_code}")
        return None


def send_telemetry(thingsboard_url, access_token, telemetry):
    url = f"{thingsboard_url}/api/v1/{access_token}/telemetry"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(telemetry))
    return response.status_code == 200


def send_to_thingsboard(csv_file, thingsboard_url, provision_device_key, provision_device_secret, keys_file):
    with open(csv_file, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            device_name = f"AQI_Weather_{row['Location'].replace(' ', '_')}"

            
            access_token = provision_device(thingsboard_url, provision_device_key, provision_device_secret, device_name, keys_file)

            if access_token:

                try:
                    telemetry = {
                        'Status_AQI': row['Status (AQI)'],
                        'AQI_USA': float(row['AQI (USA)']) if row['AQI (USA)'] and row['AQI (USA)'] != 'N/A' else None,
                        'AQI_India': float(row['AQI (India)']) if row['AQI (India)'] and row['AQI (India)'] != 'N/A' else None,
                        'PM2_5': float(row['PM2.5']) if row['PM2.5'] and row['PM2.5'] != 'N/A' else None,
                        'PM10': float(row['PM10']) if row['PM10'] and row['PM10'] != 'N/A' else None,
                        'NO2': float(row['NO2']) if row['NO2'] and row['NO2'] != 'N/A' else None,
                        'Status_Weather': row['Status (Weather)'],
                        'Temperature': float(row['Temp'].rstrip('°C')) if row['Temp'] and row['Temp'] != 'N/A' else None,
                        'Wind': row['Wind'],
                        'Pressure': float(row['Pressure'].rstrip('hPa')) if row['Pressure'] and row['Pressure'] != 'N/A' else None,
                        'Humidity': float(row['Humidity'].rstrip('%')) if row['Humidity'] and row['Humidity'] != 'N/A' else None,
                        'Latitude': float(row['Latitude']) if row['Latitude'] and row['Latitude'] != 'N/A' else None,
                        'Longitude': float(row['Longitude']) if row['Longitude'] and row['Longitude'] != 'N/A' else None
                    }
                    
                    telemetry = {k: v for k, v in telemetry.items() if v is not None}

                    if send_telemetry(thingsboard_url, access_token, telemetry):
                        print(f"Data sent successfully for {row['Location']}")
                    else:
                        print(f"Failed to send data for {row['Location']}")
                except ValueError as e:
                    print(f"Skipping row due to value error: {e}")
            else:
                print(f"Skipping data send for {row['Location']} due to provisioning failure")



def print_combined_data(combined_data):
    for location, data in combined_data.items():
        print(f"\n{location}:")
        for key, value in data.items():
            print(f"  {key}: {value}")
def save_location_temp_csv(data, filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(['Timestamp', 'Location', 'Temperature'])
        # Get current timestamp
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Write data
        for location, values in data.items():
            temp = values.get('Temp', 'N/A')
            writer.writerow([current_time, location, temp])

if __name__ == "__main__":
    api_key = "GOOGLE API KEY" 
    thingsboard_url = "https://thingsboard.cloud/"  # e.g., "http://localhost:8080" or your cloud instance URL
    provision_device_key = "THINGSBOARD PROVISION KEY"
    provision_device_secret = "THINGSBOARD PROVISION SECRET"
    keys_file = 'keys.txt'

    aqi_data = scrape_aqi_data()
    weather_data = scrape_weather_data()
    combined_data = combine_data(aqi_data, weather_data)
    combined_data_with_coords = add_lat_long_to_data(combined_data, api_key)
    print_combined_data(combined_data_with_coords)
    save_to_csv(combined_data_with_coords, 'combined_aqi_weather_data.csv')
    save_location_temp_csv(combined_data_with_coords, 'location_temperature_data.csv')

    send_to_thingsboard('combined_aqi_weather_data.csv', thingsboard_url, provision_device_key, provision_device_secret, keys_file)



