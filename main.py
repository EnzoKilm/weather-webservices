import web
from dotenv import load_dotenv
import os
import httplib2
import json
import sqlite3
import time

# Import the API key from the .env file
load_dotenv()
api_key = os.getenv("OPENWEATHERMAP_KEY")

# Generating routes
urls = (
    '/weather(.*)', 'WeatherZip'
)
app = web.application(urls, globals())

h = httplib2.Http('.cache')


def request_api(zip_code, country_code):
    api_link = f"http://api.openweathermap.org/data/2.5/weather?zip={zip_code},{country_code}&appid={api_key}"

    # Getting the request from OpenWeatherMap API and formatting it to Dict/JSON format
    response, content = h.request(api_link, headers={'cache-control': 'no-cache'})
    api_content_json = json.loads(content)
    api_response_formatted = {
        "zipCode": zip_code,
        "countryCode": country_code,
        "actualTemp": round(api_content_json["main"]["temp"] - 273.15, 2),
        "minTemp": round(api_content_json["main"]["temp_min"] - 273.15, 2),
        "maxTemp": round(api_content_json["main"]["temp_max"] - 273.15, 2),
        "weather": api_content_json["weather"][0]["description"],
        "weatherIcon": f'http://openweathermap.org/img/wn/{api_content_json["weather"][0]["icon"]}@2x.png',
        "timestamp": round(time.time())
    }
    api_response_json = json.dumps(api_response_formatted)

    return api_response_formatted, api_response_json


class WeatherZip:
    @staticmethod
    def GET(self):
        zip_code, country_code = web.input(_method='get').zipcode, "FR"

        if len(zip_code) == 5:
            # Headers
            web.header('Content-Type', 'application/json')
            web.header('charset', 'utf-8')
            web.header('Cache-Control', 'no-cache')

            # Initializing the database
            database = WeatherDatabase()
            # Getting the last entry of the database
            last_weather_result = database.get_last_weather(zip_code)
            if last_weather_result is not None:
                last_row = last_weather_result[0]
                table_info = last_weather_result[1]

                # Formatting last row data
                database_response = {}
                for column in table_info:
                    database_response[column[1]] = last_row[column[0]]

                # Checking if the last row was created more than 15 minutes ago
                if database_response["timestamp"] + 900 < round(time.time()):
                    api_result = request_api(zip_code, country_code)
                    database.insert_weather(api_result[0])
                    return api_result[1]
                # If not, we display old value
                else:
                    database_json_response = json.dumps(database_response)
                    return database_json_response
            else:
                api_result = request_api(zip_code, country_code)
                database.insert_weather(api_result[0])
                return api_result[1]
        else:
            return "Bad ZIP code"


class WeatherDatabase:
    def __init__(self):
        self.con = sqlite3.connect('weather.db')
        self.cur = self.con.cursor()

        # Create table if not exists
        self.cur.execute('''CREATE TABLE IF NOT EXISTS weather 
                       (id INTEGER PRIMARY KEY, zipCode varchar(5) NOT NULL, countryCode varchar(10) NOT NULL, actualTemp FLOAT NOT NULL, minTemp FLOAT NOT NULL, maxTemp FLOAT NOT NULL, weather varchar(100) NOT NULL, weatherIcon varchar(255) NOT NULL, timestamp DATE NOT NULL)''')

    def insert_weather(self, data):
        # Insert a row of data
        self.cur.execute(
            f"INSERT INTO weather(zipCode, countryCode, actualTemp, minTemp, maxTemp, weather, weatherIcon, timestamp) VALUES ('{data['zipCode']}', '{data['countryCode']}', '{data['actualTemp']}', '{data['minTemp']}', '{data['maxTemp']}', '{data['weather']}', '{data['weatherIcon']}', '{data['timestamp']}')")
        # Save (commit) the changes
        self.con.commit()

    def get_last_weather(self, zip_code):
        # Getting the last entry
        self.cur.execute(f"SELECT * FROM weather WHERE zipCode = '{zip_code}' ORDER BY id DESC LIMIT 1")

        last_row = self.cur.fetchone()
        if last_row is not None:
            # Getting columns names
            table_info = self.cur.execute("PRAGMA table_info(weather)").fetchall()
            return last_row, table_info
        else:
            return None

    def __exit__(self):
        # Closing connection
        self.con.close()


if __name__ == "__main__":
    app.run()
