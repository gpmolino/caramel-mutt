from bs4 import BeautifulSoup
from datetime import datetime
import requests
import json


def scrapper_trips(origin: str, destination: str, date: str) -> dict:
    """
    Given two cities and a date it will return, if available, a list with two dict containing raw data gathered from Clickbus website and API.
    :param origin: departure city according clickbus alias
    :param destination: arrival city according clickbus alias
    :param date: date of trips departure in YYYT-MM-DD format
    :return: dict
    """
    trip_list = []

    # Get data from Clickbus Webpage
    try:
        url_website = f'https://www.clickbus.com.br/onibus/{origin}/{destination}?departureDate={date}'
        website_response = requests.get(url_website)

        # Success in reaching Clickbus Webpage
        if website_response.status_code == 200:
            soup = BeautifulSoup(website_response.text, 'html.parser')
            trips_list_html_data = soup.find_all('div', class_='search-item')
            trips_number = len(trips_list_html_data)

            # It was found at least one trip
            if trips_number > 0:
                print(f'[INFO] It was found {trips_number} between {origin} -> {destination} at {date}.')
                for trip_html_data in trips_list_html_data:
                    page_data = json.loads(trip_html_data['data-content'])
                    api_data = {}

                    # Try to get trip details from API
                    try:
                        url_api = 'https://www.clickbus.com.br/seat-map/{id}'.format(id=page_data['tripId'])
                        api_response = requests.get(url_api)

                        if api_response.status_code == 200:
                            api_data = api_response.json()
                        else:
                            print(f'[ERROR] response code:{api_response.status_code} | url: {url_api}')

                    except Exception as err:
                        print(f'Unexpected {err=}, {type(err)=}')

                    trip_list.append({'page_data': page_data, 'api_data': api_data})

            else:
                print(f'[INFO] No trips was found between {origin} -> {destination} at {date}.')
        else:
            print(f'[ERROR] response code:{website_response.status_code} | url: {url_website}')

    except Exception as err:
        print(f'Unexpected {err=}, {type(err)=}')

    result = {
        'origin': origin,
        'destination': destination,
        'date': date,
        'source': 'clickbus',
        'gathered_at': datetime.utcnow(),
        'count_trips': len(trip_list),
        'trips': json.dumps(trip_list)
    }
    return result
