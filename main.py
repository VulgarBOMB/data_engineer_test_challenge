import requests
import json
import pandas as pd
from bs4 import BeautifulSoup

API_KEY = '**************'
API_HOST = 'https://apidata.mos.ru/'

VELO_PARKING = 916
SPORTS_HALL = 60622
DOG_WALKING_AREA = 2663

# Получение данных о велопарковках Москвы
response = requests.get(API_HOST + f'v1/datasets/{str(VELO_PARKING)}/rows?api_key={API_KEY}')
data = response.json()
with open('velo_parking.json', 'w', encoding='utf-8') as f:
    json.dump(data, f)
velo_parking_arr = []
for item in data:
    velo_parking_arr.append([
        item['Cells']['global_id'],
        item['Cells']['Name'],
        item['Cells']['AdmArea'],
        item['Cells']['District'].replace('район ', '').replace(' район', ''),
        item['Cells']['Address'],
        item['Cells']['Photo'],
        item['Cells']['Capacity'],
        item['Cells']['DepartmentalAffiliation'],
        item['Cells']['ObjectOperOrgName'],
        item['Cells']['ObjectOperOrgPhone'][0]['OperationOrganizationPhone'],
        item['Cells']['Longitude_WGS84'],
        item['Cells']['Latitude_WGS84'],
    ])
velo_parking_df = pd.DataFrame(velo_parking_arr,
                               columns=[
                                   'global_id',
                                   'Name',
                                   'AdmArea',
                                   'District',
                                   'Address',
                                   'Photo',
                                   'Capacity',
                                   'DepartmentalAffiliation',
                                   'ObjectOperOrgName',
                                   'ObjectOperOrgPhone',
                                   'Longitude',
                                   'Latitude',
                               ])
velo_parking_df.to_csv('velo_parking.csv', encoding='utf-8', index=False)
print('Датасет \"Велосипедные парковки\" был выгружен')

# Получение данных о спортзалах Москвы
response = requests.get(API_HOST + f'v1/datasets/{str(SPORTS_HALL)}/rows?api_key={API_KEY}')
data = response.json()
with open('sports_hall.json', 'w', encoding='utf-8') as f:
    json.dump(data, f)
sports_hall_arr = []
for item in data:
    sports_hall_arr.append([
        item['Cells']['global_id'],
        item['Cells']['ObjectName'],
        item['Cells']['AdmArea'],
        item['Cells']['District'].replace('район ', '').replace(' район', ''),
        item['Cells']['Address'],
        item['Cells']['PhotoWinter'][0]['Photo'],
        item['Cells']['Email'],
        item['Cells']['WebSite'],
        item['Cells']['HelpPhone'],
        item['Cells']['geoData']['coordinates'][0],
        item['Cells']['geoData']['coordinates'][1],
    ])
sports_hall_df = pd.DataFrame(sports_hall_arr,
                              columns=[
                                  'global_id',
                                  'ObjectName',
                                  'AdmArea',
                                  'District',
                                  'Address',
                                  'Photo',
                                  'Email',
                                  'WebSite',
                                  'HelpPhone',
                                  'Longitude',
                                  'Latitude',
                              ])
sports_hall_df.to_csv('sports_hall.csv', encoding='utf-8', index=False)
print('Датасет \"Залы спортивные\" был выгружен')

# Получение данных о площадках для выгула собак
# API падает с 500 ответом
response = requests.get(API_HOST + f'v1/datasets/{str(DOG_WALKING_AREA)}/rows?api_key={API_KEY}')
data = response.json()
with open('dog_walking_area.json', 'w', encoding='utf-8') as f:
    json.dump(data, f)
print(pd.json_normalize(data).to_string())

# Получение данных с Wiki о районах Москвы
url = 'https://ru.wikipedia.org/wiki/Районы_и_поселения_Москвы'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')
table = soup.find('table', {'class': 'standard'})
rows = table.find_all('tr')
districts_arr = []
for row in rows:
    cells = row.find_all('td')
    if cells:
        population = cells[7].text.replace('↗', '').replace('↘', '').replace(' ', '')
        districts_arr.append([
            cells[0].text,
            cells[4].text,
            cells[5].text,
            cells[6].text,
            population
        ])
        # print(f'{cells[0].text} | {cells[4].text} | {cells[5].text} | {cells[6].text} км^2 | {population} чел.')
districts_df = pd.DataFrame(districts_arr,
                            columns=[
                                'ID',
                                'name',
                                'abbreviation',
                                'area',
                                'population',
                            ])
districts_df.to_csv('districts.csv', encoding='utf-8', index=False)
