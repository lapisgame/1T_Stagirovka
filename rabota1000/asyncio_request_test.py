import asyncio
import aiohttp

import time
import requests
from tqdm import tqdm

from dotenv import dotenv_values

import json

URL = 'https://api.hh.ru/vacancies/'

lst = [
    '84462656', '86788792', '86664203',
    '86597536', '86336141', '85939099',
    '87656347', '86865790', '86656559',
    '86686326', '87822342', '85941989',
    '86111429', '86582361', '87636026',
    '84353994', '87410025', '85586413',
    '86864302', '87284308', '87269358',
    '87216328', '86581680', '86726818',
    '86571284', '87495323', '86927596',
    '86936001', '87327544', '81501211',
    '87323539', '84462462', '87428421',
    '87197328', '84845366', '83816998',
    '83816997', '76391976', '85563135',
    '86261386', '87303305', '86782313',
    '86223957', '87791595', '87226850',
    '87400111', '87497127', '83116089',
    '84916399', '84959490', '86485724',
    '86485721', '86485719', '86485720',
    '86681109', '86388127', '86320524',
    '87424041', '87833619', '84508925',
    '67924416', '87452598', '87659075',
    '87543885', '87543884', '86580120',
    '79041835', '76974214', '86178242',
    '69631571', '85665963', '86728117',
    '87305258', '80232010', '85984377',
    '85984375', '81856616', '87559225',
    '86064937', '85562079', '85562078',
    '85562075', '86485723', '86452791'
]

# Подгрузка данных из файла окружения
config = dotenv_values(".env")

# Обращения
# config['hh_api_name']
# config['hh_api_Client_ID']
# config['hh_api_Client_Secret']

params = {
    'grant_type':'client_credentials',
    'client_id':config['hh_api_Client_ID'],
    'client_secret':config['hh_api_Client_Secret']
}
access_token = 'APPLRI19VND7JM313H5GH18P687AIM7SHP907E4V1G5P7EGKMUN2NCE13GC2306A'
#access_token = json.loads(requests.post(f'https://hh.ru/oauth/token', params=params).content.decode())['access_token']
#print(access_token)
#access_token = config['hh_token_app']

class Api:
    def __init__(self, url: str):
        self.url = url

    async def async_gather_http_get(self, lst: list):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in tqdm(lst, desc='Async gather fetching data...', colour='YELLOW'):
                tasks.append(asyncio.create_task(session.get(self.url + i, headers = {'Authorization': f'Bearer {access_token}'})))

            responses = await asyncio.gather(*tasks)
            return [await r.json() for r in responses]
    

api = Api(URL)
res = asyncio.run(api.async_gather_http_get(lst))

for item in res:
    try:
        print(item['name'])
    except Exception as e:
        print(e)
        print(item)