from bs4 import BeautifulSoup
import lxml
from lxml.etree import tostring as htmlstring
import requests
import psycopg2 as psy

from fake_useragent import FakeUserAgent

from dotenv import dotenv_values

import asyncio
import aiohttp


import re
import csv
import os.path
from datetime import date, timedelta
import json
import time

import progressbar

import pandas as pd

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
# access_token = json.loads(requests.post(f'https://hh.ru/oauth/token', params=params).content.decode())['access_token']
# print(access_token)
# access_token = config['hh_token_app']

# class Api:
#     def __init__(self, url: str):
#         self.url = url

#     async def async_gather_http_get(self, lst: list):
#         async with aiohttp.ClientSession() as session:
#             tasks = []
#             for i in tqdm(lst, desc='Async gather fetching data...', colour='YELLOW'):
#                 tasks.append(asyncio.create_task(session.get(self.url + i, headers = {'Authorization': f'Bearer {access_token}'})))

#             responses = await asyncio.gather(*tasks)
#             return [await r.json() for r in responses]
    

# api = Api(URL)
# res = asyncio.run(api.async_gather_http_get(lst))

# for item in res:
#     try:
#         print(item['name'])
#     except Exception as e:
#         print(e)
#         print(item)



class Rabota1000_Parser:
    # Класс для парсинга вакансий с ресурса Rabota1000.ru
    def __init__(self, city:str='russia'):
        self.pre_resualt = []
        self.max_page_count = 5
        self.basic_url = 'https://rabota1000.ru/russia/'
        self.vac_name_list = []
        self.vac_name_list = [
            'data+scientist', 'data+science', 'дата+сайентист',
            'младший+дата+сайентист', 'стажер+дата+сайентист',
            'machine+learning', 'ml', 'ml+engineer'
        ]
        
        self.ua = FakeUserAgent()
        headers = {'user-agent':self.ua.random}

    async def pre_async_pars(self):
        for vac_name in self.vac_name_list:
            try:
                tasks = []                
                async with aiohttp.ClientSession() as session:
                    tasks.append(asyncio.create_task(self.async_pars_ALL_pages_by_vac_name(vac_name=vac_name)))

                responses = await asyncio.gather(*tasks)
                links_to_save_as = [await r for r in responses]
                
                
                vac_name = item.keys()[0]
                links_to_save = item[vac_name]

                if not os.path.exists(f'data_csv/pars_link_{vac_name}.csv'):
                    with open(f'data_csv/pars_link_{vac_name}.csv', 'w', newline='', encoding='utf-8') as csv_file:
                        names = ['vac_name', 'link', 'source', 'vac_id']
                        file_writer = csv.DictWriter(csv_file, delimiter = ",", lineterminator="\r", fieldnames=names)
                        file_writer.writeheader()
                            

                with open(f'data_csv/pars_link_{vac_name}.csv', 'a', newline='', encoding='utf-8') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerows(links_to_save_as)
            except Exception as e:
                print(e)

    async def async_pars_ALL_pages_by_vac_name (self, vac_name)->dict:
        print(vac_name)
        links = []
        sources = []
        for i in range(1, 11):
            print(i, end=' ')
            used_url = f'{self.basic_url}{vac_name}?p={i}'
            page = requests.get(used_url)
            soup = BeautifulSoup(page.text, 'html.parser')

            # 20 ссылок на одной странице
            tasks = []
            async with aiohttp.ClientSession() as session:
                for link in soup.findAll('a', attrs={'@click':'vacancyLinkClickHandler'}):
                    tasks.append(asyncio.create_task(session.get(link['href'], 
                                headers = {'user_agent':self.ua.random,
                                            'Authorization': f'Bearer {access_token}'})))
            
                responses = await asyncio.gather(*tasks)
            app = [r.url for r in responses]
            links = links + app
            s = [source.text for source in soup.findAll('span', attrs={'class':'text-sky-600'})]
            sources = sources + s

        links_to_save = [[vac_name, link, source] for link, source in zip(links, sources)]

        return {vac_name:links_to_save}


async def main():
    parser = Rabota1000_Parser()
    res = await parser.async_pars_ALL_pages_by_vac_name('ml')

asyncio.run(main())