import aiohttp
import asyncio

import re

from dotenv import dotenv_values

from fake_useragent import FakeUserAgent

import time



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

access_token = 'APPLL4MRP3BV11JFBAF3SB6V3CUG9CBLNJ52H5DB0TKFJ13N08KJ0Q6I29PPPI9B'
# access_token = json.loads(requests.post(f'https://hh.ru/oauth/token', params=params).content.decode())['access_token']
print(access_token)

# Основные регулярные выражения для проекта
re_vacancy_id_hh = r'\/vacancy\/(\d+)\?'
re_vacancy_id_rabota = r'\/vacancy\/(\d+)'
re_vacancy_id_finder = r'\/vacancies\/(\d+)'
re_vacancy_id_zarplata = r'\/vacancy\/card\/id(\d+)'
# re.search(re_vacancy_id, string).group(1)

class Rabota1000_parser_async:
    #* Функция инициализации
    def __init__(self, city='russia') -> None:
        self.max_page_count = 10
        self.basic_url = f'https://rabota1000.ru/{city}/'
        
        #! Чтение название вакансий из файла
        self.vac_name_list = [
            'data+scientist', 'data+science', 'дата+сайентист',
            'младший+дата+сайентист', 'стажер+дата+сайентист',
            'machine+learning', 'ml', 'ml+engineer',
            'инженер+машинного+обучения', 'data+engineering',
            'инженер+данных', 'младший+инженер+данных',
            'junior+data+analyst', 'junior+data+scientist',
            'junior+data+engineer', 'data+analyst',
            'data+analytics','аналитик+данных', 'big+data+junior'
        ]

    #* Достает id вакансии и название сайта для дальнейшей обработки 
    def get_vac_id_into_url(self, url:str)->dict[str, str]:
        if 'hh.ru' in url:
            return {'source': 'hh', 'vac_id':re.search(re_vacancy_id_hh, url).group(1)}
        elif 'finder.vc' in url:
            return {'source': 'finder', 'vac_id':re.search(re_vacancy_id_finder, url).group(1)}
        elif 'zarplata.ru' in url:
            return {'source': 'zarpalta', 'vac_id':re.search(re_vacancy_id_zarplata, url).group(1)}
        else:
            return {'source': 'rabota', 'vac_id':re.search(re_vacancy_id_rabota, url).group(1)}

    #& Вспомогательная функция для объединения списков
    def list_simple_merge(list1, list2):
        i, j = 0, 0
        res = []
        while i < len(list1) and j < len(list2):
            res.append(list1[i])
            i += 1
            res.append(list2[j])
            j += 1
        res += list1[i:]
        res += list2[j:] 
        return res


    async def fetch_vacancy_data(self, session, rabota_url, ua)->dict:
        try:
            url = f"{rabota_url}"
            data = await session.get(url, headers = {'user-agent':ua, 'Authorization': f'Bearer {access_token}'})
            url = data.url

            return self.get_vac_id_into_url(str(url))

        except Exception as e:
            url = data.url
            return self.get_vac_id_into_url(str(url))

    async def main(self, links):
        tasks = []
        
        async with aiohttp.ClientSession() as session:
            ua = FakeUserAgent()
            for vacancy_id in links:
                task = asyncio.create_task(self.fetch_vacancy_data(session, vacancy_id, ua.random))
                tasks.append(task)
            
            return [await asyncio.gather(*tasks)]

    

    def async_pars_url_list(self, links:list)->list[dict]:
        res = []
        step = 20
        for i in range(0, len(links), step):
            res += asyncio.run(self.main(links[i:i+step]))

        merge = []
        for item in res:
            merge = self.list_simple_merge(merge, item)


