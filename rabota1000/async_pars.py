import aiohttp
import asyncio

import re
import json

import requests
from bs4 import BeautifulSoup
import lxml

import progressbar

from dotenv import dotenv_values
from fake_useragent import FakeUserAgent
from datetime import date, timedelta

import pandas as pd


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

# access_token = 'APPLKVNBURD1P25TR20J9MAE1P445D8E94TV0J9PCC1P1F8EVQMPS0Q7JETVOJ5M'
access_token = json.loads(requests.post(f'https://hh.ru/oauth/token', params=params).content.decode())['access_token']
print(access_token)

# Основные регулярные выражения для проекта
re_vacancy_id_hh = r'\/vacancy\/(\d+)\?'
re_vacancy_id_rabota = r'\/vacancy\/(\d+)'
re_vacancy_id_finder = r'\/vacancies\/(\d+)'
re_vacancy_id_zarplata = r'\/vacancy\/card\/id(\d+)'
# re.search(re_vacancy_id, string).group(1)

re_html_tag_remove = r'<[^>]+>'
# re.sub(re_html_tag_remove, replace, string)

class Rabota1000_parser_async:
    #* Функция инициализации
    def __init__(self, city='russia') -> None:
        self.max_page_count = 5
        self.basic_url = f'https://rabota1000.ru/{city}/'
        self.df = pd.DataFrame(columns=['vac_link', 'name', 
                                        'city', 'company', 'experience', 
                                        'schedule', 'employment', 
                                        'skills', 'description', 
                                        'salary', 'time'])
        self.vac_name_list = []
        self.get_vac_name_list_into_csv()

    #TODO ПЕРЕДЕЛАТЬ Чтение название вакансий ИЗ ФАЙЛА
    def get_vac_name_list_into_csv(self):
        with open('vac_name_list.csv', encoding='utf-8') as f:
            for line in f:
                self.vac_name_list.append(line)

    def to_pars(self)->None:
        for vac_name in self.vac_name_list:
            print(vac_name)
            links = self.get_list_links_into_rabota1000(vac_name)
            pre_pars_dict = self.async_pars_url_list(links)
            print('pre_pars_dict')
            bar = progressbar.ProgressBar(maxval=len(pre_pars_dict)).start()
            k = 0
            for item in pre_pars_dict:
                self.fetch_data_into_url(item)
                k += 1
                bar.update(k)
            
            print()
            self.df = self.df.drop_duplicates()
            self.df.to_csv('async_pars.csv', index=False)


    #* Достает id вакансии и название сайта для дальнейшей обработки 
    def get_vac_id_into_url(self, url:str)->dict[str, str]:
        if 'hh.ru' in url:
            return {'source': 'hh.ru', 'vac_id':re.search(re_vacancy_id_hh, url).group(1)}
        elif 'finder.vc' in url:
            return {'source': 'finder.vc', 'vac_id':re.search(re_vacancy_id_finder, url).group(1)}
        elif 'zarplata.ru' in url:
            return {'source': 'zarplata.ru', 'vac_id':re.search(re_vacancy_id_zarplata, url).group(1)}
        else:
            return {'source': 'rabota.ru', 'vac_id':re.search(re_vacancy_id_rabota, url).group(1)}

    #& Вспомогательная функция для объединения списков
    def list_simple_merge(self, list1:list, list2:list)->list:
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

    #* Асинхронная функция запросов на редирект
    #* принимает aiohttp.ClientSession(), ссылку с rabota1000, FakeUserAgent().random
    #* возвращает в результате dict[source, vac_id]
    async def fetch_vacancy_redirect_url(self, session, rabota_url, ua)->dict:
        try:
            url = f"{rabota_url}"
            data = await session.get(url, headers={'user-agent':ua, 'Authorization':f'Bearer {access_token}'}, timeout=5)
            url = data.url
            return self.get_vac_id_into_url(str(url))

        except Exception as e:
            print(e)
            # url = data.url
            # return self.get_vac_id_into_url(str(url))
            return {'source':'', 'vac_id':''}

    #* Асинхронная функция принимает список ссылок, возвращает список из dict[source, vac_id]
    async def async_pars_url_list_main(self, links)->list[dict]:
        tasks = []
        
        async with aiohttp.ClientSession() as session:
            ua = FakeUserAgent()
            for vacancy_id in links:
                task = asyncio.create_task(self.fetch_vacancy_redirect_url(session, vacancy_id, ua.random))
                tasks.append(task)
            
            return [await asyncio.gather(*tasks)]

    #* Запускает tasks с задачей fetch_vacancy_redirect_url, разделяя полный список ссылок на кластеры по step штук
    #* Возвращает единый список из dict[source, vac_id]
    def async_pars_url_list(self, links:list)->list[dict]:
        res = []
        step = 20
        for i in range(0, len(links), step):
            res += asyncio.run(self.async_pars_url_list_main(links[i:i+step]))

        merge = []
        for item in res:
            merge = self.list_simple_merge(merge, item)

        return merge

    #* Парсит все ссылки на вакансии без редиректа по названию вакансий, на max_page_count страниц выдачи
    def get_list_links_into_rabota1000(self, vac_name)->list[str]:
        res = []
        for page_num in range(1, self.max_page_count+1):
            used_url = f'{self.basic_url}{vac_name}?p={page_num}'
            page = requests.get(used_url)
            soup = BeautifulSoup(page.text, 'html.parser')

            links = [link['href'] for link in soup.findAll('a', attrs={'@click':'vacancyLinkClickHandler'})]

            res = self.list_simple_merge(res, links)

        return res

    #* Получаем все данные из dict[source, vac_id] и записываем их в датафрейм
    def fetch_data_into_url(self, link_dict:dict[str, str]):
        if link_dict['source'] != '':
            if link_dict['source'] == 'hh.ru':
                # print(pd.json_normalize(self._pars_url_hh(link_dict['vac_id'])))
                self.df = pd.concat([self.df, pd.DataFrame(pd.json_normalize(self._pars_url_hh(link_dict['vac_id'])))], ignore_index=True)
            elif link_dict['source'] == 'zarplata.ru':
                self.df = pd.concat([self.df, pd.DataFrame(pd.json_normalize(self._pars_url_zarplata(link_dict['vac_id'])))], ignore_index=True)
            elif link_dict['source'] == 'finder.vc':
                self.df = pd.concat([self.df, pd.DataFrame(pd.json_normalize(self._pars_url_finder(link_dict['vac_id'])))], ignore_index=True)
            else:
                self.df = pd.concat([self.df, pd.DataFrame(pd.json_normalize(self._pars_url_other(link_dict['vac_id'])))], ignore_index=True)

    #& Парсинг HH.RU            (use API)    
    def _pars_url_hh(self, id:str)->dict:
        res = {}
        try:
            data = requests.get(f'https://api.hh.ru/vacancies/{id}', headers = {'Authorization': f'Bearer {access_token}'}).json()
            if data['description'] != 'Not Found':
                res['vac_link'] = f'https://hh.ru/vacancy/{id}'                             # Ссылка
                res['name'] = data['name']                                                  # Название
                res['city'] = data['area']['name']                                          # Город
                res['company'] = data['employer']['name']                                   # Назвнание компании публикующей вакансию
                res['experience'] = data['experience']['name']                              # Опыт работы (нет замены на jun mid и sin)
                res['schedule'] = data['schedule']['name']                                  # Тип работы (офис/удаленка и тд)
                res['employment'] = data['employment']['name']                              # График работы
                res['skills'] = '  '.join([item['name'] for item in data['key_skills']])    # Ключевые навыки
                res['description'] = re.sub(re_html_tag_remove, '', data['description'])    # Полное описание (html теги не убраны)
                if data['salary'] == None: 
                    res['salary'] = 'Договорная'                                            # Если ЗП не указано то пишем договорная
                else:
                    if data['salary']['from'] != None:                                      # Если есть то берем как есть
                        res['salary.from'] = data['salary']['from']
                    else:
                        res['salary.from'] = '0'   

                    if data['salary']['to'] != None:                                      # Если есть то берем как есть
                        res['salary.to'] = data['salary']['to']
                    else:
                        res['salary.to'] = '0'   

                    if data['salary']['currency'] != None:
                        res['salary.currency'] = data['salary']['currency']
                    else:
                        res['salary.currency'] = 'Тургрики'

                res['time'] = data['published_at']                                          # Дата и время публикации
        except Exception as e:
            print(f'Not Found {e}')
            print(f'https://api.hh.ru/vacancies/{id}')
            data = requests.get(f'https://api.hh.ru/vacancies/{id}', headers = {'Authorization': f'Bearer {access_token}'}).json()
            print(data)

        return res

    #& Парсинг ZARPLATA.RU      (use API)
    def _pars_url_zarplata(self, id:str)->dict:
        res = {}
        try:
            data = requests.get(f'https://api.zarplata.ru/vacancies/{id}').json()
            res['vac_link'] = f'https://www.zarplata.ru/vacancy/card/id{id}'            # Ссылка
            res['name'] = data['name']                                                  # Название
            res['city'] = data['area']['name']                                          # Город
            res['company'] = data['employer']['name']                                   # Назвнание компании публикующей вакансию
            res['experience'] = data['experience']['name']                              # Опыт работы (нет замены на jun mid и sin)
            res['schedule'] = data['schedule']['name']                                  # Тип работы (офис/удаленка и тд)
            res['employment'] = data['employment']['name']                              # График работы
            res['skills'] = '  '.join([item['name'] for item in data['key_skills']])    # Ключевые навыки
            res['description'] = re.sub(re_html_tag_remove, '', data['description'])    # Полное описание
            if data['salary'] == None: 
                res['salary'] = 'Договорная'                                            # Если ЗП не указано то пишем договорная
            else:
                if data['salary']['from'] != None:                                      # Если есть то берем как есть
                    res['salary.from'] = data['salary']['from']
                else:
                    res['salary.from'] = '0'   

                if data['salary']['to'] != None:                                      # Если есть то берем как есть
                    res['salary.to'] = data['salary']['to']
                else:
                    res['salary.to'] = '0'   

                if data['salary']['currency'] != None:
                    res['salary.currency'] = data['salary']['currency']
                else:
                    res['salary.currency'] = 'Тургрики'
            res['time'] = data['published_at']
            
        except Exception as e:
            print(f'Not Found {e}')
            print(f'https://api.zarplata.ru/vacancies/{id}')
            data = requests.get(f'https://api.zarplata.ru/vacancies/{id}').json()
            print(data)

        return res

    #& Парсинг RABOTA1000.RU    (use xpath)
    def _pars_url_other(self, id:str)->dict:
        res = {}
        try:
            soup = BeautifulSoup(requests.get(f'https://rabota1000.ru/vacancy/{id}').text, 'html.parser')
            dom = lxml.etree.HTML(str(soup)) 
            res['vac_link'] = f'https://rabota1000.ru/vacancy/{id}'                                                                                             # Ссылка
            res['name'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[1]/h2')[0].text.replace('\n', '').lstrip().rstrip()            # Название
            res['city'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[3]/p[2]/span')[0].text.replace('\n', '')                       # Город (НЕТ)
            res['company'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[3]/p[1]')[0].text.replace('\n', '').lstrip().rstrip()       # Назвнание компании публикующей вакансию
            res['experience'] = ''                                                                                                                              # Опыт работы (нет замены на jun mid и sin)
            res['schedule'] = ''                                                                                                                                # Тип работы (офис/удаленка и тд) (НЕТ)
            res['employment'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[3]/ul/li[2]/span')[0].text.replace('\n', '')                                      # График работы
            res['skills'] = ''                                                                                                                                  # Ключевые навыки
            res['description'] = re.sub(re_html_tag_remove, '', dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[4]')[0].text).replace('\n', '')                                                   # Полное описание (НЕТ)
            if len(dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[2]/span'))>0:
                res['salary'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[2]/span')[0].text.replace('\n', '').lstrip().rstrip()        # ЗП
            else:
                res['salary'] = 'Договорная'
            res['time'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[3]/ul/li[1]/span')[0].text.replace('\n', '').lstrip().rstrip()        # Дата публикации
        except Exception as e:
            print(f'https://rabota1000.ru/vacancy/{id}')
        return res

    #& Парсинг FINDER.VC        (use xpath)
    def _pars_url_finder(self, id:str)->list:
        res = {}
        soup = BeautifulSoup(requests.get(f'https://finder.vc/vacancies/{id}').text, 'html.parser')
        dom = lxml.etree.HTML(str(soup)) 
        res['vac_link'] = f'https://finder.vc/vacancies/{id}' # Ссылка
        res['name'] = soup.find('h1', attrs={'class':'vacancy-info-header__title'}).text # Название
        res['city'] = ''              # Город (НЕТ)
        res['company'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/a')[0].text.replace('\n', '')        # Назвнание компании публикующей вакансию
        res['experience'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div[3]/div[1]/div[2]/div')[0].text.replace('\n', '')  # Опыт работы (нет замены на jun mid и sin)
        res['schedule'] = ''     # Тип работы (офис/удаленка и тд) (НЕТ
        res['employment'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[3]/div/div[2]/a')[0].text.replace('\n', '') # График работы
        res['skills'] = [li.text.replace('\n', '') for li in dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[3]/div[1]/div[2]/div[1]/ul')[0]]           # Ключевые навыки
        res['description'] = ''    # Полное описание (НЕТ)
        res['salary'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div[2]/div[2]/div')[0].text.replace(u'\xa0', '').replace('\n', '')

        if 'сегодня' in dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[1]')[0].text:
            res['time'] = str(date.today())
        elif 'вчера' in dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[1]')[0].text:
            res['time'] = str(date.today() - timedelta(days=1))
        else:
            res['time'] = str(date.today() - timedelta(days=int(re.search(r'Опубликована (\d+)', dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[1]')[0].text).group(1))))

        return res












parser = Rabota1000_parser_async()
parser.to_pars()
print(parser.df)