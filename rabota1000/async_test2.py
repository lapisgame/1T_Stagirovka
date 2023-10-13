import aiohttp
import asyncio

import re

from dotenv import dotenv_values

from fake_useragent import FakeUserAgent

import time

vac_name_list = [
    'data+scientist', 'data+science', 'дата+сайентист',
    'младший+дата+сайентист', 'стажер+дата+сайентист',
    'machine+learning', 'ml', 'ml+engineer',
    'инженер+машинного+обучения', 'data+engineering',
    'инженер+данных', 'младший+инженер+данных',
    'junior+data+analyst', 'junior+data+scientist',
    'junior+data+engineer', 'data+analyst',
    'data+analytics','аналитик+данных', 'big+data+junior'
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

access_token = 'APPLL4MRP3BV11JFBAF3SB6V3CUG9CBLNJ52H5DB0TKFJ13N08KJ0Q6I29PPPI9B'
# access_token = json.loads(requests.post(f'https://hh.ru/oauth/token', params=params).content.decode())['access_token']
print(access_token)

# Основные регулярные выражения для проекта
re_vacancy_id_hh = r'\/vacancy\/(\d+)\?'
re_vacancy_id_rabota = r'\/vacancy\/(\d+)'
re_vacancy_id_finder = r'\/vacancies\/(\d+)'
re_vacancy_id_zarplata = r'\/vacancy\/card\/id(\d+)'
# re.search(re_vacancy_id, string).group(1)

def get_vac_id_into_url(url:str)->dict:
    if 'hh.ru' in url:
        return {'source': 'hh', 'vac_id':re.search(re_vacancy_id_hh, url).group(1)}
    elif 'finder.vc' in url:
        return {'source': 'finder', 'vac_id':re.search(re_vacancy_id_finder, url).group(1)}
    elif 'zarplata.ru' in url:
        return {'source': 'zarpalta', 'vac_id':re.search(re_vacancy_id_zarplata, url).group(1)}
    else:
        return {'source': 'rabota', 'vac_id':re.search(re_vacancy_id_rabota, url).group(1)}

async def fetch_vacancy_data(session, vacancy_id, ua)->dict:
    try:
        url = f"{vacancy_id}"
        data = await session.get(url, headers = {'user-agent':ua, 'Authorization': f'Bearer {access_token}'})
        url = data.url

        return get_vac_id_into_url(str(url))

    except Exception as e:
        url = data.url
        return get_vac_id_into_url(str(url))

async def main(links):
    tasks = []
    
    async with aiohttp.ClientSession() as session:
        ua = FakeUserAgent()
        for vacancy_id in links:
            task = asyncio.create_task(fetch_vacancy_data(session, vacancy_id, ua.random))
            tasks.append(task)
        
        return [await asyncio.gather(*tasks)]

links = [
    'https://rabota1000.ru/vacancy/104770616',
    'https://rabota1000.ru/vacancy/103341030',
    'https://rabota1000.ru/vacancy/88087645',
    'https://rabota1000.ru/vacancy/103698750',
    'https://rabota1000.ru/vacancy/101447379',
    'https://rabota1000.ru/vacancy/98928217',
    'https://rabota1000.ru/vacancy/110167964',
    'https://rabota1000.ru/vacancy/105369250',
    'https://rabota1000.ru/vacancy/103693182',
    'https://rabota1000.ru/vacancy/103706177',
    'https://rabota1000.ru/vacancy/103177742',
    'https://rabota1000.ru/vacancy/98948136',
    'https://rabota1000.ru/vacancy/99892552',
    'https://rabota1000.ru/vacancy/110020884',
    'https://rabota1000.ru/vacancy/113203965',
    'https://rabota1000.ru/vacancy/114054291',
    'https://rabota1000.ru/vacancy/87585211',
    'https://rabota1000.ru/vacancy/108607355',
    'https://rabota1000.ru/vacancy/113995882',
    'https://rabota1000.ru/vacancy/94928170',
    'https://rabota1000.ru/vacancy/105358186',
    'https://rabota1000.ru/vacancy/107728714',
    'https://rabota1000.ru/vacancy/107705580',
    'https://rabota1000.ru/vacancy/106782907',
    'https://rabota1000.ru/vacancy/103324760',
    'https://rabota1000.ru/vacancy/104136390',
    'https://rabota1000.ru/vacancy/103169218',
    'https://rabota1000.ru/vacancy/105572305',
    'https://rabota1000.ru/vacancy/105576513',
    'https://rabota1000.ru/vacancy/108834453',
    'https://rabota1000.ru/vacancy/107964935',
    'https://rabota1000.ru/vacancy/107970963',
    'https://rabota1000.ru/vacancy/88087633',
    'https://rabota1000.ru/vacancy/61914562',
    'https://rabota1000.ru/vacancy/108633776',
    'https://rabota1000.ru/vacancy/90753526',
    'https://rabota1000.ru/vacancy/87826846',
    'https://rabota1000.ru/vacancy/87826788',
    'https://rabota1000.ru/vacancy/106750861',
    'https://rabota1000.ru/vacancy/114018507',
    'https://rabota1000.ru/vacancy/25091466',
    'https://rabota1000.ru/vacancy/101160704',
    'https://rabota1000.ru/vacancy/107892727',
    'https://rabota1000.ru/vacancy/104672577',
    'https://rabota1000.ru/vacancy/114724525',
    'https://rabota1000.ru/vacancy/114025437',
    'https://rabota1000.ru/vacancy/100957325',
    'https://rabota1000.ru/vacancy/108842086',
    'https://rabota1000.ru/vacancy/78041571',
    'https://rabota1000.ru/vacancy/112372307',
    'https://rabota1000.ru/vacancy/107027867',
    'https://rabota1000.ru/vacancy/108555675',
    'https://rabota1000.ru/vacancy/90893874',
    'https://rabota1000.ru/vacancy/114031026',
    'https://rabota1000.ru/vacancy/110175569',
    'https://rabota1000.ru/vacancy/94889537',
    'https://rabota1000.ru/vacancy/108790279',
    'https://rabota1000.ru/vacancy/104408519',
    'https://rabota1000.ru/vacancy/510785',
    'https://rabota1000.ru/vacancy/95686551',
    'https://rabota1000.ru/vacancy/107895375',
    'https://rabota1000.ru/vacancy/55700375',
    'https://rabota1000.ru/vacancy/99024039',
    'https://rabota1000.ru/vacancy/65470905',
    'https://rabota1000.ru/vacancy/109746279',
    'https://rabota1000.ru/vacancy/113225432',
    'https://rabota1000.ru/vacancy/103322800',
    'https://rabota1000.ru/vacancy/101825668',
    'https://rabota1000.ru/vacancy/94889540',
    'https://rabota1000.ru/vacancy/94889541',
    'https://rabota1000.ru/vacancy/94889542',
    'https://rabota1000.ru/vacancy/94889539',
    'https://rabota1000.ru/vacancy/102119614',
    'https://rabota1000.ru/vacancy/114030573',
    'https://rabota1000.ru/vacancy/104079757',
    'https://rabota1000.ru/vacancy/101405084',
    'https://rabota1000.ru/vacancy/95385363',
    'https://rabota1000.ru/vacancy/108625213',
    'https://rabota1000.ru/vacancy/114025918',
    'https://rabota1000.ru/vacancy/112922507',
    'https://rabota1000.ru/vacancy/454188',
    'https://rabota1000.ru/vacancy/112021710',
    'https://rabota1000.ru/vacancy/109404589',
    'https://rabota1000.ru/vacancy/109404588',
    'https://rabota1000.ru/vacancy/42507636',
    'https://rabota1000.ru/vacancy/25599789',
    'https://rabota1000.ru/vacancy/60173299',
    'https://rabota1000.ru/vacancy/100733405',
    'https://rabota1000.ru/vacancy/99024038',
    'https://rabota1000.ru/vacancy/99024037',
    'https://rabota1000.ru/vacancy/99255452',
    'https://rabota1000.ru/vacancy/114686338',
    'https://rabota1000.ru/vacancy/94889538',
    'https://rabota1000.ru/vacancy/85030794',
    'https://rabota1000.ru/vacancy/113212722',
    'https://rabota1000.ru/vacancy/113258362',
    'https://rabota1000.ru/vacancy/105985490',
    'https://rabota1000.ru/vacancy/111906282',
    'https://rabota1000.ru/vacancy/107986402',
    'https://rabota1000.ru/vacancy/110170388',
    'https://rabota1000.ru/vacancy/533485',
    'https://rabota1000.ru/vacancy/114725096',
    'https://rabota1000.ru/vacancy/112930600',
    'https://rabota1000.ru/vacancy/114224156',
    'https://rabota1000.ru/vacancy/105557375',
    'https://rabota1000.ru/vacancy/110825905',
    'https://rabota1000.ru/vacancy/114390608',
    'https://rabota1000.ru/vacancy/105362560',
    'https://rabota1000.ru/vacancy/105922508',
    'https://rabota1000.ru/vacancy/108769259',
    'https://rabota1000.ru/vacancy/105331322',
    'https://rabota1000.ru/vacancy/110223373',
    'https://rabota1000.ru/vacancy/109831875',
    'https://rabota1000.ru/vacancy/114398936',
    'https://rabota1000.ru/vacancy/104677064',
    'https://rabota1000.ru/vacancy/103654986',
    'https://rabota1000.ru/vacancy/111248398',
    'https://rabota1000.ru/vacancy/108536947',
    'https://rabota1000.ru/vacancy/105484654',
    'https://rabota1000.ru/vacancy/104596316',
    'https://rabota1000.ru/vacancy/108616052',
    'https://rabota1000.ru/vacancy/108616051',
    'https://rabota1000.ru/vacancy/106347104',
    'https://rabota1000.ru/vacancy/111902433',
    'https://rabota1000.ru/vacancy/97465285',
    'https://rabota1000.ru/vacancy/103174822',
    'https://rabota1000.ru/vacancy/105454036',
    'https://rabota1000.ru/vacancy/103344293',
    'https://rabota1000.ru/vacancy/78034702',
    'https://rabota1000.ru/vacancy/113201770',
    'https://rabota1000.ru/vacancy/104139159',
    'https://rabota1000.ru/vacancy/103191712',
    'https://rabota1000.ru/vacancy/114206125',
    'https://rabota1000.ru/vacancy/88320984',
    'https://rabota1000.ru/vacancy/97480461',
    'https://rabota1000.ru/vacancy/112367347',
    'https://rabota1000.ru/vacancy/90751563',
    'https://rabota1000.ru/vacancy/111248291',
    'https://rabota1000.ru/vacancy/114027186',
    'https://rabota1000.ru/vacancy/105573043',
    'https://rabota1000.ru/vacancy/103157120',
    'https://rabota1000.ru/vacancy/107916751',
    'https://rabota1000.ru/vacancy/109399297',
    'https://rabota1000.ru/vacancy/105485537',
    'https://rabota1000.ru/vacancy/114746523',
    'https://rabota1000.ru/vacancy/112328939',
    'https://rabota1000.ru/vacancy/93213438',
    'https://rabota1000.ru/vacancy/92045790',
    'https://rabota1000.ru/vacancy/104584307',
    'https://rabota1000.ru/vacancy/109399299',
    'https://rabota1000.ru/vacancy/105355796',
    'https://rabota1000.ru/vacancy/98985891',
    'https://rabota1000.ru/vacancy/108797837',
    'https://rabota1000.ru/vacancy/107714052',
    'https://rabota1000.ru/vacancy/106774493',
    'https://rabota1000.ru/vacancy/105990718',
    'https://rabota1000.ru/vacancy/105577309',
    'https://rabota1000.ru/vacancy/104579578',
    'https://rabota1000.ru/vacancy/109399298',
    'https://rabota1000.ru/vacancy/70197392'
]

def iter_merge(list1, list2):
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

res = []

step = 20
for i in range(0, len(links), step):
    res += asyncio.run(main(links[i:i+step]))

merge = []
for item in res:
    merge = iter_merge(merge, item)
