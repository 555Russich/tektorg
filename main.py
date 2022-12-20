import asyncio
import random
import re
import logging
import time
from pathlib import Path
import platform

import aiohttp
import schedule
from bs4 import BeautifulSoup
import pandas as pd

from my_logging import get_logger

DIR_PROCEDURES = r'D:\procedures' if platform.system() == 'Windows' else 'procedures'
FILENAME_XLSX = r'procedures.xlsx'
FILEPATH_XLSX = Path(DIR_PROCEDURES, FILENAME_XLSX)
Path(DIR_PROCEDURES).mkdir(exist_ok=True)

RETRIES = 31

ua_platform = '(X11; Ubuntu; Linux x86_64; rv:108.0)' if platform.system() == 'Linux'\
    else '(Windows NT 10.0; Win64; x64; rv:108.0)'
headers = {
    'User-Agent': f'Mozilla/5.0 {ua_platform} Gecko/20100101 Firefox/108.0',
    'Accept': '*/*',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    # 'Cookie': 'Drupal.visitor.procedures_theme=blocks; SSESS8aa208d9665c28bb20b7c818a7f80de5=zziXwHZkbXLflFg6lbZF3Tj8T4cXwxt_dguDdbrG5dk; session-cookie=1730a716ddf4c24575c675b0b4b53d11486f16c50ee7b5484df407fa464ffa77c38a8489ff43a2b3bab5a3ea6dce12e3; rerf=AAAAAGOZuqsqqmtwAzImAg==; ipp_uid=1671019178404/npkdDowiuX4fUSaV/jIq/fD5+W/IO0dp15zU8RQ==; ipp_key=v1671023170423/v33947245ba5adc7a72e273/DPNXA26JAYsrmCYr3EjM6A==; _ga_69E4MLGLTE=GS1.1.1671023170.4.0.1671023170.0.0.0; _ga_MBKDKGVXSM=GS1.1.1671023170.2.0.1671023170.0.0.0; _ga=GA1.2.1363288480.1671019181; _gid=GA1.2.1936499132.1671019182; _ym_uid=16710191821027696080; _ym_d=1671019182; _ym_isad=2',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
}


async def collect_data() -> None:
    urls = (
        'https://www.tektorg.ru/rosneft/procedures',
        'https://www.tektorg.ru/rosnefttkp/procedures'
    )
    async with aiohttp.ClientSession(headers=headers) as s:
        for url in urls:
            appended = 0
            procedures_urls_to_append = await do_with_retries(
                func=get_procedures_urls, _args=(s, url, ),
                retries=RETRIES, sleep_range=(30, 60)
            )

            for procedure_url in procedures_urls_to_append:
                res = await do_with_retries(
                    handle_procedure, _args=(s, procedure_url),
                    retries=RETRIES, sleep_range=(30, 60)
                )
                appended += 1 if res else 0
            logging.info(f'Collected data of {appended} procedures for {url}\n\n')


async def get_procedures_urls(s: aiohttp.ClientSession, url: str) -> list:
    procedures_urls_to_append = []
    appended_all_new = False
    last_page_number = None
    procedures_numbers_appended = list(pd.read_excel(str(FILEPATH_XLSX))['Номер']) \
        if FILEPATH_XLSX.exists() else []
    logging.info(f'Start collecting procedures urls. Already in file {len(procedures_numbers_appended)}')

    params = {
        'limit': 100,
        'page': 1,
        'sort': 'datestart'
    }
    while not appended_all_new:
        procedures_urls_to_append_temp = procedures_urls_to_append.copy()
        async with s.get(url, params=params) as r:
            match r.status:
                case 200:
                    soup = BeautifulSoup(await r.text(), 'lxml')
                    if not soup.find('header', class_='page-header'):
                        with open('empty_search_page.html', 'w', encoding='utf-8') as f:
                            f.write(await r.text())
                        raise ConnectionError(f'{r.status=}. Page data unavailable')
                case _:
                    raise ConnectionError(f'{r.status=}')

        if not last_page_number:
            last_page_number = int(soup.find('ul', class_='pagination').find_all('li', class_='')[-1].text)

        for procedure_div in soup.find_all('div', class_='section-procurement__item'):
            procedure_number = procedure_div.find('div', class_='section-procurement__item-numbers') \
                .find('span').text.strip().replace('Номер закупки на сайте ЭТП:', '') \
                .replace(' ', '').replace('\n', '')
            procedure_href = procedure_div.find('a', class_='section-procurement__item-title').get('href')

            if procedure_number not in procedures_numbers_appended:
                procedures_urls_to_append.append('https://tektorg.ru' + procedure_href)

        # if len(procedures_urls_to_append_temp) == len(procedures_urls_to_append) or \
        if params['page'] == last_page_number:
            appended_all_new = True

        logging.info(f'Collected urls from page №:{params["page"]}')
        params['page'] += 1

    logging.info(f'Collected {len(procedures_urls_to_append)} new procedures urls')
    return procedures_urls_to_append


async def handle_procedure(s: aiohttp.ClientSession, url: str) -> bool:
    logging.info(f'Start collecting data for {url}')
    async with s.get(url) as r:
        match r.status:
            case 200:
                soup = BeautifulSoup(await r.text(), 'lxml')
                if 'Вы не авторизированы для доступа к этой странице' in soup.text:
                    logging.info(f'Code=200. Unavailable procedure {url}')
                    return False
                elif not soup.find('header', class_='page-header'):
                    with open('empty_procedure_page.html', 'w', encoding='utf-8') as f:
                        f.write(soup.text)
                    raise ConnectionError(f'{r.status=}. Page data Unavailable')
            case 403:
                logging.info(f'Code=403. Unavailable procedure {url}')
                return False
            case _:
                raise ConnectionError(f'{r.status=}')

    procedure_data = {'Наименование закупки': soup.find('span', class_='procedure__item-name').text}
    pattern_to_parse = [
        (
            'div', {
                'class': 'procedure__item procedure__item--commonInfo',
                'id': 'commonInfo'
            }
        ),
        (
            'div', {'class': 'procedure__item procedure__item--timing'},
        ),
        (
            'div', {
                'class': 'procedure__item',
                'id': 'orgInfo'
            }
        )
    ]

    for pattern in pattern_to_parse:
        for tr in soup.find(*pattern).find('table', class_='procedure__item-table').find_all('tr'):
            tds = tr.find_all('td')
            k = tds[0].text.strip()
            k = k[:-1] if k[-1] == ':' else k
            k = 'Номер' if k in ('Номер закупки', 'Номер процедуры') else k
            v = tds[1].text.strip().replace(' GMT+3', '')
            procedure_data[k] = v

    procedure_number = procedure_data['Номер']
    Path(DIR_PROCEDURES, procedure_number).mkdir(exist_ok=True)

    files_data = []
    filenames = []
    for doc in soup.find_all('div', class_='procedure__item--documents-item'):
        a_doc = doc.find('div', class_='item-name').find('a')
        doc_url = 'https://tektorg.ru' + a_doc.get('href')

        if 'docprotocol' in doc_url:
            extension = re.search(r'(?<=\()[a-zA-Z\d]+(?=\)$)', a_doc.text).group(0)
            filename = a_doc.text.replace(f'({extension})', '')
            filename = ''.join(list(filename)[:-1]) if list(filename)[-1] == ' ' else filename
            filename = f"{filename}.{extension}"
        else:
            filename = re.search(r'(?<=\().+(?=\))', a_doc.text).group(0) \
                if re.search(r'(?<=\().+(?=\))', a_doc.text) else a_doc.text
        filename = re.sub(r'[<>:"/\\|?*]', '', filename)
        filepath = Path(DIR_PROCEDURES, procedure_number, filename)

        same_filenames = [x for x in filenames if filename in x]
        if len(same_filenames) > 0:
            filepath = Path(
                filepath.parent, f'{filepath.stem}_{len(same_filenames)}'
            ).with_suffix(filepath.suffix)
        filenames.append(filename)

        files_data.append({'url': doc_url, 'path': filepath})

    for d in files_data:
        await do_with_retries(
            func=download_file, _args=(s, d['url'], d['path']),
            retries=3, sleep_range=(30, 60)
        )

    append_row_to_xlsx(
            Path(DIR_PROCEDURES, FILENAME_XLSX),
            procedure_data
        )
    return True


async def download_file(s: aiohttp.ClientSession, url: str, filepath: Path) -> bool:
    logging.info(f'Downloading "{filepath.name}" from {url}')
    async with s.get(url, timeout=60*20) as r:
        with open(str(filepath), 'wb') as f:
            f.write(await r.read())
            return True


def append_row_to_xlsx(filepath: Path, row: dict) -> None:
    while True:
        try:
            if not filepath.exists():
                pd.DataFrame().to_excel(str(filepath), index=False)

            df = pd.read_excel(str(filepath))
            df = pd.concat([df, pd.DataFrame([row])])
            df.to_excel(str(filepath), index=False)
            break
        except PermissionError:
            logging.info(f'Please CLOSE {str(FILEPATH_XLSX)}. Data can\'t be written while file is opened')
            time.sleep(5)
        except:
            raise


async def do_with_retries(func, _args, retries: int, sleep_range: tuple):
    for retry in range(1, retries+1):
        try:
            return await func(*_args)
        except Exception as ex:
            if retry == retries:
                logging.error(f'{ex}\n\n', exc_info=True)
                raise
            elif retry % 10 == 0:
                time.sleep(random.randint(3*60, 6*60))
            else:
                time.sleep(random.randint(*sleep_range))
            logging.warning(ex, exc_info=True)


def sync_collect_data():
    asyncio.run(collect_data())


def looping_collect_data():
    sync_collect_data()

    schedule.every().day.at('20:00').do(sync_collect_data)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    get_logger('scrapper.log')
    looping_collect_data()
