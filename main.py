import asyncio
import re
import logging
import time
from pathlib import Path

import aiohttp
import schedule
from bs4 import BeautifulSoup
import pandas as pd

from my_logging import get_logger

DIR_PROCEDURES = r'procedures'
FILENAME_XLSX = r'procedures.xlsx'
FILEPATH_XLSX = Path(DIR_PROCEDURES, FILENAME_XLSX)
Path(DIR_PROCEDURES).mkdir(exist_ok=True)

RETRIES = 30


async def collect_data() -> None:
    appended = 0
    async with aiohttp.ClientSession() as s:

        for retry in range(1, RETRIES+1):
            try:
                procedures_urls_to_append = await get_procedures_urls(s)
                break
            except Exception as ex:
                if retry == RETRIES:
                    logging.error(ex, exc_info=True)
                    raise
                logging.warning(ex, exc_info=True)
                time.sleep(10)

        for procedure_url in procedures_urls_to_append:
            for retry in range(1, RETRIES+1):
                try:
                    res = await handle_procedure(s, procedure_url)
                    break
                except Exception as ex:
                    if retry == RETRIES:
                        logging.error(ex, exc_info=True)
                        raise
                    logging.warning(ex, exc_info=True)
                    time.sleep(10)
            appended += 1 if res else 0

        logging.info(f'Collected data of {appended} procedures\n\n')


async def get_procedures_urls(s: aiohttp.ClientSession) -> list:
    procedures_urls_to_append = []
    appended_all_new = False
    last_page_number = None
    procedures_numbers_appended = list(pd.read_excel(str(FILEPATH_XLSX))['Номер процедуры']) \
        if FILEPATH_XLSX.exists() else []
    logging.info('Start collecting procedures urls')

    params = {
        'limit': 100,
        'page': 1,
        'sort': 'datestart'
    }
    while not appended_all_new:
        procedures_urls_to_append_temp = procedures_urls_to_append.copy()
        async with s.get('https://www.tektorg.ru/rosneft/procedures', params=params) as r:
            soup = BeautifulSoup(await r.text(), 'lxml')

        if not last_page_number:
            last_page_number = int(soup.find('ul', class_='pagination').find_all('li', class_='')[-1].text)

        for procedure_div in soup.find_all('div', class_='section-procurement__item'):
            procedure_number = procedure_div.find('div', class_='section-procurement__item-numbers') \
                .find('span').text.strip().replace('Номер закупки на сайте ЭТП:', '') \
                .replace(' ', '').replace('\n', '')
            procedure_href = procedure_div.find('a', class_='section-procurement__item-title').get('href')

            if procedure_number not in procedures_numbers_appended:
                procedures_urls_to_append.append('https://tektorg.ru' + procedure_href)

        if len(procedures_urls_to_append_temp) == len(procedures_urls_to_append) or\
                params['page'] == last_page_number:
            appended_all_new = True

        logging.info(f'Collected urls from page №:{params["page"]}')
        params['page'] += 1

    logging.info(f'Collected {len(procedures_urls_to_append)} new procedures urls')
    return procedures_urls_to_append


async def handle_procedure(s: aiohttp.ClientSession, url: str) -> bool:
    async with s.get(url) as r:
        logging.info(f'Start collecting data for {url}')

        soup = BeautifulSoup(await r.text(), 'lxml')
        if 'Вы не авторизированы для доступа к этой странице' in soup.text:
            logging.info('Unauthorized')
            return False

        procedure_number = soup.find('td', text='Номер закупки:').find_next('td').text
        procedure_name = soup.find('span', class_='procedure__item-name').text

        Path(DIR_PROCEDURES, procedure_number).mkdir(exist_ok=True)
        append_row_to_xlsx(
            Path(DIR_PROCEDURES, FILENAME_XLSX),
            {
                'Номер процедуры': procedure_number,
                'Наименование закупки': procedure_name
            }
        )

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

        for retry in range(1, RETRIES+1):
            tasks_download = [download_file(s, d['url'], d['path']) for d in files_data]
            results = await asyncio.gather(*tasks_download, return_exceptions=True)

            for i in reversed(range(len(results))):
                if results[i] is True:
                    del files_data[i]

            if not files_data:
                break
            time.sleep(10)
            logging.warning(f'Retry №:{retry} while downloading files. Tasks count: {len(tasks_download)}')
        else:
            logging.error('ERROR while downloading files')
            raise ConnectionError('ERROR while downloading files')
    return True


async def download_file(s: aiohttp.ClientSession, url: str, filepath: Path) -> bool:
    logging.info(f'Downloading "{filepath.name}" from {url}')
    async with s.get(url) as r:
        with open(str(filepath), 'wb') as f:
            f.write(await r.read())
            return True


def append_row_to_xlsx(filepath: Path, row: dict) -> None:
    if not filepath.exists():
        pd.DataFrame().to_excel(str(filepath), index=False)

    df = pd.read_excel(str(filepath))
    df = pd.concat([df, pd.DataFrame([row])])
    df.to_excel(str(filepath), index=False)


def sync_collect_data():
    asyncio.run(collect_data())


def looping_collect_data():
    sync_collect_data()

    schedule.every().day.at('13:00').do(sync_collect_data)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    get_logger('scrapper.log')
    looping_collect_data()
