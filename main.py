import asyncio
import random
import re
import logging
import time
from pathlib import Path
import platform
from typing import Callable
from datetime import datetime

from aiohttp import ClientSession, ClientResponse
import schedule
import pandas as pd

from my_logging import get_logger

DOMAIN = 'https://www.tektorg.ru'

LOGFILE = 'tektorg.log'
DIR_PROCEDURES = r'D:\procedures' if platform.system() == 'Windows' else 'procedures'
FILENAME_XLSX = r'procedures.xlsx'
FILEPATH_XLSX = Path(DIR_PROCEDURES, FILENAME_XLSX)
Path(DIR_PROCEDURES).mkdir(exist_ok=True)

RETRIES = 31

ua_platform = '(X11; Ubuntu; Linux x86_64; rv:108.0)' if platform.system() == 'Linux'\
    else '(Windows NT 10.0; Win64; x64; rv:108.0)'
headers = {
    'User-Agent': f'Mozilla/5.0 {ua_platform} Gecko/20100101 Firefox/108.0',
    'Accept': 'application/json, */*',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
}

TEMP_URL_PART = ''  #  https://www.tektorg.ru/_next/data/{TEMP_URL_PART}/ru/rosneft/procedures/{id}.json
SECTIONS = (
    'rosneft',
    'rosnefttkp',
)


def do_with_retries(func: Callable,
                    retries: int = RETRIES,
                    sleep_range: tuple = (30, 60),
                    ) -> Callable:
    async def wrapper(*args, **kwargs):
        for retry in range(1, retries+1):
            try:
                return await func(*args, **kwargs)
            except Exception as ex:
                logging.warning(ex, exc_info=True)
                if retry == retries:
                    logging.error(f'{ex}\n\n', exc_info=True)
                    raise
                elif retry % 10 == 0:
                    time.sleep(random.randint(1*60, 3*60))
                else:
                    time.sleep(random.randint(*sleep_range))
    return wrapper


async def session_request(method: Callable,
                          url: str,
                          response_type: str,
                          json_data: dict = None
                          ) -> dict | str | int | bool:
    global TEMP_URL_PART

    async with method(url,
                      json=json_data
                      ) as r:
        match r.status:
            case 200:
                match response_type:
                    case 'json':
                        return await r.json()
                    case 'html':
                        return await r.text()
            case 404:
                return 404
            case _:
                raise ConnectionError(f'{r.status=}')


async def get_url_temp_part() -> bool:
    global TEMP_URL_PART

    url = f'{DOMAIN}/{SECTIONS[0]}/procedures'
    async with ClientSession(headers=headers) as s:
        r = await session_request(s.get, url, 'html')
        return re.search(r'(?<=buildId\":\").+?(?=\")', r).group(0)


async def collect_data() -> None:
    async with ClientSession(headers=headers) as s:
        for section in SECTIONS:
            appended = 0
            procedures_ids = await get_procedures_urls(s, section)

            for procedure_id in procedures_ids:
                res = await handle_procedure(s, procedure_id, section)
                appended += 1 if res else 0
            logging.info(f'Collected data of {appended} procedures for {section}\n\n')


@do_with_retries
async def get_procedures_urls(s: ClientSession, section: str) -> list:
    url = DOMAIN + '/api/getProcedures'
    appended_all_new = False
    procedures_ids = []
    procedures_numbers_appended = list(pd.read_excel(str(FILEPATH_XLSX))['Номер']) \
        if FILEPATH_XLSX.exists() else []
    logging.info(f'Start collecting {section=}.'
                 f' Procedures already appended (all): {len(procedures_numbers_appended)}')

    json_data = {
        'params': {
            'sectionsCodes[0]': section,
            'page': 1,
            'sort': 'datePublished_desc',
            'limit': 100
        },
    }
    while not appended_all_new:
        procedures_ids_temp = procedures_ids.copy()
        r = await session_request(s.post, url, 'json', json_data)

        for d in r['data']:
            if d['registryNumber'] not in procedures_numbers_appended:
                procedures_ids.append(d["id"])

        if len(procedures_ids_temp) == len(procedures_ids) or \
                json_data['params']['page'] == r['totalPages']:
            appended_all_new = True

        logging.info(f'Page {json_data["params"]["page"]}/{r["totalPages"]}.'
                     f' Collected ids {len(procedures_ids)}')
        json_data["params"]['page'] += 1

    logging.info(f'Collected {len(procedures_ids)} new procedures ids')
    return procedures_ids


@do_with_retries
async def handle_procedure(s: ClientSession, id_: int, section: str) -> bool:
    global TEMP_URL_PART

    url = f'{DOMAIN}/_next/data/{TEMP_URL_PART}/ru/{section}/procedures/{id_}.json'
    logging.info(f'Start collecting data for {url}')

    if (r := await session_request(s.get, url, 'json')) == 404:
        logging.info(f'Code=404. Going to check TEMP_URL_PART')
        if (temp := await get_url_temp_part()) == TEMP_URL_PART:
            logging.info(f'Page was not found. Probably time expired. {url}')
            return False
        else:
            TEMP_URL_PART = temp
            logging.info(f'Changed {TEMP_URL_PART=}. Do request again')
            return await handle_procedure(s, id_, section)

    r: dict = r['pageProps']['procedureItem']
    procedure_data = {
        'Наименование закупки': r['title'],
        'Номер': r['registryNumber'],
        'Способ закупки': r['typeName'],
        'Текущая стадия': r['statusName'],
        'Дата публикации процедуры': r['dates'].get('datePublished'),
        'Дата окончания срока подачи технико-коммерческих частей': r['dates'].get('dateEndRegistration'),
        'Подведение итогов не позднее': r['dates'].get('dateEndSecondPartsReview'),
        'Наименование организатора': r.get('organizerName'),
        'Контактный телефон': r.get('contactPhone'),
        'Адрес электронной почты': r.get('contactEmail'),
        'ФИО контактного лица': r.get('contactPerson'),
        'Дата окончания срока подачи технических частей': r['dates'].get('dateRegistrationTech'),
        'Дата начала срока подачи коммерческих частей': r['dates'].get('dateStartRegistrationCom'),
        'Дата окончания срока подачи коммерческих частей': r['dates'].get('dateEndRegistrationCom'),
        'Дата и время окончания срока приема квалификационных частей': None,
    }

    for k, v in procedure_data.items():
        if re.search(r'Дата|Подведение итогов не позднее', k) and v:
            procedure_data[k] = datetime.fromisoformat(procedure_data[k]).strftime('%d-%m-%Y %H:%M:%S')

    Path(DIR_PROCEDURES, procedure_data['Номер']).mkdir(exist_ok=True)
    files_data = []
    for doc_data in r['documents']:
        filename = re.sub(r'[<>:"/\\|?*]', '', doc_data['filename'])
        filepath = Path(DIR_PROCEDURES, procedure_data['Номер'], filename)
        if filepath.exists():
            logging.warning(f'Path "{str(filepath)}" already exists')
        files_data.append({'url': doc_data['httpLink'], 'path': filepath})

    for d in files_data:
        await download_file(s, d['url'], d['path'])

    append_row_to_xlsx(Path(DIR_PROCEDURES, FILENAME_XLSX), procedure_data)
    return True


@do_with_retries
async def download_file(s: ClientSession, url: str, filepath: Path) -> bool:
    logging.info(f'Downloading "{filepath.name}" from {url}')
    async with s.get(url, timeout=60*20) as r:
        with open(str(filepath), 'wb') as f:
            f.write(await r.read())
            return True


def append_row_to_xlsx(filepath: Path, row: dict) -> None:
    interrupted = False
    while True:
        try:
            if not filepath.exists():
                pd.DataFrame().to_excel(str(filepath), index=False)

            df = pd.read_excel(str(filepath))
            df = pd.concat([df, pd.DataFrame([row])])
            df.to_excel(str(filepath), index=False)
            df.to_excel(str(
                Path(FILEPATH_XLSX.parent, f'{FILEPATH_XLSX.stem}_copy')
                .with_suffix(FILEPATH_XLSX.suffix)
            ), index=False)
            if interrupted:
                logging.info('File was successfully written after keyboard interrupt')
                exit()
            break
        except PermissionError:
            logging.info(f'Please CLOSE {str(FILEPATH_XLSX)}. Data can\'t be written while file is opened')
            time.sleep(5)
        except KeyboardInterrupt:
            logging.info(f'Interrupted by keyboard. Trying write file again')
            interrupted = True
        except:
            raise


def sync_collect_data():
    asyncio.run(collect_data())


def looping_collect_data():
    sync_collect_data()

    schedule.every().day.at('20:00').do(sync_collect_data)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    get_logger(LOGFILE)
    looping_collect_data()
