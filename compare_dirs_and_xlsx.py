from pathlib import Path

import pandas

from main import FILEPATH_XLSX, DIR_PROCEDURES

procedures_xlsx = set(pandas.read_excel(FILEPATH_XLSX)['Номер'])
procedures_dirs = {filepath.stem for filepath in Path(DIR_PROCEDURES).iterdir() if filepath.suffix != '.xlsx'}
not_in_xlsx = procedures_dirs.difference(procedures_xlsx)
not_in_dirs = procedures_xlsx.difference(procedures_dirs)
print(f'Список номеров, одноименные папки которых существуют, но не находятся в xlsx файле\n{not_in_xlsx=}\n')
print(f'Список номеров, которые находятся в xlsx файле, но папки с таким названием не существуют\n{not_in_dirs=}\n')
