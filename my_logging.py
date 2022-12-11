import logging
import sys


def get_logger(filename):
    logging.basicConfig(
        level=logging.INFO,
        encoding='utf-8',
        format="[{asctime}]:[{levelname}]:{message}",
        style='{',
        handlers=[
            logging.FileHandler(filename, mode='a'),
            logging.StreamHandler(sys.stdout),
        ]
    )
