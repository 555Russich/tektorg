import logging
import sys


def get_logger(filename):
    logging.basicConfig(
        level=logging.INFO,
        format="[{asctime}]:[{levelname}]:{message}",
        style='{',
        handlers=[
            logging.FileHandler(filename, mode='a'),
            logging.StreamHandler(sys.stdout),
        ]
    )
