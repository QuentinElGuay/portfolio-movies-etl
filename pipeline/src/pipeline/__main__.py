import logging
import os
import sys

from pipeline import job


def main():

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    logger = logging.getLogger('pipeline')
    logger.setLevel(os.getenv('LOG_LEVEL', 'INFO').upper())
    logger.addHandler(handler)
    logger.propagate = False

    job.run()


if __name__ == '__main__':
    main()
