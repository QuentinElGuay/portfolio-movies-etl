import logging
import os
import sys

import movie_etl.pipeline as pipeline


def main():

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    logger = logging.getLogger('movie_etl')
    logger.setLevel(os.getenv('LOG_LEVEL', 'INFO').upper())
    logger.addHandler(handler)
    logger.propagate = False

    pipeline.run()


if __name__ == '__main__':
    main()
