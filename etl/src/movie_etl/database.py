from dataclasses import dataclass
from importlib.resources import files
import logging

from pandas import DataFrame
from sqlalchemy import create_engine, text

from etl.src.movie_etl.config import Settings

logger = logging.getLogger(__name__)


@dataclass
class PostgresConnection:
    """
    Information required to connect to a Postgres instances
    """

    user: str
    password: str
    database: str
    host: str
    port: str

    @property
    def url(self) -> str:
        return (
            'postgresql+psycopg2://'
            f'{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        )


class Database:
    """
    Class representing a database instance
    """

    def __init__(self, settings: Settings):

        try:
            self.engine = create_engine(
                PostgresConnection(
                    settings.postgres_user,
                    settings.postgres_password,
                    settings.postgres_db,
                    settings.postgres_host,
                    settings.postgres_port,
                ).url
            )
            logger.debug('SQLAlchemy: Successful connection to Postgres')

        except Exception as error:
            logger.error('SQLAlchemy: Error configuring the engine: %s', error)
            raise

    def create_movie_table(self):
        """
        Create the `movie` table if required
        """
        schema = files('movie_etl.sql.ddl').joinpath('movie.sql').read_text()

        with self.engine.begin() as connection:
            connection.execute(text(schema))

            connection.execute(text('TRUNCATE TABLE movie;'))

        logger.info('Table "movie" initialized with success')

    def load_movies(self, df: DataFrame):
        try:
            df.to_sql(name='movie', con=self.engine, if_exists='append', index=False)
            logger.info('DataFrame successfully loaded into the movie table')

        except Exception as error:
            logger.error('Error loading data: %s', error)
            raise

    def count_movies(self) -> None:
        with self.engine.connect() as connection:
            count_query = text('SELECT COUNT(*) FROM movie;')
            result = connection.execute(count_query)
            total_lines = result.scalar()

        logger.debug('The "movies" table currently contains %s line(s)', total_lines)
