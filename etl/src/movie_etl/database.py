from dataclasses import dataclass
from importlib.resources import files
import logging

from pandas import DataFrame
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from movie_etl.config import Settings

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

    def create_movie_table(self) -> None:
        """
        Create the `movie` table or truncate it.
        """
        schema = files('movie_etl.sql.ddl').joinpath('movie.sql').read_text()

        with self.engine.begin() as connection:
            connection.execute(text(schema))

        logger.info('Table "movie" initialized with success')

    def load_movies(self, df: DataFrame):
        """
        Load the DataFrame into the `movie` table using an UPSERT operation to ensure idempotency.
        """
        metadata = MetaData()
        movie = Table('movie', metadata, autoload_with=self.engine)

        rows = df.to_dict(orient='records')

        stmt = insert(movie).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_={
                column.name: getattr(stmt.excluded, column.name)
                for column in movie.columns
                if column.name != 'id'
            },
        )

        with self.engine.begin() as conn:
            try:
                result = conn.execute(stmt)
                logger.info(
                    'Upserted %d row(s) into the "movie" table.', result.rowcount
                )

            except SQLAlchemyError as error:
                # 1. Log only the core database error message (hides the raw rows)
                logger.error('Error loading data: %s', getattr(error, 'orig', error))

                # 2. Raise a clean exception without the massive DataFrame dump
                raise RuntimeError(
                    'Database upsert failed due to a constraint or type error.'
                ) from None

    def count_movies(self) -> int:
        """
        Count the number of line in the `movie` table.
        """
        with self.engine.connect() as connection:
            count_query = text('SELECT COUNT(*) FROM movie;')
            result = connection.execute(count_query)
            total_lines = result.scalar_one()

        return total_lines
