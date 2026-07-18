from dataclasses import dataclass
from importlib.resources import files
import logging

from pandas import DataFrame, notna
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from pipeline.config import Settings

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

    def create_table(self, table_name: str) -> None:
        """
        Create table from DDL file
        """
        schema = files('pipeline.sql.ddl').joinpath(f'{table_name}.sql').read_text()

        with self.engine.begin() as connection:
            connection.execute(text(schema))

        logger.info('Table "%s" initialized', table_name)

    def upsert(self, table_name: str, df: DataFrame):
        """
        Load the DataFrame into a table using an UPSERT operation to ensure idempotency.
        """
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine)

        df = df.where(notna(df), None)
        rows = df.to_dict(orient='records')
        stmt = insert(table).values(rows)

        # Handle conflicts
        conflict_columns = [column.name for column in table.primary_key.columns]

        update_columns = {
            column.name: getattr(stmt.excluded, column.name)
            for column in table.columns
            if column.name not in conflict_columns
        }

        if update_columns:
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_={
                    column.name: getattr(stmt.excluded, column.name)
                    for column in table.columns
                    if column.name not in conflict_columns
                },
            )
        else:
            stmt = stmt.on_conflict_do_nothing(
                index_elements=conflict_columns,
            )

        with self.engine.begin() as conn:
            try:
                result = conn.execute(stmt)
                logger.info(
                    'Upserted %d row(s) into the "%s" table.',
                    result.rowcount,
                    table_name,
                )

            except SQLAlchemyError as error:
                orig = getattr(error, 'orig', None)

                logger.error(
                    "Failed to upsert table '%s' (%d rows): %s",
                    table_name,
                    len(df),
                    getattr(error, 'orig', error),
                )

                if orig is not None:
                    logger.error('Database error: %s', orig)

                    diag = getattr(orig, 'diag', None)
                    if diag:
                        logger.error('Detail: %s', diag.message_detail)
                        logger.error('Hint: %s', diag.message_hint)
                        logger.error('Context: %s', diag.context)

                logger.debug('Columns:\n%s', df.dtypes)

                logger.debug(
                    'Sample rows:\n%s',
                    df.head(3).to_string(index=False),
                )

                raise RuntimeError(
                    f"Database upsert failed for table '{table_name}'."
                ) from error

    def count_movies(self) -> int:
        """
        Count the number of line in the `movie` table.
        """
        with self.engine.connect() as connection:
            count_query = text('SELECT COUNT(*) FROM movie;')
            result = connection.execute(count_query)
            total_lines = result.scalar_one()

        return total_lines
