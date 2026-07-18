from dataclasses import dataclass
import os
from typing import Self


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    api_username: str
    api_password: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: str
    postgres_db: str

    @classmethod
    def from_env(cls) -> Self:

        from dotenv import load_dotenv

        load_dotenv()

        return cls(
            os.environ['API_BASE_URL'],
            os.environ['API_USERNAME'],
            os.environ['API_PASSWORD'],
            os.environ['POSTGRES_USER'],
            os.environ['POSTGRES_PASSWORD'],
            os.environ['POSTGRES_HOST'],
            os.environ['POSTGRES_PORT'],
            os.environ['POSTGRES_DB'],
        )
