from anystore.settings import Settings as AnySettings
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ftmqs_")

    uri: str = "sqlite:///ftmqs.store"
    yaml_uri: str | None = None
    json_uri: str | None = None

    # sql
    sql_table_name: str = "ftmqs"


class ElasticSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="elastic_")

    index: str = "ftmqs"
    user: str = ""
    password: str = ""


DEBUG = AnySettings().debug
