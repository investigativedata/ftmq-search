from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ftmqs_")

    debug: bool = Field(False, alias="debug")

    uri: str = "sqlite:///ftmqs.store"
    yaml_uri: str | None = None
    json_uri: str | None = None
    index_props: list[str] = []
    name_props: list[str] = []

    # sql
    sql_table_name: str = "ftmqs"


class ElasticSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="elastic_")

    index: str = "ftmqs"
    cloud_id: str = ""
    user: str = ""
    password: str = ""
    sniff: bool = False
    query_concurrency: int = 5
