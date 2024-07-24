from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ftmqs_")

    uri: str = "sqlite:///ftmqs.store"
    yaml_uri: str | None = None
    json_uri: str | None = None
    display_props: list[str] = []
    index_props: list[str] = []
    name_props: list[str] = []

    debug: bool = False
