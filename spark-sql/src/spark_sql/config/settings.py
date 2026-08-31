from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_warehouse_id: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


settings = Settings()
