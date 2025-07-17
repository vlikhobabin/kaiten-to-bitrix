import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Загружаем переменные из стандартного .env файла
load_dotenv()

class Settings(BaseSettings):
    """
    Настройки приложения, загружаемые из переменных окружения.
    Pydantic-settings автоматически считывает переменные из .env файла.
    """
    kaiten_base_url: str
    kaiten_api_token: str

    bitrix_webhook_url: str

    class Config:
        # Стандартная практика - использование .env файла
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = 'ignore'

settings = Settings()
