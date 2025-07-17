import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Ищем .env файл в корне проекта и загружаем его
# Это стандартная практика для python-dotenv
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
        # Pydantic-settings по умолчанию ищет .env файл
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = 'ignore'

settings = Settings()
