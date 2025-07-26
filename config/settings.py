import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from pathlib import Path

# Загружаем переменные из .env файла (или env.txt для тестирования)
env_file = ".env" if Path(".env").exists() else "env.txt"
load_dotenv(dotenv_path=env_file)

class Settings(BaseSettings):
    """
    Настройки приложения, загружаемые из переменных окружения.
    Pydantic-settings автоматически считывает переменные из .env файла.
    """
    kaiten_base_url: str
    kaiten_api_token: str

    bitrix_webhook_url: str
    
    # SSH Settings for VPS server (comment dates update) - Optional
    ssh_host: str = ""
    ssh_user: str = "root"
    ssh_key_path: str = ""
    ssh_key_path_putty: str = ""
    vps_script_path: str = "/root/kaiten-vps-scripts/update_comment_dates.py"

    class Config:
        # Использует .env или env.txt (в зависимости от наличия)
        env_file = env_file
        env_file_encoding = 'utf-8'
        extra = 'ignore'

settings = Settings()
