import os
from typing import List
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
    # API Settings
    kaiten_base_url: str
    kaiten_api_token: str
    bitrix_webhook_url: str
    
    # SSH Settings for VPS server (comment dates update) - Optional
    ssh_host: str = ""
    ssh_user: str = "root"
    ssh_key_path: str = ""
    ssh_key_path_putty: str = ""
    vps_script_path: str = "/root/kaiten-vps-scripts/update_comment_dates.py"

    # Migration Settings
    excluded_spaces: List[str] = [
        "Удаленные",
        "ТЕСТ Входящие задачи", 
        "Процесс настройки Kaiten",
        "Изменения и автоматизация",
        "Дирекция по персоналу: Заявления",
        "Дирекция по персоналу: Входящие задачи", 
        "Дирекция по персоналу",
        "Личные задачи, тесты"
    ]

    class Config:
        # Использует .env или env.txt (в зависимости от наличия)
        env_file = env_file
        env_file_encoding = 'utf-8'
        extra = 'ignore'

    def is_space_excluded(self, space_title: str) -> bool:
        """Проверяет, исключено ли пространство из миграции"""
        return space_title.strip() in self.excluded_spaces

    def get_excluded_spaces(self) -> List[str]:
        """Возвращает список исключенных пространств"""
        return self.excluded_spaces.copy()

settings = Settings()
