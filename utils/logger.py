import sys
from loguru import logger

def get_logger(name: str):
    """
    Получить логгер с предустановленными настройками
    """
    logger.remove()
    # Консоль - только важная информация (INFO и выше)
    logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level} | {message}")
    # Файл - подробные логи (DEBUG и выше)
    logger.add("logs/app.log", rotation="5 MB", level="DEBUG", format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{function}:{line} - {message}")
    return logger
