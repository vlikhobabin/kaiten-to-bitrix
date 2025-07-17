import sys
from loguru import logger

def get_logger(name: str):
    """
    Возвращает настроенный экземпляр логгера.
    """
    logger.remove()
    logger.add(
        sys.stderr,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO",
        colorize=True
    )
    logger.add(
        "logs/app.log",
        rotation="10 MB",
        retention="10 days",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        encoding="utf-8"
    )
    return logger.bind(name=name)
