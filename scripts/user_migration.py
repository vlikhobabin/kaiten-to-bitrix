#!/usr/bin/env python3
"""
Скрипт для миграции пользователей из Kaiten в Bitrix24.
Использует унифицированную архитектуру с классом UserMigrator.

Примеры использования:
    python scripts/user_migration.py
"""

import asyncio
import sys
from pathlib import Path

# Добавляем корневую директорию в путь для импорта модулей
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from migrators.user_migrator import UserMigrator
from utils.logger import get_logger

logger = get_logger(__name__)


async def main():
    """Основная функция запуска миграции пользователей"""
    
    print("🚀 МИГРАЦИЯ ПОЛЬЗОВАТЕЛЕЙ KAITEN -> BITRIX24")
    print("=" * 70)
    print("🔄 Полная миграция пользователей с сохранением маппинга ID")
    print()
    
    try:
        # Создаем мигратор и запускаем миграцию
        logger.info("🔗 Инициализация мигратора пользователей...")
        migrator = UserMigrator()
        
        # Запускаем миграцию
        result = await migrator.migrate_users()
        
        # Проверяем результаты
        if result["success"]:
            logger.success("✅ Миграция пользователей завершена успешно!")
            return 0
        else:
            logger.error(f"❌ Миграция завершена с ошибкой: {result.get('error', 'Unknown error')}")
            return 1
            
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 