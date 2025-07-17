#!/usr/bin/env python3
"""
Скрипт для миграции пространств из Kaiten в рабочие группы Bitrix24.
Обрабатывает первые 5 пространств для тестирования.
"""

import asyncio
import sys
from pathlib import Path

# Добавляем корневую директорию в путь для импорта модулей
sys.path.append(str(Path(__file__).parent.parent))

from migrators.space_migrator import SpaceMigrator
from utils.logger import logger

async def main():
    """Запуск миграции пространств"""
    
    logger.info("🚀 ЗАПУСК МИГРАЦИИ ПРОСТРАНСТВ KAITEN -> BITRIX24")
    logger.info("=" * 70)
    
    try:
        # Создаем мигратор
        migrator = SpaceMigrator()
        
        # Запускаем миграцию с ограничением в 5 пространств
        logger.info("🔢 Ограничение: обрабатываем первые 5 пространств")
        result = await migrator.migrate_spaces(limit=5)
        
        # Проверяем результат
        if "error" in result:
            logger.error(f"❌ Ошибка миграции: {result['error']}")
            return False
        
        # Выводим краткую сводку
        logger.info("=" * 70)
        logger.info("📋 КРАТКАЯ СВОДКА:")
        logger.info(f"  ✅ Обработано: {result.get('processed', 0)}")
        logger.info(f"  ➕ Создано: {result.get('created', 0)}")
        logger.info(f"  🔄 Обновлено: {result.get('updated', 0)}")
        logger.info(f"  👥 Участников добавлено: {result.get('members_added', 0)}")
        logger.info(f"  ❌ Ошибок: {result.get('errors', 0)}")
        logger.info("=" * 70)
        
        # Определяем успешность
        total = result.get('processed', 0)
        success = result.get('created', 0) + result.get('updated', 0)
        
        if total > 0 and success == total:
            logger.info("🎉 Миграция пространств завершена успешно!")
        elif total > 0 and success > 0:
            logger.info("⚠️ Миграция завершена с частичным успехом")
        else:
            logger.error("❌ Миграция завершена с ошибками")
        
        return True
        
    except Exception as e:
        logger.error(f"💥 Критическая ошибка при миграции пространств: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    if not success:
        sys.exit(1) 