#!/usr/bin/env python3
"""
Скрипт миграции пространств Kaiten в группы Bitrix24.
НОВАЯ ЛОГИКА: Переносим пространства (НЕ доски!)
1. Конечные пространства (без дочерних) → группы Bitrix24
2. Пространства 2-го уровня (если родитель имеет дочерние) → группы Bitrix24
3. Исключение пространств из списка (см. config/space_exclusions.py)

Примеры использования:
    python scripts/board_migration.py --list-spaces    # Показать список доступных пространств
    python scripts/board_migration.py                  # Все подходящие пространства  
    python scripts/board_migration.py --limit 10       # Первые 10 пространств (для тестирования)
    python scripts/board_migration.py --space-id 123   # Конкретное пространство
"""

import asyncio
import argparse
import sys
from pathlib import Path

# Добавляем корневую папку проекта в sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from migrators.space_migrator import SpaceMigrator
from utils.logger import get_logger

logger = get_logger(__name__)

async def main():
    parser = argparse.ArgumentParser(
        description="Миграция пространств Kaiten в группы Bitrix24",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s --list-spaces      # Показать список доступных пространств
  %(prog)s                    # Мигрировать все подходящие пространства
  %(prog)s --limit 5          # Мигрировать первые 5 пространств (тест)
  %(prog)s --limit 50         # Мигрировать первые 50 пространств
  %(prog)s --space-id 12345   # Мигрировать конкретное пространство

НОВАЯ логика миграции:
  • НЕ переносим доски!
  • Конечные пространства (без дочерних) → группы Bitrix24
  • Пространства 2-го уровня → группы Bitrix24
  • Исключение пространств из config/space_exclusions.py
  • Участники: из пространства

Полезная команда для начала:
  Сначала выполните --list-spaces для просмотра доступных пространств,
  затем выберите нужные для миграции.
        """
    )
    
    parser.add_argument(
        '--limit', 
        type=int, 
        help='Максимальное количество пространств для миграции (по умолчанию: все)'
    )
    
    parser.add_argument(
        '--space-id',
        type=int,
        help='ID конкретного пространства Kaiten для миграции (например: 12345)'
    )
    
    parser.add_argument(
        '--list-spaces',
        action='store_true',
        help='Показать список всех доступных пространств для миграции (без выполнения миграции)'
    )
    
    args = parser.parse_args()
    
    # Проверяем взаимоисключающие параметры
    if args.list_spaces and (args.limit or args.space_id):
        logger.error("❌ Параметр --list-spaces нельзя использовать с --limit или --space-id")
        return 1
    
    # Режим просмотра списка пространств
    if args.list_spaces:
        logger.info("📋 ПРОСМОТР ДОСТУПНЫХ ПРОСТРАНСТВ")
        logger.info("=" * 70)
        
        try:
            migrator = SpaceMigrator()
            success = await migrator.list_available_spaces()
            return 0 if success else 1
        except Exception as e:
            logger.error(f"💥 Критическая ошибка: {e}")
            return 1
    
    # Режим миграции
    logger.info("🚀 ЗАПУСК МИГРАЦИИ ПРОСТРАНСТВ KAITEN -> BITRIX24")
    logger.info("🔄 НОВАЯ ЛОГИКА: Переносим пространства (НЕ доски!)")
    logger.info("=" * 70)
    
    if args.space_id:
        logger.info(f"🎯 Режим: обрабатываем конкретное пространство ID {args.space_id}")
    elif args.limit:
        logger.info(f"🔢 Лимит: обрабатываем первые {args.limit} пространств")
    else:
        logger.info("🔄 Режим: обрабатываем ВСЕ подходящие пространства")
    
    try:
        # Создаем мигратор и запускаем миграцию
        migrator = SpaceMigrator()
        stats = await migrator.migrate_spaces(limit=args.limit, space_id=args.space_id)
        
        # Проверяем результаты
        if stats["errors"] > 0:
            logger.error("❌ Миграция завершена с ошибками")
            return 1
        else:
            logger.success("✅ Миграция завершена успешно!")
            return 0
            
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 