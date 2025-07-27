"""
Скрипт для миграции карточек из Kaiten в задачи Bitrix24.
Поддерживает два режима:
1. --list-only - только просмотр списка карточек без миграции
2. обычный режим - полная миграция карточек
"""

import asyncio
import argparse
import sys
import os
from pathlib import Path

# Добавляем корневую директорию в путь для импортов
sys.path.append(str(Path(__file__).parent.parent))

from migrators.card_migrator import CardMigrator
from utils.logger import get_logger

logger = get_logger(__name__)

async def main():
    """Основная функция скрипта миграции карточек"""
    
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(
        description="Миграция карточек из Kaiten в задачи Bitrix24",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

1. Просмотр списка карточек без миграции:
   python scripts/card_migration.py --space-id 426722 --list-only

2. Полная миграция карточек (группа определяется автоматически):
   python scripts/card_migration.py --space-id 426722

3. Миграция ограниченного количества карточек (первые 5 карточек первой доски):
   python scripts/card_migration.py --space-id 426722 --limit 5

4. Просмотр конкретной карточки:
   python scripts/card_migration.py --space-id 426722 --card-id 50562607 --list-only

5. Миграция конкретной карточки:
   python scripts/card_migration.py --space-id 426722 --card-id 50562607

6. Просмотр первых 10 карточек:
   python scripts/card_migration.py --space-id 426722 --list-only --limit 10

Примечание: Группа Bitrix24 определяется автоматически из файла mappings/space_mapping.json.
Если пространство не найдено в маппинге, сначала выполните: python scripts/space_migration.py --space-id <ID>
        """
    )
    
    parser.add_argument(
        '--space-id', 
        type=int, 
        required=True,
        help='ID пространства Kaiten для миграции карточек'
    )
    
    parser.add_argument(
        '--group-id', 
        type=int, 
        help='ID группы (проекта) в Bitrix24 (опционально, автоматически определяется из space_mapping.json)'
    )
    
    parser.add_argument(
        '--card-id', 
        type=int, 
        help='ID конкретной карточки для обработки (если не указан, обрабатываются все карточки)'
    )
    
    parser.add_argument(
        '--list-only', 
        action='store_true',
        help='Только вывести список карточек без выполнения миграции'
    )
    
    parser.add_argument(
        '--verbose', 
        action='store_true',
        help='Подробный вывод (включая отладочную информацию)'
    )
    
    parser.add_argument(
        '--limit', 
        type=int,
        help='Ограничить количество переносимых карточек (обрабатывается только первая доска)'
    )
    
    args = parser.parse_args()
    
    # Валидация взаимоисключающих параметров
    if args.card_id and args.limit:
        logger.error("❌ Параметры --card-id и --limit взаимоисключающие")
        return 1
    
    # Подробный вывод будет через logger.debug() без изменения уровня
    
    # Выводим информацию о запуске
    if args.card_id:
        mode_text = f"{'ПРОСМОТР' if args.list_only else 'МИГРАЦИЯ'} КОНКРЕТНОЙ КАРТОЧКИ {args.card_id}"
    else:
        mode_text = "ПРОСМОТР СПИСКА КАРТОЧЕК" if args.list_only else "МИГРАЦИЯ КАРТОЧЕК"
        if args.limit:
            mode_text += f" (ЛИМИТ: {args.limit})"
    
    logger.info("=" * 80)
    logger.info(f"🚀 ЗАПУСК: {mode_text}")
    logger.info("=" * 80)
    logger.info(f"Пространство Kaiten: {args.space_id}")
    
    if args.card_id:
        logger.info(f"Карточка Kaiten: {args.card_id}")
    
    if args.group_id:
        logger.info(f"Группа Bitrix24 (ручная): {args.group_id}")
    else:
        logger.info("Группа Bitrix24: будет определена автоматически")
        
    if args.limit:
        logger.info(f"Лимит карточек: {args.limit} (только первая доска)")
    
    if args.list_only:
        logger.info("Режим: только просмотр (без создания задач)")
        logger.info("\n📋 Будут показаны карточки для миграции:")
        logger.info("   - type: 1 (начальные колонки) -> стадия 'Новые'")
        logger.info("   - type: 2 и другие -> стадия 'Выполняются'")
        logger.info("   - type: 3 (финальные) -> ПРОПУСКАЮТСЯ")
    else:
        logger.info("Режим: полная миграция карточек в задачи")
    
    logger.info("=" * 80)
    
    try:
        # Создаем мигратор
        migrator = CardMigrator()
        
        # Определяем ID группы Bitrix24
        if args.group_id:
            # Группа указана вручную
            target_group_id = args.group_id
        elif args.list_only:
            # Для просмотра группа не нужна
            target_group_id = 0
        else:
            # Автоматически определяем группу из маппинга
            target_group_id = await migrator.get_group_id_for_space(args.space_id)
            if not target_group_id:
                logger.error(f"❌ Пространство {args.space_id} не найдено в маппинге")
                logger.error("💡 Сначала выполните миграцию пространства:")
                logger.error(f"   python scripts/space_migration.py --space-id {args.space_id}")
                return 1
            
            logger.info(f"✅ Автоматически определена группа Bitrix24: {target_group_id}")
        
        success = await migrator.migrate_cards_from_space(
            space_id=args.space_id,
            target_group_id=target_group_id,
            list_only=args.list_only,
            limit=args.limit,
            card_id=args.card_id
        )
        
        if success:
            if args.list_only:
                logger.info("\n✅ Просмотр списка карточек завершен успешно")
                logger.info("\nДля запуска реальной миграции используйте команду:")
                logger.info(f"python scripts/card_migration.py --space-id {args.space_id}")
            else:
                logger.info("\n✅ Миграция карточек завершена успешно!")
            
            return 0
        else:
            logger.error("\n❌ Миграция карточек завершилась с ошибками")
            return 1
            
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Миграция прервана пользователем")
        return 1
    except Exception as e:
        logger.error(f"\n❌ Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 