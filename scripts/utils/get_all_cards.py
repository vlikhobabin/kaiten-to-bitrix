"""
Скрипт для получения всех карточек из Kaiten с возможностью фильтрации.
"""

import asyncio
import argparse
import sys
from pathlib import Path

# Добавляем корневую директорию в путь для импортов
sys.path.append(str(Path(__file__).parent.parent))

from connectors.kaiten_client import KaitenClient
from utils.logger import get_logger

logger = get_logger(__name__)

async def get_space_hierarchy(client, target_space_id: int):
    """Получает все пространства в иерархии (родительское + дочерние)"""
    spaces = await client.get_spaces()
    
    target_space = None
    child_spaces = []
    
    # Ищем целевое пространство
    for space in spaces:
        if space.id == target_space_id:
            target_space = space
            break
    
    if not target_space:
        return []
    
    # Ищем дочерние пространства
    for space in spaces:
        if space.parent_entity_uid == target_space.uid:
            child_spaces.append(space)
    
    all_spaces = [target_space] + child_spaces
    logger.info(f"🏗️ Найдено {len(all_spaces)} пространств в иерархии:")
    for space in all_spaces:
        logger.info(f"   - {space.title} (ID: {space.id})")
    
    return all_spaces

def should_migrate_card(card: dict) -> bool:
    """
    Проверяет, нужно ли переносить карточку согласно правилам CardMigrator.
    
    Args:
        card: Карточка Kaiten (dict)
        
    Returns:
        True если карточку нужно переносить, False иначе
    """
    # Фильтр архивных карточек
    if card.get('archived', False):
        return False
        
    # Фильтр по типу колонки
    column = card.get('column', {})
    column_type = column.get('type')
    
    if column_type == 3:  # Финальная колонка - не переносим
        return False
    
    return True

async def get_all_cards(space_filter: int = 0, limit: int = 200, migration_only: bool = False):
    """
    Получает все карточки из Kaiten с возможностью фильтрации по пространству.
    
    Args:
        space_filter: ID пространства для фильтрации (опционально)
        limit: Максимальное количество карточек для получения
        migration_only: Если True, показывает только карточки для миграции
    """
    
    client = KaitenClient()
    
    try:
        if space_filter:
            logger.info(f"📥 Получение карточек из пространства {space_filter} и его дочерних...")
            
            # Получаем иерархию пространств
            hierarchy_spaces = await get_space_hierarchy(client, space_filter)
            
            if not hierarchy_spaces:
                logger.warning(f"⚠️ Пространство {space_filter} не найдено")
                return
            
            # Получаем карточки напрямую через space_id
            all_cards = []
            total_boards = 0
            
            for space in hierarchy_spaces:
                logger.info(f"📋 Обработка пространства '{space.title}'...")
                
                # Получаем карточки пространства напрямую
                try:
                    space_cards_data = await client._request('GET', f'/api/v1/cards?space_id={space.id}')
                    if space_cards_data:
                        all_cards.extend(space_cards_data)
                        logger.info(f"   🃏 Получено {len(space_cards_data)} карточек из пространства")
                    else:
                        logger.info(f"   📭 Пространство не содержит карточек")
                except Exception as e:
                    logger.warning(f"   ❌ Ошибка получения карточек пространства: {e}")
                
                # Дополнительно получаем доски и пробуем через board_id
                try:
                    boards = await client.get_boards(space.id)
                    total_boards += len(boards)
                    
                    if boards:
                        logger.info(f"   📊 Найдено {len(boards)} досок, пробуем получить карточки через board_id...")
                        
                        for board in boards:
                            try:
                                board_cards_data = await client._request('GET', f'/api/v1/cards?board_id={board.id}')
                                if board_cards_data:
                                    # Добавляем только новые карточки (избегаем дубликатов)
                                    existing_ids = {card.get('id') for card in all_cards}
                                    new_cards = [card for card in board_cards_data if card.get('id') not in existing_ids]
                                    
                                    if new_cards:
                                        all_cards.extend(new_cards)
                                        logger.info(f"      🃏 Доска '{board.title}': +{len(new_cards)} новых карточек")
                                    else:
                                        logger.debug(f"      📭 Доска '{board.title}': дубликаты или нет карточек")
                                else:
                                    logger.debug(f"      📭 Доска '{board.title}': карточек нет")
                            except Exception as e:
                                logger.debug(f"      ❌ Доска '{board.title}': ошибка {e}")
                                
                except Exception as e:
                    logger.warning(f"   ❌ Ошибка получения досок: {e}")
                
                logger.info(f"   ✅ Пространство '{space.title}': обработано")
            
            # Убираем дубликаты по ID
            unique_cards = {}
            for card in all_cards:
                card_id = card.get('id')
                if card_id and card_id not in unique_cards:
                    unique_cards[card_id] = card
            
            all_cards = list(unique_cards.values())
            logger.info(f"📊 Всего обработано {total_boards} досок, получено {len(all_cards)} уникальных карточек")
            filtered_cards = all_cards
            
        else:
            logger.info(f"📥 Получение карточек из всей системы (лимит: {limit})...")
            
            # Получаем все карточки через общий API
            all_cards = await client._request('GET', f'/api/v1/cards?limit={limit}')
            
            if not all_cards:
                logger.warning("⚠️ Карточки не найдены")
                return
            
            logger.info(f"📊 Получено {len(all_cards)} карточек")
            filtered_cards = all_cards
        
        if not filtered_cards:
            logger.warning(f"⚠️ Карточки не найдены")
            return
        
        # Применяем фильтр миграции если нужно
        if migration_only:
            migration_cards = [card for card in filtered_cards if should_migrate_card(card)]
            filtered_cards = migration_cards
            
            if not filtered_cards:
                logger.warning(f"⚠️ Карточек для миграции не найдено")
                return
                
            logger.info(f"🎯 Отфильтровано {len(filtered_cards)} карточек для миграции")
        
        # Анализируем и выводим карточки
        print("\n" + "="*80)
        if migration_only:
            print("🎯 КАРТОЧКИ ДЛЯ МИГРАЦИИ")
        else:
            print("📄 СПИСОК КАРТОЧЕК")
        print("="*80)
        
        # Группируем по типам колонок
        type_stats = {}
        migration_count = 0
        displayed_count = 0
        
        for i, card in enumerate(filtered_cards):
            column = card.get('column', {})
            column_type = column.get('type', 'unknown')
            archived = card.get('archived', False)
            
            # Статистика
            type_stats[column_type] = type_stats.get(column_type, 0) + 1
            
            # Определяем целевую стадию и статус миграции
            will_be_migrated = should_migrate_card(card)
            
            if column_type == 1:
                target_stage = "Новые"
                migrate_status = "✅ Будет мигрирована" if will_be_migrated else "🚫 Пропускается (архивная)"
            elif column_type == 3:
                target_stage = "НЕ ПЕРЕНОСИТСЯ"
                migrate_status = "🚫 Пропускается (финальная)"
            else:
                target_stage = "Выполняются"
                migrate_status = "✅ Будет мигрирована" if will_be_migrated else "🚫 Пропускается (архивная)"
            
            if will_be_migrated:
                migration_count += 1
            
            # В режиме migration_only показываем только карточки для миграции
            if migration_only and not will_be_migrated:
                continue
                
            displayed_count += 1
            
            # Информация о доске и пространстве
            board = card.get('board', {})
            board_title = board.get('title', 'Неизвестная доска')
            board_space_id = board.get('space_id', 'unknown')
            
            # Владелец
            owner = card.get('owner', {})
            owner_name = owner.get('full_name', 'Неизвестный')
            
            print(f"{displayed_count:3d}. ID: {card.get('id'):>8} | {migrate_status}")
            print(f"     Title: {card.get('title', 'Без названия')[:70]}")
            print(f"     Board: {board_title} (Space: {board_space_id})")
            print(f"     Owner: {owner_name}")
            print(f"     Column type: {column_type} -> {target_stage}")
            if not migration_only:  # Показываем статус архива только в полном режиме
                print(f"     Archived: {archived}")
            print()
        
        # Статистика
        print("="*80)
        if migration_only:
            print("📊 СТАТИСТИКА КАРТОЧЕК ДЛЯ МИГРАЦИИ")
        else:
            print("📊 СТАТИСТИКА ПО ТИПАМ КОЛОНОК")
        print("="*80)
        
        if migration_only:
            # В режиме миграции показываем только карточки для переноса
            type_1_count = type_stats.get(1, 0)
            type_2_count = sum(type_stats.get(t, 0) for t in type_stats.keys() if t not in [1, 3, 'unknown'])
            type_unknown_count = type_stats.get('unknown', 0)
            
            if type_1_count > 0:
                print(f"type: 1 (начальные) -> Новые: {type_1_count} карточек")
            if type_2_count > 0:
                print(f"type: 2+ (остальные) -> Выполняются: {type_2_count} карточек")
            if type_unknown_count > 0:
                print(f"type: unknown -> Выполняются: {type_unknown_count} карточек")
            
            print(f"\n🎯 Всего карточек для миграции: {displayed_count}")
        else:
            # В полном режиме показываем всю статистику
            for col_type, count in sorted(type_stats.items()):
                if col_type == 1:
                    stage_name = "type: 1 (начальные) -> Новые"
                elif col_type == 3:
                    stage_name = "type: 3 (финальные) -> НЕ ПЕРЕНОСЯТСЯ"
                else:
                    stage_name = f"type: {col_type} (остальные) -> Выполняются"
                
                print(f"{stage_name}: {count} карточек")
            
            print(f"\n🎯 Карточек для миграции (не архивные, не type:3): {migration_count}")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Ошибка получения карточек: {e}")

async def main():
    parser = argparse.ArgumentParser(
        description="Получение всех карточек из Kaiten с фильтрацией",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

1. Все карточки системы:
   python scripts/get_all_cards.py

2. Карточки конкретного пространства:
   python scripts/get_all_cards.py --space-id 426722

3. Только карточки для миграции:
   python scripts/get_all_cards.py --space-id 426722 --migration-only

4. Увеличить лимит карточек:
   python scripts/get_all_cards.py --space-id 426722 --limit 500

5. Комбинация опций:
   python scripts/get_all_cards.py --space-id 426722 --migration-only --limit 300
        """
    )
    
    parser.add_argument(
        '--space-id',
        type=int,
        help='ID пространства для фильтрации карточек'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=200,
        help='Максимальное количество карточек для получения (по умолчанию: 200)'
    )
    
    parser.add_argument(
        '--migration-only',
        action='store_true',
        help='Показать только карточки, которые подлежат миграции (не архивные, не type:3)'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    if args.space_id:
        if args.migration_only:
            logger.info(f"🎯 ПОИСК КАРТОЧЕК ДЛЯ МИГРАЦИИ В ПРОСТРАНСТВЕ {args.space_id}")
        else:
            logger.info(f"🔍 ПОИСК КАРТОЧЕК В ПРОСТРАНСТВЕ {args.space_id}")
    else:
        if args.migration_only:
            logger.info("🎯 ПОИСК КАРТОЧЕК ДЛЯ МИГРАЦИИ В СИСТЕМЕ")
        else:
            logger.info("🔍 ПОИСК ВСЕХ КАРТОЧЕК В СИСТЕМЕ")
    logger.info("=" * 80)
    
    await get_all_cards(space_filter=args.space_id, limit=args.limit, migration_only=args.migration_only)

if __name__ == "__main__":
    asyncio.run(main()) 