#!/usr/bin/env python3
"""
Скрипт для удаления групп из Bitrix24.
Поддерживает два режима:
1. Удаление групп созданных по старой логике (1 Space = 1 Group) из маппинга
2. Удаление конкретной группы по её ID

Примеры использования:
    python scripts/delete_space_groups_final.py                # Удалить все группы из space_mapping
    python scripts/delete_space_groups_final.py --group-id 145 # Удалить конкретную группу
"""

import asyncio
import sys
import os
import json
import argparse
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.bitrix_client import BitrixClient
from utils.logger import get_logger

logger = get_logger(__name__)

async def delete_group(bitrix: BitrixClient, group_id: int) -> bool:
    """Удаление группы рабочим способом (GROUP_ID параметр)"""
    try:
        result = await bitrix._request('POST', 'sonet_group.delete', {'GROUP_ID': group_id})
        if result is True:
            logger.success(f"✅ Группа ID {group_id} удалена")
            return True
        else:
            logger.error(f"❌ Группа ID {group_id} НЕ удалена: {result}")
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка удаления группы {group_id}: {e}")
        return False

async def delete_single_group(group_id: int) -> bool:
    """Удаление конкретной группы по ID"""
    bitrix = BitrixClient()
    
    print("\n" + "="*80)
    print("⚠️  ВНИМАНИЕ: УДАЛЕНИЕ КОНКРЕТНОЙ ГРУППЫ")
    print("="*80)
    print(f"Будет удалена группа с ID: {group_id}")
    
    # Получаем информацию о группе для подтверждения
    try:
        groups = await bitrix.get_workgroup_list()
        target_group = None
        for group in groups:
            if int(group['ID']) == group_id:
                target_group = group
                break
        
        if target_group:
            print(f"Название группы: '{target_group.get('NAME', 'Неизвестно')}'")
            print(f"Описание: '{target_group.get('DESCRIPTION', 'Нет описания')}'")
        else:
            print(f"⚠️  Группа с ID {group_id} не найдена в списке групп")
            
    except Exception as e:
        logger.warning(f"⚠️ Не удалось получить информацию о группе: {e}")
    
    # Подтверждение
    confirm = input(f"\n❓ Подтвердить удаление группы ID {group_id}? (y/N): ").strip().lower()
    if confirm != 'y':
        print("❌ Удаление отменено")
        return False
    
    print(f"\n🗑️ Удаление группы ID {group_id}...")
    print("="*80)
    
    success = await delete_group(bitrix, group_id)
    
    if success:
        print(f"\n🎉 Группа ID {group_id} успешно удалена!")
    else:
        print(f"\n❌ Ошибка при удалении группы ID {group_id}")
    
    return success

async def delete_groups_from_mapping():
    """Удаление групп созданных по старой логике Space->Group"""
    
    # Читаем маппинг пространств
    mapping_file = Path("mappings/space_mapping.json")
    if not mapping_file.exists():
        logger.error("❌ Файл space_mapping.json не найден")
        return False
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        space_mapping = json.load(f)
    
    logger.info(f"📂 Загружен файл {mapping_file}")
    logger.info(f"📊 Всего записей в маппинге: {len(space_mapping.get('mapping', {}))}")
    
    # Получаем ID групп для удаления
    groups_to_delete = []
    for space_id, group_data in space_mapping['mapping'].items():
        # Проверяем две возможные структуры данных
        if isinstance(group_data, dict) and 'group_id' in group_data:
            # Старая структура: {"group_id": "4"}
            group_id = int(group_data['group_id'])
        elif isinstance(group_data, str):
            # Новая структура: "4"
            group_id = int(group_data)
        else:
            continue
            
        if 4 <= group_id <= 69:  # Диапазон space-групп
            groups_to_delete.append(group_id)
    
    logger.info(f"🔍 Найдено {len(groups_to_delete)} групп в диапазоне 4-69")
    
    if not groups_to_delete:
        logger.warning("❌ Групп для удаления не найдено")
        return False

    # Инициализация клиента для проверки существования групп
    bitrix = BitrixClient()
    
    # Проверяем какие группы реально существуют в Bitrix24
    logger.info("🔍 Проверяем существование групп в Bitrix24...")
    existing_groups = await bitrix.get_workgroup_list()
    existing_group_ids = set(int(group['ID']) for group in existing_groups)
    
    # Фильтруем только существующие группы
    existing_groups_to_delete = [gid for gid in groups_to_delete if gid in existing_group_ids]
    non_existing_groups = [gid for gid in groups_to_delete if gid not in existing_group_ids]
    
    logger.info(f"✅ Существующих групп для удаления: {len(existing_groups_to_delete)}")
    logger.info(f"⚠️ Групп уже НЕ существует (устаревшие записи): {len(non_existing_groups)}")
    
    if non_existing_groups:
        logger.warning(f"🗑️ Устаревшие записи в маппинге: {sorted(non_existing_groups)}")
    
    if not existing_groups_to_delete:
        logger.warning("❌ Нет групп для удаления (все уже удалены)")
        if non_existing_groups:
            # Предлагаем очистить устаревший маппинг
            print(f"\n💡 В маппинге есть {len(non_existing_groups)} устаревших записей о несуществующих группах")
            clean_mapping = input("❓ Очистить устаревший маппинг? (y/N): ").strip().lower()
            if clean_mapping == 'y':
                # Удаляем файл маппинга
                mapping_file.unlink()
                logger.success(f"✅ Устаревший файл {mapping_file} удален")
                return True
        return False
    
    # Используем только существующие группы
    groups_to_delete = existing_groups_to_delete
    groups_to_delete.sort(reverse=True)  # Удаляем от больших ID к меньшим
    
    print("\n" + "="*80)
    print("⚠️  ВНИМАНИЕ: УДАЛЕНИЕ ГРУПП ИЗ МАППИНГА")
    print("="*80)
    print(f"Найдено {len(groups_to_delete)} групп в диапазоне ID 4-69 для удаления:")
    print("Эти группы созданы по устаревшей логике миграции пространств")
    print(f"Диапазон ID: {min(groups_to_delete)}-{max(groups_to_delete)}")
    print(f"📋 Список ID групп: {sorted(groups_to_delete)}")
    print("\n🟢 Группы созданные по новой логике (ID > 69) НЕ будут затронуты")
    
    # Подтверждение
    confirm = input(f"\n❓ Продолжить удаление {len(groups_to_delete)} групп? (y/N): ").strip().lower()
    if confirm != 'y':
        print("❌ Удаление отменено")
        return False
    
    print(f"\n🗑️ Начинаем удаление {len(groups_to_delete)} групп...")
    print("="*80)
    
    # Удаление групп
    deleted_count = 0
    failed_count = 0
    
    for i, group_id in enumerate(groups_to_delete, 1):
        logger.info(f"🗑️ [{i}/{len(groups_to_delete)}] Удаление группы ID {group_id}")
        
        success = await delete_group(bitrix, group_id)
        if success:
            deleted_count += 1
        else:
            failed_count += 1
        
        # Прогресс каждые 10 групп
        if i % 10 == 0:
            logger.info(f"📊 Обработано {i}/{len(groups_to_delete)} групп...")
        
        # Небольшая пауза между запросами
        await asyncio.sleep(0.1)
    
    # Финальная статистика
    print("\n" + "="*80)
    print("📊 РЕЗУЛЬТАТЫ УДАЛЕНИЯ")
    print("="*80)
    print(f"✅ Успешно удалено: {deleted_count} групп")
    print(f"❌ Ошибки при удалении: {failed_count} групп")
    print(f"📋 Всего обработано: {len(groups_to_delete)} групп")
    
    if deleted_count > 0:
        print(f"\n🎉 Удаление завершено! Освобождено {deleted_count} групп")
        
        # Создаем бэкап маппинга
        backup_file = f"mappings/space_mapping_backup_{deleted_count}_deleted.json"
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(space_mapping, f, ensure_ascii=False, indent=2)
        print(f"💾 Создан бэкап маппинга: {backup_file}")
        
    return deleted_count > 0

async def main():
    """Основная функция с парсером аргументов"""
    parser = argparse.ArgumentParser(
        description="Удаление групп из Bitrix24",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s                    # Удалить все группы из space_mapping.json
  %(prog)s --group-id 145     # Удалить конкретную группу с ID 145
  %(prog)s --group-id 1009633 # Удалить группу созданную для доски 1009633

Режимы работы:
  1. БЕЗ ПАРАМЕТРОВ - удаление всех групп из старого маппинга (1 Space = 1 Group)
  2. С --group-id - удаление конкретной группы по её ID в Bitrix24
        """
    )
    
    parser.add_argument(
        '--group-id',
        type=int,
        help='ID конкретной группы Bitrix24 для удаления (например: 145)'
    )
    
    args = parser.parse_args()
    
    logger.info("🗑️ ЗАПУСК СКРИПТА УДАЛЕНИЯ ГРУПП BITRIX24")
    logger.info("=" * 70)
    
    try:
        if args.group_id:
            # Режим удаления конкретной группы
            logger.info(f"🎯 Режим: удаление конкретной группы ID {args.group_id}")
            success = await delete_single_group(args.group_id)
            return 0 if success else 1
        else:
            # Режим удаления групп из маппинга
            logger.info("📋 Режим: удаление групп из space_mapping.json")
            success = await delete_groups_from_mapping()
            return 0 if success else 1
            
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 