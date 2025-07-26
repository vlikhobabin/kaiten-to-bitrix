#!/usr/bin/env python3
"""
Скрипт для удаления ВСЕХ групп из Bitrix24, кроме исключенных.
По умолчанию исключаются группы с ID 1 и 2 (созданы вручную).

Примеры использования:
    python scripts/delete_all_groups.py                    # Удалить все группы кроме ID 1,2
    python scripts/delete_all_groups.py --exclude 1 2 5    # Удалить все кроме ID 1,2,5
    python scripts/delete_all_groups.py --dry-run          # Показать что будет удалено без удаления
"""

import asyncio
import sys
import os
import argparse
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.bitrix_client import BitrixClient
from utils.logger import get_logger

logger = get_logger(__name__)

async def delete_group(bitrix: BitrixClient, group_id: int) -> bool:
    """Удаление группы по ID"""
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

async def delete_all_groups(excluded_ids: list | None = None, dry_run: bool = False):
    """
    Удаление всех групп кроме исключенных
    
    Args:
        excluded_ids: Список ID групп для исключения (по умолчанию [1, 2])
        dry_run: Если True, только показать что будет удалено
    """
    if excluded_ids is None:
        excluded_ids = [1, 2]
    
    bitrix = BitrixClient()
    
    print("\n" + "="*80)
    print("⚠️  ВНИМАНИЕ: МАССОВОЕ УДАЛЕНИЕ ГРУПП")
    print("="*80)
    
    # Получаем все группы
    logger.info("🔍 Получаем список всех групп в Bitrix24...")
    try:
        all_groups = await bitrix.get_workgroup_list()
        logger.info(f"📊 Найдено групп всего: {len(all_groups)}")
    except Exception as e:
        logger.error(f"❌ Ошибка получения списка групп: {e}")
        return False
    
    if not all_groups:
        logger.warning("❌ Групп для обработки не найдено")
        return False
    
    # Фильтруем группы для удаления
    groups_to_delete = []
    excluded_groups = []
    
    for group in all_groups:
        group_id = int(group['ID'])
        group_name = group.get('NAME', 'Без названия')
        
        if group_id in excluded_ids:
            excluded_groups.append({
                'id': group_id,
                'name': group_name,
                'description': group.get('DESCRIPTION', '')
            })
        else:
            groups_to_delete.append({
                'id': group_id,
                'name': group_name,
                'description': group.get('DESCRIPTION', '')
            })
    
    # Сортируем по ID для красивого вывода
    groups_to_delete.sort(key=lambda x: x['id'])
    excluded_groups.sort(key=lambda x: x['id'])
    
    # Выводим статистику
    print(f"📊 Исключенные группы (НЕ будут удалены): {len(excluded_groups)}")
    for group in excluded_groups:
        print(f"   🔒 ID {group['id']}: '{group['name']}'")
    
    print(f"\n🗑️  Группы к удалению: {len(groups_to_delete)}")
    if groups_to_delete:
        print("   Список групп для удаления:")
        for group in groups_to_delete[:10]:  # Показываем первые 10
            print(f"   🚮 ID {group['id']}: '{group['name']}'")
        
        if len(groups_to_delete) > 10:
            print(f"   ... и еще {len(groups_to_delete) - 10} групп")
    
    if not groups_to_delete:
        logger.info("✅ Групп для удаления не найдено!")
        return True
    
    # Режим dry-run
    if dry_run:
        print("\n🧪 РЕЖИМ DRY-RUN: Удаление НЕ будет выполнено")
        print("="*80)
        print(f"Будет удалено {len(groups_to_delete)} групп")
        print(f"Исключено {len(excluded_groups)} групп")
        return True
    
    # Подтверждение удаления
    print(f"\n❗ ВНИМАНИЕ: Будет удалено {len(groups_to_delete)} групп!")
    print("Это действие НЕОБРАТИМО!")
    
    confirm = input(f"\n❓ Подтвердить удаление {len(groups_to_delete)} групп? (введите 'DELETE' для подтверждения): ").strip()
    if confirm != 'DELETE':
        print("❌ Удаление отменено")
        return False
    
    print(f"\n🗑️ Начинаем удаление {len(groups_to_delete)} групп...")
    print("="*80)
    
    # Удаление групп
    deleted_count = 0
    failed_count = 0
    
    for i, group in enumerate(groups_to_delete, 1):
        group_id = group['id']
        group_name = group['name']
        
        logger.info(f"🗑️ [{i}/{len(groups_to_delete)}] Удаление группы ID {group_id}: '{group_name}'")
        
        success = await delete_group(bitrix, group_id)
        if success:
            deleted_count += 1
        else:
            failed_count += 1
        
        # Прогресс каждые 10 групп
        if i % 10 == 0:
            logger.info(f"📊 Обработано {i}/{len(groups_to_delete)} групп...")
        
        # Небольшая пауза между запросами
        await asyncio.sleep(0.2)
    
    # Финальная статистика
    print("\n" + "="*80)
    print("📊 РЕЗУЛЬТАТЫ УДАЛЕНИЯ")
    print("="*80)
    print(f"✅ Успешно удалено: {deleted_count} групп")
    print(f"❌ Ошибки при удалении: {failed_count} групп")
    print(f"🔒 Исключено из удаления: {len(excluded_groups)} групп")
    print(f"📋 Всего обработано: {len(groups_to_delete)} групп")
    
    if deleted_count > 0:
        print(f"\n🎉 Массовое удаление завершено! Удалено {deleted_count} групп")
        print(f"💡 Сохранены группы с ID: {excluded_ids}")
        
    return deleted_count > 0

async def main():
    """Основная функция с парсером аргументов"""
    parser = argparse.ArgumentParser(
        description="Удаление ВСЕХ групп из Bitrix24 кроме исключенных",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s                           # Удалить все группы кроме ID 1,2 (по умолчанию)
  %(prog)s --exclude 1 2 5           # Удалить все кроме ID 1,2,5
  %(prog)s --dry-run                 # Показать что будет удалено БЕЗ удаления
  %(prog)s --exclude 1 2 --dry-run   # Показать план удаления кроме ID 1,2

⚠️  ВНИМАНИЕ: Этот скрипт удаляет ВСЕ группы кроме указанных!
    Убедитесь что вы указали все нужные исключения в --exclude
        """
    )
    
    parser.add_argument(
        '--exclude',
        type=int,
        nargs='+',
        default=[1, 2],
        help='ID групп для исключения из удаления (по умолчанию: 1 2)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Показать что будет удалено без фактического удаления'
    )
    
    args = parser.parse_args()
    
    logger.info("🗑️ ЗАПУСК СКРИПТА МАССОВОГО УДАЛЕНИЯ ГРУПП BITRIX24")
    logger.info("=" * 70)
    
    try:
        success = await delete_all_groups(
            excluded_ids=args.exclude,
            dry_run=args.dry_run
        )
        return 0 if success else 1
            
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 