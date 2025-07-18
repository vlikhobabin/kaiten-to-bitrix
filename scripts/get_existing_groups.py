#!/usr/bin/env python3
"""
Скрипт для получения информации о всех существующих группах в Bitrix24
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.bitrix_client import BitrixClient
from utils.logger import get_logger

logger = get_logger(__name__)

async def main():
    """Получаем информацию о всех группах"""
    bitrix = BitrixClient()
    
    logger.info("🔍 Получение всех рабочих групп из Bitrix24...")
    groups = await bitrix.get_workgroup_list()
    
    if not groups:
        logger.warning("❌ Группы не найдены")
        return
    
    logger.info(f"📋 Найдено групп: {len(groups)}")
    
    # Группируем по источнику создания
    space_groups = []  # ID 4-69
    board_groups = []  # ID 70+
    other_groups = []  # ID 1-3
    
    for group in groups:
        group_id = int(group.get('ID', 0))
        group_name = group.get('NAME', 'Без названия')
        
        if 4 <= group_id <= 69:
            space_groups.append((group_id, group_name))
        elif group_id >= 70:
            board_groups.append((group_id, group_name))
        else:
            other_groups.append((group_id, group_name))
    
    # Выводим результаты
    print("\n" + "="*80)
    print("📊 АНАЛИЗ СУЩЕСТВУЮЩИХ ГРУПП")
    print("="*80)
    
    if other_groups:
        print(f"\n🔵 СИСТЕМНЫЕ ГРУППЫ (ID 1-3): {len(other_groups)} шт.")
        for group_id, name in sorted(other_groups):
            print(f"  {group_id}: {name}")
    
    if space_groups:
        print(f"\n🟡 ГРУППЫ ОТ SPACE-МИГРАЦИИ (ID 4-69): {len(space_groups)} шт.")
        print("   (созданы по старой логике 1 Space = 1 Group)")
        for group_id, name in sorted(space_groups)[:10]:  # Показываем первые 10
            print(f"  {group_id}: {name}")
        if len(space_groups) > 10:
            print(f"  ... и еще {len(space_groups) - 10} групп")
    
    if board_groups:
        print(f"\n🟢 ГРУППЫ ОТ BOARD-МИГРАЦИИ (ID 70+): {len(board_groups)} шт.")
        print("   (созданы по новой правильной логике 1 Board = 1 Group)")
        for group_id, name in sorted(board_groups):
            print(f"  {group_id}: {name}")
    
    print("\n" + "="*80)
    print("💡 РЕКОМЕНДАЦИИ:")
    print("="*80)
    print("1. 🗑️  УДАЛИТЬ space-группы (неправильная логика)")
    print("2. ✅ ОСТАВИТЬ board-группы (правильная логика)")
    print("3. 🔄 ПРОДОЛЖИТЬ миграцию всех досок по новой логике")

if __name__ == "__main__":
    asyncio.run(main()) 