#!/usr/bin/env python3
"""
Скрипт для просмотра рабочих групп в Bitrix24.
"""

import asyncio
import sys
from pathlib import Path

# Добавляем корневую директорию в путь для импорта модулей
sys.path.append(str(Path(__file__).parent.parent))

from connectors.bitrix_client import BitrixClient
from utils.logger import logger

async def main():
    """Получение и вывод списка рабочих групп из Bitrix24"""
    
    print("🚀 Получение списка рабочих групп из Bitrix24...")
    print("=" * 70)
    
    try:
        # Инициализация клиента Bitrix24
        bitrix_client = BitrixClient()
        
        # Получаем все рабочие группы
        logger.info("📥 Запрос рабочих групп из Bitrix24...")
        workgroups = await bitrix_client.get_workgroup_list()
        
        if not workgroups:
            print("❌ Не удалось получить рабочие группы из Bitrix24")
            return
        
        print(f"✅ Получено {len(workgroups)} рабочих групп\n")
        
        # Заголовок таблицы
        print(f"{'ID':<5} {'НАЗВАНИЕ':<40} {'ОПИСАНИЕ':<50}")
        print("-" * 95)
        
        # Выводим информацию о каждой группе
        for group in workgroups:
            group_id = group.get('ID', 'N/A')
            name = group.get('NAME', 'Без названия')
            description = group.get('DESCRIPTION', 'Без описания')
            
            # Ограничиваем длину для красивого вывода
            name_short = name[:37] + "..." if len(name) > 40 else name
            desc_short = description[:47] + "..." if len(description) > 50 else description
            
            print(f"{group_id:<5} {name_short:<40} {desc_short:<50}")
        
        print("\n" + "=" * 70)
        print(f"📊 Всего рабочих групп в Bitrix24: {len(workgroups)}")
        
    except Exception as e:
        logger.error(f"Критическая ошибка при получении рабочих групп: {e}")
        print(f"❌ Ошибка: {e}")
        return

if __name__ == "__main__":
    asyncio.run(main()) 