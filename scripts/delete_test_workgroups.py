#!/usr/bin/env python3
"""
Скрипт для удаления тестовых рабочих групп из Bitrix24 (ID 4-8).
"""

import asyncio
import sys
from pathlib import Path

# Добавляем корневую директорию в путь для импорта модулей
sys.path.append(str(Path(__file__).parent.parent))

from connectors.bitrix_client import BitrixClient
from utils.logger import logger

async def main():
    """Удаление тестовых рабочих групп"""
    
    print("🗑️ Удаление тестовых рабочих групп из Bitrix24...")
    print("=" * 70)
    
    try:
        # Инициализация клиента Bitrix24
        bitrix_client = BitrixClient()
        
        # ID групп для удаления (созданные в тестах)
        test_group_ids = [4, 5, 6, 7, 8]
        
        for group_id in test_group_ids:
            try:
                # Пытаемся удалить группу
                print(f"🗑️ Удаление группы ID {group_id}...")
                
                # Используем метод delete_workgroup если есть, иначе делаем запрос напрямую
                result = await bitrix_client._request('POST', 'sonet_group.delete', {'ID': group_id})
                
                if result:
                    print(f"✅ Группа ID {group_id} удалена")
                else:
                    print(f"⚠️ Группа ID {group_id} не найдена или уже удалена")
                    
            except Exception as e:
                print(f"❌ Ошибка удаления группы ID {group_id}: {e}")
        
        print("\n" + "=" * 70)
        print("🧹 Очистка завершена!")
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        return

if __name__ == "__main__":
    asyncio.run(main()) 