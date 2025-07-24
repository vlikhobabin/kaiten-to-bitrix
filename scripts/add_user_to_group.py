"""
Скрипт для добавления пользователя в группу Bitrix24.
"""
import asyncio
import sys
from pathlib import Path

# Добавляем корневую директорию в Python path
sys.path.append(str(Path(__file__).parent.parent))

from connectors.bitrix_client import BitrixClient
from utils.logger import get_logger

logger = get_logger(__name__)

async def add_user_to_group(group_id: int, user_id: int, user_name: str = ""):
    """
    Добавляет пользователя в группу Bitrix24.
    
    Args:
        group_id: ID группы в Bitrix24
        user_id: ID пользователя в Bitrix24
        user_name: Имя пользователя (для логов)
    """
    client = BitrixClient()
    
    print("=" * 80)
    print(f"👥 ДОБАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯ В ГРУППУ BITRIX24")
    print("=" * 80)
    print(f"📋 Группа: {group_id}")
    print(f"👤 Пользователь: {user_id} ({user_name})")
    print("-" * 80)
    
    try:
        # Сначала проверяем существует ли пользователь
        user = await client.get_user(user_id)
        if not user:
            print(f"❌ Пользователь {user_id} не найден в Bitrix24")
            return False
        
        print(f"✅ Пользователь найден:")
        print(f"   ID: {user.ID}")
        print(f"   Имя: {getattr(user, 'NAME', 'N/A')} {getattr(user, 'LAST_NAME', 'N/A')}")
        print(f"   Email: {getattr(user, 'EMAIL', 'N/A')}")
        print(f"   Активен: {getattr(user, 'ACTIVE', 'N/A')}")
        
        # Добавляем пользователя в группу
        print(f"\n🔄 Добавляем пользователя {user_id} в группу {group_id}...")
        success = await client.add_user_to_workgroup(group_id, user_id)
        
        if success:
            print(f"✅ Пользователь {getattr(user, 'EMAIL', user_id)} успешно добавлен в группу {group_id}")
            return True
        else:
            print(f"❌ Не удалось добавить пользователя {user_id} в группу {group_id}")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка добавления пользователя: {e}")
        return False

async def main():
    """
    Основная функция для добавления Станислава Толстова в группу.
    """
    # Станислав Толстов: Kaiten ID 488906 -> Bitrix24 ID 108
    # Группа: "Финансовая дирекция/Отдел бюджетирования и кредитования" -> ID 37
    
    group_id = 37
    user_id = 108
    user_name = "Станислав Толстов (tsv@eg-holding.ru)"
    
    success = await add_user_to_group(group_id, user_id, user_name)
    
    if success:
        print(f"\n🎉 УСПЕШНО! Станислав Толстов добавлен в группу.")
        print(f"   Теперь его комментарии должны переноситься от его имени.")
    else:
        print(f"\n❌ ОШИБКА! Не удалось добавить Станислава Толстова в группу.")

if __name__ == "__main__":
    asyncio.run(main()) 