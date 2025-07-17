#!/usr/bin/env python3
"""
Тестовый скрипт для получения списка пользователей из Kaiten API.
Выводит в консоль id, email, full_name всех пользователей.
"""

import asyncio
import sys
from pathlib import Path

# Добавляем корневую директорию в путь для импорта модулей
sys.path.append(str(Path(__file__).parent.parent))

from connectors.kaiten_client import KaitenClient
from utils.logger import logger

async def main():
    """Получение и вывод списка пользователей из Kaiten"""
    
    print("🚀 Получение списка пользователей из Kaiten API...")
    print("=" * 70)
    
    try:
        # Инициализация клиента Kaiten
        kaiten_client = KaitenClient()
        
        # Получаем всех пользователей
        logger.info("📥 Запрос пользователей из Kaiten...")
        users = await kaiten_client.get_users()
        
        if not users:
            print("❌ Не удалось получить пользователей из Kaiten")
            return
        
        print(f"✅ Получено {len(users)} пользователей\n")
        
        # Заголовок таблицы
        print(f"{'ID':<8} {'EMAIL':<35} {'ФИО':<40}")
        print("-" * 83)
        
        # Выводим информацию о каждом пользователе
        for i, user in enumerate(users, 1):
            # Обрабатываем случаи, когда full_name может быть пустым
            display_name = user.full_name.strip() if user.full_name else f"[{user.username}]"
            if not display_name:
                display_name = f"[{user.email.split('@')[0]}]"
            
            # Ограничиваем длину для красивого вывода
            email_short = user.email[:32] + "..." if len(user.email) > 35 else user.email
            name_short = display_name[:37] + "..." if len(display_name) > 40 else display_name
            
            print(f"{user.id:<8} {email_short:<35} {name_short:<40}")
            
            # Прерываем при достижении 150 строк (с учетом заголовков)
            if i >= 147:  # 147 + 3 заголовка = 150 строк
                print(f"\n... и еще {len(users) - i} пользователей")
                break
        
        print("\n" + "=" * 70)
        print(f"📊 Всего пользователей в Kaiten: {len(users)}")
        
        # Дополнительная статистика
        active_users = sum(1 for user in users if user.activated)
        users_with_names = sum(1 for user in users if user.full_name and user.full_name.strip())
        users_with_emails = sum(1 for user in users if user.email)
        
        print(f"✅ Активированных: {active_users}")
        print(f"👤 С именами: {users_with_names}")
        print(f"📧 С email: {users_with_emails}")
        
    except Exception as e:
        logger.error(f"Критическая ошибка при получении пользователей: {e}")
        print(f"❌ Ошибка: {e}")
        return

if __name__ == "__main__":
    asyncio.run(main()) 