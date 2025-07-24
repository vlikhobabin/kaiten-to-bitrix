"""
Альтернативный скрипт для исследования доступа к пространству 426722.
Ищет информацию о группах доступа через другие API endpoints.
"""
import asyncio
import json
import sys
from pathlib import Path

# Добавляем корневую директорию в Python path
sys.path.append(str(Path(__file__).parent.parent))

from connectors.kaiten_client import KaitenClient
from utils.logger import get_logger

logger = get_logger(__name__)

async def investigate_space_access():
    """
    Исследует доступ к пространству 426722 альтернативными методами.
    """
    client = KaitenClient()
    target_space_id = 426722
    
    print("=" * 80)
    print(f"🔍 ИССЛЕДОВАНИЕ ДОСТУПА К ПРОСТРАНСТВУ {target_space_id}")
    print("=" * 80)
    
    # 1. Получаем базовую информацию о пространстве
    print(f"\n1️⃣ ПОЛУЧЕНИЕ ИНФОРМАЦИИ О ПРОСТРАНСТВЕ {target_space_id}")
    print("-" * 50)
    
    try:
        # Получаем все пространства
        all_spaces = await client.get_spaces()
        target_space = None
        
        for space in all_spaces:
            if space.id == target_space_id:
                target_space = space
                break
        
        if target_space:
            print(f"✅ Пространство найдено:")
            print(f"   ID: {target_space.id}")
            print(f"   Название: {target_space.title}")
            # Используем правильные атрибуты модели KaitenSpace
            print(f"   Родительское: {getattr(target_space, 'parent_id', 'N/A')}")
            print(f"   Создан: {getattr(target_space, 'created_at', 'N/A')}")
        else:
            print(f"❌ Пространство {target_space_id} не найдено в списке пространств")
            return
            
    except Exception as e:
        print(f"❌ Ошибка получения информации о пространстве: {e}")
        return
    
    # 2. Получаем прямых участников пространства
    print(f"\n2️⃣ ПОЛУЧЕНИЕ ПРЯМЫХ УЧАСТНИКОВ ПРОСТРАНСТВА {target_space_id}")
    print("-" * 50)
    
    try:
        # Пробуем получить участников разными способами
        members = await client.get_space_members(target_space_id)
        users_with_roles = await client.get_space_users_with_roles(target_space_id)
        
        print(f"📋 Прямые участники (get_space_members): {len(members)}")
        print(f"📋 Участники с ролями (get_space_users_with_roles): {len(users_with_roles)}")
        
        # Проверяем есть ли Станислав Толстов среди прямых участников
        stanislov_found = False
        
        # Преобразуем объекты в словари для удобства работы
        all_direct_users = []
        
        # Добавляем участников из get_space_members
        for member in members:
            if hasattr(member, 'model_dump'):
                all_direct_users.append(member.model_dump())
            elif hasattr(member, 'dict'):
                all_direct_users.append(member.dict())
            else:
                all_direct_users.append(member.__dict__ if hasattr(member, '__dict__') else {})
        
        # Добавляем участников с ролями
        for user in users_with_roles:
            if isinstance(user, dict):
                all_direct_users.append(user)
            elif hasattr(user, 'model_dump'):
                all_direct_users.append(user.model_dump())
            elif hasattr(user, 'dict'):
                all_direct_users.append(user.dict())
            else:
                all_direct_users.append(user.__dict__ if hasattr(user, '__dict__') else {})
        
        print(f"📋 Всего уникальных участников для проверки: {len(all_direct_users)}")
        
        for user in all_direct_users:
            user_name = user.get('name', '').lower()
            user_email = user.get('email', '').lower()
            if ('станислав' in user_name and 'толстов' in user_name) or 'tsv@eg-holding.ru' in user_email:
                stanislov_found = True
                print(f"✅ Станислав Толстов найден среди ПРЯМЫХ участников:")
                print(f"   ID: {user.get('id')}")
                print(f"   Имя: {user.get('name')}")
                print(f"   Email: {user.get('email')}")
                break
        
        if not stanislov_found:
            print(f"❌ Станислав Толстов НЕ найден среди прямых участников")
            print(f"   Это ПОДТВЕРЖДАЕТ что его доступ осуществляется через группы!")
            
        # Показываем несколько участников для анализа
        if all_direct_users:
            print(f"\n📋 Первые 5 участников:")
            for i, user in enumerate(all_direct_users[:5], 1):
                user_id = user.get('id', 'N/A')
                user_name = user.get('name', 'Без имени')
                user_email = user.get('email', 'Без email')
                role_id = user.get('space_role_id', user.get('role_id', 'N/A'))
                print(f"   {i}. ID: {user_id:6s} | Роль: {role_id} | Имя: {user_name:20s} | Email: {user_email}")
        
        # Проверяем есть ли пользователи с email eg-holding.ru
        eg_users = []
        for user in all_direct_users:
            user_email = user.get('email', '').lower()
            if '@eg-holding.ru' in user_email:
                eg_users.append(user)
        
        if eg_users:
            print(f"\n📋 Найдено {len(eg_users)} пользователей @eg-holding.ru среди прямых участников:")
            for user in eg_users[:10]:  # Показываем первых 10
                print(f"   - {user.get('name', 'N/A')} ({user.get('email', 'N/A')})")
        else:
            print(f"\n❌ Пользователи @eg-holding.ru НЕ найдены среди прямых участников")
                
    except Exception as e:
        print(f"❌ Ошибка получения участников: {e}")
    
    # 3. Исследуем другие API endpoints, которые могут содержать информацию о группах
    print(f"\n3️⃣ ИССЛЕДОВАНИЕ АЛЬТЕРНАТИВНЫХ API ENDPOINTS")
    print("-" * 50)
    
    # Пробуем различные endpoints, которые могут содержать информацию о группах
    test_endpoints = [
        "/api/v1/user_groups",
        "/api/v1/access_groups", 
        "/api/v1/permissions",
        "/api/v1/roles",
        f"/api/v1/spaces/{target_space_id}/permissions",
        f"/api/v1/spaces/{target_space_id}/access",
        f"/api/v1/spaces/{target_space_id}/roles",
        "/api/latest/permissions",
        "/api/latest/roles",
        "/api/latest/access_groups",
    ]
    
    successful_endpoints = []
    
    for endpoint in test_endpoints:
        try:
            logger.info(f"🔍 Тестируем endpoint: {endpoint}")
            data = await client._request("GET", endpoint)
            
            if data is not None:
                print(f"✅ Endpoint {endpoint} РАБОТАЕТ!")
                print(f"   Тип ответа: {type(data)}")
                if isinstance(data, list):
                    print(f"   Количество элементов: {len(data)}")
                    if data and isinstance(data[0], dict):
                        print(f"   Ключи первого элемента: {list(data[0].keys())}")
                elif isinstance(data, dict):
                    print(f"   Ключи: {list(data.keys())}")
                successful_endpoints.append(endpoint)
                
                # Сохраняем небольшую выборку для анализа
                if len(successful_endpoints) <= 3:  # Ограничиваем количество сохраняемых данных
                    sample_data = data[:5] if isinstance(data, list) else data
                    print(f"   Пример данных: {json.dumps(sample_data, ensure_ascii=False, indent=2)[:500]}...")
            
        except Exception as e:
            logger.debug(f"Endpoint {endpoint} не работает: {e}")
    
    if successful_endpoints:
        print(f"\n✅ Найдены рабочие endpoints: {len(successful_endpoints)}")
        for endpoint in successful_endpoints:
            print(f"   - {endpoint}")
    else:
        print(f"\n❌ Рабочие endpoints для групп не найдены")
    
    # 4. Проверяем, есть ли информация о группах в данных пространства
    print(f"\n4️⃣ ПОИСК ИНФОРМАЦИИ О ГРУППАХ В ДАННЫХ ПРОСТРАНСТВА")
    print("-" * 50)
    
    try:
        # Получаем полную информацию о пространстве через прямой API запрос
        space_data = await client._request("GET", f"/api/v1/spaces/{target_space_id}")
        
        if space_data:
            print(f"✅ Получены подробные данные о пространстве")
            print(f"📋 Доступные поля: {list(space_data.keys())}")
            
            # Ищем поля, которые могут содержать информацию о группах
            group_related_fields = []
            for key in space_data.keys():
                if any(keyword in key.lower() for keyword in ['group', 'access', 'permission', 'role']):
                    group_related_fields.append(key)
                    value = space_data[key]
                    print(f"🔍 Поле '{key}': {type(value)} = {str(value)[:100]}...")
            
            if group_related_fields:
                print(f"✅ Найдены поля связанные с группами: {group_related_fields}")
            else:
                print(f"❌ Поля связанные с группами не найдены")
                
        else:
            print(f"❌ Не удалось получить подробные данные о пространстве")
            
    except Exception as e:
        print(f"❌ Ошибка получения данных пространства: {e}")
    
    # 5. Итоговые выводы и рекомендации
    print("\n" + "=" * 80)
    print("📊 ИТОГОВЫЕ ВЫВОДЫ И РЕКОМЕНДАЦИИ")
    print("=" * 80)
    
    print(f"🔍 ПРОБЛЕМА: Станислав Толстов имеет доступ к пространству {target_space_id} в Kaiten,")
    print(f"   но НЕ является прямым участником пространства.")
    print(f"   Это означает, что доступ осуществляется через группы доступа.")
    print()
    print(f"💡 ВОЗМОЖНЫЕ РЕШЕНИЯ:")
    print(f"   1. API групп доступа может быть недоступен через публичный API")
    print(f"   2. Требуется использовать другой подход для получения всех пользователей")
    print(f"   3. Возможно, нужны дополнительные права для доступа к API групп")
    print()
    print(f"🔄 ТЕКУЩЕЕ РЕШЕНИЕ ПРОБЛЕМЫ:")
    print(f"   - Система успешно переносит карточки")
    print(f"   - Комментарии создаются от администратора с указанием автора")
    print(f"   - Это рабочее решение до получения доступа к API групп")
    print()
    print(f"📋 СЛЕДУЮЩИЕ ШАГИ:")
    print(f"   1. Обратиться к разработчикам Kaiten за документацией API групп")
    print(f"   2. Возможно, использовать прямое добавление пользователей в Bitrix24")
    print(f"   3. Текущее решение является работоспособным")

    # Сохраняем результаты
    results = {
        "space_id": target_space_id,
        "space_info": target_space.model_dump() if target_space else None,
        "direct_members_count": len(members) if 'members' in locals() else 0,
        "users_with_roles_count": len(users_with_roles) if 'users_with_roles' in locals() else 0,
        "stanislov_found_in_direct": stanislov_found if 'stanislov_found' in locals() else False,
        "successful_endpoints": successful_endpoints if 'successful_endpoints' in locals() else [],
        "recommendations": [
            "API групп доступа недоступен через стандартные endpoints",
            "Станислав Толстов не является прямым участником пространства",
            "Доступ осуществляется через группы доступа",
            "Текущее решение с комментариями от администратора работает",
            "Требуется дополнительное исследование API или прямое добавление пользователей"
        ]
    }
    
    results_file = Path(__file__).parent.parent / "logs" / "space_access_investigation.json"
    results_file.parent.mkdir(exist_ok=True)
    
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n💾 Результаты сохранены в: {results_file}")
    print("\n🔍 Исследование завершено!")

if __name__ == "__main__":
    asyncio.run(investigate_space_access()) 