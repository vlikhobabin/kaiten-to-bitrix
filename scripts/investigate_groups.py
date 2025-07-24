"""
Скрипт для исследования API групп доступа в Kaiten.
Помогает понять структуру групп и их связь с пространствами.
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

async def investigate_groups():
    """
    Исследует API групп доступа Kaiten.
    """
    client = KaitenClient()
    
    print("=" * 80)
    print("🔍 ИССЛЕДОВАНИЕ API ГРУПП ДОСТУПА KAITEN")
    print("=" * 80)
    
    # 1. Получаем список всех групп
    print("\n1️⃣ ПОЛУЧЕНИЕ СПИСКА ВСЕХ ГРУПП")
    print("-" * 50)
    
    all_groups = await client.get_all_groups()
    
    if all_groups:
        print(f"✅ Найдено {len(all_groups)} групп доступа:")
        for i, group in enumerate(all_groups, 1):
            group_id = group.get('id')
            group_name = group.get('name', 'Без названия')
            description = group.get('description', 'Нет описания')
            print(f"   {i:2d}. ID: {group_id:3d} | Название: '{group_name}' | Описание: {description[:50]}...")
    else:
        print("❌ Группы доступа не найдены")
        return
    
    # 2. Ищем группу "Отдел Бюджетирования и Кредитования"
    print("\n2️⃣ ПОИСК ГРУППЫ 'ОТДЕЛ БЮДЖЕТИРОВАНИЯ И КРЕДИТОВАНИЯ'")
    print("-" * 50)
    
    target_group_names = [
        "Отдел Бюджетирования и Кредитования", 
        "Отдел бюджетирования и кредитования",
        "ОТДЕЛ БЮДЖЕТИРОВАНИЯ И КРЕДИТОВАНИЯ"
    ]
    
    target_group = None
    for group_name in target_group_names:
        target_group = await client.find_group_by_name(group_name)
        if target_group:
            break
    
    if not target_group:
        # Ищем по частичному совпадению
        print("🔍 Точное совпадение не найдено, ищем по частичному совпадению...")
        for group in all_groups:
            group_name = group.get('name', '').lower()
            if any(keyword in group_name for keyword in ['бюджет', 'кредит', 'отдел']):
                target_group = group
                print(f"✅ Найдена похожая группа: '{group.get('name')}' (ID: {group.get('id')})")
                break
    
    if not target_group:
        print("❌ Группа 'Отдел Бюджетирования и Кредитования' не найдена")
        # Используем первую группу для демонстрации
        if all_groups:
            target_group = all_groups[0]
            print(f"📋 Для демонстрации используем первую группу: '{target_group.get('name')}' (ID: {target_group.get('id')})")
    
    if not target_group:
        print("❌ Нет групп для исследования")
        return
    
    group_id = target_group.get('id')
    group_name = target_group.get('name')
    
    print(f"\n🎯 ИССЛЕДУЕМ ГРУППУ: '{group_name}' (ID: {group_id})")
    
    # 3. Получаем пользователей группы
    print("\n3️⃣ ПОЛУЧЕНИЕ ПОЛЬЗОВАТЕЛЕЙ ГРУППЫ")
    print("-" * 50)
    
    group_users = await client.get_group_users(group_id)
    
    if group_users:
        print(f"✅ Найдено {len(group_users)} пользователей в группе:")
        for i, user in enumerate(group_users, 1):
            user_id = user.get('id', 'N/A')
            user_name = user.get('name', 'Без имени')
            user_email = user.get('email', 'Без email')
            print(f"   {i:2d}. ID: {user_id:6s} | Имя: {user_name:20s} | Email: {user_email}")
            
            # Проверяем есть ли Станислав Толстов
            if 'станислав' in user_name.lower() and 'толстов' in user_name.lower():
                print(f"   🎯 НАЙДЕН: Станислав Толстов в группе!")
            elif 'tsv@eg-holding.ru' in user_email.lower():
                print(f"   🎯 НАЙДЕН: tsv@eg-holding.ru в группе!")
    else:
        print("❌ Пользователи группы не найдены")
    
    # 4. Получаем сущности (entities) группы
    print("\n4️⃣ ПОЛУЧЕНИЕ СУЩНОСТЕЙ (ENTITIES) ГРУППЫ")
    print("-" * 50)
    
    group_entities = await client.get_group_entities(group_id)
    
    if group_entities:
        print(f"✅ Найдено {len(group_entities)} сущностей для группы:")
        for i, entity in enumerate(group_entities, 1):
            entity_id = entity.get('id', 'N/A')
            entity_type = entity.get('type', 'unknown')
            entity_name = entity.get('name', 'Без названия')
            parent_id = entity.get('parent_id', 'N/A')
            print(f"   {i:2d}. ID: {entity_id:6s} | Тип: {entity_type:10s} | Название: {entity_name:30s} | Parent: {parent_id}")
            
            # Проверяем есть ли пространство 426722
            if str(entity_id) == "426722":
                print(f"   🎯 НАЙДЕНО: Пространство 426722 в сущностях группы!")
    else:
        print("❌ Сущности группы не найдены")
    
    # 5. Получаем общую информацию о древовидных сущностях
    print("\n5️⃣ ПОЛУЧЕНИЕ ДРЕВОВИДНЫХ СУЩНОСТЕЙ")
    print("-" * 50)
    
    tree_entities = await client.get_tree_entities()
    
    if tree_entities:
        print(f"✅ Найдено {len(tree_entities)} древовидных сущностей:")
        # Показываем только первые 10 для краткости
        for i, entity in enumerate(tree_entities[:10], 1):
            entity_id = entity.get('id', 'N/A')
            entity_type = entity.get('type', 'unknown')
            entity_name = entity.get('name', 'Без названия')
            print(f"   {i:2d}. ID: {entity_id:6s} | Тип: {entity_type:10s} | Название: {entity_name}")
        
        if len(tree_entities) > 10:
            print(f"   ... и еще {len(tree_entities) - 10} сущностей")
            
        # Ищем пространство 426722
        space_426722 = None
        for entity in tree_entities:
            if str(entity.get('id')) == "426722":
                space_426722 = entity
                break
        
        if space_426722:
            print(f"\n🎯 НАЙДЕНО ПРОСТРАНСТВО 426722:")
            print(f"   ID: {space_426722.get('id')}")
            print(f"   Тип: {space_426722.get('type')}")
            print(f"   Название: {space_426722.get('name')}")
            print(f"   Parent ID: {space_426722.get('parent_id')}")
        
    else:
        print("❌ Древовидные сущности не найдены")
    
    # 6. Получаем роли сущностей
    print("\n6️⃣ ПОЛУЧЕНИЕ РОЛЕЙ ДРЕВОВИДНЫХ СУЩНОСТЕЙ")
    print("-" * 50)
    
    tree_roles = await client.get_tree_entity_roles()
    
    if tree_roles:
        print(f"✅ Найдено {len(tree_roles)} ролей:")
        for i, role in enumerate(tree_roles, 1):
            role_id = role.get('id', 'N/A')
            role_name = role.get('name', 'Без названия')
            role_description = role.get('description', 'Нет описания')
            print(f"   {i:2d}. ID: {role_id:3s} | Название: {role_name:20s} | Описание: {role_description}")
    else:
        print("❌ Роли сущностей не найдены")
    
    # 7. Итоговые выводы
    print("\n" + "=" * 80)
    print("📊 ИТОГОВЫЕ ВЫВОДЫ")
    print("=" * 80)
    
    if group_users:
        # Проверяем есть ли Станислав Толстов среди пользователей группы
        stanislov_found = False
        for user in group_users:
            user_name = user.get('name', '').lower()
            user_email = user.get('email', '').lower()
            if ('станислав' in user_name and 'толстов' in user_name) or 'tsv@eg-holding.ru' in user_email:
                stanislov_found = True
                print(f"✅ Станислав Толстов НАЙДЕН в группе '{group_name}':")
                print(f"   ID: {user.get('id')}")
                print(f"   Имя: {user.get('name')}")
                print(f"   Email: {user.get('email')}")
                break
        
        if not stanislov_found:
            print(f"❌ Станислав Толстов НЕ НАЙДЕН в группе '{group_name}'")
    
    if group_entities:
        # Проверяем есть ли пространство 426722 среди сущностей группы
        space_found = False
        for entity in group_entities:
            if str(entity.get('id')) == "426722":
                space_found = True
                print(f"✅ Пространство 426722 НАЙДЕНО в сущностях группы '{group_name}':")
                print(f"   ID: {entity.get('id')}")
                print(f"   Тип: {entity.get('type')}")
                print(f"   Название: {entity.get('name')}")
                break
        
        if not space_found:
            print(f"❌ Пространство 426722 НЕ НАЙДЕНО в сущностях группы '{group_name}'")
    
    # Сохраняем результаты в файл для дальнейшего анализа
    results = {
        "target_group": target_group,
        "group_users": group_users,
        "group_entities": group_entities,
        "tree_entities": tree_entities[:20],  # Ограничиваем для размера файла
        "tree_roles": tree_roles
    }
    
    results_file = Path(__file__).parent.parent / "logs" / "groups_investigation.json"
    results_file.parent.mkdir(exist_ok=True)
    
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n💾 Результаты сохранены в: {results_file}")
    print("\n🔍 Исследование завершено!")

if __name__ == "__main__":
    asyncio.run(investigate_groups()) 