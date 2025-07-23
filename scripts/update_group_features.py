#!/usr/bin/env python3
"""
Скрипт для управления возможностями рабочих групп Bitrix24.
Выполняется на VPS сервере с прямым доступом к MySQL.

Использование:
    python3 update_group_features.py --view-group 38                    # Просмотр возможностей группы
    python3 update_group_features.py --update-group 38                 # Установка стандартных возможностей
    python3 update_group_features.py --update-all                      # Массовое обновление всех групп  
    python3 update_group_features.py --set-features tasks,files,chat   # Установка конкретных возможностей для группы
"""

import sys
import json
import pymysql
import argparse
from datetime import datetime
from typing import Dict, List, Set, Optional

def connect_to_mysql() -> pymysql.Connection:
    """
    Подключение к MySQL используя конфигурацию из /root/.my.cnf
    """
    try:
        # Читаем конфигурацию MySQL
        connection = pymysql.connect(
            read_default_file='/root/.my.cnf',
            database='sitemanager',
            charset='utf8mb4',
            autocommit=False
        )
        return connection
    except Exception as e:
        print(f"❌ Ошибка подключения к MySQL: {e}")
        sys.exit(1)

# Стандартный набор возможностей для новых групп
STANDARD_FEATURES = {
    'tasks': 'Задачи',
    'files': 'Диск', 
    'calendar': 'Календарь',
    'chat': 'Чат',
    'landing_knowledge': 'База знаний',
    'search': 'Поиск'
}

# Все доступные возможности
ALL_AVAILABLE_FEATURES = {
    'tasks': 'Задачи',
    'files': 'Диск',
    'calendar': 'Календарь', 
    'chat': 'Чат',
    'forum': 'Форум',
    'blog': 'Блог',
    'landing_knowledge': 'База знаний',
    'search': 'Поиск',
    'photo': 'Фотогалерея',
    'marketplace': 'Маркет',
    'group_lists': 'Списки'
}

def get_group_info(group_id: int) -> Optional[Dict]:
    """
    Получает информацию о группе из таблицы b_sonet_group.
    """
    connection = None
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        
        cursor.execute("SELECT ID, NAME, DESCRIPTION, ACTIVE FROM b_sonet_group WHERE ID = %s", (group_id,))
        row = cursor.fetchone()
        
        if row:
            return {
                'id': row[0],
                'name': row[1], 
                'description': row[2],
                'active': row[3]
            }
        return None
        
    except Exception as e:
        print(f"💥 Ошибка получения информации о группе {group_id}: {e}")
        return None
    finally:
        if connection:
            connection.close()

def get_group_features(group_id: int) -> Dict[str, bool]:
    """
    Получает текущие возможности группы из таблицы b_sonet_features.
    
    Returns:
        Словарь {feature_name: is_active}
    """
    connection = None
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        
        cursor.execute(
            "SELECT FEATURE, ACTIVE FROM b_sonet_features WHERE ENTITY_TYPE = 'G' AND ENTITY_ID = %s",
            (group_id,)
        )
        rows = cursor.fetchall()
        
        features = {}
        for row in rows:
            feature_name = row[0]
            is_active = row[1] == 'Y'
            features[feature_name] = is_active
            
        return features
        
    except Exception as e:
        print(f"💥 Ошибка получения возможностей группы {group_id}: {e}")
        return {}
    finally:
        if connection:
            connection.close()

def view_group_features(group_id: int) -> bool:
    """
    Показывает информацию о группе и её возможностях.
    """
    print(f"🔍 ПРОСМОТР ВОЗМОЖНОСТЕЙ ГРУППЫ ID={group_id}")
    print("=" * 60)
    
    # Получаем информацию о группе
    group_info = get_group_info(group_id)
    if not group_info:
        print(f"❌ Группа с ID {group_id} не найдена!")
        return False
    
    print(f"📋 Информация о группе:")
    print(f"   ID: {group_info['id']}")
    print(f"   Название: {group_info['name']}")
    print(f"   Описание: {group_info['description'] or 'Не указано'}")
    print(f"   Активна: {'Да' if group_info['active'] == 'Y' else 'Нет'}")
    
    # Получаем возможности
    features = get_group_features(group_id)
    
    print(f"\n🎯 Текущие возможности:")
    if not features:
        print("   ⚠️ Возможности не настроены (все отключены)")
    else:
        for feature, is_active in features.items():
            feature_name = ALL_AVAILABLE_FEATURES.get(feature, feature)
            status = "✅ Включено" if is_active else "❌ Отключено"
            print(f"   {feature_name:15} ({feature:10}) - {status}")
    
    print(f"\n💡 Стандартные возможности для новых групп:")
    for feature, name in STANDARD_FEATURES.items():
        print(f"   ✅ {name} ({feature})")
    
    return True

def set_group_features(group_id: int, features_to_set: List[str], clear_existing: bool = True) -> bool:
    """
    Устанавливает возможности для группы.
    
    Args:
        group_id: ID группы
        features_to_set: Список возможностей для установки  
        clear_existing: Очистить существующие возможности перед установкой
        
    Returns:
        True если операция прошла успешно
    """
    connection = None
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        
        print(f"🔧 Установка возможностей для группы ID={group_id}")
        
        # Проверяем существование группы
        group_info = get_group_info(group_id)
        if not group_info:
            print(f"❌ Группа с ID {group_id} не найдена!")
            return False
        
        print(f"📋 Группа: {group_info['name']}")
        
        # УСОВЕРШЕНСТВОВАННЫЙ АЛГОРИТМ: Создаем записи для ВСЕХ возможностей
        current_time = datetime.now()
        success_count = 0
        
        # Сначала создаем записи для ВСЕХ доступных возможностей если их нет
        print(f"🔧 Обеспечиваем наличие записей для всех возможностей...")
        for feature_code in ALL_AVAILABLE_FEATURES.keys():
            cursor.execute(
                "SELECT ID FROM b_sonet_features WHERE ENTITY_TYPE = 'G' AND ENTITY_ID = %s AND FEATURE = %s",
                (group_id, feature_code)
            )
            if not cursor.fetchone():
                # Создаем отсутствующую запись в неактивном состоянии
                cursor.execute(
                    """INSERT INTO b_sonet_features 
                       (ENTITY_TYPE, ENTITY_ID, FEATURE, ACTIVE, DATE_CREATE, DATE_UPDATE) 
                       VALUES ('G', %s, %s, 'N', %s, %s)""",
                    (group_id, feature_code, current_time, current_time)
                )
                print(f"➕ Создана запись: {ALL_AVAILABLE_FEATURES[feature_code]} ({feature_code}) - отключено")
        
        # Теперь устанавливаем ВСЕ возможности в НЕАКТИВНОЕ состояние
        if clear_existing:
            cursor.execute(
                "UPDATE b_sonet_features SET ACTIVE = 'N', DATE_UPDATE = %s WHERE ENTITY_TYPE = 'G' AND ENTITY_ID = %s",
                (current_time, group_id)
            )
            print(f"🔄 Все возможности установлены в неактивное состояние")
        
        # Теперь устанавливаем нужные возможности в активное состояние
        for feature in features_to_set:
            if feature not in ALL_AVAILABLE_FEATURES:
                print(f"⚠️ Неизвестная возможность: {feature}")
                continue
                
            try:
                # Проверяем, есть ли уже такая возможность
                cursor.execute(
                    "SELECT ID FROM b_sonet_features WHERE ENTITY_TYPE = 'G' AND ENTITY_ID = %s AND FEATURE = %s",
                    (group_id, feature)
                )
                existing = cursor.fetchone()
                
                if existing:
                    # Обновляем существующую на активную
                    cursor.execute(
                        "UPDATE b_sonet_features SET ACTIVE = 'Y', DATE_UPDATE = %s WHERE ID = %s",
                        (current_time, existing[0])
                    )
                    print(f"✅ Активировано: {ALL_AVAILABLE_FEATURES[feature]} ({feature})")
                else:
                    # Создаем новую запись
                    cursor.execute(
                        """INSERT INTO b_sonet_features 
                           (ENTITY_TYPE, ENTITY_ID, FEATURE, ACTIVE, DATE_CREATE, DATE_UPDATE) 
                           VALUES ('G', %s, %s, 'Y', %s, %s)""",
                        (group_id, feature, current_time, current_time)
                    )
                    print(f"➕ Создано: {ALL_AVAILABLE_FEATURES[feature]} ({feature})")
                
                success_count += 1
                
            except Exception as e:
                print(f"❌ Ошибка установки возможности {feature}: {e}")
        
        # Подтверждаем изменения
        connection.commit()
        
        print(f"\n✅ Успешно установлено {success_count} возможностей для группы '{group_info['name']}'")
        return success_count > 0
        
    except Exception as e:
        print(f"💥 Ошибка установки возможностей для группы {group_id}: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            connection.close()

def get_all_groups() -> List[Dict]:
    """
    Получает список всех активных групп.
    """
    connection = None
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        
        cursor.execute(
            "SELECT ID, NAME FROM b_sonet_group WHERE ACTIVE = 'Y' ORDER BY ID"
        )
        rows = cursor.fetchall()
        
        groups = []
        for row in rows:
            groups.append({
                'id': row[0],
                'name': row[1]
            })
            
        return groups
        
    except Exception as e:
        print(f"💥 Ошибка получения списка групп: {e}")
        return []
    finally:
        if connection:
            connection.close()

def update_all_groups() -> bool:
    """
    Массовое обновление возможностей для всех групп.
    """
    print("🚀 МАССОВОЕ ОБНОВЛЕНИЕ ВОЗМОЖНОСТЕЙ ВСЕХ ГРУПП")
    print("=" * 60)
    
    groups = get_all_groups()
    if not groups:
        print("❌ Активные группы не найдены!")
        return False
    
    print(f"📋 Найдено {len(groups)} активных групп")
    
    # Подтверждение
    confirm = input(f"\n⚠️ ВНИМАНИЕ! Будут обновлены возможности для {len(groups)} групп.\nПродолжить? (yes/no): ")
    if confirm.lower() not in ['yes', 'y', 'да']:
        print("❌ Операция отменена пользователем")
        return False
    
    print(f"\n🔧 Установка стандартных возможностей:")
    for feature, name in STANDARD_FEATURES.items():
        print(f"   ✅ {name} ({feature})")
    
    print(f"\n🔄 Обработка групп:")
    success_count = 0
    error_count = 0
    
    for i, group in enumerate(groups, 1):
        try:
            print(f"\n[{i:3d}/{len(groups)}] Группа ID={group['id']}: {group['name']}")
            
            # Устанавливаем стандартные возможности
            if set_group_features(group['id'], list(STANDARD_FEATURES.keys()), clear_existing=True):
                success_count += 1
            else:
                error_count += 1
                
        except Exception as e:
            print(f"❌ Ошибка обработки группы {group['id']}: {e}")
            error_count += 1
    
    print(f"\n🎯 ИТОГИ МАССОВОГО ОБНОВЛЕНИЯ:")
    print("=" * 50)
    print(f"✅ Успешно обновлено: {success_count} групп")
    print(f"❌ Ошибок: {error_count} групп")
    print(f"📊 Всего обработано: {len(groups)} групп")
    
    return error_count == 0

def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(
        description="Управление возможностями рабочих групп Bitrix24",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s --view-group 38                           # Просмотр возможностей группы
  %(prog)s --update-group 38                         # Установка стандартных возможностей
  %(prog)s --update-group 38 --features tasks,chat  # Установка конкретных возможностей
  %(prog)s --update-all                              # Массовое обновление всех групп

Доступные возможности:
  tasks    - Задачи
  files    - Диск  
  calendar - Календарь
  chat     - Чат
  forum    - Форум
  blog     - Блог
  landing  - База знаний
  search   - Поиск

Стандартный набор: tasks, files, calendar, chat, landing
        """
    )
    
    parser.add_argument(
        '--view-group',
        type=int,
        help='Просмотр возможностей группы по ID'
    )
    
    parser.add_argument(
        '--update-group', 
        type=int,
        help='Обновление возможностей группы по ID'
    )
    
    parser.add_argument(
        '--update-all',
        action='store_true',
        help='Массовое обновление возможностей всех активных групп'
    )
    
    parser.add_argument(
        '--features',
        type=str,
        help='Список возможностей через запятую (например: tasks,files,chat)'
    )
    
    args = parser.parse_args()
    
    # Проверяем аргументы
    if not any([args.view_group, args.update_group, args.update_all]):
        parser.print_help()
        return 1
    
    # Режим просмотра группы
    if args.view_group:
        if args.features or args.update_all:
            print("❌ Параметр --view-group нельзя использовать с другими операциями")
            return 1
        
        success = view_group_features(args.view_group)
        return 0 if success else 1
    
    # Режим обновления конкретной группы
    if args.update_group:
        if args.update_all:
            print("❌ Параметры --update-group и --update-all взаимоисключающие")
            return 1
        
        # Определяем какие возможности устанавливать
        if args.features:
            features_list = [f.strip() for f in args.features.split(',')]
            print(f"🎯 Установка пользовательских возможностей: {features_list}")
        else:
            features_list = list(STANDARD_FEATURES.keys())
            print(f"🎯 Установка стандартных возможностей: {features_list}")
        
        success = set_group_features(args.update_group, features_list)
        return 0 if success else 1
    
    # Режим массового обновления
    if args.update_all:
        if args.features:
            print("❌ Параметр --features нельзя использовать с --update-all")
            return 1
        
        success = update_all_groups()
        return 0 if success else 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 