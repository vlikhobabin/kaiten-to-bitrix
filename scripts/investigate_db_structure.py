#!/usr/bin/env python3
"""
Скрипт для исследования структуры базы данных Bitrix24 - поиск таблиц с возможностями рабочих групп.
Выполняется на VPS сервере с прямым доступом к MySQL.

Использование:
    python3 investigate_db_structure.py
"""

import sys
import json
import pymysql
from typing import List, Dict, Any

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

def find_workgroup_tables() -> List[str]:
    """
    Ищет таблицы, связанные с рабочими группами и возможностями.
    """
    connection = None
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        
        print("🔍 ПОИСК ТАБЛИЦ РАБОЧИХ ГРУПП И ВОЗМОЖНОСТЕЙ")
        print("=" * 70)
        
        # Поиск таблиц, связанных с группами
        search_patterns = [
            'sonet',      # социальные сети
            'group',      # группы  
            'workgroup',  # рабочие группы
            'feature',    # возможности
            'tool'        # инструменты
        ]
        
        relevant_tables = []
        
        for pattern in search_patterns:
            print(f"\n🔎 Поиск таблиц с паттерном '{pattern}':")
            
            sql = "SHOW TABLES LIKE %s"
            cursor.execute(sql, (f'%{pattern}%',))
            tables = cursor.fetchall()
            
            if tables:
                pattern_tables = [table[0] for table in tables]
                relevant_tables.extend(pattern_tables)
                
                for table in pattern_tables:
                    print(f"   ✅ {table}")
            else:
                print(f"   ⚠️ Таблицы с '{pattern}' не найдены")
        
        return list(set(relevant_tables))  # убираем дубликаты
        
    except Exception as e:
        print(f"💥 Ошибка поиска таблиц: {e}")
        return []
    finally:
        if connection:
            connection.close()

def analyze_table_structure(table_name: str) -> Dict[str, Any]:
    """
    Анализирует структуру указанной таблицы.
    """
    connection = None
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        
        print(f"\n📋 АНАЛИЗ СТРУКТУРЫ ТАБЛИЦЫ: {table_name}")
        print("-" * 50)
        
        # Получаем структуру таблицы
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        
        print("📊 Поля таблицы:")
        features_fields = []
        
        for column in columns:
            field_name = column[0]
            field_type = column[1]
            field_null = column[2]
            field_key = column[3]
            field_default = column[4]
            
            print(f"   {field_name:20} | {field_type:15} | NULL: {field_null:3} | Key: {field_key:3} | Default: {field_default}")
            
            # Ищем поля, связанные с возможностями
            field_lower = field_name.lower()
            if any(keyword in field_lower for keyword in ['feature', 'tool', 'active', 'enable', 'use', 'task', 'file', 'calendar', 'chat', 'forum', 'blog', 'photo', 'wiki', 'search']):
                features_fields.append(field_name)
        
        if features_fields:
            print(f"\n🎯 ПОЛЯ ВОЗМОЖНОСТЕЙ: {features_fields}")
        
        # Получаем количество записей
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"\n📈 Количество записей: {count}")
        
        # Если записей не много, показываем примеры
        if count > 0 and count <= 20:
            print(f"\n📄 Примеры записей (первые 5):")
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
            rows = cursor.fetchall()
            
            for i, row in enumerate(rows, 1):
                print(f"   Запись {i}: {row}")
        
        return {
            'table_name': table_name,
            'columns': [col[0] for col in columns],
            'features_fields': features_fields,
            'record_count': count
        }
        
    except Exception as e:
        print(f"💥 Ошибка анализа таблицы {table_name}: {e}")
        return {}
    finally:
        if connection:
            connection.close()

def find_group_features_data(group_id: int = 38) -> None:
    """
    Ищет данные о возможностях для конкретной группы.
    """
    connection = None
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        
        print(f"\n🎯 ПОИСК ДАННЫХ О ВОЗМОЖНОСТЯХ ДЛЯ ГРУППЫ ID={group_id}")
        print("=" * 60)
        
        # Ищем группу в основных таблицах
        possible_group_tables = ['b_sonet_group', 'b_sonet_groups', 'b_workgroup', 'b_group']
        
        group_found = False
        for table in possible_group_tables:
            try:
                cursor.execute(f"SELECT * FROM {table} WHERE ID = %s", (group_id,))
                row = cursor.fetchone()
                
                if row:
                    group_found = True
                    print(f"✅ Группа найдена в таблице: {table}")
                    
                    # Получаем названия колонок
                    cursor.execute(f"DESCRIBE {table}")
                    columns = [col[0] for col in cursor.fetchall()]
                    
                    # Создаем словарь для удобства
                    group_data = dict(zip(columns, row))
                    
                    print(f"📋 Основные поля группы:")
                    for key, value in group_data.items():
                        if key in ['ID', 'NAME', 'DESCRIPTION', 'ACTIVE', 'VISIBLE', 'OPENED', 'PROJECT']:
                            print(f"   {key}: {value}")
                    
                    # Ищем поля с возможностями
                    features_data = {}
                    for key, value in group_data.items():
                        if any(keyword in key.lower() for keyword in ['feature', 'tool', 'active', 'enable', 'use', 'task', 'file', 'calendar', 'chat', 'forum', 'blog', 'photo', 'wiki', 'search']):
                            features_data[key] = value
                    
                    if features_data:
                        print(f"\n🎯 Поля возможностей в основной таблице:")
                        for key, value in features_data.items():
                            print(f"   {key}: {value}")
                    else:
                        print("⚠️ Поля возможностей не найдены в основной таблице")
                    
                    break
            except Exception as e:
                print(f"⚠️ Таблица {table} не существует или недоступна: {e}")
        
        if not group_found:
            print(f"❌ Группа ID={group_id} не найдена в основных таблицах")
        
        # Ищем связанные таблицы с возможностями
        possible_features_tables = [
            'b_sonet_group_features',
            'b_sonet_features', 
            'b_workgroup_features',
            'b_group_features',
            'b_sonet_group_feature',
            'b_group_feature'
        ]
        
        print(f"\n🔍 Поиск в таблицах возможностей:")
        for table in possible_features_tables:
            try:
                # Пробуем разные варианты связывания
                queries = [
                    f"SELECT * FROM {table} WHERE GROUP_ID = %s",
                    f"SELECT * FROM {table} WHERE ENTITY_ID = %s", 
                    f"SELECT * FROM {table} WHERE SONET_GROUP_ID = %s"
                ]
                
                found_data = False
                for query in queries:
                    try:
                        cursor.execute(query, (group_id,))
                        rows = cursor.fetchall()
                        
                        if rows:
                            found_data = True
                            print(f"✅ Данные найдены в таблице {table}:")
                            
                            # Получаем названия колонок
                            cursor.execute(f"DESCRIBE {table}")
                            columns = [col[0] for col in cursor.fetchall()]
                            
                            for row in rows:
                                row_data = dict(zip(columns, row))
                                print(f"   📄 {row_data}")
                            break
                    except Exception as e:
                        continue
                
                if not found_data:
                    print(f"⚠️ Данные в таблице {table} не найдены")
                    
            except Exception as e:
                print(f"⚠️ Таблица {table} не существует: {e}")
        
        # Поиск в таблицах с JSON или текстовыми полями
        print(f"\n🔍 Поиск JSON/сериализованных данных с возможностями:")
        json_tables = ['b_sonet_group', 'b_option']
        
        for table in json_tables:
            try:
                if table == 'b_option':
                    # Ищем настройки по умолчанию
                    cursor.execute("SELECT * FROM b_option WHERE MODULE_ID = 'socialnetwork' AND NAME LIKE '%feature%' OR NAME LIKE '%tool%'")
                    rows = cursor.fetchall()
                    
                    if rows:
                        print(f"✅ Настройки возможностей в {table}:")
                        for row in rows:
                            print(f"   {row}")
                else:
                    # Ищем поля с JSON данными
                    cursor.execute(f"DESCRIBE {table}")
                    columns = cursor.fetchall()
                    
                    text_fields = [col[0] for col in columns if 'text' in col[1].lower() or 'json' in col[1].lower()]
                    
                    if text_fields:
                        cursor.execute(f"SELECT ID, {', '.join(text_fields)} FROM {table} WHERE ID = %s", (group_id,))
                        row = cursor.fetchone()
                        
                        if row:
                            print(f"✅ Текстовые поля в {table}:")
                            for i, field in enumerate(['ID'] + text_fields):
                                value = row[i]
                                if value and ('feature' in str(value).lower() or 'tool' in str(value).lower()):
                                    print(f"   {field}: {value}")
                        
            except Exception as e:
                print(f"⚠️ Ошибка поиска в {table}: {e}")
        
    except Exception as e:
        print(f"💥 Ошибка поиска данных группы: {e}")
    finally:
        if connection:
            connection.close()

def search_feature_keywords() -> None:
    """
    Ищет таблицы и поля содержащие ключевые слова возможностей.
    """
    connection = None
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        
        print(f"\n🔍 ПОИСК ПО КЛЮЧЕВЫМ СЛОВАМ ВОЗМОЖНОСТЕЙ")
        print("=" * 60)
        
        # Ключевые слова для поиска
        keywords = ['tasks', 'files', 'calendar', 'chat', 'forum', 'blog', 'photo', 'wiki', 'search', 'landing']
        
        # Получаем все таблицы
        cursor.execute("SHOW TABLES")
        all_tables = [table[0] for table in cursor.fetchall()]
        
        for keyword in keywords:
            print(f"\n🔎 Поиск таблиц и полей с '{keyword}':")
            
            # Поиск таблиц
            matching_tables = [table for table in all_tables if keyword in table.lower()]
            if matching_tables:
                print(f"   📋 Таблицы: {matching_tables}")
            
            # Поиск полей в существующих таблицах групп
            group_tables = [table for table in all_tables if 'sonet' in table.lower() or 'group' in table.lower()]
            
            for table in group_tables[:5]:  # Ограничиваем количество для скорости
                try:
                    cursor.execute(f"DESCRIBE {table}")
                    columns = cursor.fetchall()
                    
                    matching_fields = [col[0] for col in columns if keyword in col[0].lower()]
                    if matching_fields:
                        print(f"   📊 {table}: {matching_fields}")
                        
                except Exception as e:
                    continue
        
    except Exception as e:
        print(f"💥 Ошибка поиска по ключевым словам: {e}")
    finally:
        if connection:
            connection.close()

def main():
    """Основная функция"""
    print("🚀 ИССЛЕДОВАНИЕ СТРУКТУРЫ БД BITRIX24 ДЛЯ ВОЗМОЖНОСТЕЙ ГРУПП")
    print("=" * 80)
    
    # 1. Поиск релевантных таблиц
    print("📍 ШАГ 1: Поиск таблиц")
    relevant_tables = find_workgroup_tables()
    
    if relevant_tables:
        print(f"\n📋 Найдено {len(relevant_tables)} релевантных таблиц:")
        for table in relevant_tables:
            print(f"   • {table}")
    else:
        print("❌ Релевантные таблицы не найдены")
    
    # 2. Анализ структуры каждой таблицы
    print(f"\n📍 ШАГ 2: Анализ структуры таблиц")
    table_analysis = []
    
    for table in relevant_tables:
        analysis = analyze_table_structure(table)
        if analysis:
            table_analysis.append(analysis)
    
    # 3. Поиск данных для тестовой группы
    print(f"\n📍 ШАГ 3: Поиск данных для тестовой группы")
    find_group_features_data(38)  # Тестовая группа ID=38
    
    # 4. Поиск по ключевым словам
    print(f"\n📍 ШАГ 4: Поиск по ключевым словам")
    search_feature_keywords()
    
    # 5. Итоговый отчет
    print(f"\n🎯 ИТОГОВЫЙ ОТЧЕТ:")
    print("=" * 50)
    
    tables_with_features = [t for t in table_analysis if t.get('features_fields')]
    
    if tables_with_features:
        print("✅ ТАБЛИЦЫ С ВОЗМОЖНЫМИ ПОЛЯМИ ВОЗМОЖНОСТЕЙ:")
        for table in tables_with_features:
            print(f"   📋 {table['table_name']}: {table['features_fields']}")
    else:
        print("⚠️ Таблицы с явными полями возможностей не найдены")
    
    print(f"\n💡 РЕКОМЕНДАЦИИ:")
    print("1. Проверьте найденные таблицы в веб-интерфейсе БД")
    print("2. Анализируйте AJAX запросы при изменении возможностей группы")
    print("3. Возможности могут храниться в JSON полях или сериализованном виде")
    print("4. Проверьте настройки по умолчанию в админке Bitrix24")
    print("5. Возможно возможности хранятся в таблице b_option с ключами модуля socialnetwork")

if __name__ == "__main__":
    main() 