#!/usr/bin/env python3
"""
VPS скрипт для создания пользовательских полей Bitrix24 через SQL.
Читает JSON данные из Kaiten, создает поля в БД, обновляет маппинг.

Использует флаговые файлы для мониторинга:
- При старте создает custom-fields-in-progress.log
- По завершению переименовывает в custom-fields-app.log
"""
import json
import sys
import pymysql
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

# Пути к файлам
JSON_DATA_FILE = "/root/kaiten-to-bitrix/mappings/custom_fields_data.json"
MAPPING_FILE = "/root/kaiten-to-bitrix/mappings/custom_fields_mapping.json"
PROGRESS_LOG = "/root/kaiten-to-bitrix/logs/custom-fields-in-progress.log" 
COMPLETED_LOG = "/root/kaiten-to-bitrix/logs/custom-fields-app.log"

def setup_logging():
    """Настройка логирования в флаговый файл"""
    log_dir = Path(PROGRESS_LOG).parent
    log_dir.mkdir(exist_ok=True, parents=True)
    
    # Удаляем старые логи
    for old_log in [PROGRESS_LOG, COMPLETED_LOG]:
        if Path(old_log).exists():
            Path(old_log).unlink()

def log(message: str):
    """Логирование в файл и stdout"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    
    print(log_entry)
    
    with open(PROGRESS_LOG, 'a', encoding='utf-8') as f:
        f.write(log_entry + '\n')

def connect_to_mysql() -> pymysql.Connection:
    """Подключение к MySQL базе данных Bitrix"""
    try:
        connection = pymysql.connect(
            read_default_file='/root/.my.cnf',
            database='sitemanager',
            charset='utf8mb4',
            autocommit=False
        )
        log("✅ Подключение к MySQL успешно")
        return connection
    except Exception as e:
        log(f"❌ Ошибка подключения к MySQL: {e}")
        raise

def generate_field_name(kaiten_name: str, kaiten_id: str) -> str:
    """Генерирует имя поля для Bitrix"""
    # Очищаем название от спецсимволов
    clean_name = re.sub(r'[^a-zA-Zа-яА-Я0-9_]', '_', kaiten_name)
    
    # Транслитерация русских букв
    translit_map = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
    }
    
    transliterated = ''
    for char in clean_name.lower():
        transliterated += translit_map.get(char, char)
    
    # Ограничиваем длину и добавляем префикс
    prefix = "UF_KAITEN_"
    max_name_length = 50 - len(prefix) - len(kaiten_id) - 1
    short_name = transliterated[:max_name_length].rstrip('_')
    
    return f"{prefix}{short_name}_{kaiten_id}".upper()

def find_existing_field(connection: pymysql.Connection, kaiten_field_id: str) -> Optional[Dict[str, Any]]:
    """Ищет существующее поле по XML_ID"""
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT ID, FIELD_NAME, XML_ID FROM b_user_field WHERE XML_ID = %s AND ENTITY_ID = 'TASKS_TASK'",
            (kaiten_field_id,)
        )
        
        row = cursor.fetchone()
        if row:
            return {
                'id': row[0],
                'field_name': row[1],
                'xml_id': row[2]
            }
        return None
        
    except Exception as e:
        log(f"❌ Ошибка поиска поля {kaiten_field_id}: {e}")
        return None

def get_existing_field_values_mapping(connection: pymysql.Connection, field_id: int) -> Dict[str, int]:
    """
    ✅ КРИТИЧНО: Получает маппинг существующих значений поля
    Нужно для заполнения values_mapping когда поле уже существует
    """
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT XML_ID, ID FROM b_user_field_enum 
            WHERE USER_FIELD_ID = %s AND XML_ID IS NOT NULL AND XML_ID != ''
        """, (field_id,))
        
        results = cursor.fetchall()
        values_mapping = {}
        
        for xml_id, enum_id in results:
            values_mapping[str(xml_id)] = enum_id
            log(f"   📋 Найдено существующее значение: Kaiten {xml_id} -> Bitrix {enum_id}")
        
        log(f"✅ Получен маппинг существующих значений: {len(values_mapping)} элементов")
        return values_mapping
        
    except Exception as e:
        log(f"❌ Ошибка получения существующих значений: {e}")
        return {}

def get_next_available_ids(connection: pymysql.Connection) -> tuple:
    """Получает следующие доступные ID"""
    try:
        cursor = connection.cursor()
        
        # Следующий ID для поля
        cursor.execute("SELECT MAX(ID) FROM b_user_field")
        max_field_id = cursor.fetchone()[0] or 0
        next_field_id = max_field_id + 1
        
        # Следующий ID для enum
        cursor.execute("SELECT MAX(ID) FROM b_user_field_enum")
        max_enum_id = cursor.fetchone()[0] or 0
        next_enum_id = max_enum_id + 1
        
        # Следующий SORT для задач
        cursor.execute("SELECT MAX(SORT) FROM b_user_field WHERE ENTITY_ID = 'TASKS_TASK'")
        max_sort = cursor.fetchone()[0] or 0
        next_sort = max_sort + 100
        
        return next_field_id, next_enum_id, next_sort
        
    except Exception as e:
        log(f"❌ Ошибка получения следующих ID: {e}")
        return 116, 27, 200  # Значения по умолчанию

def create_field_in_db(connection: pymysql.Connection, field_data: Dict[str, Any]) -> bool:
    """Создает пользовательское поле в БД"""
    try:
        cursor = connection.cursor()
        
        sql = """
            INSERT INTO b_user_field 
            (ID, ENTITY_ID, FIELD_NAME, USER_TYPE_ID, XML_ID, SORT, MULTIPLE, 
             MANDATORY, SHOW_FILTER, SHOW_IN_LIST, EDIT_IN_LIST, IS_SEARCHABLE, SETTINGS)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(sql, (
            field_data['id'],
            field_data['entity_id'],
            field_data['field_name'],
            field_data['user_type_id'],
            field_data['xml_id'],
            field_data['sort'],
            field_data['multiple'],
            field_data['mandatory'],
            field_data['show_filter'],
            field_data['show_in_list'],
            field_data['edit_in_list'],
            field_data['is_searchable'],
            field_data['settings']
        ))
        
        connection.commit()
        log(f"✅ Поле {field_data['field_name']} создано (ID: {field_data['id']})")
        return True
        
    except Exception as e:
        log(f"❌ Ошибка создания поля: {e}")
        connection.rollback()
        return False

def create_field_enum_in_db(connection: pymysql.Connection, enum_data: Dict[str, Any]) -> bool:
    """Создает значение поля-списка в БД"""
    try:
        cursor = connection.cursor()
        
        sql = """
            INSERT INTO b_user_field_enum 
            (ID, USER_FIELD_ID, VALUE, DEF, SORT, XML_ID)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(sql, (
            enum_data['id'],
            enum_data['user_field_id'],
            enum_data['value'],
            enum_data['def'],
            enum_data['sort'],
            enum_data['xml_id']
        ))
        
        connection.commit()
        log(f"✅ Значение '{enum_data['value']}' создано (ID: {enum_data['id']})")
        return True
        
    except Exception as e:
        log(f"❌ Ошибка создания значения: {e}")
        connection.rollback()
        return False

def create_uts_column(connection: pymysql.Connection, field_name: str, field_type: str) -> bool:
    """
    ✅ КРИТИЧНО: Создает столбец в таблице b_uts_tasks_task для пользовательского поля
    Без этого поле не будет работать в UI задач!
    """
    try:
        cursor = connection.cursor()
        
        # Определяем тип столбца MySQL на основе типа поля Kaiten
        if field_type in ['select', 'multi_select']:
            mysql_type = 'text'  # Для списков используем text (как UF_PROJECT)
        elif field_type in ['string', 'text']:
            mysql_type = 'text'
        elif field_type in ['number', 'integer']:
            mysql_type = 'int'
        elif field_type in ['date', 'datetime']:
            mysql_type = 'datetime'
        else:
            mysql_type = 'text'  # По умолчанию text
        
        # Проверяем, существует ли столбец
        check_sql = """
            SELECT COUNT(*) as column_exists 
            FROM information_schema.columns 
            WHERE table_name = 'b_uts_tasks_task' 
            AND table_schema = 'sitemanager'
            AND column_name = %s
        """
        
        cursor.execute(check_sql, (field_name,))
        result = cursor.fetchone()
        
        if result and result[0] > 0:
            log(f"⏭️ Столбец {field_name} уже существует в b_uts_tasks_task")
            return True
        
        # Создаем столбец
        add_column_sql = f"""
            ALTER TABLE b_uts_tasks_task 
            ADD COLUMN {field_name} {mysql_type} NULL
        """
        
        cursor.execute(add_column_sql)
        connection.commit()
        
        log(f"✅ Столбец {field_name} создан в b_uts_tasks_task (тип: {mysql_type})")
        return True
        
    except Exception as e:
        log(f"❌ Ошибка создания столбца UTS: {e}")
        connection.rollback()
        return False

def create_field_lang_in_db(connection: pymysql.Connection, lang_data: Dict[str, Any]) -> bool:
    """Создает языковую версию поля в БД"""
    try:
        cursor = connection.cursor()
        
        sql = """
            INSERT INTO b_user_field_lang 
            (USER_FIELD_ID, LANGUAGE_ID, EDIT_FORM_LABEL, LIST_COLUMN_LABEL, 
             LIST_FILTER_LABEL, ERROR_MESSAGE, HELP_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(sql, (
            lang_data['user_field_id'],
            lang_data['language_id'],
            lang_data['edit_form_label'],
            lang_data['list_column_label'],
            lang_data['list_filter_label'],
            lang_data['error_message'],
            lang_data['help_message']
        ))
        
        connection.commit()
        log(f"✅ Языковая версия {lang_data['language_id']} создана")
        return True
        
    except Exception as e:
        log(f"❌ Ошибка создания языковой версии: {e}")
        connection.rollback()
        return False

def process_single_field(connection: pymysql.Connection, kaiten_field_id: str, 
                        field_data: Dict[str, Any], current_field_id: int, 
                        current_enum_id: int, current_sort: int) -> Dict[str, Any]:
    """Обрабатывает одно поле"""
    
    field_info = field_data['field_info']
    kaiten_values = field_data['values']
    
    kaiten_field_name = field_info.get('name', 'Unknown')
    field_type = field_info.get('type', 'select')
    
    log(f"📄 Обрабатываю поле '{kaiten_field_name}' (ID: {kaiten_field_id})")
    
    # Проверяем существование поля
    existing_field = find_existing_field(connection, kaiten_field_id)
    if existing_field:
        log(f"⏭️ Поле уже существует (ID: {existing_field['id']})")
        
        # ✅ ИСПРАВЛЕНО: Получаем существующий маппинг значений
        existing_values_mapping = get_existing_field_values_mapping(connection, existing_field['id'])
        
        return {
            'success': True,
            'bitrix_field_id': existing_field['id'],
            'bitrix_field_name': existing_field['field_name'],
            'values_mapping': existing_values_mapping,  # ✅ Теперь получаем реальный маппинг!
            'created': False
        }
    
    # Генерируем имя поля
    field_name = generate_field_name(kaiten_field_name, kaiten_field_id)
    
    # Создаем поле
    field_db_data = {
        'id': current_field_id,
        'entity_id': 'TASKS_TASK',
        'field_name': field_name,
        'user_type_id': 'enumeration' if field_type in ['select', 'multi_select'] else 'string',
        'xml_id': kaiten_field_id,
        'sort': 500 + current_field_id,  # ✅ Исправлено: начинаем с 500 как в рабочем поле
        'multiple': 'Y',  # ✅ Исправлено: всегда множественное (как поле 142)
        'mandatory': 'N',
        'show_filter': 'S',  # ✅ Исправлено: поиск по подстроке (как поле 142)
        'show_in_list': 'Y',
        'edit_in_list': 'Y',
        'is_searchable': 'Y',
        'settings': 'a:4:{s:7:"DISPLAY";s:2:"UI";s:11:"LIST_HEIGHT";i:1;s:16:"CAPTION_NO_VALUE";s:0:"";s:13:"SHOW_NO_VALUE";s:1:"Y";}'  # ✅ Исправлено: DISPLAY="UI"
    }
    
    if not create_field_in_db(connection, field_db_data):
        return {'success': False, 'error': 'Failed to create field'}
    
    # ✅ КРИТИЧНО: Создаем столбец в таблице значений b_uts_tasks_task
    if not create_uts_column(connection, field_name, field_type):
        log(f"⚠️ Не удалось создать столбец UTS для поля {field_name}")
        # Продолжаем выполнение, но поле может не работать в UI
    
    # Создаем значения поля
    values_mapping = {}
    enum_id = current_enum_id
    
    if kaiten_values and field_type in ['select', 'multi_select']:
        log(f"📋 Создаю {len(kaiten_values)} значений для поля")
        
        for i, kaiten_value in enumerate(kaiten_values):
            kaiten_value_id = str(kaiten_value.get('id', ''))
            value_text = kaiten_value.get('value', f'Значение_{kaiten_value_id}')
            
            enum_db_data = {
                'id': enum_id,
                'user_field_id': current_field_id,
                'value': value_text,
                'def': 'N',
                'sort': 500 + (i * 100),  # ✅ Исправлено: начинаем с 500 как в рабочем поле
                'xml_id': kaiten_value_id
            }
            
            if create_field_enum_in_db(connection, enum_db_data):
                values_mapping[kaiten_value_id] = enum_id
                enum_id += 1
            else:
                log(f"⚠️ Не удалось создать значение '{value_text}'")
    
    # Создаем языковые версии (простые как в рабочем поле)
    for lang_id in ['ru', 'en']:
        if lang_id == 'ru':
            lang_name = kaiten_field_name  # Оригинальное название
        else:
            # Простое английское название без излишеств
            lang_name = kaiten_field_name  # Можно оставить то же название
        
        lang_db_data = {
            'user_field_id': current_field_id,
            'language_id': lang_id,
            'edit_form_label': lang_name,
            'list_column_label': '',  # ✅ Пустые как в рабочем поле
            'list_filter_label': '',  # ✅ Пустые как в рабочем поле
            'error_message': '',      # ✅ Пустые как в рабочем поле
            'help_message': ''        # ✅ Пустые как в рабочем поле
        }
        
        create_field_lang_in_db(connection, lang_db_data)
    
    log(f"✅ Поле '{kaiten_field_name}' успешно создано")
    
    return {
        'success': True,
        'bitrix_field_id': current_field_id,
        'bitrix_field_name': field_name,
        'values_mapping': values_mapping,
        'created': True
    }

def main():
    """Основная функция"""
    
    # Настраиваем логирование
    setup_logging()
    
    log("🚀 НАЧАЛО СОЗДАНИЯ ПОЛЬЗОВАТЕЛЬСКИХ ПОЛЕЙ BITRIX24")
    log("=" * 60)
    
    try:
        # Проверяем наличие JSON файла
        if not Path(JSON_DATA_FILE).exists():
            log(f"❌ JSON файл не найден: {JSON_DATA_FILE}")
            return False
        
        # Читаем данные Kaiten
        log("📥 Чтение данных Kaiten...")
        with open(JSON_DATA_FILE, 'r', encoding='utf-8') as f:
            kaiten_data = json.load(f)
        
        fields_data = kaiten_data.get('fields', {})
        if not fields_data:
            log("⚠️ Нет полей для обработки")
            return True
        
        log(f"📊 Найдено {len(fields_data)} полей для обработки")
        
        # Подключаемся к БД
        log("🔗 Подключение к базе данных...")
        connection = connect_to_mysql()
        
        # Получаем следующие доступные ID
        current_field_id, current_enum_id, current_sort = get_next_available_ids(connection)
        log(f"📍 Следующие ID: Field={current_field_id}, Enum={current_enum_id}, Sort={current_sort}")
        
        # Обрабатываем поля
        mapping_result = {
            'created_at': datetime.now().isoformat(),
            'description': 'Маппинг пользовательских полей Kaiten -> Bitrix',
            'fields': {}
        }
        
        created_fields = 0
        updated_fields = 0
        total_values = 0
        
        for kaiten_field_id, field_data in fields_data.items():
            try:
                result = process_single_field(
                    connection, kaiten_field_id, field_data,
                    current_field_id, current_enum_id, current_sort
                )
                
                if result['success']:
                    # Сохраняем маппинг
                    mapping_result['fields'][kaiten_field_id] = {
                        'kaiten_field': field_data['field_info'],
                        'bitrix_field_id': result['bitrix_field_id'],
                        'bitrix_field_name': result['bitrix_field_name'],
                        'values_mapping': result['values_mapping'],
                        'created_at': datetime.now().isoformat()
                    }
                    
                    if result['created']:
                        created_fields += 1
                        current_field_id += 1
                        current_enum_id += len(result['values_mapping'])
                        current_sort += 100
                    else:
                        updated_fields += 1
                    
                    total_values += len(result['values_mapping'])
                else:
                    log(f"❌ Ошибка обработки поля {kaiten_field_id}: {result.get('error', 'Unknown')}")
                
            except Exception as e:
                log(f"❌ Критическая ошибка обработки поля {kaiten_field_id}: {e}")
                continue
        
        # Сохраняем маппинг
        log("💾 Сохранение маппинга...")
        mapping_file_path = Path(MAPPING_FILE)
        mapping_file_path.parent.mkdir(exist_ok=True, parents=True)
        
        with open(MAPPING_FILE, 'w', encoding='utf-8') as f:
            json.dump(mapping_result, f, ensure_ascii=False, indent=2)
        
        # Закрываем соединение
        connection.close()
        
        # Итоговая статистика
        log("📊 ИТОГОВАЯ СТАТИСТИКА:")
        log(f"   Полей создано: {created_fields}")
        log(f"   Полей обновлено: {updated_fields}")
        log(f"   Значений создано: {total_values}")
        log(f"   Маппинг сохранен: {MAPPING_FILE}")
        
        log("✅ СОЗДАНИЕ ПОЛЬЗОВАТЕЛЬСКИХ ПОЛЕЙ ЗАВЕРШЕНО УСПЕШНО!")
        
        # Переименовываем лог файл для сигнализации завершения
        Path(PROGRESS_LOG).rename(COMPLETED_LOG)
        
        return True
        
    except Exception as e:
        log(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 