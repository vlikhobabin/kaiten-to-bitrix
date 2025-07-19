#!/usr/bin/env python3
"""
Скрипт для обновления дат комментариев в базе данных Bitrix24.
Выполняется на VPS сервере с прямым доступом к MySQL.

Использование:
    python3 update_comment_dates.py '{"comment_id": "2025-07-08 14:22:00", ...}'
"""

import sys
import json
import pymysql
from datetime import datetime
from typing import Dict, Any

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

def update_comment_dates(comment_dates: Dict[str, str]) -> bool:
    """
    Обновляет даты комментариев в таблице b_forum_message
    
    Args:
        comment_dates: Словарь {comment_id: datetime_string}
        
    Returns:
        True если все обновления прошли успешно
    """
    if not comment_dates:
        print("⚠️ Нет данных для обновления")
        return True
    
    connection = None
    success_count = 0
    error_count = 0
    
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        
        print(f"🔄 Начинаем обновление дат для {len(comment_dates)} комментариев...")
        
        for comment_id, date_str in comment_dates.items():
            try:
                # Проверяем формат даты
                try:
                    # Ожидаем формат ISO: 2025-07-08T14:22:00 или MySQL: 2025-07-08 14:22:00
                    if 'T' in date_str:
                        # ISO формат - конвертируем в MySQL формат
                        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        mysql_date = date_obj.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # Уже в MySQL формате
                        mysql_date = date_str
                        # Проверяем корректность
                        datetime.strptime(mysql_date, '%Y-%m-%d %H:%M:%S')
                except ValueError as e:
                    print(f"❌ Некорректный формат даты для комментария {comment_id}: {date_str}")
                    error_count += 1
                    continue
                
                # SQL запрос для обновления даты комментария
                sql = """
                UPDATE b_forum_message 
                SET POST_DATE = %s 
                WHERE ID = %s
                """
                
                cursor.execute(sql, (mysql_date, int(comment_id)))
                
                if cursor.rowcount > 0:
                    print(f"✅ Комментарий {comment_id}: дата обновлена на {mysql_date}")
                    success_count += 1
                else:
                    print(f"⚠️ Комментарий {comment_id}: не найден в базе данных")
                    error_count += 1
                    
            except Exception as e:
                print(f"❌ Ошибка обновления комментария {comment_id}: {e}")
                error_count += 1
                continue
        
        # Подтверждаем изменения
        connection.commit()
        
        print(f"\n📊 Результат обновления:")
        print(f"✅ Успешно обновлено: {success_count}")
        print(f"❌ Ошибки: {error_count}")
        
        return error_count == 0
        
    except Exception as e:
        print(f"💥 Критическая ошибка: {e}")
        if connection:
            connection.rollback()
        return False
        
    finally:
        if connection:
            connection.close()

def main():
    """Основная функция"""
    if len(sys.argv) != 2:
        print("❌ Использование: python3 update_comment_dates.py '<json_data>'")
        print("Пример: python3 update_comment_dates.py '{\"601\": \"2025-07-08 14:22:00\"}'")
        sys.exit(1)
    
    try:
        # Парсим JSON из аргумента
        json_data = sys.argv[1]
        comment_dates = json.loads(json_data)
        
        if not isinstance(comment_dates, dict):
            print("❌ JSON должен быть объектом с парами comment_id: datetime")
            sys.exit(1)
        
        print(f"🚀 Запуск обновления дат комментариев...")
        print(f"📝 Получено {len(comment_dates)} комментариев для обновления")
        
        # Выполняем обновление
        success = update_comment_dates(comment_dates)
        
        if success:
            print("✅ Все обновления выполнены успешно!")
            sys.exit(0)
        else:
            print("❌ Обновление завершено с ошибками!")
            sys.exit(1)
            
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка парсинга JSON: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"💥 Неожиданная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 