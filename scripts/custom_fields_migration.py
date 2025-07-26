#!/usr/bin/env python3
"""
Скрипт для миграции пользовательских полей из Kaiten в Bitrix24.
Использует двухэтапный процесс: локально получает данные, на VPS создает поля.

Использование:
    python3 scripts/custom_fields_migration.py [--dry-run]
"""

import sys
import asyncio
import argparse
from pathlib import Path

# Добавляем корневую директорию проекта в путь
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from connectors.kaiten_client import KaitenClient
from migrators.custom_field_migrator import CustomFieldMigrator
from utils.logger import get_logger

logger = get_logger(__name__)


async def main():
    """Основная функция миграции пользовательских полей"""
    
    parser = argparse.ArgumentParser(description='Миграция пользовательских полей Kaiten -> Bitrix24')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Тестовый запуск - только получение данных из Kaiten без создания на VPS')
    
    args = parser.parse_args()
    
    print("🚀 МИГРАЦИЯ ПОЛЬЗОВАТЕЛЬСКИХ ПОЛЕЙ KAITEN -> BITRIX24")
    print("=" * 70)
    
    if args.dry_run:
        print("⚠️  ТЕСТОВЫЙ РЕЖИМ - поля на VPS создаваться не будут")
        print()
    
    try:
        # Инициализируем клиенты
        print("🔗 Инициализация подключений...")
        kaiten_client = KaitenClient()
        migrator = CustomFieldMigrator(kaiten_client)
        
        # Проверяем подключение к Kaiten API
        print("🧪 Проверка подключения к Kaiten API...")
        test_properties = await kaiten_client.get_custom_properties()
        
        if not test_properties:
            print("❌ Не удалось получить пользовательские поля из Kaiten")
            print("💡 Проверьте настройки KAITEN_API_TOKEN в env.txt")
            return
        
        print(f"✅ Kaiten API: найдено {len(test_properties)} пользовательских полей")
        
        # В dry-run режиме только получаем и показываем данные
        if args.dry_run:
            print()
            print("📊 АНАЛИЗ ПОЛЕЙ ДЛЯ МИГРАЦИИ:")
            print("-" * 50)
            
            # Получаем данные из Kaiten
            kaiten_data = await migrator._fetch_kaiten_data()
            
            if not kaiten_data.get('fields'):
                print("⚠️ Нет пользовательских полей для миграции")
                return
            
            # Анализируем поля
            field_types = {}
            total_values = 0
            
            for field_id, field_data in kaiten_data['fields'].items():
                field_info = field_data['field_info']
                field_values = field_data['values']
                
                field_type = field_info.get('type', 'unknown')
                field_name = field_info.get('name', 'N/A')
                multi_select = field_info.get('multi_select', False)
                
                field_types[field_type] = field_types.get(field_type, 0) + 1
                total_values += len(field_values)
                
                print(f"   📄 {field_name} (ID: {field_id})")
                print(f"       Тип: {field_type}, Множественный: {'Да' if multi_select else 'Нет'}")
                print(f"       Значений: {len(field_values)}")
                
                # Показываем первые несколько значений
                if field_values:
                    sample_values = field_values[:3]
                    values_text = [v.get('value', 'N/A') for v in sample_values]
                    if len(field_values) > 3:
                        values_text.append('...')
                    print(f"       Примеры: {', '.join(values_text)}")
                print()
            
            print(f"📈 Статистика по типам:")
            for field_type, count in field_types.items():
                print(f"   {field_type}: {count} полей")
            
            print(f"\n📊 Общая статистика:")
            print(f"   Всего полей: {len(kaiten_data['fields'])}")
            print(f"   Всего значений: {total_values}")
            
            print()
            print("✅ АНАЛИЗ ЗАВЕРШЕН (dry-run режим)")
            print("💡 Для выполнения миграции запустите скрипт без --dry-run")
            print("💡 Убедитесь что SSH настройки корректны в env.txt:")
            print("   SSH_HOST, SSH_USER, SSH_KEY_PATH")
            
            return
        
        # Проверяем SSH настройки для VPS
        from config.settings import settings
        if not settings.ssh_host or not settings.ssh_key_path:
            print("❌ SSH настройки не настроены!")
            print("💡 Добавьте в env.txt:")
            print("   SSH_HOST=your.vps.server")
            print("   SSH_USER=root")
            print("   SSH_KEY_PATH=/path/to/ssh/key")
            return
        
        print(f"✅ SSH настройки: {settings.ssh_user}@{settings.ssh_host}")
        
        print()
        print("⚠️  ВНИМАНИЕ: Будут внесены изменения в базу данных Bitrix24 на VPS!")
        print("   - Создание пользовательских полей через SQL")
        print("   - Создание значений полей")
        print("   - Создание языковых версий")
        print()
        
        confirm = input("Продолжить миграцию? (yes/no): ").lower().strip()
        if confirm not in ['yes', 'y', 'да', 'д']:
            print("❌ Миграция отменена пользователем")
            return
        
        print()
        print("🚀 НАЧАЛО МИГРАЦИИ...")
        print("=" * 50)
        
        # Выполняем миграцию
        result = await migrator.migrate_all_custom_fields()
        
        # Выводим результаты
        print()
        print("📊 РЕЗУЛЬТАТЫ МИГРАЦИИ:")
        print("=" * 50)
        
        if result['success']:
            mapping_data = result.get('mapping', {})
            fields_count = len(mapping_data.get('fields', {}))
            
            print(f"✅ Миграция завершена успешно!")
            print(f"📋 Обработано полей: {fields_count}")
            
            if 'log_file' in result:
                print(f"📄 Лог VPS: {result['log_file']}")
            
            # Показываем созданные поля
            if fields_count > 0:
                print()
                print("🔗 СОЗДАННЫЕ ПОЛЯ:")
                print("-" * 30)
                
                for kaiten_id, field_mapping in mapping_data.get('fields', {}).items():
                    kaiten_field = field_mapping.get('kaiten_field', {})
                    field_name = kaiten_field.get('name', 'N/A')
                    bitrix_field_id = field_mapping.get('bitrix_field_id', 'N/A')
                    bitrix_field_name = field_mapping.get('bitrix_field_name', 'N/A')
                    values_count = len(field_mapping.get('values_mapping', {}))
                    
                    print(f"   📄 {field_name}")
                    print(f"       Kaiten ID: {kaiten_id}")
                    print(f"       Bitrix ID: {bitrix_field_id}")
                    print(f"       Bitrix Name: {bitrix_field_name}")
                    print(f"       Значений: {values_count}")
                    print()
            
            print("💡 СЛЕДУЮЩИЕ ШАГИ:")
            print("1. Проверьте созданные поля в админке Bitrix24")
            print("2. Настройте отображение полей в интерфейсе задач")
            print("3. Поля будут автоматически применяться при миграции карточек")
            print("4. Маппинг сохранен для использования в CardMigrator")
            
        else:
            print(f"❌ Миграция завершилась с ошибкой: {result.get('error', 'Unknown error')}")
            
            if 'error_log' in result:
                print(f"📄 Лог ошибки: {result['error_log']}")
                print("💡 Проверьте лог для диагностики проблемы")
            
            print()
            print("🔍 ВОЗМОЖНЫЕ ПРИЧИНЫ ОШИБОК:")
            print("1. Нет доступа к MySQL на VPS сервере")
            print("2. Недостаточно прав для создания полей")
            print("3. Ошибка в SSH подключении")
            print("4. Неправильная конфигурация MySQL (/root/.my.cnf)")
            
    except KeyboardInterrupt:
        print("\n❌ Миграция прервана пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        print(f"\n💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
        print("Проверьте логи для детальной информации")


if __name__ == "__main__":
    # Запускаем асинхронную функцию
    asyncio.run(main()) 