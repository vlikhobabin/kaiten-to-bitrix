import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime

# Добавляем корневую директорию в путь для импорта модулей
sys.path.append(str(Path(__file__).parent.parent))

from connectors.kaiten_client import KaitenClient
from connectors.bitrix_client import BitrixClient
from transformers.user_transformer import UserTransformer
from config.settings import settings
from utils.logger import logger

async def main():
    """Полная миграция пользователей из Kaiten в Bitrix24"""
    
    # Инициализация клиентов
    kaiten_client = KaitenClient()
    bitrix_client = BitrixClient()
    
    # Маппинг kaiten_user_id -> bitrix_user_id
    user_mapping = {}
    
    try:
        logger.info("🚀 НАЧИНАЕМ ПОЛНУЮ МИГРАЦИЮ ПОЛЬЗОВАТЕЛЕЙ ИЗ KAITEN В BITRIX24")
        logger.info("=" * 80)
        
        # Получаем пользователей из Kaiten
        logger.info("📥 Получение всех пользователей из Kaiten...")
        kaiten_users = await kaiten_client.get_users()
        
        # Фильтруем только пользователей с email
        users_with_email = [user for user in kaiten_users if user.email and user.email.strip()]
        
        logger.info(f"📊 Получено {len(kaiten_users)} пользователей из Kaiten")
        logger.info(f"📧 Из них {len(users_with_email)} имеют email адреса")
        
        if not users_with_email:
            logger.warning("❌ Нет пользователей с email для миграции!")
            return
        
        # Получаем ВСЕХ пользователей из Bitrix24 (включая неактивных)
        logger.info("📥 Получение ВСЕХ существующих пользователей из Bitrix24...")
        bitrix_users = await bitrix_client.get_users()  # Без фильтра - получаем всех
        logger.info(f"👥 В Bitrix24 уже есть {len(bitrix_users)} пользователей (всех статусов)")
        
        # Создаем трансформер с актуальными пользователями Bitrix24
        transformer = UserTransformer(bitrix_users)
        
        # Статистика
        stats = {
            'total_kaiten': len(kaiten_users),
            'with_email': len(users_with_email),
            'processed': 0,
            'created': 0, 
            'updated': 0,
            'skipped': 0,
            'errors': 0,
            'mapping_saved': 0
        }
        
        logger.info("=" * 80)
        logger.info(f"⚙️ ОБРАБОТКА {len(users_with_email)} ПОЛЬЗОВАТЕЛЕЙ С EMAIL...")
        logger.info("=" * 80)
        
        # Обрабатываем каждого пользователя с email из Kaiten
        for i, kaiten_user in enumerate(users_with_email, 1):
            stats['processed'] += 1
            
            # Показываем прогресс каждые 10 пользователей или последнего
            if stats['processed'] % 10 == 0 or stats['processed'] == len(users_with_email):
                logger.info(f"📈 Прогресс: {stats['processed']}/{len(users_with_email)} "
                           f"({stats['processed']/len(users_with_email)*100:.1f}%)")
                
            try:
                # Проверяем, есть ли пользователь в Bitrix24
                existing_user = transformer.transform(kaiten_user)
                
                # Подготавливаем данные для Bitrix24
                user_data = transformer.kaiten_to_bitrix_data(kaiten_user)
                if not user_data:
                    logger.warning(f"⚠️ Не удалось подготовить данные для {kaiten_user.email}")
                    stats['errors'] += 1
                    continue
                
                bitrix_user = None
                
                if existing_user:
                    # Пользователь уже существует - обновляем его
                    logger.debug(f"🔄 Обновление существующего пользователя: {kaiten_user.email}")
                    bitrix_user = await bitrix_client.update_user(existing_user.ID, user_data)
                    
                    if bitrix_user:
                        stats['updated'] += 1
                        # Используем ID существующего пользователя
                        user_mapping[str(kaiten_user.id)] = str(existing_user.ID)
                        stats['mapping_saved'] += 1
                        logger.debug(f"✅ Обновлен: {kaiten_user.email} (Kaiten ID: {kaiten_user.id} -> Bitrix ID: {existing_user.ID})")
                    else:
                        stats['errors'] += 1
                        logger.warning(f"❌ Ошибка обновления: {kaiten_user.email}")
                else:
                    # Создаем нового пользователя
                    logger.debug(f"➕ Создание нового пользователя: {kaiten_user.email}")
                    bitrix_user = await bitrix_client.create_user(user_data)
                    
                    if bitrix_user:
                        stats['created'] += 1
                        user_mapping[str(kaiten_user.id)] = str(bitrix_user.ID)
                        stats['mapping_saved'] += 1
                        logger.debug(f"✅ Создан: {kaiten_user.email} (Kaiten ID: {kaiten_user.id} -> Bitrix ID: {bitrix_user.ID})")
                    else:
                        stats['errors'] += 1
                        logger.warning(f"❌ Ошибка создания: {kaiten_user.email}")
                        
            except Exception as e:
                logger.error(f"💥 Критическая ошибка при обработке {kaiten_user.email}: {e}")
                stats['errors'] += 1
        
        # Сохраняем/обновляем маппинг в файл
        mapping_file = Path(__file__).parent.parent / "mappings" / "user_mapping.json"
        mapping_file.parent.mkdir(exist_ok=True)
        
        # Если файл существует, загружаем и объединяем данные
        existing_mapping = {}
        existing_stats = {"created": 0, "updated": 0, "errors": 0}
        
        if mapping_file.exists():
            try:
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    existing_mapping = existing_data.get("mapping", {})
                    existing_stats = existing_data.get("stats", existing_stats)
                logger.info(f"📂 Загружен существующий маппинг: {len(existing_mapping)} записей")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка загрузки существующего маппинга: {e}")
        
        # Объединяем маппинги (новые данные имеют приоритет)
        combined_mapping = {**existing_mapping, **user_mapping}
        
        # Объединяем статистику
        combined_stats = {
            "created": existing_stats["created"] + stats["created"],
            "updated": existing_stats["updated"] + stats["updated"], 
            "errors": existing_stats["errors"] + stats["errors"]
        }
        
        mapping_data = {
            "created_at": datetime.now().isoformat(),
            "description": "Маппинг ID пользователей Kaiten -> Bitrix24",
            "stats": combined_stats,
            "mapping": combined_mapping
        }
        
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Маппинг сохранен в файл: {mapping_file}")
        
        # Получаем финальную статистику из Bitrix24
        logger.info("📊 Получение финальной статистики из Bitrix24...")
        final_bitrix_users = await bitrix_client.get_users()  # Всех пользователей
        
        # Финальный отчет
        logger.info("=" * 80)
        logger.info("🎉 МИГРАЦИЯ ПОЛЬЗОВАТЕЛЕЙ ЗАВЕРШЕНА!")
        logger.info("=" * 80)
        logger.info(f"📊 СТАТИСТИКА МИГРАЦИИ:")
        logger.info(f"  📋 Всего пользователей в Kaiten: {stats['total_kaiten']}")
        logger.info(f"  📧 С email адресами: {stats['with_email']}")
        logger.info(f"  ⚙️ Обработано: {stats['processed']}")
        logger.info(f"  ➕ Создано новых: {stats['created']}")
        logger.info(f"  🔄 Обновлено существующих: {stats['updated']}")
        logger.info(f"  ❌ Ошибок: {stats['errors']}")
        logger.info(f"  🔗 Маппинг сохранен: {stats['mapping_saved']} записей")
        logger.info("")
        logger.info(f"📈 РЕЗУЛЬТАТ В BITRIX24:")
        logger.info(f"  👥 Было пользователей: {len(bitrix_users)}")
        logger.info(f"  👥 Стало пользователей: {len(final_bitrix_users)}")
        logger.info(f"  ➕ Прирост: {len(final_bitrix_users) - len(bitrix_users)}")
        logger.info("=" * 80)
        
        # Проверяем успешность миграции
        success_rate = ((stats['created'] + stats['updated']) / stats['with_email']) * 100
        logger.info(f"✅ УСПЕШНОСТЬ МИГРАЦИИ: {success_rate:.1f}%")
        
        if success_rate >= 95:
            logger.info("🏆 ОТЛИЧНО! Миграция прошла успешно!")
        elif success_rate >= 80:
            logger.info("👍 ХОРОШО! Миграция завершена с минимальными ошибками")
        else:
            logger.warning("⚠️ ВНИМАНИЕ! Много ошибок при миграции, требуется проверка")
        
    except Exception as e:
        logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА МИГРАЦИИ: {e}")
        return

if __name__ == "__main__":
    asyncio.run(main()) 