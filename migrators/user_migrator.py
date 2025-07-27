"""
Мигратор пользователей Kaiten в Bitrix24.
Обеспечивает полную миграцию пользователей с сохранением маппинга ID.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from connectors.kaiten_client import KaitenClient
from connectors.bitrix_client import BitrixClient
from transformers.user_transformer import UserTransformer
from models.kaiten_models import KaitenUser
from models.bitrix_models import BitrixUser
from utils.logger import get_logger

logger = get_logger(__name__)


class UserMigrator:
    """
    Мигратор пользователей из Kaiten в Bitrix24.
    
    Логика миграции:
    1. Получение пользователей из Kaiten (только с email)
    2. Получение существующих пользователей из Bitrix24
    3. Создание/обновление пользователей в Bitrix24
    4. Сохранение маппинга ID пользователей
    """
    
    def __init__(self):
        self.kaiten_client = KaitenClient()
        self.bitrix_client = BitrixClient()
        
        # Маппинг пользователей kaiten_user_id -> bitrix_user_id
        self.user_mapping: Dict[str, str] = {}
        
        # Статистика миграции
        self.stats = {
            'total_kaiten': 0,
            'with_email': 0,
            'processed': 0,
            'created': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0,
            'mapping_saved': 0
        }
        
        # Трансформер пользователей (будет инициализирован после получения данных)
        self.transformer: Optional[UserTransformer] = None

    async def migrate_users(self) -> Dict:
        """
        Выполняет полную миграцию пользователей из Kaiten в Bitrix24.
        
        Returns:
            Словарь с результатами миграции
        """
        try:
            logger.info("🚀 НАЧИНАЕМ ПОЛНУЮ МИГРАЦИЮ ПОЛЬЗОВАТЕЛЕЙ ИЗ KAITEN В BITRIX24")
            logger.info("=" * 80)
            
            # Загружаем существующий маппинг
            await self._load_user_mapping()
            
            # Получаем пользователей из Kaiten
            logger.info("📥 Получение всех пользователей из Kaiten...")
            kaiten_users = await self.kaiten_client.get_users()
            
            # Фильтруем только пользователей с email
            users_with_email = [user for user in kaiten_users if user.email and user.email.strip()]
            
            self.stats['total_kaiten'] = len(kaiten_users)
            self.stats['with_email'] = len(users_with_email)
            
            logger.info(f"📊 Получено {len(kaiten_users)} пользователей из Kaiten")
            logger.info(f"📧 Из них {len(users_with_email)} имеют email адреса")
            
            if not users_with_email:
                logger.warning("❌ Нет пользователей с email для миграции!")
                return self._get_migration_result(False, "No users with email found")
            
            # Получаем ВСЕХ пользователей из Bitrix24 (включая неактивных)
            logger.info("📥 Получение ВСЕХ существующих пользователей из Bitrix24...")
            bitrix_users = await self.bitrix_client.get_users()
            initial_bitrix_count = len(bitrix_users)
            logger.info(f"👥 В Bitrix24 уже есть {initial_bitrix_count} пользователей (всех статусов)")
            
            # Создаем трансформер с актуальными пользователями Bitrix24
            self.transformer = UserTransformer(bitrix_users)
            
            logger.info("=" * 80)
            logger.info(f"⚙️ ОБРАБОТКА {len(users_with_email)} ПОЛЬЗОВАТЕЛЕЙ С EMAIL...")
            logger.info("=" * 80)
            
            # Обрабатываем каждого пользователя с email из Kaiten
            for i, kaiten_user in enumerate(users_with_email, 1):
                # Показываем прогресс каждые 10 пользователей или последнего
                if self.stats['processed'] % 10 == 0 or self.stats['processed'] == len(users_with_email):
                    logger.info(f"📈 Прогресс: {self.stats['processed']}/{len(users_with_email)} "
                               f"({self.stats['processed']/len(users_with_email)*100:.1f}%)")
                
                await self._process_single_user(kaiten_user)
            
            # Сохраняем маппинг
            await self._save_user_mapping()
            
            # Получаем финальную статистику из Bitrix24
            logger.info("📊 Получение финальной статистики из Bitrix24...")
            final_bitrix_users = await self.bitrix_client.get_users()
            final_bitrix_count = len(final_bitrix_users)
            
            # Выводим финальный отчет
            self._print_migration_stats(initial_bitrix_count, final_bitrix_count)
            
            return self._get_migration_result(True)
            
        except Exception as e:
            logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА МИГРАЦИИ: {e}")
            return self._get_migration_result(False, str(e))

    async def _process_single_user(self, kaiten_user: KaitenUser) -> None:
        """
        Обрабатывает одного пользователя Kaiten.
        
        Args:
            kaiten_user: Пользователь Kaiten для обработки
        """
        self.stats['processed'] += 1
        
        try:
            # Проверяем, есть ли пользователь в Bitrix24
            existing_user = self.transformer.transform(kaiten_user)
            
            # Подготавливаем данные для Bitrix24
            user_data = self.transformer.kaiten_to_bitrix_data(kaiten_user)
            if not user_data:
                logger.warning(f"⚠️ Не удалось подготовить данные для {kaiten_user.email}")
                self.stats['errors'] += 1
                return
            
            bitrix_user = None
            
            if existing_user:
                # Пользователь уже существует - обновляем его
                logger.debug(f"🔄 Обновление существующего пользователя: {kaiten_user.email}")
                bitrix_user = await self.bitrix_client.update_user(existing_user.ID, user_data)
                
                if bitrix_user:
                    self.stats['updated'] += 1
                    # Используем ID существующего пользователя
                    self.user_mapping[str(kaiten_user.id)] = str(existing_user.ID)
                    self.stats['mapping_saved'] += 1
                    logger.debug(f"✅ Обновлен: {kaiten_user.email} (Kaiten ID: {kaiten_user.id} -> Bitrix ID: {existing_user.ID})")
                else:
                    self.stats['errors'] += 1
                    logger.warning(f"❌ Ошибка обновления: {kaiten_user.email}")
            else:
                # Создаем нового пользователя
                logger.debug(f"➕ Создание нового пользователя: {kaiten_user.email}")
                bitrix_user = await self.bitrix_client.create_user(user_data)
                
                if bitrix_user:
                    self.stats['created'] += 1
                    self.user_mapping[str(kaiten_user.id)] = str(bitrix_user.ID)
                    self.stats['mapping_saved'] += 1
                    logger.debug(f"✅ Создан: {kaiten_user.email} (Kaiten ID: {kaiten_user.id} -> Bitrix ID: {bitrix_user.ID})")
                else:
                    self.stats['errors'] += 1
                    logger.warning(f"❌ Ошибка создания: {kaiten_user.email}")
                    
        except Exception as e:
            logger.error(f"💥 Критическая ошибка при обработке {kaiten_user.email}: {e}")
            self.stats['errors'] += 1

    async def _load_user_mapping(self) -> bool:
        """
        Загружает существующий маппинг пользователей из файла.
        
        Returns:
            True в случае успеха
        """
        try:
            mapping_file = Path(__file__).parent.parent / "mappings" / "user_mapping.json"
            
            if mapping_file.exists():
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    existing_mapping = data.get("mapping", {})
                    
                if existing_mapping:
                    logger.info(f"📂 Загружен существующий маппинг: {len(existing_mapping)} записей")
                    # Сохраняем существующий маппинг для объединения позже
                    self.user_mapping.update(existing_mapping)
                    
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Ошибка загрузки существующего маппинга: {e}")
            return False

    async def _save_user_mapping(self) -> bool:
        """
        Сохраняет маппинг пользователей в файл.
        
        Returns:
            True в случае успеха
        """
        try:
            mapping_file = Path(__file__).parent.parent / "mappings" / "user_mapping.json"
            mapping_file.parent.mkdir(exist_ok=True)
            
            # Загружаем существующую статистику если файл существует
            existing_stats = {"created": 0, "updated": 0, "errors": 0}
            
            if mapping_file.exists():
                try:
                    with open(mapping_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        existing_stats = existing_data.get("stats", existing_stats)
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка загрузки существующей статистики: {e}")
            
            # Объединяем статистику
            combined_stats = {
                "created": existing_stats["created"] + self.stats["created"],
                "updated": existing_stats["updated"] + self.stats["updated"], 
                "errors": existing_stats["errors"] + self.stats["errors"]
            }
            
            mapping_data = {
                "created_at": datetime.now().isoformat(),
                "description": "Маппинг ID пользователей Kaiten -> Bitrix24",
                "stats": combined_stats,
                "mapping": self.user_mapping
            }
            
            with open(mapping_file, 'w', encoding='utf-8') as f:
                json.dump(mapping_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"💾 Маппинг сохранен в файл: {mapping_file}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения маппинга пользователей: {e}")
            return False

    def _print_migration_stats(self, initial_bitrix_count: int, final_bitrix_count: int) -> None:
        """
        Выводит финальную статистику миграции.
        
        Args:
            initial_bitrix_count: Количество пользователей в Bitrix24 до миграции
            final_bitrix_count: Количество пользователей в Bitrix24 после миграции
        """
        logger.info("=" * 80)
        logger.info("🎉 МИГРАЦИЯ ПОЛЬЗОВАТЕЛЕЙ ЗАВЕРШЕНА!")
        logger.info("=" * 80)
        logger.info(f"📊 СТАТИСТИКА МИГРАЦИИ:")
        logger.info(f"  📋 Всего пользователей в Kaiten: {self.stats['total_kaiten']}")
        logger.info(f"  📧 С email адресами: {self.stats['with_email']}")
        logger.info(f"  ⚙️ Обработано: {self.stats['processed']}")
        logger.info(f"  ➕ Создано новых: {self.stats['created']}")
        logger.info(f"  🔄 Обновлено существующих: {self.stats['updated']}")
        logger.info(f"  ❌ Ошибок: {self.stats['errors']}")
        logger.info(f"  🔗 Маппинг сохранен: {self.stats['mapping_saved']} записей")
        logger.info("")
        logger.info(f"📈 РЕЗУЛЬТАТ В BITRIX24:")
        logger.info(f"  👥 Было пользователей: {initial_bitrix_count}")
        logger.info(f"  👥 Стало пользователей: {final_bitrix_count}")
        logger.info(f"  ➕ Прирост: {final_bitrix_count - initial_bitrix_count}")
        logger.info("=" * 80)
        
        # Проверяем успешность миграции
        if self.stats['with_email'] > 0:
            success_rate = ((self.stats['created'] + self.stats['updated']) / self.stats['with_email']) * 100
            logger.info(f"✅ УСПЕШНОСТЬ МИГРАЦИИ: {success_rate:.1f}%")
            
            if success_rate >= 95:
                logger.info("🏆 ОТЛИЧНО! Миграция прошла успешно!")
            elif success_rate >= 80:
                logger.info("👍 ХОРОШО! Миграция завершена с минимальными ошибками")
            else:
                logger.warning("⚠️ ВНИМАНИЕ! Много ошибок при миграции, требуется проверка")

    def _get_migration_result(self, success: bool, error: str = None) -> Dict:
        """
        Формирует результат миграции.
        
        Args:
            success: Успешность миграции
            error: Сообщение об ошибке (если есть)
            
        Returns:
            Словарь с результатами
        """
        result = {
            'success': success,
            'stats': self.stats,
            'mapping_file': 'mappings/user_mapping.json'
        }
        
        if error:
            result['error'] = error
            
        return result

    async def get_user_mapping(self) -> Dict[str, str]:
        """
        Получает текущий маппинг пользователей.
        
        Returns:
            Словарь маппинга {kaiten_user_id: bitrix_user_id}
        """
        await self._load_user_mapping()
        return self.user_mapping.copy()

    def print_migration_stats_summary(self) -> None:
        """Выводит краткую сводку статистики миграции"""
        logger.info("📊 КРАТКАЯ СВОДКА МИГРАЦИИ ПОЛЬЗОВАТЕЛЕЙ:")
        logger.info(f"  📋 Обработано: {self.stats['processed']}")
        logger.info(f"  ➕ Создано: {self.stats['created']}")
        logger.info(f"  🔄 Обновлено: {self.stats['updated']}")
        logger.info(f"  ❌ Ошибок: {self.stats['errors']}") 