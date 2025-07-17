import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from connectors.kaiten_client import KaitenClient
from connectors.bitrix_client import BitrixClient
from transformers.space_transformer import SpaceTransformer
from models.kaiten_models import KaitenSpace
from utils.logger import logger

class SpaceMigrator:
    """
    Мигратор для переноса пространств Kaiten в рабочие группы Bitrix24.
    """

    def __init__(self):
        self.kaiten_client = KaitenClient()
        self.bitrix_client = BitrixClient()
        self.user_mapping: Dict[str, str] = {}
        self.space_mapping: Dict[str, str] = {}

    async def load_user_mapping(self) -> bool:
        """Загружает маппинг пользователей из последнего файла миграции"""
        try:
            logs_dir = Path(__file__).parent.parent / "logs"
            mapping_files = list(logs_dir.glob("user_mapping_*.json"))
            
            if not mapping_files:
                logger.error("❌ Не найден файл маппинга пользователей. Запустите сначала миграцию пользователей!")
                return False
            
            # Берем последний файл по дате
            latest_file = max(mapping_files, key=lambda x: x.stat().st_mtime)
            
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.user_mapping = data.get('mapping', {})
            
            logger.info(f"📥 Загружен маппинг пользователей из {latest_file.name}: {len(self.user_mapping)} записей")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки маппинга пользователей: {e}")
            return False

    async def migrate_spaces(self, limit: Optional[int] = None) -> Dict:
        """
        Выполняет миграцию пространств из Kaiten в рабочие группы Bitrix24.
        
        Args:
            limit: Максимальное количество пространств для миграции (None = все)
            
        Returns:
            Словарь со статистикой миграции
        """
        logger.info("🚀 НАЧИНАЕМ МИГРАЦИЮ ПРОСТРАНСТВ ИЗ KAITEN В BITRIX24")
        logger.info("=" * 80)
        
        # Загружаем маппинг пользователей
        if not await self.load_user_mapping():
            return {"error": "Не удалось загрузить маппинг пользователей"}
        
        # Получаем пространства из Kaiten
        logger.info("📥 Получение пространств из Kaiten...")
        kaiten_spaces = await self.kaiten_client.get_spaces()
        
        if not kaiten_spaces:
            logger.warning("❌ Не найдено пространств в Kaiten!")
            return {"error": "Нет пространств для миграции"}
        
        # Применяем лимит если указан
        spaces_to_process = kaiten_spaces
        if limit:
            spaces_to_process = kaiten_spaces[:limit]
            logger.info(f"🔢 Ограничение: будет обработано {len(spaces_to_process)} из {len(kaiten_spaces)} пространств")
        
        logger.info(f"📊 Найдено {len(spaces_to_process)} пространств для миграции")
        
        # Получаем существующие рабочие группы из Bitrix24
        logger.info("📥 Получение существующих рабочих групп из Bitrix24...")
        bitrix_workgroups = await self.bitrix_client.get_workgroup_list()
        logger.info(f"👥 В Bitrix24 найдено {len(bitrix_workgroups)} рабочих групп")
        
        # Создаем трансформер с полным списком пространств для построения иерархии
        transformer = SpaceTransformer(bitrix_workgroups, self.user_mapping, kaiten_spaces)
        
        # Статистика
        stats = {
            'total_spaces': len(spaces_to_process),
            'processed': 0,
            'created': 0,
            'updated': 0,
            'errors': 0,
            'members_added': 0,
            'mapping_saved': 0
        }
        
        logger.info("=" * 80)
        logger.info(f"⚙️ ОБРАБОТКА {len(spaces_to_process)} ПРОСТРАНСТВ...")
        logger.info("=" * 80)
        
        # Обрабатываем каждое пространство
        for i, kaiten_space in enumerate(spaces_to_process, 1):
            stats['processed'] += 1
            
            # Показываем прогресс
            if stats['processed'] % 5 == 0 or stats['processed'] == len(spaces_to_process):
                logger.info(f"📈 Прогресс: {stats['processed']}/{len(spaces_to_process)} "
                           f"({stats['processed']/len(spaces_to_process)*100:.1f}%)")
            
            try:
                await self._migrate_single_space(kaiten_space, transformer, stats)
                
            except Exception as e:
                logger.error(f"💥 Критическая ошибка при обработке пространства '{kaiten_space.title}': {e}")
                stats['errors'] += 1
        
        # Сохраняем маппинг
        await self._save_space_mapping(stats)
        
        # Финальный отчет
        await self._print_final_report(stats)
        
        return stats

    async def _migrate_single_space(self, kaiten_space: KaitenSpace, transformer: SpaceTransformer, stats: Dict):
        """Мигрирует одно пространство"""
        space_title = kaiten_space.title or f"Space-{kaiten_space.id}"
        
        # Проверяем, существует ли группа в Bitrix24
        existing_group = transformer.find_existing_workgroup(kaiten_space)
        
        # Подготавливаем данные для группы
        group_data = transformer.kaiten_to_bitrix_workgroup_data(kaiten_space)
        if not group_data:
            logger.warning(f"⚠️ Не удалось подготовить данные для пространства '{space_title}'")
            stats['errors'] += 1
            return
        
        bitrix_group = None
        
        if existing_group:
            # Группа уже существует - можно обновить описание
            logger.debug(f"🔄 Группа для '{space_title}' уже существует (ID: {existing_group.get('ID')})")
            bitrix_group = existing_group
            stats['updated'] += 1
            
            # Сохраняем в маппинг
            self.space_mapping[str(kaiten_space.id)] = str(existing_group.get('ID'))
            stats['mapping_saved'] += 1
        else:
            # Создаем новую группу
            logger.debug(f"➕ Создание новой группы для пространства '{space_title}'")
            
            # Получаем владельца группы
            owner_id = transformer.get_space_owner_bitrix_id(kaiten_space)
            if owner_id:
                group_data['OWNER_ID'] = owner_id
            
            # Создаем группу
            bitrix_group = await self.bitrix_client.create_workgroup(group_data)
            
            if bitrix_group and bitrix_group.get('ID'):
                stats['created'] += 1
                self.space_mapping[str(kaiten_space.id)] = str(bitrix_group['ID'])
                stats['mapping_saved'] += 1
                logger.debug(f"✅ Создана группа '{space_title}' (Kaiten ID: {kaiten_space.id} -> Bitrix ID: {bitrix_group['ID']})")
            else:
                stats['errors'] += 1
                logger.warning(f"❌ Ошибка создания группы для пространства '{space_title}'")
                return
        
        # Добавляем участников в группу
        if bitrix_group and bitrix_group.get('ID'):
            group_id = int(bitrix_group['ID'])
            member_ids = transformer.get_space_members_bitrix_ids(kaiten_space)
            
            if member_ids:
                logger.debug(f"👥 Добавление {len(member_ids)} участников в группу '{space_title}'")
                
                for member_id in member_ids:
                    try:
                        success = await self.bitrix_client.add_user_to_workgroup(group_id, member_id)
                        if success:
                            stats['members_added'] += 1
                    except Exception as e:
                        logger.debug(f"Ошибка добавления участника {member_id} в группу {group_id}: {e}")
                
                logger.debug(f"✅ Участники добавлены в группу '{space_title}'")

    async def _save_space_mapping(self, stats: Dict):
        """Сохраняет маппинг пространств в файл"""
        mapping_file = Path(__file__).parent.parent / "logs" / f"space_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        mapping_file.parent.mkdir(exist_ok=True)
        
        mapping_data = {
            "created_at": datetime.now().isoformat(),
            "description": "Маппинг ID пространств Kaiten -> рабочих групп Bitrix24",
            "stats": stats,
            "mapping": self.space_mapping
        }
        
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Маппинг пространств сохранен в файл: {mapping_file}")

    async def _print_final_report(self, stats: Dict):
        """Выводит финальный отчет миграции"""
        # Получаем финальную статистику из Bitrix24
        final_workgroups = await self.bitrix_client.get_workgroup_list()
        
        logger.info("=" * 80)
        logger.info("🎉 МИГРАЦИЯ ПРОСТРАНСТВ ЗАВЕРШЕНА!")
        logger.info("=" * 80)
        logger.info(f"📊 СТАТИСТИКА МИГРАЦИИ:")
        logger.info(f"  📋 Всего пространств в Kaiten: {stats['total_spaces']}")
        logger.info(f"  ⚙️ Обработано: {stats['processed']}")
        logger.info(f"  ➕ Создано новых групп: {stats['created']}")
        logger.info(f"  🔄 Обновлено существующих: {stats['updated']}")
        logger.info(f"  👥 Участников добавлено: {stats['members_added']}")
        logger.info(f"  ❌ Ошибок: {stats['errors']}")
        logger.info(f"  🔗 Маппинг сохранен: {stats['mapping_saved']} записей")
        logger.info("")
        logger.info(f"📈 РЕЗУЛЬТАТ В BITRIX24:")
        logger.info(f"  👥 Всего рабочих групп: {len(final_workgroups)}")
        logger.info("=" * 80)
        
        # Проверяем успешность миграции
        success_rate = ((stats['created'] + stats['updated']) / stats['total_spaces']) * 100
        logger.info(f"✅ УСПЕШНОСТЬ МИГРАЦИИ: {success_rate:.1f}%")
        
        if success_rate >= 95:
            logger.info("🏆 ОТЛИЧНО! Миграция пространств прошла успешно!")
        elif success_rate >= 80:
            logger.info("👍 ХОРОШО! Миграция завершена с минимальными ошибками")
        else:
            logger.warning("⚠️ ВНИМАНИЕ! Много ошибок при миграции, требуется проверка") 