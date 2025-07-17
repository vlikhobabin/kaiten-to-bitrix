import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import asyncio

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
        """Загружает маппинг пользователей из файла mapping"""
        try:
            mapping_file = Path(__file__).parent.parent / "mappings" / "user_mapping.json"
            
            if not mapping_file.exists():
                logger.error("❌ Не найден файл маппинга пользователей. Запустите сначала миграцию пользователей!")
                return False
            
            with open(mapping_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.user_mapping = data.get('mapping', {})
            
            logger.info(f"📥 Загружен маппинг пользователей из {mapping_file.name}: {len(self.user_mapping)} записей")
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
        logger.info(f"📊 Найдено {len(bitrix_workgroups)} существующих рабочих групп в Bitrix24")
        
        # Создаем трансформер с передачей KaitenClient для получения участников
        transformer = SpaceTransformer(bitrix_workgroups, self.user_mapping, kaiten_spaces, self.kaiten_client)
        
        # Статистика миграции
        stats = {
            'processed': 0,
            'created': 0,
            'updated': 0,
            'errors': 0,
            'members_added': 0
        }
        
        # Обрабатываем каждое пространство
        for i, kaiten_space in enumerate(spaces_to_process, 1):
            space_title = transformer._build_hierarchical_name(kaiten_space)
            logger.info(f"🔄 [{i}/{len(spaces_to_process)}] Обрабатываем пространство: '{space_title}'")
            
            await self._process_single_space(kaiten_space, transformer, stats)
            
            # Небольшая пауза между обработкой пространств
            if i < len(spaces_to_process):
                await asyncio.sleep(0.5)
        
        # Сохраняем маппинг пространств
        await self._save_space_mapping(stats)
        
        logger.info("🎉 МИГРАЦИЯ ПРОСТРАНСТВ ЗАВЕРШЕНА")
        logger.info("=" * 80)
        return stats

    async def _process_single_space(self, kaiten_space: KaitenSpace, transformer: SpaceTransformer, stats: Dict):
        """Обрабатывает одно пространство: создает группу и добавляет участников"""
        space_title = transformer._build_hierarchical_name(kaiten_space)
        stats['processed'] += 1
        
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
            # Группа уже существует
            logger.debug(f"🔄 Группа для '{space_title}' уже существует (ID: {existing_group.get('ID')})")
            bitrix_group = existing_group
            stats['updated'] += 1
            
            # Сохраняем в маппинг
            self.space_mapping[str(kaiten_space.id)] = str(existing_group.get('ID'))
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
                logger.success(f"✅ Создана группа '{space_title}' (Kaiten ID: {kaiten_space.id} -> Bitrix ID: {bitrix_group['ID']})")
            else:
                stats['errors'] += 1
                logger.warning(f"❌ Ошибка создания группы для пространства '{space_title}'")
                return
        
        # Добавляем участников в группу
        if bitrix_group and bitrix_group.get('ID'):
            await self._add_members_to_group(kaiten_space, transformer, bitrix_group, stats, space_title)

    async def _add_members_to_group(self, kaiten_space: KaitenSpace, transformer: SpaceTransformer, bitrix_group: Dict, stats: Dict, space_title: str):
        """Добавляет участников пространства в рабочую группу Bitrix24"""
        group_id = int(bitrix_group['ID'])
        
        try:
            # Используем асинхронный метод для получения участников
            member_ids = await transformer.get_space_members_bitrix_ids_async(kaiten_space)
            
            if not member_ids:
                logger.debug(f"👥 Участники для пространства '{space_title}' не найдены")
                return
            
            logger.info(f"👥 Добавление {len(member_ids)} участников в группу '{space_title}'")
            
            # Добавляем каждого участника в группу
            added_count = 0
            for member_id in member_ids:
                try:
                    success = await self.bitrix_client.add_user_to_workgroup(group_id, member_id)
                    if success:
                        added_count += 1
                        stats['members_added'] += 1
                except Exception as e:
                    logger.debug(f"Ошибка добавления участника {member_id} в группу {group_id}: {e}")
            
            if added_count > 0:
                logger.success(f"✅ Добавлено {added_count} участников в группу '{space_title}'")
            else:
                logger.warning(f"⚠️ Не удалось добавить участников в группу '{space_title}'")
                
        except Exception as e:
            logger.warning(f"Ошибка при добавлении участников в группу '{space_title}': {e}")

    async def _save_space_mapping(self, stats: Dict):
        """Сохраняет/обновляет маппинг пространств в файл"""
        mapping_file = Path(__file__).parent.parent / "mappings" / "space_mapping.json"
        mapping_file.parent.mkdir(exist_ok=True)
        
        # Если файл существует, загружаем и объединяем данные
        existing_mapping = {}
        existing_stats = {"created": 0, "updated": 0, "members_added": 0, "errors": 0}
        
        if mapping_file.exists():
            try:
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    existing_mapping = existing_data.get("mapping", {})
                    existing_stats = existing_data.get("stats", existing_stats)
                logger.info(f"📂 Загружен существующий маппинг пространств: {len(existing_mapping)} записей")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка загрузки существующего маппинга пространств: {e}")
        
        # Объединяем маппинги (новые данные имеют приоритет)
        combined_mapping = {**existing_mapping, **self.space_mapping}
        
        # Объединяем статистику
        combined_stats = {}
        for key in existing_stats.keys():
            combined_stats[key] = existing_stats.get(key, 0) + stats.get(key, 0)
        
        mapping_data = {
            "created_at": datetime.now().isoformat(),
            "description": "Маппинг ID пространств Kaiten -> рабочих групп Bitrix24",
            "stats": combined_stats,
            "mapping": combined_mapping
        }
        
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Маппинг пространств сохранен/обновлен в файл: {mapping_file}")

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