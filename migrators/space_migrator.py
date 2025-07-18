"""
Мигратор пространств Kaiten в группы Bitrix24.
Логика: 
1. НЕ переносим доски
2. Переносим только конечные пространства (без дочерних) или пространства 2-го уровня
3. Исключаем пространства из списка исключений
"""

import asyncio
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from connectors.kaiten_client import KaitenClient
from connectors.bitrix_client import BitrixClient
from models.kaiten_models import KaitenSpace
from config.space_exclusions import is_space_excluded, get_excluded_spaces
from utils.logger import get_logger

logger = get_logger(__name__)

class SpaceMigrator:
    """
    Мигратор пространств из Kaiten в группы Bitrix24.
    Логика: переносим пространства, а не доски.
    """
    
    def __init__(self):
        self.kaiten_client = KaitenClient()
        self.bitrix_client = BitrixClient()
        self.user_mapping: Dict[str, str] = {}
        self.space_mapping: Dict[str, str] = {}
        self.spaces_hierarchy: Dict[str, KaitenSpace] = {}

    async def load_user_mapping(self) -> bool:
        """Загружает маппинг пользователей из файла"""
        try:
            mapping_file = Path(__file__).parent.parent / "mappings" / "user_mapping.json"
            
            if not mapping_file.exists():
                logger.error("❌ Не найден файл маппинга пользователей. Запустите сначала миграцию пользователей!")
                return False
            
            with open(mapping_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.user_mapping = data.get('mapping', {})
            
            logger.info(f"📥 Загружен маппинг пользователей: {len(self.user_mapping)} записей")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки маппинга пользователей: {e}")
            return False

    async def build_spaces_hierarchy(self) -> bool:
        """Строит полную иерархию пространств"""
        try:
            logger.info("📥 Получение иерархии пространств из Kaiten...")
            spaces = await self.kaiten_client.get_spaces()
            
            if not spaces:
                logger.error("❌ Не удалось получить пространства из Kaiten")
                return False
            
            # Создаем словарь пространств по UID для быстрого поиска
            for space in spaces:
                self.spaces_hierarchy[space.uid] = space
            
            logger.info(f"📊 Загружено {len(spaces)} пространств в иерархию")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка построения иерархии пространств: {e}")
            return False

    def get_root_spaces(self) -> List[KaitenSpace]:
        """Получает корневые пространства (без родителей)"""
        root_spaces = []
        for space in self.spaces_hierarchy.values():
            if not space.parent_entity_uid:
                root_spaces.append(space)
        return root_spaces

    def get_child_spaces(self, parent_space: KaitenSpace) -> List[KaitenSpace]:
        """Получает дочерние пространства для указанного родителя"""
        child_spaces = []
        for space in self.spaces_hierarchy.values():
            if space.parent_entity_uid == parent_space.uid:
                child_spaces.append(space)
        return child_spaces

    def is_space_in_excluded_tree(self, space: KaitenSpace) -> bool:
        """
        Проверяет, находится ли пространство в дереве исключенных пространств.
        Проходит вверх по иерархии до корня и проверяет каждый уровень.
        """
        current_space = space
        max_depth = 10  # Защита от циклов
        depth = 0
        
        while current_space and depth < max_depth:
            # Проверяем текущее пространство
            if is_space_excluded(current_space.title):
                return True
            
            # Идем к родителю
            if current_space.parent_entity_uid:
                current_space = self.spaces_hierarchy.get(current_space.parent_entity_uid)
                depth += 1
            else:
                break
                
        return False

    def get_space_level(self, space: KaitenSpace) -> int:
        """
        Определяет уровень пространства в иерархии (1 = корневое, 2 = дочернее корневого, и т.д.)
        """
        level = 1
        current_space = space
        max_depth = 10
        
        while current_space.parent_entity_uid and level < max_depth:
            current_space = self.spaces_hierarchy.get(current_space.parent_entity_uid)
            if current_space:
                level += 1
            else:
                break
        
        return level

    def get_spaces_to_migrate(self) -> List[KaitenSpace]:
        """
        Определяет какие пространства нужно мигрировать согласно новой логике:
        1. Конечные пространства (без дочерних) любого уровня
        2. Пространства строго 2-го уровня (независимо от наличия дочерних)
        3. НЕ переносим: пространства 1-го уровня с дочерними, пространства глубже 2-го уровня
        4. Исключаем пространства из списка исключений
        """
        spaces_to_migrate = []
        
        logger.info("🔍 Анализ пространств для миграции...")
        logger.info(f"📋 Исключенные пространства: {get_excluded_spaces()}")
        
        for space in self.spaces_hierarchy.values():
            # Пропускаем пространства из исключенного дерева
            if self.is_space_in_excluded_tree(space):
                logger.debug(f"⏭️ Пропускаем пространство '{space.title}' (в исключенном дереве)")
                continue
            
            # Определяем уровень пространства
            level = self.get_space_level(space)
            
            # Получаем дочерние пространства
            child_spaces = self.get_child_spaces(space)
            
            # Логика отбора:
            if level == 1 and child_spaces:
                # Пространство 1-го уровня с дочерними - НЕ переносим
                logger.debug(f"⏭️ Пропускаем пространство 1-го уровня с дочерними: '{space.title}'")
                continue
            elif level == 2:
                # Пространство 2-го уровня - переносим всегда
                spaces_to_migrate.append(space)
                logger.debug(f"✅ Пространство 2-го уровня: '{space.title}'")
            elif level > 2:
                # Пространство глубже 2-го уровня - НЕ переносим
                logger.debug(f"⏭️ Пропускаем пространство {level}-го уровня: '{space.title}'")
                continue
            elif level == 1 and not child_spaces:
                # Конечное пространство 1-го уровня - переносим
                spaces_to_migrate.append(space)
                logger.debug(f"✅ Конечное пространство 1-го уровня: '{space.title}'")
            elif not child_spaces:
                # Любое другое конечное пространство - переносим
                spaces_to_migrate.append(space)
                logger.debug(f"✅ Конечное пространство {level}-го уровня: '{space.title}'")
        
        logger.info(f"📊 Найдено {len(spaces_to_migrate)} пространств для миграции")
        return spaces_to_migrate

    def build_space_path(self, space: KaitenSpace) -> str:
        """
        Строит полный иерархический путь для пространства.
        """
        path_parts = []
        current_space = space
        max_depth = 10
        depth = 0
        
        # Идем вверх по иерархии, собирая названия
        while current_space and depth < max_depth:
            path_parts.insert(0, current_space.title)
            
            # Ищем родительское пространство
            if current_space.parent_entity_uid:
                current_space = self.spaces_hierarchy.get(current_space.parent_entity_uid)
                depth += 1
            else:
                break
        
        return "/".join(path_parts)

    async def get_space_members_bitrix_ids(self, space_id: int) -> List[str]:
        """Получает ID участников пространства в формате Bitrix24"""
        try:
            # Получаем участников пространства
            space_members = await self.kaiten_client.get_space_members(space_id)
            
            bitrix_ids = []
            for member in space_members:
                # Ищем соответствие в маппинге пользователей
                kaiten_id = str(member.id)
                bitrix_id = self.user_mapping.get(kaiten_id)
                
                if bitrix_id:
                    bitrix_ids.append(bitrix_id)
                else:
                    logger.warning(f"⚠️ Пользователь {member.full_name} (ID: {kaiten_id}) не найден в маппинге")
            
            logger.info(f"👥 Найдено {len(bitrix_ids)} участников для пространства {space_id}")
            return bitrix_ids
            
        except Exception as e:
            logger.error(f"Ошибка получения участников пространства {space_id}: {e}")
            return []

    async def list_available_spaces(self, verbose: bool = False) -> bool:
        """
        Выводит список всех доступных пространств для миграции.
        """
        logger.info("📋 СПИСОК ДОСТУПНЫХ ПРОСТРАНСТВ ДЛЯ МИГРАЦИИ")
        logger.info("=" * 80)
        
        try:
            # Строим иерархию пространств
            if not await self.build_spaces_hierarchy():
                return False
            
            # Получаем пространства для миграции
            spaces_to_migrate = self.get_spaces_to_migrate()
            
            if not spaces_to_migrate:
                logger.warning("❌ Не найдено пространств для миграции")
                return False
            
            logger.info(f"🎯 Найдено {len(spaces_to_migrate)} пространств для миграции:")
            logger.info("")
            
            # Сортируем пространства по пути для удобства просмотра
            spaces_with_paths = [(space, self.build_space_path(space)) for space in spaces_to_migrate]
            spaces_with_paths.sort(key=lambda x: x[1])
            
            for i, (space, path) in enumerate(spaces_with_paths, 1):
                logger.info(f"{i:3d}. {space.id:8d} {path}")
            
            logger.info("=" * 80)
            logger.info("💡 Для миграции конкретного пространства используйте:")
            logger.info("   python scripts/board_migration.py --space-id <ID>")
            logger.info("")
            logger.info("💡 Для миграции первых N пространств используйте:")
            logger.info("   python scripts/board_migration.py --limit <N>")
            logger.info("")
            logger.info("💡 Для миграции всех доступных пространств используйте:")
            logger.info("   python scripts/board_migration.py")
            
            return True
            
        except Exception as e:
            logger.error(f"💥 Ошибка при получении списка пространств: {e}")
            return False

    async def migrate_spaces(self, limit: Optional[int] = None, space_id: Optional[int] = None) -> Dict:
        """
        Выполняет миграцию пространств из Kaiten в группы Bitrix24.
        
        Args:
            limit: Максимальное количество пространств для миграции (None = все)
            space_id: ID конкретного пространства для миграции (None = все пространства)
            
        Returns:
            Словарь со статистикой миграции
        """
        logger.info("🚀 НАЧИНАЕМ МИГРАЦИЮ ПРОСТРАНСТВ ИЗ KAITEN В BITRIX24")
        logger.info("🔄 ЛОГИКА: Переносим пространства (НЕ доски)")
        logger.info("=" * 80)
        
        # Проверка взаимоисключающих параметров
        if limit and space_id:
            logger.warning("⚠️ Параметры --limit и --space-id взаимоисключающие. Используется --space-id")
            limit = None
            
        if space_id:
            logger.info(f"🎯 Режим: миграция конкретного пространства ID {space_id}")
        elif limit:
            logger.info(f"🔢 Режим: миграция первых {limit} пространств")
        else:
            logger.info("🔄 Режим: миграция ВСЕХ подходящих пространств")
        
        stats = {
            "processed": 0,
            "created": 0,
            "updated": 0,
            "errors": 0,
            "spaces_migrated": 0,
            "members_added": 0
        }
        
        try:
            # Загружаем маппинг пользователей
            if not await self.load_user_mapping():
                return stats
            
            # Строим иерархию пространств
            if not await self.build_spaces_hierarchy():
                return stats
            
            # Получаем пространства для миграции
            if space_id:
                # Режим конкретного пространства
                target_space = None
                for space in self.spaces_hierarchy.values():
                    if space.id == space_id:
                        target_space = space
                        break
                
                if not target_space:
                    logger.error(f"❌ Пространство с ID {space_id} не найдено в Kaiten!")
                    stats["errors"] += 1
                    return stats
                
                spaces_to_migrate = [target_space]
            else:
                # Режим автоматического определения пространств
                spaces_to_migrate = self.get_spaces_to_migrate()
            
            # Применяем лимит если указан
            if limit:
                spaces_to_migrate = spaces_to_migrate[:limit]
                logger.info(f"🔢 Ограничение: будет обработано {len(spaces_to_migrate)} пространств")
            
            # Получаем существующие группы из Bitrix24
            logger.info("📥 Получение существующих рабочих групп из Bitrix24...")
            existing_groups = await self.bitrix_client.get_workgroup_list()
            groups_map = {group['NAME']: group for group in existing_groups}
            logger.info(f"📊 Найдено {len(existing_groups)} существующих рабочих групп в Bitrix24")
            
            # Обрабатываем каждое пространство
            for i, space in enumerate(spaces_to_migrate, 1):
                try:
                    stats["processed"] += 1
                    
                    # Формируем название группы
                    group_name = self.build_space_path(space)
                    
                    logger.info(f"🔄 [{i}/{len(spaces_to_migrate)}] Обрабатываем пространство: '{group_name}'")
                    
                    # Проверяем существует ли группа
                    if group_name in groups_map:
                        logger.info(f"♻️ Группа '{group_name}' уже существует, обновляем участников...")
                        group_id = str(groups_map[group_name]['ID'])
                        stats["updated"] += 1
                    else:
                        # Создаем новую группу
                        logger.info(f"➕ Создание новой группы '{group_name}'...")
                        
                        group_data = {
                            'NAME': group_name,
                            'DESCRIPTION': f"Пространство из Kaiten: {space.title}",
                            'VISIBLE': 'Y',
                            'OPENED': 'Y',
                            'PROJECT': 'Y'
                        }
                        
                        group_result = await self.bitrix_client.create_workgroup(group_data)
                        if group_result:
                            # Извлекаем ID из результата
                            if isinstance(group_result, dict) and 'ID' in group_result:
                                group_id = group_result['ID']
                            else:
                                group_id = str(group_result)
                            
                            logger.success(f"✅ Создана группа '{group_name}' с ID: {group_id}")
                            stats["created"] += 1
                            groups_map[group_name] = {'ID': group_id, 'NAME': group_name}
                        else:
                            logger.error(f"❌ Ошибка создания группы '{group_name}'")
                            stats["errors"] += 1
                            continue
                    
                    # Сохраняем маппинг пространства -> группы
                    self.space_mapping[str(space.id)] = str(group_id)
                    stats["spaces_migrated"] += 1
                    
                    # Добавляем участников пространства в группу
                    space_members = await self.get_space_members_bitrix_ids(space.id)
                    if space_members:
                        logger.info(f"👥 Добавляем {len(space_members)} участников в группу...")
                        
                        for user_id in space_members:
                            try:
                                success = await self.bitrix_client.add_user_to_workgroup(int(group_id), int(user_id))
                                if success:
                                    stats["members_added"] += 1
                                else:
                                    logger.warning(f"⚠️ Не удалось добавить пользователя {user_id} в группу {group_id}")
                            except Exception as e:
                                logger.warning(f"⚠️ Ошибка добавления пользователя {user_id}: {e}")
                        
                        logger.info(f"✅ Добавлено участников в группу '{group_name}': {len(space_members)}")
                    else:
                        logger.warning(f"⚠️ Нет участников для добавления в группу '{group_name}'")
                    
                except Exception as e:
                    logger.error(f"💥 Ошибка обработки пространства '{space.title}': {e}")
                    stats["errors"] += 1
            
            # Сохраняем маппинг пространств
            await self._save_space_mapping(stats)
            
            # Выводим финальный отчет
            await self._print_final_report(stats)
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка миграции пространств: {e}")
            stats["errors"] += 1
        
        return stats

    async def _save_space_mapping(self, stats: Dict):
        """Сохраняет/обновляет маппинг пространств в файл"""
        mapping_file = Path(__file__).parent.parent / "mappings" / "space_mapping.json"
        mapping_file.parent.mkdir(exist_ok=True)
        
        # Если файл существует, загружаем и объединяем данные
        existing_mapping = {}
        existing_stats = {"processed": 0, "created": 0, "updated": 0, "errors": 0, "spaces_migrated": 0, "members_added": 0}
        
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
            "migration_logic": "Переносим пространства, НЕ доски. Конечные пространства или 2-й уровень.",
            "excluded_spaces": get_excluded_spaces(),
            "stats": combined_stats,
            "mapping": combined_mapping
        }
        
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Маппинг пространств сохранен/обновлен в файл: {mapping_file}")

    async def _print_final_report(self, stats: Dict):
        """Выводит финальный отчет миграции"""
        logger.info("🎉 МИГРАЦИЯ ПРОСТРАНСТВ ЗАВЕРШЕНА")
        logger.info("=" * 80)
        
        logger.info("📋 КРАТКАЯ СВОДКА:")
        logger.info(f"  ✅ Обработано пространств: {stats['processed']}")
        logger.info(f"  ➕ Создано групп: {stats['created']}")
        logger.info(f"  🔄 Обновлено групп: {stats['updated']}")
        logger.info(f"  📋 Пространств мигрировано: {stats['spaces_migrated']}")
        logger.info(f"  👥 Участников добавлено: {stats['members_added']}")
        logger.info(f"  ❌ Ошибок: {stats['errors']}")
        logger.info("=" * 80)
        
        if stats["errors"] > 0:
            logger.error("❌ Миграция пространств завершена с ошибками")
        else:
            logger.success("✅ Миграция пространств завершена успешно!") 