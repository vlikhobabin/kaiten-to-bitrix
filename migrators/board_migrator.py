import asyncio
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from connectors.kaiten_client import KaitenClient
from connectors.bitrix_client import BitrixClient
from models.kaiten_models import KaitenSpace, KaitenBoard, KaitenColumn
from utils.logger import get_logger

logger = get_logger(__name__)

class BoardMigrator:
    """
    Мигратор досок из Kaiten в группы Bitrix24.
    Логика: 1 Board Kaiten = 1 Workgroup Bitrix24
    Пространства используются для построения иерархических путей в названиях групп.
    """
    
    def __init__(self):
        self.kaiten_client = KaitenClient()
        self.bitrix_client = BitrixClient()
        self.user_mapping: Dict[str, str] = {}
        self.board_mapping: Dict[str, str] = {}
        self.column_mapping: Dict[str, str] = {}
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
            
            logger.info(f"📥 Загружен маппинг пользователей из {mapping_file.name}: {len(self.user_mapping)} записей")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки маппинга пользователей: {e}")
            return False

    async def build_spaces_hierarchy(self) -> bool:
        """Строит полную иерархию пространств для построения путей"""
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

    def build_space_path(self, space: KaitenSpace) -> str:
        """
        Строит полный иерархический путь для пространства.
        
        Args:
            space: Пространство Kaiten
            
        Returns:
            Полный путь вида "Родитель1/Родитель2/ТекущееПространство"
        """
        path_parts = []
        current_space = space
        
        # Идем вверх по иерархии, собирая названия
        while current_space:
            path_parts.insert(0, current_space.title)
            
            # Ищем родительское пространство
            if current_space.parent_entity_uid:
                current_space = self.spaces_hierarchy.get(current_space.parent_entity_uid)
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

    async def migrate_boards(self, limit: Optional[int] = None, board_id: Optional[int] = None) -> Dict:
        """
        Выполняет миграцию досок из Kaiten в группы Bitrix24.
        
        Args:
            limit: Максимальное количество досок для миграции (None = все)
            board_id: ID конкретной доски для миграции (None = все доски)
            
        Returns:
            Словарь со статистикой миграции
        """
        logger.info("🚀 НАЧИНАЕМ МИГРАЦИЮ ДОСОК ИЗ KAITEN В BITRIX24")
        logger.info("=" * 80)
        
        # Проверка взаимоисключающих параметров
        if limit and board_id:
            logger.warning("⚠️ Параметры --limit и --board-id взаимоисключающие. Используется --board-id")
            limit = None
            
        if board_id:
            logger.info(f"🎯 Режим: миграция конкретной доски ID {board_id}")
        elif limit:
            logger.info(f"🔢 Режим: миграция первых {limit} досок")
        else:
            logger.info("🔄 Режим: миграция ВСЕХ досок")
        
        stats = {
            "processed": 0,
            "created": 0,
            "updated": 0,
            "errors": 0,
            "boards_migrated": 0,
            "members_added": 0
        }
        
        try:
            # Загружаем маппинг пользователей
            if not await self.load_user_mapping():
                return stats
            
            # Строим иерархию пространств
            if not await self.build_spaces_hierarchy():
                return stats
            
            # Получаем все существующие группы из Bitrix24
            logger.info("📥 Получение существующих рабочих групп из Bitrix24...")
            existing_groups = await self.bitrix_client.get_workgroup_list()
            groups_map = {group['NAME']: group for group in existing_groups}
            logger.info(f"📊 Найдено {len(existing_groups)} существующих рабочих групп в Bitrix24")
            
            # Обходим все пространства и их доски
            board_count = 0
            found_target_board = False  # Флаг для выхода из циклов при обработке конкретной доски
            
            for space_uid, space in self.spaces_hierarchy.items():
                try:
                    # Получаем доски в пространстве
                    boards = await self.kaiten_client.get_boards(space.id)
                    
                    if not boards:
                        logger.debug(f"📭 Пространство '{space.title}' не содержит досок")
                        continue
                    
                    # Строим базовый путь для пространства
                    space_path = self.build_space_path(space)
                    logger.info(f"📁 Обрабатываем пространство: '{space_path}' ({len(boards)} досок)")
                    
                    # Получаем участников пространства
                    space_members = await self.get_space_members_bitrix_ids(space.id)
                    
                    # Обрабатываем каждую доску в пространстве
                    for board in boards:
                        # Если указан конкретный board_id, пропускаем все остальные доски
                        if board_id and board.id != board_id:
                            continue
                            
                        if limit and board_count >= limit:
                            logger.info(f"🔢 Достигнут лимит {limit} досок")
                            break
                        
                        board_count += 1
                        stats["processed"] += 1
                        
                        # Формируем полное название группы: Путь/К/Пространству/НазваниеДоски
                        group_name = f"{space_path}/{board.title}"
                        
                        logger.info(f"🔄 [{board_count}] Обрабатываем доску: '{group_name}'")
                        
                        try:
                            # Проверяем существует ли группа
                            if group_name in groups_map:
                                logger.info(f"♻️ Группа '{group_name}' уже существует, обновляем участников...")
                                group_id = str(groups_map[group_name]['ID'])
                                stats["updated"] += 1
                            else:
                                # Создаем новую группу
                                logger.info(f"➕ Создаем новую группу '{group_name}'...")
                                
                                group_data = {
                                    'NAME': group_name,
                                    'DESCRIPTION': f"Доска из Kaiten: {board.title}. Пространство: {space_path}",
                                    'VISIBLE': 'Y',
                                    'OPENED': 'Y',
                                    'PROJECT': 'Y'  # Создаем как проект для возможности использования канбана
                                }
                                
                                group_result = await self.bitrix_client.create_workgroup(group_data)
                                if group_result:
                                    # Извлекаем ID из результата (может быть словарь или строка)
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
                            
                            # Сохраняем маппинг доски -> группы (используем чистый ID)
                            self.board_mapping[str(board.id)] = str(group_id)
                            stats["boards_migrated"] += 1
                            
                            # Добавляем участников пространства в группу
                            if space_members:
                                logger.info(f"👥 Добавляем {len(space_members)} участников в группу...")
                                
                                for user_id in space_members:
                                    try:
                                        # Конвертируем ID в int для API
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
                            
                            # TODO: Миграция колонок доски в стадии задач
                            # await self.migrate_board_columns(board.id, group_id)
                            
                        except Exception as e:
                            logger.error(f"💥 Ошибка обработки доски '{board.title}': {e}")
                            stats["errors"] += 1
                        
                        # Если обработали конкретную доску, выходим
                        if board_id and board.id == board_id:
                            logger.info(f"✅ Доска {board_id} успешно обработана, завершаем миграцию")
                            found_target_board = True
                            break
                        
                        if limit and board_count >= limit:
                            break
                    
                    # Если обработали конкретную доску, выходим из внешнего цикла
                    if found_target_board:
                        break
                        
                    if limit and board_count >= limit:
                        break
                        
                except Exception as e:
                    logger.error(f"💥 Ошибка обработки пространства '{space.title}': {e}")
                    stats["errors"] += 1
            
            # Проверяем, что конкретная доска была найдена (если указан board_id)
            if board_id and not found_target_board:
                logger.error(f"❌ Доска с ID {board_id} не найдена в Kaiten!")
                stats["errors"] += 1
            
            # Сохраняем маппинг досок
            await self._save_board_mapping(stats)
            
            # Выводим финальный отчет
            await self._print_final_report(stats)
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка миграции досок: {e}")
            stats["errors"] += 1
        
        return stats

    async def _save_board_mapping(self, stats: Dict):
        """Сохраняет/обновляет маппинг досок в файл"""
        mapping_file = Path(__file__).parent.parent / "mappings" / "board_mapping.json"
        mapping_file.parent.mkdir(exist_ok=True)
        
        # Если файл существует, загружаем и объединяем данные
        existing_mapping = {}
        existing_stats = {"processed": 0, "created": 0, "updated": 0, "errors": 0, "boards_migrated": 0, "members_added": 0}
        
        if mapping_file.exists():
            try:
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    existing_mapping = existing_data.get("mapping", {})
                    existing_stats = existing_data.get("stats", existing_stats)
                logger.info(f"📂 Загружен существующий маппинг досок: {len(existing_mapping)} записей")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка загрузки существующего маппинга досок: {e}")
        
        # Объединяем маппинги (новые данные имеют приоритет)
        combined_mapping = {**existing_mapping, **self.board_mapping}
        
        # Объединяем статистику
        combined_stats = {}
        for key in existing_stats.keys():
            combined_stats[key] = existing_stats.get(key, 0) + stats.get(key, 0)
        
        mapping_data = {
            "created_at": datetime.now().isoformat(),
            "description": "Маппинг ID досок Kaiten -> рабочих групп Bitrix24",
            "stats": combined_stats,
            "mapping": combined_mapping
        }
        
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Маппинг досок сохранен/обновлен в файл: {mapping_file}")

    async def _print_final_report(self, stats: Dict):
        """Выводит финальный отчет миграции"""
        logger.info("🎉 МИГРАЦИЯ ДОСОК ЗАВЕРШЕНА")
        logger.info("=" * 80)
        
        logger.info("📋 КРАТКАЯ СВОДКА:")
        logger.info(f"  ✅ Обработано досок: {stats['processed']}")
        logger.info(f"  ➕ Создано групп: {stats['created']}")
        logger.info(f"  🔄 Обновлено групп: {stats['updated']}")
        logger.info(f"  📋 Досок мигрировано: {stats['boards_migrated']}")
        logger.info(f"  👥 Участников добавлено: {stats['members_added']}")
        logger.info(f"  ❌ Ошибок: {stats['errors']}")
        logger.info("=" * 80)
        
        if stats["errors"] > 0:
            logger.error("❌ Миграция досок завершена с ошибками")
        else:
            logger.success("✅ Миграция досок завершена успешно!")

    # TODO: Добавить миграцию колонок досок в стадии задач
    # async def migrate_board_columns(self, board_id: int, group_id: str):
    #     """Мигрирует колонки доски в стадии задач группы"""
    #     pass 