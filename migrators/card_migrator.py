"""
Мигратор карточек Kaiten в задачи Bitrix24.
Реализует логику переноса карточек согласно Задаче 8.
"""

import asyncio
import json
import subprocess
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union, Any

from connectors.kaiten_client import KaitenClient
from connectors.bitrix_client import BitrixClient
from models.kaiten_models import KaitenCard, KaitenBoard, KaitenColumn, KaitenUser
from models.simple_kaiten_models import SimpleKaitenCard, SimpleKaitenUser
from transformers.card_transformer import CardTransformer
from transformers.user_transformer import UserTransformer
from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)

class UserMappingTransformer(UserTransformer):
    """
    Упрощенный трансформер пользователей для работы с заранее созданным маппингом.
    """
    
    def __init__(self, user_mapping: Dict[str, str]):
        self.user_mapping = user_mapping  # kaiten_user_id -> bitrix_user_id
    
    def get_user_id(self, kaiten_user: Union[KaitenUser, SimpleKaitenUser]) -> Optional[str]:
        """
        Возвращает ID пользователя Bitrix24 для пользователя Kaiten.
        
        Args:
            kaiten_user: Объект пользователя Kaiten
            
        Returns:
            ID пользователя Bitrix24 или None
        """
        if not kaiten_user:
            return None
            
        kaiten_user_id = str(kaiten_user.id)
        bitrix_user_id = self.user_mapping.get(kaiten_user_id)
        
        if bitrix_user_id:
            user_name = getattr(kaiten_user, 'full_name', 'Unknown')
            logger.debug(f"Найден маппинг: Kaiten user {user_name} (ID: {kaiten_user_id}) -> Bitrix ID: {bitrix_user_id}")
            return bitrix_user_id
        else:
            user_name = getattr(kaiten_user, 'full_name', 'Unknown')
            logger.warning(f"Не найден маппинг для пользователя {user_name} (ID: {kaiten_user_id})")
            return None

class CardMigrator:
    """
    Мигратор карточек из Kaiten в задачи Bitrix24.
    
    Правила миграции:
    1. Карточки из колонок type: 1 -> стадия "Новые"
    2. Карточки из колонок type: 3 -> НЕ ПЕРЕНОСЯТСЯ (если не указан include_archived=True)
    3. Карточки из колонок type: 3 -> стадия "Сделаны" + STATUS=5 (если указан include_archived=True)
    4. Карточки из остальных колонок -> стадия "Выполняются"
    """
    
    def __init__(self):
        self.kaiten_client = KaitenClient()
        self.bitrix_client = BitrixClient()
        # Создаем пустой UserTransformer, он будет инициализирован после загрузки маппинга
        self.user_transformer = None
        self.card_transformer = None
        
        # Маппинг пользователей, стадий и карточек
        self.user_mapping: Dict[str, str] = {}
        self.stage_mapping: Dict[str, str] = {}  # {"Новые": "stage_id", "Выполняются": "stage_id"}
        self.card_mapping: Dict[str, str] = {}  # {"kaiten_card_id": "bitrix_task_id"}
        
        # Статистика миграции
        self.stats = {
            'cards_total': 0,
            'cards_filtered_out': 0,
            'cards_migrated': 0,
            'cards_updated': 0,  # Счетчик обновленных карточек
            'cards_failed': 0,
            'boards_processed': 0,
            'checklists_migrated': 0,  # Счетчик перенесенных чек-листов
            'checklist_items_migrated': 0,  # Счетчик перенесенных элементов чек-листов
            'comments_migrated': 0,  # Счетчик перенесенных комментариев
            'comments_skipped': 0,   # Счетчик пропущенных комментариев (от ботов)
            'description_files_migrated': 0  # Счетчик файлов из описания
        }

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
            
            # Инициализируем трансформеры после загрузки маппинга
            self.user_transformer = UserMappingTransformer(self.user_mapping)
            self.card_transformer = CardTransformer(self.user_transformer)
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки маппинга пользователей: {e}")
            return False

    async def load_card_mapping(self) -> bool:
        """Загружает маппинг карточек из файла"""
        try:
            mapping_file = Path(__file__).parent.parent / "mappings" / "card_mapping.json"
            
            if not mapping_file.exists():
                # Создаем пустой файл маппинга если его нет
                await self.save_card_mapping()
                logger.info("📄 Создан новый файл маппинга карточек")
                return True
            
            with open(mapping_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.card_mapping = data.get('mapping', {})
            
            logger.info(f"📥 Загружен маппинг карточек: {len(self.card_mapping)} записей")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки маппинга карточек: {e}")
            return False

    async def save_card_mapping(self) -> bool:
        """Сохраняет маппинг карточек в файл"""
        try:
            mapping_file = Path(__file__).parent.parent / "mappings" / "card_mapping.json"
            
            # Создаем директорию если её нет
            mapping_file.parent.mkdir(exist_ok=True)
            
            data = {
                "created_at": datetime.now().isoformat(),
                "description": "Маппинг ID карточек Kaiten -> задач Bitrix24",
                "stats": {
                    "total_migrated": len(self.card_mapping),
                    "last_updated": datetime.now().isoformat()
                },
                "mapping": self.card_mapping
            }
            
            with open(mapping_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"📤 Сохранен маппинг карточек: {len(self.card_mapping)} записей")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения маппинга карточек: {e}")
            return False

    async def get_group_id_for_space(self, space_id: int) -> Optional[int]:
        """
        Получает ID группы Bitrix24 для указанного пространства Kaiten из маппинга.
        
        Args:
            space_id: ID пространства Kaiten
            
        Returns:
            ID группы Bitrix24 или None если маппинг не найден
        """
        try:
            mapping_file = Path(__file__).parent.parent / "mappings" / "space_mapping.json"
            
            if not mapping_file.exists():
                logger.error("❌ Не найден файл space_mapping.json")
                return None
            
            with open(mapping_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                mapping = data.get('mapping', {})
            
            # Ищем пространство в маппинге
            space_id_str = str(space_id)
            if space_id_str in mapping:
                group_id = int(mapping[space_id_str])
                logger.debug(f"📋 Найден маппинг: пространство {space_id} -> группа {group_id}")
                return group_id
            else:
                logger.debug(f"❌ Пространство {space_id} не найдено в маппинге")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка чтения маппинга пространств: {e}")
            return None

    async def get_task_stages_by_names(self, group_id: int, stage_names: List[str]) -> Dict[str, str]:
        """
        Получает ID стадий задач по их названиям.
        Если не может получить - создает стандартные стадии.
        
        Args:
            group_id: ID группы в Bitrix24
            stage_names: Список названий стадий для поиска
            
        Returns:
            Словарь {название_стадии: stage_id}
        """
        try:
            logger.info(f"🔍 Получение стадий задач для группы {group_id}...")
            
            # Пытаемся получить существующие стадии
            access_denied = False
            try:
                stages_data = await self.bitrix_client.get_task_stages(group_id)
                
                stage_mapping = {}
                if stages_data:
                    # API возвращает словарь {stage_id: stage_object}
                    if isinstance(stages_data, dict):
                        for stage_id, stage in stages_data.items():
                            if isinstance(stage, dict):
                                title = stage.get('TITLE', '') or stage.get('title', '')
                                
                                if title in stage_names:
                                    stage_mapping[title] = str(stage_id)
                                    logger.debug(f"✅ Найдена стадия '{title}' с ID {stage_id}")
                
                # Если нашли все нужные стадии - возвращаем их
                if len(stage_mapping) == len(stage_names):
                    logger.debug(f"📊 Найдено {len(stage_mapping)} из {len(stage_names)} требуемых стадий")
                    return stage_mapping
                    
            except Exception as e:
                error_message = str(e)
                if "ACCESS_DENIED" in error_message:
                    access_denied = True
                    logger.warning(f"⚠️ Нет доступа к стадиям группы {group_id}: {e}")
                    logger.warning("💡 Стадии скорее всего существуют, но недоступны для просмотра")
                    logger.warning("🔄 Продолжаем без привязки к стадиям - задачи будут создаваться в стадии по умолчанию")
                    return {}  # Возвращаем пустой маппинг
                else:
                    logger.warning(f"⚠️ Не удалось получить существующие стадии: {e}")
            
            # Если была ошибка доступа - НЕ пытаемся создавать стадии
            if access_denied:
                logger.info("⚠️ Пропускаем создание стадий из-за ошибки доступа")
                return {}
            
            # Если не смогли получить стадии или нашли не все - создаем недостающие
            logger.info("📝 Создаем недостающие стандартные стадии для группы...")
            stage_mapping = {}
            
            for i, stage_name in enumerate(stage_names):
                try:
                    stage_data = await self.bitrix_client.create_task_stage(
                        entity_id=group_id,
                        title=stage_name,
                        sort=(i + 1) * 100,  # 100, 200, 300...
                        color="0066CC"
                    )
                    # Проверяем, что stage_data это словарь и содержит ID
                    if stage_data and isinstance(stage_data, dict) and 'ID' in stage_data:
                        stage_mapping[stage_name] = str(stage_data['ID'])
                        logger.info(f"✅ Создана стадия '{stage_name}' с ID {stage_data['ID']}")
                    elif stage_data and isinstance(stage_data, (int, str)):
                        # Если API вернул просто ID
                        stage_mapping[stage_name] = str(stage_data)
                        logger.info(f"✅ Создана стадия '{stage_name}' с ID {stage_data}")
                    else:
                        logger.warning(f"⚠️ Не удалось создать стадию '{stage_name}': неожиданный ответ {stage_data}")
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка создания стадии '{stage_name}': {e}")
            
            logger.info(f"📊 Итого настроено {len(stage_mapping)} из {len(stage_names)} требуемых стадий")
            return stage_mapping
            
        except Exception as e:
            logger.error(f"Критическая ошибка получения/создания стадий: {e}")
            return {}

    def get_target_stage_for_card(self, card: Union[KaitenCard, SimpleKaitenCard], include_archived: bool = False) -> Optional[str]:
        """
        Определяет целевую стадию для карточки на основе правил миграции.
        
        Args:
            card: Карточка Kaiten
            include_archived: Если True, карточки type: 3 попадают в стадию "Сделаны"
            
        Returns:
            Название целевой стадии или None если карточку переносить не нужно
        """
        if hasattr(card, 'column') and card.column:
            column_type = card.column.type
        else:
            column_type = None
        
        if column_type == 1:  # Начальная колонка
            return "Новые"
        elif column_type == 3:  # Финальная колонка
            if include_archived:
                return "Сделаны"  # Если включены архивные - переносим в стадию "Сделаны"
            else:
                return None  # Иначе не переносим
        else:  # Все остальные колонки (включая None)
            return "Выполняются"

    def should_migrate_card(self, card: Union[KaitenCard, SimpleKaitenCard], include_archived: bool = False) -> bool:
        """
        Проверяет, нужно ли переносить карточку.
        
        Args:
            card: Карточка Kaiten
            include_archived: Если True, карточки type: 3 будут включены в миграцию
            
        Returns:
            True если карточку нужно переносить, False иначе
        """
        # Фильтр по типу колонки
        if hasattr(card, 'column') and card.column and card.column.type == 3:
            if not include_archived:
                logger.debug(f"🚫 Карточка '{card.title}' пропущена (финальная колонка type: 3)")
                return False
            else:
                logger.debug(f"✅ Карточка '{card.title}' включена (финальная колонка type: 3, но включены архивные)")
                # Продолжаем проверки ниже
            
        # Фильтр архивных карточек
        if card.archived:
            logger.debug(f"🚫 Карточка '{card.title}' пропущена (архивная)")
            return False
            
        return True

    async def migrate_cards_from_space(self, space_id: int, target_group_id: int, 
                                     list_only: bool = False, limit: int | None = None, card_id: int | None = None, include_archived: bool = False) -> bool:
        """
        Мигрирует карточки из всех досок указанного пространства.
        
        Args:
            space_id: ID пространства Kaiten
            target_group_id: ID группы в Bitrix24
            list_only: Если True, только выводит список карточек без миграции
            limit: Если указан, обрабатывает только первые N карточек первой доски
            card_id: Если указан, обрабатывает только конкретную карточку
            include_archived: Если True, включает карточки из финальных колонок (type: 3)
            
        Returns:
            True в случае успеха
        """
        try:
            # Обработка конкретной карточки
            if card_id:
                return await self.migrate_single_card_by_id(card_id, target_group_id, list_only, include_archived)
            
            logger.info(f"🚀 Начинаем обработку пространства {space_id}")
            
            # Загружаем маппинги пользователей и карточек
            if not await self.load_user_mapping():
                return False
            
            if not await self.load_card_mapping():
                return False
            
            # Получаем список досок пространства
            logger.info(f"📥 Получение досок пространства {space_id}...")
            boards = await self.kaiten_client.get_boards(space_id)
            
            if not boards:
                logger.warning(f"⚠️ В пространстве {space_id} не найдено досок")
                return True
            
            logger.info(f"📊 Найдено {len(boards)} досок в пространстве")
            
            # Если не в режиме просмотра, получаем стадии для миграции
            if not list_only:
                required_stages = ["Новые", "Выполняются"]
                if include_archived:
                    required_stages.append("Сделаны")
                self.stage_mapping = await self.get_task_stages_by_names(target_group_id, required_stages)
                
                if len(self.stage_mapping) == 0:
                    logger.warning("⚠️ Не удалось получить или создать ни одной стадии")
                    logger.warning("🔄 Продолжаем миграцию без привязки к стадиям")
                elif len(self.stage_mapping) != len(required_stages):
                    missing_stages = set(required_stages) - set(self.stage_mapping.keys())
                    logger.warning(f"⚠️ Не удалось получить стадии: {missing_stages}")
                    logger.warning("🔄 Продолжаем миграцию с доступными стадиями")
                else:
                    logger.success("✅ Все необходимые стадии настроены")
            
            # Обрабатываем каждую доску
            processed_cards = 0
            for board in boards:
                remaining_limit = limit - processed_cards if limit else None
                cards_processed_from_board = await self.process_board(
                    board, target_group_id, list_only, remaining_limit, include_archived
                )
                processed_cards += cards_processed_from_board
                self.stats['boards_processed'] += 1
                
                # Если указан лимит и мы его достигли, или обработали первую доску при лимите
                if limit and (processed_cards >= limit or cards_processed_from_board > 0):
                    if processed_cards >= limit:
                        logger.info(f"🎯 Достигнут лимит: обработано {processed_cards} карточек")
                    else:
                        logger.info(f"🎯 Обработана первая доска с карточками: {cards_processed_from_board} карточек")
                    break
            
            # Выводим итоговую статистику
            self.print_migration_stats()
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка миграции карточек из пространства {space_id}: {e}")
            return False

    async def migrate_single_card_by_id(self, card_id: int, target_group_id: int, list_only: bool = False, include_archived: bool = False) -> bool:
        """
        Мигрирует конкретную карточку по ее ID.
        
        Args:
            card_id: ID карточки Kaiten
            target_group_id: ID группы в Bitrix24
            list_only: Если True, только выводит информацию о карточке
            include_archived: Если True, включает карточки из финальных колонок (type: 3)
            
        Returns:
            True в случае успеха
        """
        try:
            logger.info(f"🎯 Обработка конкретной карточки {card_id}")
            
            # Загружаем маппинги пользователей и карточек
            if not await self.load_user_mapping():
                return False
            
            if not await self.load_card_mapping():
                return False
            
            # Получаем карточку по ID
            logger.info(f"📥 Получение карточки {card_id} из Kaiten...")
            card = await self.kaiten_client.get_card_by_id(card_id)
            
            if not card:
                logger.error(f"❌ Карточка {card_id} не найдена в Kaiten")
                return False
            
            logger.info(f"✅ Карточка найдена: '{card.title}'")
            
            # Если не в режиме просмотра, получаем стадии для миграции
            if not list_only:
                required_stages = ["Новые", "Выполняются"]
                if include_archived:
                    required_stages.append("Сделаны")
                self.stage_mapping = await self.get_task_stages_by_names(target_group_id, required_stages)
                
                if len(self.stage_mapping) == 0:
                    logger.warning("⚠️ Не удалось получить или создать ни одной стадии")
                    logger.warning("🔄 Продолжаем миграцию без привязки к стадиям")
                elif len(self.stage_mapping) != len(required_stages):
                    missing_stages = set(required_stages) - set(self.stage_mapping.keys())
                    logger.warning(f"⚠️ Не удалось получить стадии: {missing_stages}")
                    logger.warning("🔄 Продолжаем миграцию с доступными стадиями")
                else:
                    logger.success("✅ Все необходимые стадии настроены")
            
            # Обрабатываем карточку
            self.stats['cards_total'] = 1
            
            # Запоминаем статистику до обработки
            errors_before = self.stats['cards_failed']
            filtered_before = self.stats['cards_filtered_out']
            
            processed = await self.process_card(card, target_group_id, list_only, include_archived)
            
            # Выводим статистику
            self.print_migration_stats()
            
            # Если карточка была отфильтрована (но без ошибок) - это успех
            if not processed:
                # Проверяем: была ли ошибка или просто фильтрация?
                if self.stats['cards_failed'] == errors_before and self.stats['cards_filtered_out'] > filtered_before:
                    logger.info("💡 Карточка отфильтрована согласно правилам миграции")
                    return True  # Корректная фильтрация не является ошибкой
            
            return processed
            
        except Exception as e:
            logger.error(f"Ошибка обработки карточки {card_id}: {e}")
            return False

    async def process_board(self, board: KaitenBoard, target_group_id: int, list_only: bool = False, limit: int | None = None, include_archived: bool = False):
        """
        Обрабатывает карточки одной доски.
        
        Args:
            board: Доска Kaiten
            target_group_id: ID группы в Bitrix24
            list_only: Если True, только выводит список карточек
            limit: Максимальное количество карточек для обработки
            include_archived: Если True, включает карточки из финальных колонок (type: 3)
            
        Returns:
            Количество обработанных карточек
        """
        try:
            logger.info(f"📋 Обработка доски '{board.title}' (ID: {board.id})")
            
            # Получаем карточки доски через правильный API эндпоинт (исключаем архивные)
            try:
                cards_data = await self.kaiten_client._request('GET', f'/api/v1/cards?board_id={board.id}&archived=false')
                # Получаем полную информацию для каждой карточки включая описание
                cards = []
                if cards_data:
                    logger.debug(f"   🔍 Получаем полную информацию для {len(cards_data)} карточек...")
                    for card_data in cards_data:
                        try:
                            card_id = card_data.get('id')
                            if card_id:
                                # Получаем полную карточку с описанием
                                full_card = await self.kaiten_client.get_card_by_id(card_id)
                                if full_card:
                                    cards.append(full_card)
                                else:
                                    # Fallback к краткой информации если полная недоступна
                                    card = SimpleKaitenCard(**card_data)
                                    cards.append(card)
                            else:
                                logger.debug(f"   ⚠️ Карточка без ID: {card_data}")
                        except Exception as e:
                            logger.debug(f"   ⚠️ Не удалось обработать карточку {card_data.get('id', 'unknown')}: {e}")
            except Exception as e:
                logger.debug(f"   ❌ Не удалось получить карточки доски через board_id: {e}")
                cards = []
            
            if not cards:
                logger.info(f"   📭 Доска '{board.title}' не содержит карточек")
                return 0
            
            logger.info(f"   📊 Найдено {len(cards)} карточек на доске")
            self.stats['cards_total'] += len(cards)
            
            # Применяем лимит если он задан
            cards_to_process = cards[:limit] if limit else cards
            processed_count = 0
            
            if limit and len(cards_to_process) < len(cards):
                logger.info(f"   🎯 Будет обработано {len(cards_to_process)} из {len(cards)} карточек (лимит)")
            
            # Обрабатываем каждую карточку
            for card in cards_to_process:
                processed = await self.process_card(card, target_group_id, list_only, include_archived)
                if processed:  # Учитываем только карточки, которые действительно обработались
                    processed_count += 1
                
                # Проверяем лимит для обработанных карточек
                if limit and processed_count >= limit:
                    break
            
            return processed_count
                
        except Exception as e:
            logger.error(f"Ошибка обработки доски {board.title}: {e}")
            return 0

    async def process_card(self, card: Union[KaitenCard, SimpleKaitenCard], target_group_id: int, list_only: bool = False, include_archived: bool = False):
        """
        Обрабатывает одну карточку.
        
        Args:
            card: Карточка Kaiten
            target_group_id: ID группы в Bitrix24
            list_only: Если True, только выводит информацию о карточке
            include_archived: Если True, включает карточки из финальных колонок (type: 3)
            
        Returns:
            True если карточка была обработана (не отфильтрована), False иначе
        """
        try:
            # Логируем начало обработки карточки
            if not list_only:
                logger.info(f"🔄 Карточка {card.id}")
            # Проверяем, была ли карточка уже мигрирована
            card_id_str = str(card.id)
            if card_id_str in self.card_mapping:
                existing_task_id = self.card_mapping[card_id_str]
                if list_only:
                    logger.info(f"   ⏭️  Карточка: ID {card.id}, '{card.title}' -> УЖЕ МИГРИРОВАНА (задача ID {existing_task_id})")
                    return True  # Считаем как обработанную
                else:
                    # Обновляем существующую карточку
                    logger.info(f"🔄 Карточка {card.id} -> обновляем задачу {existing_task_id}")
                    
                    # Определяем целевую стадию для обновления
                    target_stage = self.get_target_stage_for_card(card, include_archived)
                    if not target_stage:
                        if hasattr(card, 'column') and card.column and card.column.type == 3:
                            # Это финальная колонка - пропускаем
                            self.stats['cards_filtered_out'] += 1
                            return False
                        else:
                            # Не финальная колонка, но стадия не определена - используем дефолтную
                            target_stage = "Выполняются"
                            logger.warning(f"⚠️ Не удалось определить стадию для карточки '{card.title}', используем '{target_stage}'")
                    
                    # Обновляем существующую задачу
                    await self.update_existing_card(card, int(existing_task_id), target_group_id, target_stage)
                    return True
            
            # Проверяем, нужно ли переносить карточку
            if not self.should_migrate_card(card, include_archived):
                self.stats['cards_filtered_out'] += 1
                return False
            
            # Определяем целевую стадию
            target_stage = self.get_target_stage_for_card(card, include_archived)
            if not target_stage:
                if hasattr(card, 'column') and card.column and card.column.type == 3:
                    # Это финальная колонка - пропускаем
                    self.stats['cards_filtered_out'] += 1
                    return False
                else:
                    # Не финальная колонка, но стадия не определена - используем дефолтную
                    target_stage = "Выполняются"
                    logger.warning(f"⚠️ Не удалось определить стадию для карточки '{card.title}', используем '{target_stage}'")
            
            if list_only:
                # Режим просмотра - выводим информацию о карточке
                column_type = card.column.type if (hasattr(card, 'column') and card.column) else 'unknown'
                logger.info(f"   📄 Карточка: ID {card.id}, '{card.title}', колонка type: {column_type} -> стадия '{target_stage}'")
                return True
            
            # Режим миграции - создаем задачу
            await self.migrate_single_card(card, target_group_id, target_stage)
            return True
            
        except Exception as e:
            logger.error(f"Ошибка обработки карточки {card.id}: {e}")
            self.stats['cards_failed'] += 1
            return False

    async def migrate_single_card(self, card: Union[KaitenCard, SimpleKaitenCard], target_group_id: int, target_stage: str):
        """
        Мигрирует одну карточку в задачу Bitrix24.
        
        Args:
            card: Карточка Kaiten
            target_group_id: ID группы в Bitrix24
            target_stage: Название целевой стадии
        """
        try:
            # Получаем исходное описание
            original_description = getattr(card, 'description', '') or ""
            
            # Получаем пользовательские поля (но НЕ добавляем их в описание)
            custom_properties = await self.get_custom_properties_from_card(card)
            
            # Используем исходное описание БЕЗ пользовательских полей
            enhanced_description = original_description
            
            # Трансформируем карточку в формат Bitrix24
            if not self.card_transformer:
                logger.error(f"❌ CardTransformer не инициализирован")
                self.stats['cards_failed'] += 1
                return
                
            task_data = self.card_transformer.transform(card, str(target_group_id))
            
            if not task_data:
                logger.error(f"❌ Карточка {card.id}: не удалось трансформировать")
                self.stats['cards_failed'] += 1
                return
            
            # Добавляем стадию
            stage_id = self.stage_mapping.get(target_stage)
            if stage_id:
                task_data['STAGE_ID'] = stage_id
                logger.debug(f"Задача будет создана в стадии '{target_stage}' (ID: {stage_id})")
            else:
                logger.debug(f"Стадия '{target_stage}' не найдена в маппинге, создаем задачу без стадии")
            
            # Для архивных карточек устанавливаем статус "Завершена" (STATUS = 5)
            if target_stage == "Сделаны":
                task_data['STATUS'] = 5
                logger.debug(f"Архивная карточка: устанавливаем STATUS = 5 (Завершена)")
            
            # Создаем задачу в Bitrix24 с исходным описанием
            task_id = await self.bitrix_client.create_task(
                title=task_data['TITLE'],
                description=task_data.get('DESCRIPTION', ''),
                responsible_id=task_data['RESPONSIBLE_ID'],
                group_id=target_group_id,
                **{k: v for k, v in task_data.items() 
                   if k not in ['TITLE', 'DESCRIPTION', 'RESPONSIBLE_ID', 'GROUP_ID']}
            )
            
            if task_id:
                logger.info(f"✅ Карточка {card.id} -> Задача {task_id}")
                
                # Добавляем в маппинг и сохраняем
                self.card_mapping[str(card.id)] = str(task_id)
                await self.save_card_mapping()
                
                # ✅ Применяем пользовательские поля к созданной задаче
                if custom_properties:
                    success = await self.apply_custom_fields_to_bitrix_task(task_id, custom_properties)
                    if success:
                        logger.info(f"✅ Применены пользовательские поля к задаче {task_id}")
                    else:
                        logger.warning(f"❌ Не удалось применить пользовательские поля к задаче {task_id}")
                
                # Обрабатываем файлы в описании с новым task_id и обновляем описание
                # Используем enhanced_description для обработки файлов (включает пользовательские поля)
                updated_description, migrated_files = await self.migrate_description_files(
                    card.id, target_group_id, enhanced_description, task_id
                )
                
                # Если описание изменилось (файлы были перенесены), обновляем задачу
                if updated_description != enhanced_description:
                    update_success = await self.bitrix_client.update_task(
                        task_id=task_id,
                        DESCRIPTION=updated_description
                    )
                    if update_success and migrated_files > 0:
                        logger.debug(f"Перенесено {migrated_files} файлов из описания в папку задачи {task_id}")
                
                # Мигрируем чек-листы
                await self.migrate_card_checklists(card.id, task_id, card.title)
                
                # Мигрируем комментарии
                await self.migrate_card_comments(card.id, task_id, card.title, target_group_id)
                
                self.stats['cards_migrated'] += 1
            else:
                logger.error(f"❌ Карточка {card.id}: не удалось создать задачу")
                self.stats['cards_failed'] += 1
                
        except Exception as e:
            logger.error(f"Ошибка миграции карточки '{card.title}': {e}")
            self.stats['cards_failed'] += 1

    async def update_existing_card(self, card: Union[KaitenCard, SimpleKaitenCard], task_id: int, target_group_id: int, target_stage: str):
        """
        Обновляет существующую задачу в Bitrix24 данными из карточки Kaiten.
        
        Args:
            card: Карточка Kaiten
            task_id: ID существующей задачи в Bitrix24
            target_group_id: ID группы в Bitrix24
            target_stage: Название целевой стадии
        """
        try:
            # Получаем исходное описание
            original_description = getattr(card, 'description', '') or ""
            
            # Получаем пользовательские поля (но НЕ добавляем их в описание)
            custom_properties = await self.get_custom_properties_from_card(card)
            logger.info(f"🔧 Пользовательские поля карточки {card.id}: {custom_properties if custom_properties else 'нет полей'}")
            
            # Используем исходное описание БЕЗ пользовательских полей
            enhanced_description = original_description
            
            # Обрабатываем файлы в расширенном описании
            updated_description, migrated_files = await self.migrate_description_files(
                card.id, target_group_id, enhanced_description, task_id
            )
            
            # Трансформируем карточку в формат Bitrix24
            if not self.card_transformer:
                logger.error(f"❌ CardTransformer не инициализирован")
                self.stats['cards_failed'] += 1
                return
                
            task_data = self.card_transformer.transform(card, str(target_group_id))
            
            if not task_data:
                logger.error(f"❌ Карточка {card.id}: не удалось трансформировать для обновления")
                self.stats['cards_failed'] += 1
                return
            
            # Добавляем стадию
            stage_id = self.stage_mapping.get(target_stage)
            if stage_id:
                task_data['STAGE_ID'] = stage_id
                logger.debug(f"Задача будет обновлена в стадии '{target_stage}' (ID: {stage_id})")
            else:
                logger.debug(f"Стадия '{target_stage}' не найдена в маппинге, обновляем задачу без изменения стадии")
            
            # Для архивных карточек устанавливаем статус "Завершена" (STATUS = 5)
            if target_stage == "Сделаны":
                task_data['STATUS'] = 5
                logger.debug(f"Архивная карточка: устанавливаем STATUS = 5 (Завершена)")
            
            # Обновляем задачу в Bitrix24
            success = await self.bitrix_client.update_task(
                task_id=task_id,
                **task_data
            )
            
            if success:
                logger.info(f"✅ Карточка {card.id} -> обновлена задача {task_id}")
                if migrated_files > 0:
                    logger.debug(f"Перенесено {migrated_files} файлов из описания в папку задачи {task_id}")
                
                # ✅ Применяем пользовательские поля к обновленной задаче
                if custom_properties:
                    logger.info(f"🔧 Применяем {len(custom_properties)} пользовательских полей к задаче {task_id}")
                    fields_success = await self.apply_custom_fields_to_bitrix_task(task_id, custom_properties)
                    if fields_success:
                        logger.info(f"✅ Применены пользовательские поля к задаче {task_id}")
                    else:
                        logger.warning(f"❌ Не удалось применить пользовательские поля к задаче {task_id}")
                else:
                    logger.debug(f"У карточки {card.id} нет пользовательских полей для применения")
                
                # Мигрируем чек-листы (при обновлении тоже синхронизируем)
                await self.migrate_card_checklists(card.id, task_id, card.title, is_update=True)
                
                # Мигрируем комментарии (при обновлении тоже синхронизируем)
                await self.migrate_card_comments(card.id, task_id, card.title, target_group_id, is_update=True)
                
                self.stats['cards_updated'] += 1
            else:
                logger.error(f"❌ Карточка {card.id}: не удалось обновить задачу {task_id}")
                self.stats['cards_failed'] += 1
                
        except Exception as e:
            logger.error(f"Ошибка обновления задачи ID {task_id} для карточки '{card.title}': {e}")
            self.stats['cards_failed'] += 1

    async def migrate_card_checklists(self, card_id: int, task_id: int, card_title: str, is_update: bool = False) -> bool:
        """
        Мигрирует чек-листы карточки Kaiten в чек-листы задачи Bitrix24.
        
        Args:
            card_id: ID карточки Kaiten
            task_id: ID задачи Bitrix24
            card_title: Название карточки (для логирования)
            is_update: Если True, то это обновление (нужно очистить старые чек-листы)
            
        Returns:
            True в случае успеха
        """
        try:
            # Если это обновление, проверяем существующие чек-листы
            existing_checklists = []
            if is_update:
                logger.info(f"🔍 Проверяем существующие чек-листы задачи {task_id}...")
                existing_items = await self.bitrix_client.get_task_checklists(task_id)
                
                # Собираем названия существующих групп чек-листов
                for item in existing_items:
                    parent_id = item.get('PARENT_ID') or item.get('parent_id')
                    # Это группа (корневой элемент)
                    if not parent_id or parent_id == 'N/A' or str(parent_id) == '0':
                        title = item.get('TITLE') or item.get('title', '')
                        if title and title not in existing_checklists:
                            existing_checklists.append(title)
                
                if existing_checklists:
                    logger.info(f"📋 Найдено {len(existing_checklists)} групп чек-листов: {', '.join(existing_checklists[:3])}{'...' if len(existing_checklists) > 3 else ''}")
                else:
                    logger.debug(f"✅ У задачи {task_id} нет чек-листов")
            
            # Получаем чек-листы карточки из Kaiten
            checklists = await self.kaiten_client.get_card_checklists(card_id)
            
            if not checklists:
                logger.debug(f"У карточки '{card_title}' нет чек-листов")
                return True
            
            logger.debug(f"Переносим {len(checklists)} чек-листов для карточки '{card_title}'")
            
            migrated_checklists = 0
            migrated_items = 0
            
            for checklist in checklists:
                try:
                    # Используем поле 'name' для названия чек-листа (как в Kaiten API)
                    checklist_title = checklist.get('name', checklist.get('title', 'Без названия'))
                    checklist_items = checklist.get('items', [])
                    
                    # Проверяем, существует ли уже такой чек-лист при обновлении
                    if is_update and checklist_title in existing_checklists:
                        logger.debug(f"   ⏭️ Чек-лист '{checklist_title}' уже существует, пропускаем")
                        continue
                    
                    logger.debug(f"   📋 Добавляем чек-лист '{checklist_title}' с {len(checklist_items)} элементами")
                    
                    # Создаем группу чек-листа с правильным названием
                    group_id = await self.bitrix_client.create_checklist_group(
                        task_id=task_id,
                        title=checklist_title
                    )
                    
                    if group_id:
                        migrated_checklists += 1
                        logger.debug(f"✅ Создана группа чек-листа '{checklist_title}' с ID {group_id}")
                    else:
                        logger.warning(f"⚠️ Не удалось создать группу для чек-листа '{checklist_title}', элементы будут добавлены без группы")
                        group_id = None  # Элементы будут добавлены как отдельные элементы
                    
                    # Переносим элементы чек-листа как дочерние к группе (или отдельно, если группа не создалась)
                    for item in checklist_items:
                        item_text = item.get('text', item.get('title', ''))
                        is_complete = item.get('checked', False) or item.get('completed', False)
                        
                        if item_text.strip():
                            if group_id:
                                await self.bitrix_client.add_checklist_item(
                                    task_id=task_id,
                                    title=item_text,  # Убираем отступ и эмодзи - теперь это обычные элементы
                                    is_complete=is_complete,
                                    parent_id=group_id  # Указываем ID группы как родительский элемент
                                )
                            else:
                                await self.bitrix_client.add_checklist_item(
                                    task_id=task_id,
                                    title=item_text,  # Добавляем как отдельный элемент без родителя
                                    is_complete=is_complete
                                )
                            migrated_items += 1
                    
                except Exception as e:
                    checklist_name = checklist.get('name', checklist.get('title', 'unknown'))
                    logger.warning(f"Ошибка переноса чек-листа '{checklist_name}': {e}")
                    continue
            
            if migrated_checklists > 0:
                logger.debug(f"Чек-листы: {migrated_checklists} перенесено, {migrated_items} элементов")
                self.stats['checklists_migrated'] += migrated_checklists
                self.stats['checklist_items_migrated'] += migrated_items
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка миграции чек-листов для карточки '{card_title}': {e}")
            return False

    def update_comment_dates_via_ssh(self, comment_dates: Dict[str, str]) -> bool:
        """
        Обновляет даты комментариев через SSH вызов скрипта на VPS сервере.
        
        Args:
            comment_dates: Словарь {comment_id: datetime_string}
            
        Returns:
            True в случае успеха
        """
        if not comment_dates:
            logger.debug("Нет комментариев для обновления дат")
            return True
        
        # Проверяем наличие SSH настроек
        if not settings.ssh_host or not settings.ssh_key_path:
            logger.warning("⚠️ SSH настройки не настроены, пропускаем обновление дат комментариев")
            logger.info("💡 Для настройки SSH добавьте SSH_HOST и SSH_KEY_PATH в .env файл")
            return True
        
        try:
            # Формируем JSON строку для передачи
            json_data = json.dumps(comment_dates)
            
            # SSH команда для выполнения скрипта на сервере
            ssh_command = [
                "ssh", 
                "-i", settings.ssh_key_path,
                f"{settings.ssh_user}@{settings.ssh_host}",
                f"python3 {settings.vps_script_path} '{json_data}'"
            ]
            
            logger.debug(f"🔄 Обновление дат для {len(comment_dates)} комментариев через SSH...")
            
            # Выполняем команду
            result = subprocess.run(
                ssh_command,
                capture_output=True,
                text=True,
                timeout=30  # Таймаут 30 секунд
            )
            
            if result.returncode == 0:
                logger.success(f"✅ Даты комментариев успешно обновлены через SSH")
                if result.stdout:
                    # Выводим последние строки вывода для подтверждения
                    output_lines = result.stdout.strip().split('\n')
                    for line in output_lines[-3:]:  # Последние 3 строки
                        if line.strip():
                            logger.debug(f"  SSH: {line}")
                return True
            else:
                logger.error(f"❌ Ошибка SSH команды (код {result.returncode})")
                if result.stderr:
                    logger.error(f"SSH stderr: {result.stderr}")
                if result.stdout:
                    logger.error(f"SSH stdout: {result.stdout}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Таймаут выполнения SSH команды")
            return False
        except Exception as e:
            logger.error(f"❌ Ошибка выполнения SSH команды: {e}")
            return False

    async def migrate_card_comments(self, card_id: int, task_id: int, card_title: str, target_group_id: int, is_update: bool = False) -> bool:
        """
        Мигрирует комментарии карточки Kaiten в комментарии задачи Bitrix24 с файлами.
        С установкой правильных дат через SSH на VPS сервер.
        
        Args:
            card_id: ID карточки Kaiten
            task_id: ID задачи Bitrix24
            card_title: Название карточки (для логирования)
            is_update: Если True, то это обновление (при обновлении не дублируем комментарии)
            
        Returns:
            True в случае успеха
        """
        try:
            # При обновлении проверяем существующие комментарии, чтобы избежать дублирования
            existing_comments = []
            if is_update:
                logger.debug(f"🔍 Проверяем существующие комментарии задачи {task_id}...")
                existing_comments_data = await self.bitrix_client.get_task_comments(task_id)
                
                # Собираем тексты существующих комментариев для сравнения
                for comment in existing_comments_data:
                    text = comment.get('POST_MESSAGE', '').strip()
                    if text:
                        existing_comments.append(text)
                
                if existing_comments:
                    logger.debug(f"📋 Найдено {len(existing_comments)} комментариев в задаче")
            
            # Получаем комментарии карточки из Kaiten
            comments = await self.kaiten_client.get_card_comments(card_id)
            
            if not comments:
                logger.debug(f"У карточки '{card_title}' нет комментариев")
                return True
            
            # Сортируем комментарии по дате создания (от старых к новым)
            # чтобы они создавались в Bitrix24 в правильном хронологическом порядке
            try:
                comments.sort(key=lambda x: x.get('created', ''), reverse=False)
                logger.debug(f"🕒 Отсортировано {len(comments)} комментариев по дате создания (от старых к новым)")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось отсортировать комментарии по дате: {e}")
                # Продолжаем с исходным порядком комментариев
            
            # Получаем файлы карточки для привязки к комментариям
            card_files = await self.kaiten_client.get_card_files(card_id)
            files_by_comment = {}  # {comment_id: [файлы]}
            
            if card_files:
                logger.debug(f"📎 Найдено {len(card_files)} файлов для карточки {card_id}")
                for file_info in card_files:
                    comment_id = file_info.get('comment_id')
                    if comment_id:
                        if comment_id not in files_by_comment:
                            files_by_comment[comment_id] = []
                        files_by_comment[comment_id].append(file_info)
            
            logger.debug(f"Переносим комментарии для карточки '{card_title}'" + 
                       (f" с {len(card_files)} файлами" if card_files else ""))
            
            migrated_comments = 0
            skipped_comments = 0
            migrated_files = 0
            comment_dates_to_update = {}  # {comment_id: original_date}
            
            for comment in comments:
                try:
                    # Получаем данные комментария
                    comment_text = comment.get('text', '').strip()
                    author_data = comment.get('author', {})
                    created_date = comment.get('created')  # Дата в формате ISO (исправлено поле!)
                    
                    if not comment_text:
                        logger.debug(f"   ⏭️ Пропускаем пустой комментарий")
                        continue
                    
                    # Проверяем, есть ли автор в маппинге пользователей
                    author_id_raw = author_data.get('id')
                    author_name = author_data.get('full_name', 'Неизвестный пользователь')
                    
                    # Фильтруем ботов (отрицательные ID) и пользователей не в маппинге
                    if author_id_raw is None or author_id_raw < 0:
                        logger.debug(f"   🤖 Пропускаем комментарий от служебного бота: {author_name}")
                        skipped_comments += 1
                        continue
                    
                    author_id_kaiten = str(author_id_raw)
                    if author_id_kaiten not in self.user_mapping:
                        logger.debug(f"   🤖 Пропускаем комментарий от пользователя вне маппинга: {author_name} (ID: {author_id_kaiten})")
                        skipped_comments += 1
                        continue
                    
                    # Получаем ID автора в Bitrix24
                    author_id_bitrix = int(self.user_mapping[author_id_kaiten])
                    
                    # Проверяем дублирование при обновлении
                    if is_update and comment_text in existing_comments:
                        logger.debug(f"Комментарий уже существует, пропускаем")
                        continue
                    
                    # Обрабатываем файлы, прикрепленные к комментарию
                    kaiten_comment_id = comment.get('id')
                    comment_files = files_by_comment.get(kaiten_comment_id, [])
                    
                    uploaded_file_ids = []
                    if comment_files:
                        logger.debug(f"   📎 К комментарию прикреплено {len(comment_files)} файлов")
                        
                        for file_info in comment_files:
                            file_name = file_info.get('name', 'unknown_file')
                            file_url = file_info.get('url')
                            
                            if not file_url:
                                logger.warning(f"   ⚠️ Файл '{file_name}' не имеет URL для скачивания")
                                continue
                            
                            # Скачиваем файл из Kaiten
                            logger.debug(f"   ⬇️ Скачиваем файл '{file_name}'...")
                            file_content = await self.kaiten_client.download_file(file_url)
                            
                            if file_content:
                                # Загружаем файл в Bitrix24 (в папку задачи)
                                logger.debug(f"   ⬆️ Загружаем файл '{file_name}' в Bitrix24 для задачи {task_id}...")
                                file_id = await self.bitrix_client.upload_file(file_content, file_name, target_group_id, task_id)
                                
                                if file_id:
                                    uploaded_file_ids.append(file_id)
                                    migrated_files += 1
                                    logger.debug(f"   ✅ Файл '{file_name}' успешно загружен с ID {file_id}")
                                else:
                                    logger.warning(f"   ⚠️ Не удалось загрузить файл '{file_name}' в Bitrix24")
                            else:
                                logger.warning(f"   ⚠️ Не удалось скачать файл '{file_name}' из Kaiten")
                    
                    # Переносим комментарий с файлами (если есть)
                    logger.debug(f"Комментарий от {author_name}: {comment_text[:50]}..." + 
                               (f" с {len(uploaded_file_ids)} файлами" if uploaded_file_ids else ""))
                    
                    # Создаем комментарий - либо с файлом (если есть), либо без файла
                    comment_id = None
                    
                    try:
                        if uploaded_file_ids:
                            # Комментарий с файлом
                            comment_id = await self.bitrix_client.add_task_comment_with_file(
                                task_id=task_id,
                                text=comment_text,
                                author_id=author_id_bitrix,
                                file_id=uploaded_file_ids[0]
                            )
                            
                            if comment_id:
                                logger.debug(f"Комментарий с файлом создан от имени {author_name} с ID {comment_id}")
                            else:
                                logger.error(f"Не удалось создать комментарий с файлом от {author_name}")
                        else:
                            # Комментарий без файла
                            comment_id = await self.bitrix_client.add_task_comment(
                                task_id=task_id,
                                text=comment_text,
                                author_id=author_id_bitrix
                            )
                            
                            if comment_id:
                                logger.debug(f"Комментарий без файла создан от имени {author_name} с ID {comment_id}")
                            else:
                                logger.error(f"Не удалось создать комментарий без файла от {author_name}")
                                
                    except Exception as e:
                        logger.error(f"   ❌ Ошибка создания комментария от {author_name}: {e}")
                        comment_id = None
                    
                    if comment_id:
                        migrated_comments += 1
                        
                        # Кэшируем для последующего обновления даты через SSH
                        if created_date:
                            # Конвертируем ISO дату в MySQL формат для SSH скрипта
                            try:
                                if 'T' in created_date:
                                    date_obj = datetime.fromisoformat(created_date.replace('Z', '+00:00'))
                                    mysql_date = date_obj.strftime('%Y-%m-%d %H:%M:%S')
                                    comment_dates_to_update[str(comment_id)] = mysql_date
                                    logger.debug(f"   📅 Запланировано обновление даты комментария {comment_id} на {mysql_date}")
                            except Exception as e:
                                                                    logger.warning(f"   ⚠️ Ошибка преобразования даты '{created_date}': {e}")
                    else:
                        logger.warning(f"Не удалось перенести комментарий от {author_name}")
                    
                except Exception as e:
                    logger.warning(f"Ошибка переноса комментария: {e}")
                    continue
            
            # Обновляем даты созданных комментариев через SSH
            if comment_dates_to_update:
                logger.debug(f"Обновляем даты для {len(comment_dates_to_update)} комментариев через SSH...")
                ssh_success = self.update_comment_dates_via_ssh(comment_dates_to_update)
                
                if not ssh_success:
                    logger.warning(f"Не удалось обновить даты комментариев через SSH")
            
            if migrated_comments > 0 or skipped_comments > 0 or migrated_files > 0:
                result_message = f"Комментарии: {migrated_comments} перенесено"
                if skipped_comments > 0:
                    result_message += f", {skipped_comments} пропущено"
                if migrated_files > 0:
                    result_message += f", файлов: {migrated_files}"
                logger.debug(result_message)
                self.stats['comments_migrated'] += migrated_comments
                self.stats['comments_skipped'] += skipped_comments
                
                # Добавляем статистику по файлам если она еще не добавлена
                if 'files_migrated' not in self.stats:
                    self.stats['files_migrated'] = 0
                self.stats['files_migrated'] += migrated_files
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка миграции комментариев для карточки '{card_title}': {e}")
            return False

    async def get_custom_properties_from_card(self, card: Union[KaitenCard, SimpleKaitenCard]) -> Dict[str, List[Any]]:
        """
        Извлекает пользовательские поля из карточки Kaiten.
        
        Args:
            card: Карточка Kaiten
            
        Returns:
            Словарь с пользовательскими полями {field_id: values}
        """
        properties = {}
        
        try:
            # Проверяем есть ли атрибут properties в карточке
            card_properties = getattr(card, 'properties', None)
            if card_properties:
                properties = card_properties
                logger.debug(f"Найдено {len(properties)} пользовательских полей в карточке {card.id}")
            else:
                # Если properties нет в модели, получаем raw данные через API
                logger.debug(f"Получаем raw данные карточки {card.id} для поиска пользовательских полей")
                raw_data = await self.kaiten_client._request("GET", f"/api/v1/cards/{card.id}")
                
                if raw_data and 'properties' in raw_data and raw_data['properties']:
                    properties = raw_data['properties']
                    logger.debug(f"Найдено {len(properties)} пользовательских полей в raw данных карточки {card.id}")
        except Exception as e:
            logger.debug(f"Ошибка получения пользовательских полей карточки {card.id}: {e}")
            
        return properties

    def parse_file_links_from_description(self, description: str) -> List[Tuple[str, str, str]]:
        """
        Парсит ссылки на файлы из описания карточки.
        
        Args:
            description: Текст описания карточки
            
        Returns:
            Список кортежей (filename, file_url, full_markdown_link)
        """
        file_links = []
        if not description:
            return file_links
        
        # Регулярное выражение для поиска Markdown ссылок на files.kaiten.ru
        # Формат: [filename](https://files.kaiten.ru/uuid.ext)
        pattern = r'\[([^\]]+)\]\((https://files\.kaiten\.ru/[^)]+)\)'
        
        matches = re.findall(pattern, description)
        for filename, file_url in matches:
            full_link = f'[{filename}]({file_url})'
            file_links.append((filename, file_url, full_link))
            logger.debug(f"Найдена ссылка на файл: {filename} -> {file_url}")
        
        return file_links

    async def migrate_description_files(self, card_id: int, target_group_id: int, 
                                      description: str, task_id: int | None = None) -> Tuple[str, int]:
        """
        Переносит файлы из описания карточки в Bitrix24 и обновляет ссылки.
        
        Args:
            card_id: ID карточки Kaiten
            target_group_id: ID группы в Bitrix24
            description: Исходное описание карточки
            task_id: ID задачи Bitrix24 (опционально, для создания подпапки)
            
        Returns:
            Кортеж (обновленное_описание, количество_перенесенных_файлов)
        """
        if not description:
            return description, 0
        
        logger.debug(f"🔍 Поиск файлов в описании карточки {card_id}...")
        
        # Парсим ссылки на файлы из описания
        file_links = self.parse_file_links_from_description(description)
        
        if not file_links:
            logger.debug(f"В описании карточки {card_id} не найдено ссылок на файлы")
            return description, 0
        
        logger.info(f"📎 Найдено {len(file_links)} файлов в описании для переноса")
        
        # Получаем все файлы карточки из API
        card_files = await self.kaiten_client.get_card_files(card_id)
        
        # Создаем маппинг URL -> файл из API для быстрого поиска
        files_by_url = {}
        for file_info in card_files:
            file_url = file_info.get('url', '')
            if file_url:
                files_by_url[file_url] = file_info
        
        updated_description = description
        migrated_files_count = 0
        
        # Обрабатываем каждую ссылку на файл
        for filename, file_url, full_link in file_links:
            try:
                # Проверяем, есть ли файл в API карточки
                if file_url not in files_by_url:
                    logger.warning(f"   ⚠️ Файл '{filename}' не найден в API карточки, пропускаем")
                    continue
                
                logger.debug(f"   ⬇️ Скачиваем файл '{filename}' из Kaiten...")
                
                # Скачиваем файл из Kaiten
                file_content = await self.kaiten_client.download_file(file_url)
                
                if not file_content:
                    logger.warning(f"   ⚠️ Не удалось скачать файл '{filename}', оставляем исходную ссылку")
                    continue
                
                # Проверяем/загружаем файл в Bitrix24
                if task_id:
                    logger.debug(f"   📤 Обрабатываем файл '{filename}' для задачи {task_id}...")
                    file_id = await self.bitrix_client.upload_file(file_content, filename, target_group_id, task_id)
                else:
                    logger.debug(f"   📤 Обрабатываем файл '{filename}' в общую папку...")
                    file_id = await self.bitrix_client.upload_file(file_content, filename, target_group_id)
                
                if file_id:
                    # Формируем новую ссылку на файл в Bitrix24
                    # Используем правильный URL для просмотра файла
                    file_url = self.bitrix_client.get_file_url(file_id)
                    new_link = f'[{filename}]({file_url})'
                    
                    # Заменяем старую ссылку на новую в описании
                    updated_description = updated_description.replace(full_link, new_link)
                    
                    migrated_files_count += 1
                    logger.debug(f"   ✅ Ссылка обновлена: {filename} -> {file_url}")
                    # Логика определения, был ли файл загружен заново или уже существовал,
                    # обрабатывается в upload_file method BitrixClient
                else:
                    logger.warning(f"   ⚠️ Не удалось обработать файл '{filename}', оставляем исходную ссылку")
                
            except Exception as e:
                logger.warning(f"   ❌ Ошибка переноса файла '{filename}': {e}")
                continue
        
        if migrated_files_count > 0:
            logger.success(f"✅ Перенесено {migrated_files_count} файлов из описания")
            self.stats['description_files_migrated'] += migrated_files_count
        
        return updated_description, migrated_files_count

    def print_migration_stats(self):
        """Выводит статистику миграции"""
        logger.info("\n" + "="*50)
        logger.info("📊 СТАТИСТИКА МИГРАЦИИ КАРТОЧЕК")
        logger.info("="*50)
        logger.info(f"Досок обработано: {self.stats['boards_processed']}")
        logger.info(f"Карточек всего: {self.stats['cards_total']}")
        logger.info(f"Карточек отфильтровано: {self.stats['cards_filtered_out']}")
        logger.info(f"Карточек создано: {self.stats['cards_migrated']}")
        logger.info(f"Карточек обновлено: {self.stats['cards_updated']}")
        logger.info(f"Карточек с ошибками: {self.stats['cards_failed']}")
        if self.stats['checklists_migrated'] > 0 or self.stats['checklist_items_migrated'] > 0:
            logger.info(f"Чек-листов перенесено: {self.stats['checklists_migrated']}")
            logger.info(f"Элементов чек-листов: {self.stats['checklist_items_migrated']}")
        if self.stats['comments_migrated'] > 0 or self.stats['comments_skipped'] > 0:
            logger.info(f"Комментариев перенесено: {self.stats['comments_migrated']}")
            logger.info(f"Комментариев пропущено (боты): {self.stats['comments_skipped']}")
        if self.stats.get('files_migrated', 0) > 0:
            logger.info(f"Файлов в комментариях перенесено: {self.stats['files_migrated']}")
        if self.stats['description_files_migrated'] > 0:
            logger.info(f"Файлов из описания обработано: {self.stats['description_files_migrated']}")
        logger.info("="*50) 

    async def apply_custom_fields_to_bitrix_task(self, bitrix_task_id: int, kaiten_properties: Dict[str, List[Any]]) -> bool:
        """
        Применяет пользовательские поля Kaiten к задаче Bitrix24.
        Использует маппинг полей для преобразования значений.
        
        Args:
            bitrix_task_id: ID задачи в Bitrix24
            kaiten_properties: Пользовательские поля из карточки Kaiten
            
        Returns:
            True в случае успеха
        """
        try:
            if not kaiten_properties:
                logger.debug(f"Нет пользовательских полей для задачи {bitrix_task_id}")
                return True
            
            # Загружаем маппинг полей
            mapping = self._load_custom_fields_mapping()
            if not mapping.get('fields'):
                logger.warning("Маппинг пользовательских полей не найден")
                return False
            
            # Формируем данные полей для Bitrix
            bitrix_fields_data = {}
            
            for kaiten_field_id, kaiten_values in kaiten_properties.items():
                # Убираем префикс id_ если есть
                clean_field_id = kaiten_field_id.replace('id_', '') if kaiten_field_id.startswith('id_') else kaiten_field_id
                
                # Ищем маппинг для этого поля
                field_mapping = mapping['fields'].get(clean_field_id)
                if not field_mapping:
                    logger.debug(f"Маппинг для поля {clean_field_id} не найден, пропускаем")
                    continue
                
                bitrix_field_name = field_mapping.get('bitrix_field_name')
                values_mapping = field_mapping.get('values_mapping', {})
                
                if not bitrix_field_name:
                    logger.warning(f"Имя поля Bitrix не найдено для {clean_field_id}")
                    continue
                
                # Преобразуем значения Kaiten в значения Bitrix
                if isinstance(kaiten_values, list):
                    # Множественные значения
                    bitrix_values = []
                    for kaiten_value in kaiten_values:
                        kaiten_value_str = str(kaiten_value)
                        if kaiten_value_str in values_mapping:
                            bitrix_values.append(values_mapping[kaiten_value_str])
                        else:
                            logger.debug(f"Маппинг для значения {kaiten_value_str} не найден")
                    
                    if bitrix_values:
                        # Для пользовательских полей типа enumeration в Bitrix24
                        # множественные значения передаются как массив
                        bitrix_fields_data[bitrix_field_name] = bitrix_values if len(bitrix_values) > 1 else bitrix_values[0]
                else:
                    # Одиночное значение
                    kaiten_value_str = str(kaiten_values)
                    if kaiten_value_str in values_mapping:
                        bitrix_fields_data[bitrix_field_name] = values_mapping[kaiten_value_str]
                    else:
                        logger.debug(f"Маппинг для значения {kaiten_value_str} не найден")
            
            # Устанавливаем поля в задаче Bitrix
            if bitrix_fields_data:
                success = await self.bitrix_client.set_task_custom_fields(bitrix_task_id, bitrix_fields_data)
                if success:
                    logger.debug(f"Применены пользовательские поля к задаче {bitrix_task_id}: {list(bitrix_fields_data.keys())}")
                    return True
                else:
                    logger.warning(f"Не удалось применить пользовательские поля к задаче {bitrix_task_id}")
                    return False
            else:
                logger.debug(f"Нет подходящих полей для применения к задаче {bitrix_task_id}")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка применения пользовательских полей к задаче {bitrix_task_id}: {e}")
            return False

    def _load_custom_fields_mapping(self) -> Dict[str, Any]:
        """
        Загружает маппинг пользовательских полей из файла.
        
        Returns:
            Словарь с маппингом полей
        """
        import json
        from pathlib import Path
        
        try:
            mapping_file = Path(__file__).parent.parent / "mappings" / "custom_fields_mapping.json"
            
            if mapping_file.exists():
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
                    logger.debug(f"Загружен маппинг пользовательских полей: {len(mapping.get('fields', {}))} полей")
                    return mapping
            else:
                logger.debug("Файл маппинга пользовательских полей не существует")
                return {}
                
        except Exception as e:
            logger.error(f"Ошибка загрузки маппинга пользовательских полей: {e}")
            return {}

    async def migrate_card(self, card: Union[KaitenCard, SimpleKaitenCard], target_group_id: int, include_archived: bool = False) -> Optional[int]:
        """
        Простой метод миграции карточки без пользовательских полей.
        Определяет стадию и создает задачу.
        
        Args:
            card: Карточка Kaiten для миграции
            target_group_id: ID группы назначения в Bitrix24
            include_archived: Если True, включает карточки из финальных колонок (type: 3)
            
        Returns:
            ID созданной задачи в Bitrix24 или None при ошибке
        """
        try:
            # Определяем целевую стадию
            target_stage = self.get_target_stage_for_card(card, include_archived)
            if not target_stage:
                logger.debug(f"Карточка {card.id} отфильтрована (финальная колонка)")
                return None
            
            # Создаем задачу используя существующую логику но с возвратом task_id
            return await self._create_task_from_card(card, target_group_id, target_stage)
            
        except Exception as e:
            logger.error(f"Ошибка миграции карточки {card.id}: {e}")
            return None

    async def _create_task_from_card(self, card: Union[KaitenCard, SimpleKaitenCard], 
                                   target_group_id: int, target_stage: str) -> Optional[int]:
        """
        Создает задачу из карточки и возвращает ее ID.
        Внутренний метод, извлеченный из migrate_single_card.
        """
        try:
            # Получаем исходное описание
            original_description = getattr(card, 'description', '') or ""
            
            # Получаем пользовательские поля (но НЕ добавляем их в описание)
            custom_properties = await self.get_custom_properties_from_card(card)
            
            # Используем исходное описание БЕЗ пользовательских полей
            enhanced_description = original_description
            
            # Трансформируем карточку в формат Bitrix24
            if not self.card_transformer:
                logger.error(f"❌ CardTransformer не инициализирован")
                return None
                
            task_data = self.card_transformer.transform(card, str(target_group_id))
            
            if not task_data:
                logger.error(f"❌ Карточка {card.id}: не удалось трансформировать")
                return None
            
            # Добавляем стадию
            stage_id = self.stage_mapping.get(target_stage)
            if stage_id:
                task_data['STAGE_ID'] = stage_id
                logger.debug(f"Задача будет создана в стадии '{target_stage}' (ID: {stage_id})")
            
            # Для архивных карточек устанавливаем статус "Завершена" (STATUS = 5)
            if target_stage == "Сделаны":
                task_data['STATUS'] = 5
                logger.debug(f"Архивная карточка: устанавливаем STATUS = 5 (Завершена)")
            
            # Создаем задачу в Bitrix24
            task_id = await self.bitrix_client.create_task(
                title=task_data['TITLE'],
                description=task_data.get('DESCRIPTION', ''),
                responsible_id=task_data['RESPONSIBLE_ID'],
                group_id=target_group_id,
                **{k: v for k, v in task_data.items() 
                   if k not in ['TITLE', 'DESCRIPTION', 'RESPONSIBLE_ID', 'GROUP_ID']}
            )
            
            if task_id:
                logger.info(f"✅ Карточка {card.id} -> Задача {task_id}")
                
                # Добавляем в маппинг и сохраняем
                self.card_mapping[str(card.id)] = str(task_id)
                await self.save_card_mapping()
                
                # ✅ Применяем пользовательские поля к созданной задаче
                if custom_properties:
                    success = await self.apply_custom_fields_to_bitrix_task(task_id, custom_properties)
                    if success:
                        logger.info(f"✅ Применены пользовательские поля к задаче {task_id}")
                    else:
                        logger.warning(f"❌ Не удалось применить пользовательские поля к задаче {task_id}")
                
                # Обрабатываем файлы в описании
                updated_description, migrated_files = await self.migrate_description_files(
                    card.id, target_group_id, enhanced_description, task_id
                )
                
                # Если описание изменилось, обновляем задачу
                if updated_description != enhanced_description:
                    await self.bitrix_client.update_task(
                        task_id=task_id,
                        DESCRIPTION=updated_description
                    )
                
                # Мигрируем чек-листы и комментарии
                await self.migrate_card_checklists(card.id, task_id, card.title)
                await self.migrate_card_comments(card.id, task_id, card.title, target_group_id)
                
                return task_id
            else:
                logger.error(f"❌ Карточка {card.id}: не удалось создать задачу")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка создания задачи из карточки {card.id}: {e}")
            return None