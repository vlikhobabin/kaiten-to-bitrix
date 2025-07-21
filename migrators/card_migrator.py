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
from models.kaiten_models import KaitenCard, KaitenBoard, KaitenColumn
from models.simple_kaiten_models import SimpleKaitenCard
from transformers.card_transformer import CardTransformer
from transformers.user_transformer import UserTransformer
from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)

class UserMappingTransformer:
    """
    Упрощенный трансформер пользователей для работы с заранее созданным маппингом.
    """
    
    def __init__(self, user_mapping: Dict[str, str]):
        self.user_mapping = user_mapping  # kaiten_user_id -> bitrix_user_id
    
    def get_user_id(self, kaiten_user) -> Optional[str]:
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
            logger.debug(f"Найден маппинг: Kaiten user {kaiten_user.full_name} (ID: {kaiten_user_id}) -> Bitrix ID: {bitrix_user_id}")
            return bitrix_user_id
        else:
            logger.warning(f"Не найден маппинг для пользователя {kaiten_user.full_name} (ID: {kaiten_user_id})")
            return None

class CardMigrator:
    """
    Мигратор карточек из Kaiten в задачи Bitrix24.
    
    Правила миграции:
    1. Карточки из колонок type: 1 -> стадия "Новые"
    2. Карточки из колонок type: 3 -> НЕ ПЕРЕНОСЯТСЯ
    3. Карточки из остальных колонок -> стадия "Выполняются"
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
        
        Args:
            group_id: ID группы в Bitrix24
            stage_names: Список названий стадий для поиска
            
        Returns:
            Словарь {название_стадии: stage_id}
        """
        try:
            logger.info(f"🔍 Получение стадий задач для группы {group_id}...")
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
                                logger.info(f"✅ Найдена стадия '{title}' с ID {stage_id}")
                # Fallback для случая, если API вернет список
                elif isinstance(stages_data, list):
                    for stage in stages_data:
                        if isinstance(stage, dict):
                            title = stage.get('TITLE', '') or stage.get('title', '')
                            stage_id = stage.get('ID') or stage.get('id')
                            
                            if title in stage_names and stage_id:
                                stage_mapping[title] = str(stage_id)
                                logger.info(f"✅ Найдена стадия '{title}' с ID {stage_id}")
            
            logger.info(f"📊 Найдено {len(stage_mapping)} из {len(stage_names)} требуемых стадий")
            return stage_mapping
            
        except Exception as e:
            logger.error(f"Ошибка получения стадий: {e}")
            return {}

    def get_target_stage_for_card(self, card: Union[KaitenCard, SimpleKaitenCard]) -> Optional[str]:
        """
        Определяет целевую стадию для карточки на основе правил миграции.
        
        Args:
            card: Карточка Kaiten
            
        Returns:
            Название целевой стадии или None если карточку переносить не нужно
        """
        if hasattr(card, 'column') and card.column:
            column_type = card.column.type
        else:
            column_type = None
        
        if column_type == 1:  # Начальная колонка
            return "Новые"
        elif column_type == 3:  # Финальная колонка - не переносим
            return None
        else:  # Все остальные колонки (включая None)
            return "Выполняются"

    def should_migrate_card(self, card: Union[KaitenCard, SimpleKaitenCard]) -> bool:
        """
        Проверяет, нужно ли переносить карточку.
        
        Args:
            card: Карточка Kaiten
            
        Returns:
            True если карточку нужно переносить, False иначе
        """
        # Фильтр по типу колонки
        if hasattr(card, 'column') and card.column and card.column.type == 3:
            logger.debug(f"🚫 Карточка '{card.title}' пропущена (финальная колонка type: 3)")
            return False
            
        # Фильтр архивных карточек
        if card.archived:
            logger.debug(f"🚫 Карточка '{card.title}' пропущена (архивная)")
            return False
            
        return True

    async def migrate_cards_from_space(self, space_id: int, target_group_id: int, 
                                     list_only: bool = False, limit: int = None, card_id: int = None) -> bool:
        """
        Мигрирует карточки из всех досок указанного пространства.
        
        Args:
            space_id: ID пространства Kaiten
            target_group_id: ID группы в Bitrix24
            list_only: Если True, только выводит список карточек без миграции
            limit: Если указан, обрабатывает только первые N карточек первой доски
            card_id: Если указан, обрабатывает только конкретную карточку
            
        Returns:
            True в случае успеха
        """
        try:
            # Обработка конкретной карточки
            if card_id:
                return await self.migrate_single_card_by_id(card_id, target_group_id, list_only)
            
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
                self.stage_mapping = await self.get_task_stages_by_names(target_group_id, required_stages)
                
                if len(self.stage_mapping) != len(required_stages):
                    missing_stages = set(required_stages) - set(self.stage_mapping.keys())
                    logger.error(f"❌ Не найдены обязательные стадии: {missing_stages}")
                    return False
            
            # Обрабатываем каждую доску
            processed_cards = 0
            for board in boards:
                remaining_limit = limit - processed_cards if limit else None
                cards_processed_from_board = await self.process_board(
                    board, target_group_id, list_only, remaining_limit
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

    async def migrate_single_card_by_id(self, card_id: int, target_group_id: int, list_only: bool = False) -> bool:
        """
        Мигрирует конкретную карточку по ее ID.
        
        Args:
            card_id: ID карточки Kaiten
            target_group_id: ID группы в Bitrix24
            list_only: Если True, только выводит информацию о карточке
            
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
                self.stage_mapping = await self.get_task_stages_by_names(target_group_id, required_stages)
                
                if len(self.stage_mapping) != len(required_stages):
                    missing_stages = set(required_stages) - set(self.stage_mapping.keys())
                    logger.error(f"❌ Не найдены обязательные стадии: {missing_stages}")
                    return False
            
            # Обрабатываем карточку
            self.stats['cards_total'] = 1
            processed = await self.process_card(card, target_group_id, list_only)
            
            # Выводим статистику
            self.print_migration_stats()
            
            return processed
            
        except Exception as e:
            logger.error(f"Ошибка обработки карточки {card_id}: {e}")
            return False

    async def process_board(self, board: KaitenBoard, target_group_id: int, list_only: bool = False, limit: int = None):
        """
        Обрабатывает карточки одной доски.
        
        Args:
            board: Доска Kaiten
            target_group_id: ID группы в Bitrix24
            list_only: Если True, только выводит список карточек
            limit: Максимальное количество карточек для обработки
            
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
                processed = await self.process_card(card, target_group_id, list_only)
                if processed:  # Учитываем только карточки, которые действительно обработались
                    processed_count += 1
                
                # Проверяем лимит для обработанных карточек
                if limit and processed_count >= limit:
                    break
            
            return processed_count
                
        except Exception as e:
            logger.error(f"Ошибка обработки доски {board.title}: {e}")
            return 0

    async def process_card(self, card: Union[KaitenCard, SimpleKaitenCard], target_group_id: int, list_only: bool = False):
        """
        Обрабатывает одну карточку.
        
        Args:
            card: Карточка Kaiten
            target_group_id: ID группы в Bitrix24
            list_only: Если True, только выводит информацию о карточке
            
        Returns:
            True если карточка была обработана (не отфильтрована), False иначе
        """
        try:
            # Проверяем, была ли карточка уже мигрирована
            card_id_str = str(card.id)
            if card_id_str in self.card_mapping:
                existing_task_id = self.card_mapping[card_id_str]
                if list_only:
                    logger.info(f"   ⏭️  Карточка: ID {card.id}, '{card.title}' -> УЖЕ МИГРИРОВАНА (задача ID {existing_task_id})")
                    return True  # Считаем как обработанную
                else:
                    # Обновляем существующую карточку
                    logger.info(f"   🔄 Обновляем карточку '{card.title}' (ID: {card.id}) -> задача ID {existing_task_id}")
                    
                    # Определяем целевую стадию для обновления
                    target_stage = self.get_target_stage_for_card(card)
                    if not target_stage:
                        self.stats['cards_filtered_out'] += 1
                        return False
                    
                    # Обновляем существующую задачу
                    await self.update_existing_card(card, int(existing_task_id), target_group_id, target_stage)
                    return True
            
            # Проверяем, нужно ли переносить карточку
            if not self.should_migrate_card(card):
                self.stats['cards_filtered_out'] += 1
                return False
            
            # Определяем целевую стадию
            target_stage = self.get_target_stage_for_card(card)
            if not target_stage:
                self.stats['cards_filtered_out'] += 1
                return False
            
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
            
            # Извлекаем пользовательские поля и добавляем к описанию
            custom_properties = await self.get_custom_properties_from_card(card)
            custom_properties_text = await self.format_custom_properties_for_description(custom_properties)
            
            # Формируем итоговое описание с пользовательскими полями
            if custom_properties_text:
                enhanced_description = custom_properties_text + original_description
                logger.debug(f"Добавлены пользовательские поля в описание карточки {card.id}")
            else:
                enhanced_description = original_description
            
            # Временно устанавливаем расширенное описание для трансформации
            if hasattr(card, 'description'):
                card.description = enhanced_description
            
            # Трансформируем карточку в формат Bitrix24 с расширенным описанием
            task_data = self.card_transformer.transform(card, str(target_group_id))
            
            if not task_data:
                logger.warning(f"⚠️ Не удалось трансформировать карточку '{card.title}'")
                self.stats['cards_failed'] += 1
                return
            
            # Добавляем стадию
            stage_id = self.stage_mapping.get(target_stage)
            if stage_id:
                task_data['STAGE_ID'] = stage_id
            
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
                logger.success(f"✅ Карточка '{card.title}' -> Задача ID {task_id} (стадия '{target_stage}')")
                
                # Добавляем в маппинг и сохраняем
                self.card_mapping[str(card.id)] = str(task_id)
                await self.save_card_mapping()
                
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
                        logger.info(f"   📎 Перенесено {migrated_files} файлов из описания в папку задачи {task_id}")
                
                # Мигрируем чек-листы
                await self.migrate_card_checklists(card.id, task_id, card.title)
                
                # Мигрируем комментарии
                await self.migrate_card_comments(card.id, task_id, card.title, target_group_id)
                
                self.stats['cards_migrated'] += 1
            else:
                logger.error(f"❌ Не удалось создать задачу для карточки '{card.title}'")
                self.stats['cards_failed'] += 1
                
        except Exception as e:
            logger.error(f"Ошибка миграции карточки '{card.title}': {e}")
            self.stats['cards_failed'] += 1
        finally:
            # Восстанавливаем исходное описание карточки
            if hasattr(card, 'description'):
                card.description = original_description

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
            
            # Извлекаем пользовательские поля и добавляем к описанию
            custom_properties = await self.get_custom_properties_from_card(card)
            custom_properties_text = await self.format_custom_properties_for_description(custom_properties)
            
            # Формируем итоговое описание с пользовательскими полями
            if custom_properties_text:
                enhanced_description = custom_properties_text + original_description
                logger.debug(f"Добавлены пользовательские поля в описание карточки {card.id} при обновлении")
            else:
                enhanced_description = original_description
            
            # Обрабатываем файлы в расширенном описании
            updated_description, migrated_files = await self.migrate_description_files(
                card.id, target_group_id, enhanced_description, task_id
            )
            
            # Временно устанавливаем обновленное описание для трансформации
            if hasattr(card, 'description'):
                card.description = updated_description
            
            # Трансформируем карточку в формат Bitrix24
            task_data = self.card_transformer.transform(card, str(target_group_id))
            
            if not task_data:
                logger.warning(f"⚠️ Не удалось трансформировать карточку '{card.title}' для обновления")
                self.stats['cards_failed'] += 1
                return
            
            # Добавляем стадию
            stage_id = self.stage_mapping.get(target_stage)
            if stage_id:
                task_data['STAGE_ID'] = stage_id
            
            # Обновляем задачу в Bitrix24
            success = await self.bitrix_client.update_task(
                task_id=task_id,
                **task_data
            )
            
            if success:
                logger.success(f"✅ Обновлена задача ID {task_id} из карточки '{card.title}' (стадия '{target_stage}')")
                if migrated_files > 0:
                    logger.info(f"   📎 Перенесено {migrated_files} файлов из описания в папку задачи {task_id}")
                
                # Мигрируем чек-листы (при обновлении тоже синхронизируем)
                await self.migrate_card_checklists(card.id, task_id, card.title, is_update=True)
                
                # Мигрируем комментарии (при обновлении тоже синхронизируем)
                await self.migrate_card_comments(card.id, task_id, card.title, target_group_id, is_update=True)
                
                self.stats['cards_updated'] += 1
            else:
                logger.error(f"❌ Не удалось обновить задачу ID {task_id} для карточки '{card.title}'")
                self.stats['cards_failed'] += 1
                
        except Exception as e:
            logger.error(f"Ошибка обновления задачи ID {task_id} для карточки '{card.title}': {e}")
            self.stats['cards_failed'] += 1
        finally:
            # Восстанавливаем исходное описание карточки
            if hasattr(card, 'description'):
                card.description = original_description

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
            
            logger.info(f"📋 Переносим {len(checklists)} чек-листов для карточки '{card_title}'")
            
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
                            await self.bitrix_client.add_checklist_item(
                                task_id=task_id,
                                title=item_text,  # Убираем отступ и эмодзи - теперь это обычные элементы
                                is_complete=is_complete,
                                parent_id=group_id  # Указываем ID группы как родительский элемент (или None)
                            )
                            migrated_items += 1
                    
                except Exception as e:
                    checklist_name = checklist.get('name', checklist.get('title', 'unknown'))
                    logger.warning(f"Ошибка переноса чек-листа '{checklist_name}': {e}")
                    continue
            
            if migrated_checklists > 0:
                logger.success(f"✅ Перенесено {migrated_checklists} чек-листов, {migrated_items} элементов")
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
            
            logger.info(f"💬 Переносим комментарии для карточки '{card_title}'" + 
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
                        logger.debug(f"   ⏭️ Комментарий уже существует, пропускаем")
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
                    logger.debug(f"   💬 Комментарий от {author_name}: {comment_text[:50]}..." + 
                               (f" с {len(uploaded_file_ids)} файлами" if uploaded_file_ids else ""))
                    
                    # Выбираем метод создания комментария в зависимости от наличия файлов
                    if uploaded_file_ids:
                        # Создаем комментарий с файлами с помощью нового метода
                        comment_id = await self.bitrix_client.add_task_comment_with_file(
                            task_id=task_id,
                            text=comment_text,
                            author_id=author_id_bitrix,
                            file_id=uploaded_file_ids[0] if len(uploaded_file_ids) == 1 else None
                            # Если файлов несколько, пока используем только первый
                            # TODO: реализовать поддержку множественных файлов
                        )
                    else:
                        # Создаем обычный комментарий без файлов
                        comment_id = await self.bitrix_client.add_task_comment(
                            task_id=task_id,
                            text=comment_text,
                            author_id=author_id_bitrix
                            # Намеренно НЕ передаем created_date, так как API его игнорирует
                        )
                    
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
                        logger.warning(f"⚠️ Не удалось перенести комментарий от {author_name}")
                    
                except Exception as e:
                    logger.warning(f"Ошибка переноса комментария: {e}")
                    continue
            
            # Обновляем даты созданных комментариев через SSH
            if comment_dates_to_update:
                logger.info(f"🕒 Обновляем даты для {len(comment_dates_to_update)} комментариев через SSH...")
                ssh_success = self.update_comment_dates_via_ssh(comment_dates_to_update)
                
                if not ssh_success:
                    logger.warning(f"⚠️ Не удалось обновить даты комментариев через SSH, но комментарии созданы")
            
            if migrated_comments > 0 or skipped_comments > 0 or migrated_files > 0:
                result_message = f"✅ Перенесено {migrated_comments} комментариев, пропущено {skipped_comments} (боты)"
                if migrated_files > 0:
                    result_message += f", файлов: {migrated_files}"
                logger.success(result_message)
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
            if hasattr(card, 'properties') and card.properties:
                properties = card.properties
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

    async def get_field_name_from_api(self, field_id: str) -> str:
        """
        Получает название поля через API Kaiten.
        
        Args:
            field_id: ID поля (например, "365518")
            
        Returns:
            Название поля или исходный ID если не найдено
        """
        try:
            # Убираем префикс id_ если есть
            clean_field_id = field_id.replace('id_', '') if field_id.startswith('id_') else field_id
            
            # Получаем информацию о поле через API
            property_info = await self.kaiten_client.get_custom_property(int(clean_field_id))
            if property_info and 'name' in property_info:
                logger.debug(f"Получено название поля {clean_field_id} через API: {property_info['name']}")
                return property_info['name']
            
            # Если поле не найдено, возвращаем исходный ID
            logger.debug(f"Поле {clean_field_id} не найдено в API")
            return f"Поле {clean_field_id}"
                
        except Exception as e:
            logger.debug(f"Ошибка получения названия поля {field_id}: {e}")
            return f"Поле {field_id}"

    async def get_field_values_from_api(self, field_id: str, value_ids: List[Any]) -> str:
        """
        Получает текстовые значения поля через API Kaiten.
        
        Args:
            field_id: ID поля
            value_ids: Список ID значений
            
        Returns:
            Отформатированная строка значений
        """
        try:
            # Убираем префикс id_ если есть  
            clean_field_id = field_id.replace('id_', '') if field_id.startswith('id_') else field_id
            
            # Получаем возможные значения поля через API
            select_values = await self.kaiten_client.get_custom_property_select_values(int(clean_field_id))
            
            if select_values:
                # Создаем маппинг ID -> текстовое значение
                value_mapping = {}
                for value_info in select_values:
                    if 'id' in value_info and 'value' in value_info:
                        value_mapping[str(value_info['id'])] = value_info['value']
                
                # Преобразуем ID значений в текст
                text_values = []
                for value_id in value_ids:
                    value_text = value_mapping.get(str(value_id), str(value_id))
                    text_values.append(value_text)
                
                logger.debug(f"Получены значения поля {clean_field_id} через API")
                return "; ".join(text_values)
            else:
                # Если значения не найдены в API, возвращаем исходные ID
                logger.debug(f"Значения для поля {clean_field_id} не найдены в API")
                return "; ".join(str(v) for v in value_ids)
            
        except Exception as e:
            logger.debug(f"Ошибка получения значений поля {field_id}: {e}")
            # Возвращаем исходные ID если API недоступен
            return "; ".join(str(v) for v in value_ids)

    async def format_custom_properties_for_description(self, properties: Dict[str, List[Any]]) -> str:
        """
        Форматирует пользовательские поля для добавления в описание.
        
        Args:
            properties: Словарь с пользовательскими полями
            
        Returns:
            Отформатированный текст для добавления в описание
        """
        if not properties:
            return ""
        
        lines = []
        
        for field_key, values in properties.items():
            # Получаем человеко-читаемое название поля через API
            field_name = await self.get_field_name_from_api(field_key)
            
            # Форматируем значения через API
            if isinstance(values, list):
                values_str = await self.get_field_values_from_api(field_key, values)
            else:
                values_str = await self.get_field_values_from_api(field_key, [values])
            
            # Используем HTML-теги для жирного выделения названия поля
            lines.append(f"<b>{field_name}:</b> {values_str}")
        
        lines.append("")  # Пустая строка для разделения от основного описания
        return "\n".join(lines)

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
                                      description: str, task_id: int = None) -> Tuple[str, int]:
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