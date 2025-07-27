"""
Мигратор для переноса колонок Kaiten в стадии задач Bitrix24.

ВАЖНО: Данный мигратор в настоящее время НЕ ИСПОЛЬЗУЕТСЯ в приложении,
так как мы отказались от миграции колонок в текущей реализации.
Код сохранен для возможного использования в будущем.
"""

import asyncio
import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime

from connectors.kaiten_client import KaitenClient
from connectors.bitrix_client import BitrixClient
from models.kaiten_models import KaitenColumn
from utils.logger import get_logger

logger = get_logger(__name__)


class ColumnMigrator:
    """
    Мигратор для переноса колонок Kaiten в стадии задач Bitrix24.
    
    ⚠️ ВНИМАНИЕ: Данный класс НЕ ИСПОЛЬЗУЕТСЯ в текущей реализации приложения!
    Мы отказались от миграции колонок и используем фиксированные стадии 
    "Новые" и "Выполняются" в CardMigrator.
    
    Код сохранен для возможного использования в будущих версиях.
    
    Логика миграции:
    - 1 Column Kaiten = 1 Task Stage Bitrix24
    - Стадии привязываются к группе по ENTITY_ID 
    - НЕ создаем стадии "Моего плана"
    - НЕ импортируем subcolumns
    """
    
    def __init__(self):
        self.kaiten_client = KaitenClient()
        self.bitrix_client = BitrixClient()
        
        # Загружаем маппинг досок для получения ID групп
        self.board_mapping = self._load_board_mapping()
        
        # Статистика миграции
        self.stats = {
            'processed': 0,
            'created': 0,
            'updated': 0,
            'errors': 0,
            'boards_processed': 0,
            'stages_created': 0
        }
        
        # Результаты миграции: {board_id: {column_id: stage_id}}
        self.mapping = {}

    def _load_board_mapping(self) -> Dict[str, str]:
        """Загружает маппинг досок Kaiten -> Группы Bitrix24"""
        mapping_file = Path("mappings/board_mapping.json")
        if mapping_file.exists():
            with open(mapping_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('mapping', {})
        return {}

    def _get_stage_color(self, column_type: int) -> str:
        """
        Определяет цвет стадии на основе типа колонки Kaiten.
        
        :param column_type: Тип колонки из Kaiten
        :return: HEX цвет для стадии Bitrix24
        """
        # Цветовая схема для разных типов колонок
        color_mapping = {
            0: "6699CC",  # Обычная колонка - синий
            1: "9999CC",  # В работе - фиолетовый  
            2: "99CC66",  # Готово - зеленый
            3: "CC9999",  # Заблокировано - красный
            4: "CCCC99",  # Ожидание - желтый
        }
        return color_mapping.get(column_type, "6699CC")  # По умолчанию синий

    async def migrate_board_columns(self, kaiten_board_id: int, 
                                   bitrix_group_id: int) -> Tuple[int, int, int]:
        """
        Мигрирует колонки одной доски в стадии задач группы.
        
        :param kaiten_board_id: ID доски в Kaiten
        :param bitrix_group_id: ID группы в Bitrix24
        :return: Кортеж (созданных, обновленных, ошибок)
        """
        created = 0
        updated = 0
        errors = 0
        
        try:
            # Получаем колонки доски из Kaiten
            logger.info(f"🔄 Получение колонок доски {kaiten_board_id}...")
            columns = await self.kaiten_client.get_board_columns(kaiten_board_id)
            
            if not columns:
                logger.warning(f"❌ Колонки для доски {kaiten_board_id} не найдены")
                return 0, 0, 1
            
            logger.info(f"📋 Найдено {len(columns)} колонок для доски {kaiten_board_id}")
            
            # Получаем существующие стадии группы
            existing_stages = await self.bitrix_client.get_task_stages(bitrix_group_id)
            existing_titles = set()
            
            # Обрабатываем существующие стадии (могут быть в разных форматах)
            if existing_stages:
                for stage in existing_stages:
                    if isinstance(stage, dict):
                        title = stage.get('TITLE', '') or stage.get('title', '')
                        if title:
                            existing_titles.add(title.lower())
                    elif isinstance(stage, str):
                        # Если стадия возвращается как строка
                        existing_titles.add(stage.lower())
            
            # Исключаем стадии "Моего плана" 
            my_plan_keywords = ['мой план', 'my plan', 'личный', 'personal']
            
            # Сортируем колонки по порядку
            columns.sort(key=lambda col: col.sort_order)
            
            # Создаем стадии для каждой колонки
            board_mapping = {}
            
            for i, column in enumerate(columns):
                try:
                    # Проверяем что это не стадия "Моего плана"
                    column_title_lower = column.title.lower()
                    if any(keyword in column_title_lower for keyword in my_plan_keywords):
                        logger.info(f"⏭️ Пропускаем стадию 'Моего плана': {column.title}")
                        continue
                    
                    # Проверяем существует ли уже такая стадия
                    if column.title.lower() in existing_titles:
                        logger.info(f"🔄 Стадия '{column.title}' уже существует, пропускаем")
                        updated += 1
                        continue
                    
                    # Создаем новую стадию
                    stage_color = self._get_stage_color(column.type)
                    sort_order = (i + 1) * 100  # 100, 200, 300...
                    
                    stage_data = await self.bitrix_client.create_task_stage(
                        entity_id=bitrix_group_id,
                        title=column.title,
                        sort=sort_order,
                        color=stage_color
                    )
                    
                    if stage_data:
                        # Результат может быть как ID (int), так и объектом (dict)
                        if isinstance(stage_data, int):
                            stage_id = stage_data
                        elif isinstance(stage_data, dict):
                            stage_id = stage_data.get('ID') or stage_data.get('id')
                        else:
                            stage_id = str(stage_data)
                        
                        board_mapping[str(column.id)] = str(stage_id)
                        created += 1
                        logger.success(f"✅ Создана стадия '{column.title}' (ID: {stage_id})")
                    else:
                        logger.error(f"❌ Ошибка создания стадии '{column.title}'")
                        errors += 1
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки колонки {column.title}: {e}")
                    errors += 1
            
            # Сохраняем маппинг для этой доски
            if board_mapping:
                self.mapping[str(kaiten_board_id)] = board_mapping
                
        except Exception as e:
            logger.error(f"❌ Ошибка миграции колонок доски {kaiten_board_id}: {e}")
            errors += 1
        
        return created, updated, errors

    async def migrate_single_board(self, kaiten_board_id: int) -> Dict:
        """
        Мигрирует колонки одной доски (для тестирования).
        
        :param kaiten_board_id: ID доски в Kaiten
        :return: Результат миграции
        """
        # Находим ID группы в Bitrix24
        bitrix_group_id = self.board_mapping.get(str(kaiten_board_id))
        
        if not bitrix_group_id:
            logger.error(f"❌ Группа для доски {kaiten_board_id} не найдена в маппинге")
            return {
                'success': False,
                'error': f"Группа для доски {kaiten_board_id} не найдена в маппинге"
            }
        
        try:
            bitrix_group_id = int(bitrix_group_id)
            logger.info(f"🎯 Миграция колонок доски {kaiten_board_id} → группа {bitrix_group_id}")
            
            created, updated, errors = await self.migrate_board_columns(
                kaiten_board_id, bitrix_group_id
            )
            
            # Обновляем общую статистику
            self.stats['boards_processed'] += 1
            self.stats['created'] += created
            self.stats['updated'] += updated
            self.stats['errors'] += errors
            self.stats['stages_created'] += created
            
            return {
                'success': True,
                'kaiten_board_id': kaiten_board_id,
                'bitrix_group_id': bitrix_group_id,
                'created': created,
                'updated': updated,
                'errors': errors
            }
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка миграции доски {kaiten_board_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    async def migrate_all_boards(self, limit: Optional[int] = None) -> Dict:
        """
        Мигрирует колонки всех досок из маппинга.
        
        :param limit: Ограничение количества досок (для тестирования)
        :return: Общий результат миграции
        """
        total_boards = len(self.board_mapping)
        if limit:
            total_boards = min(limit, total_boards)
        
        logger.info(f"🚀 Начинаем миграцию колонок для {total_boards} досок...")
        
        processed_count = 0
        
        for kaiten_board_id in list(self.board_mapping.keys())[:limit] if limit else self.board_mapping.keys():
            if processed_count >= total_boards:
                break
                
            result = await self.migrate_single_board(int(kaiten_board_id))
            processed_count += 1
            
            logger.info(f"📊 Обработано {processed_count}/{total_boards} досок")
            
            # Небольшая пауза между запросами
            await asyncio.sleep(0.1)
        
        self.stats['processed'] = processed_count
        
        # Сохраняем результаты
        await self._save_mapping()
        
        return {
            'success': True,
            'stats': self.stats,
            'mapping_file': 'mappings/column_mapping.json'
        }

    async def _save_mapping(self):
        """Сохраняет результаты миграции в файл"""
        mapping_data = {
            'created_at': datetime.now().isoformat(),
            'description': 'Маппинг ID колонок Kaiten -> стадий задач Bitrix24',
            'stats': self.stats,
            'mapping': self.mapping
        }
        
        # Создаем папку если её нет
        Path("mappings").mkdir(exist_ok=True)
        
        mapping_file = Path("mappings/column_mapping.json")
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        
        logger.success(f"💾 Маппинг колонок сохранен: {mapping_file}") 