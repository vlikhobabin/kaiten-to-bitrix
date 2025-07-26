from typing import Any, Dict, Optional, Union

from models.kaiten_models import KaitenCard
from models.simple_kaiten_models import SimpleKaitenCard
from transformers.base_transformer import BaseTransformer
from transformers.user_transformer import UserTransformer
from utils.logger import get_logger

logger = get_logger(__name__)

class CardTransformer(BaseTransformer):
    """
    Трансформер для преобразования данных карточки Kaiten 
    в формат для создания задачи в Bitrix24.
    """
    def __init__(self, user_transformer: UserTransformer):
        self.user_transformer = user_transformer

    def transform(self, card: Union[KaitenCard, SimpleKaitenCard], bitrix_group_id: str) -> Optional[Dict[str, Any]]:
        """
        Преобразует объект KaitenCard или SimpleKaitenCard в словарь для API Bitrix24.
        
        :param card: Объект карточки Kaiten.
        :param bitrix_group_id: ID группы (проекта) в Bitrix24, к которой будет привязана задача.
        :return: Словарь с данными для метода tasks.task.add или None, если ответственный не найден.
        """
        logger.debug(f"Трансформация карточки '{card.title}' (ID: {card.id}) для Bitrix24...")

        # Определяем постановщика (заказчика) - это владелец карточки
        if not card.owner:
            logger.error(f"У карточки '{card.title}' (ID: {card.id}) нет владельца. Пропуск карточки.")
            return None
            
        created_by_id = self.user_transformer.get_user_id(card.owner)
        if not created_by_id:
            owner_name = getattr(card.owner, 'full_name', 'Unknown') if card.owner else 'Unknown'
            logger.error(f"Не удалось найти постановщика для карточки '{card.title}' (Kaiten owner: {owner_name}). Пропуск карточки.")
            return None
        
        # Определяем ответственного и соисполнителей по полю type в members
        responsible_id = None
        accomplices = []
        
        if card.members and len(card.members) > 0:
            logger.debug(f"Обрабатываем {len(card.members)} участников карточки '{card.title}':")
            
            for member in card.members:
                member_type = getattr(member, 'type', None)
                member_name = getattr(member, 'full_name', 'Unknown')
                member_id = self.user_transformer.get_user_id(member)
                
                if not member_id:
                    logger.warning(f"Не удалось найти пользователя '{member_name}' в маппинге для карточки '{card.title}'")
                    continue
                
                logger.debug(f"  - {member_name} (type: {member_type})")
                
                if member_type == 2:
                    # Ответственный (type: 2)
                    if responsible_id:
                        logger.warning(f"Найдено несколько ответственных для карточки '{card.title}'. Используем первого: {member_name}")
                    else:
                        responsible_id = member_id
                        logger.debug(f"Ответственный: {member_name}")
                elif member_type == 1:
                    # Соисполнитель (type: 1)
                    accomplices.append(member_id)
                    logger.debug(f"Соисполнитель: {member_name}")
                else:
                    # Неизвестный type или None - добавляем как соисполнителя
                    accomplices.append(member_id)
                    logger.debug(f"Участник с неизвестным type ({member_type}), добавлен как соисполнитель: {member_name}")
            
            # Если не нашли ответственного среди участников, назначаем владельца
            if not responsible_id:
                responsible_id = created_by_id
                logger.warning(f"Не найден ответственный (type: 2) среди участников карточки '{card.title}', ответственным назначен владелец")
        else:
            # Если участников нет, ответственным назначаем владельца карточки
            responsible_id = created_by_id
            logger.debug(f"У карточки '{card.title}' нет участников, ответственным назначен владелец")
            
        # Получаем описание карточки
        description = getattr(card, 'description', '') or " "
        
        # Формируем теги: добавляем название доски и колонки к существующим тегам
        tags = []
        
        # Добавляем существующие теги из карточки
        if card.tags:
            tags.extend([tag.name for tag in card.tags])
            logger.debug(f"Найдено {len(card.tags)} существующих тегов: {[tag.name for tag in card.tags]}")
        
        # Добавляем название доски как тег
        board_name = self._get_board_title(card)
        if board_name:
            tags.append(board_name)
            logger.debug(f"Добавлен тег с названием доски: '{board_name}'")
        else:
            logger.warning(f"Не удалось получить название доски для карточки '{card.title}'")
        
        # Добавляем название колонки как тег
        column_name = self._get_column_title(card)
        if column_name:
            tags.append(column_name)
            logger.debug(f"Добавлен тег с названием колонки: '{column_name}'")
        else:
            logger.warning(f"Не удалось получить название колонки для карточки '{card.title}'")
        
        transformed_data = {
            "TITLE": card.title,
            "DESCRIPTION": description,
            "CREATED_BY": created_by_id,  # Постановщик - владелец карточки
            "RESPONSIBLE_ID": responsible_id,  # Исполнитель - участник с type: 2
            "GROUP_ID": bitrix_group_id,
            # Преобразование тегов: существующие теги + название доски + название колонки
            "TAGS": tags,
            # Установка крайнего срока, если он есть
            "DEADLINE": card.due_date.isoformat() if card.due_date else None,
        }
        
        # Добавляем соисполнителей, если они есть
        if accomplices:
            transformed_data["ACCOMPLICES"] = accomplices
            
        # Удаляем поля с None, так как API Bitrix24 их не любит
        transformed_data = {k: v for k, v in transformed_data.items() if v is not None}
        
        logger.debug(f"Итого тегов для задачи: {len(tags)} - {tags}" if tags else "Тегов для задачи нет")
        
        logger.debug(f"Карточка '{card.title}' успешно трансформирована. Постановщик: {created_by_id}, Исполнитель: {responsible_id}" +
                    (f", Соисполнители: {accomplices}" if accomplices else ""))
        return transformed_data

    def _get_board_title(self, card: Union[KaitenCard, SimpleKaitenCard]) -> Optional[str]:
        """
        Получает название доски для карточки из объекта board в самой карточке.
        
        :param card: Карточка Kaiten
        :return: Название доски или None
        """
        try:
            # Проверяем различные способы получения названия доски из объекта карточки
            if hasattr(card, 'board') and card.board:
                if hasattr(card.board, 'title') and card.board.title:
                    logger.debug(f"Получено название доски из card.board.title: '{card.board.title}'")
                    return card.board.title
            
            # Для случаев когда board может быть словарем
            if hasattr(card, 'board') and isinstance(card.board, dict):
                title = card.board.get('title') or card.board.get('name')
                if title:
                    logger.debug(f"Получено название доски из словаря card.board: '{title}'")
                    return title
            
            logger.debug(f"Название доски не найдено в объекте карточки {card.id}")
            return None
            
        except Exception as e:
            logger.warning(f"Ошибка получения названия доски для карточки {card.id}: {e}")
            return None

    def _get_column_title(self, card: Union[KaitenCard, SimpleKaitenCard]) -> Optional[str]:
        """
        Получает название колонки для карточки из объекта column в самой карточке.
        
        :param card: Карточка Kaiten
        :return: Название колонки или None
        """
        try:
            # Проверяем различные способы получения названия колонки из объекта карточки
            if hasattr(card, 'column') and card.column:
                if hasattr(card.column, 'title') and card.column.title:
                    logger.debug(f"Получено название колонки из card.column.title: '{card.column.title}'")
                    return card.column.title
            
            # Для случаев когда column может быть словарем
            if hasattr(card, 'column') and isinstance(card.column, dict):
                title = card.column.get('title') or card.column.get('name')
                if title:
                    logger.debug(f"Получено название колонки из словаря card.column: '{title}'")
                    return title
            
            logger.debug(f"Название колонки не найдено в объекте карточки {card.id}")
            return None
            
        except Exception as e:
            logger.warning(f"Ошибка получения названия колонки для карточки {card.id}: {e}")
            return None
