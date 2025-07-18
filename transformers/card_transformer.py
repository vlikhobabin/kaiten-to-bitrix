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
        logger.info(f"Трансформация карточки '{card.title}' (ID: {card.id}) для Bitrix24...")

        # Получаем ID ответственного через новый метод
        responsible_id = self.user_transformer.get_user_id(card.owner)
        if not responsible_id:
            logger.error(f"Не удалось найти ответственного для карточки '{card.title}' (Kaiten owner: {card.owner.full_name}). Пропуск карточки.")
            return None
            
        transformed_data = {
            "TITLE": card.title,
            "DESCRIPTION": getattr(card, 'description', '') or " ",
            "RESPONSIBLE_ID": responsible_id,
            "GROUP_ID": bitrix_group_id,
            # Простое преобразование тегов
            "TAGS": [tag.name for tag in card.tags] if card.tags else [],
            # Установка крайнего срока, если он есть
            "DEADLINE": card.due_date.isoformat() if card.due_date else None,
        }

        # Удаляем поля с None, так как API Bitrix24 их не любит
        transformed_data = {k: v for k, v in transformed_data.items() if v is not None}
        
        logger.success(f"Карточка '{card.title}' успешно трансформирована.")
        return transformed_data
