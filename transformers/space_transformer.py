from typing import Any, Dict, List

from models.kaiten_models import KaitenSpace
from transformers.base_transformer import BaseTransformer
from utils.logger import get_logger

logger = get_logger(__name__)

class SpaceTransformer(BaseTransformer):
    """
    Трансформер для преобразования данных пространства Kaiten 
    в формат для создания рабочей группы в Bitrix24.
    """
    def transform(self, space: KaitenSpace, owner_id: int, user_ids: List[int]) -> Dict[str, Any]:
        """
        Преобразует объект KaitenSpace в словарь для API Bitrix24.
        
        :param space: Объект пространства Kaiten.
        :param owner_id: ID владельца группы в Bitrix24.
        :param user_ids: Список ID пользователей для добавления в группу.
        :return: Словарь с данными для метода sonet_group.create.
        """
        logger.info(f"Трансформация пространства '{space.title}' (ID: {space.id}) для Bitrix24...")
        
        transformed_data = {
            "NAME": space.title,
            "DESCRIPTION": f"Пространство, мигрированное из Kaiten. Original ID: {space.id}",
            "OWNER_ID": owner_id,
            "USER_IDS": user_ids,
            "PROJECT": "Y" # Создаем именно проект, а не группу
        }
        
        logger.success(f"Пространство '{space.title}' успешно трансформировано.")
        return transformed_data
