from typing import List, Optional, Dict
from models.kaiten_models import KaitenUser
from models.bitrix_models import BitrixUser
from utils.logger import logger

class UserTransformer:
    """
    Трансформирует и сопоставляет пользователей Kaiten с пользователями Bitrix24.
    """

    def __init__(self, bitrix_users: List[BitrixUser]):
        # Создаем карту email -> BitrixUser объект (не ID)
        self._bitrix_user_map: Dict[str, BitrixUser] = {
            user.EMAIL.lower(): user for user in bitrix_users if user.EMAIL
        }
        logger.info(f"Карта пользователей Bitrix24 создана. Всего пользователей: {len(self._bitrix_user_map)}")

    def transform(self, kaiten_user: KaitenUser) -> Optional[BitrixUser]:
        """
        Сопоставляет пользователя Kaiten с пользователем Bitrix24 по email.

        Args:
            kaiten_user: Объект пользователя Kaiten.

        Returns:
            Объект пользователя Bitrix24 или None, если соответствие не найдено.
        """
        if not kaiten_user.email:
            logger.warning(f"Пользователь {kaiten_user.full_name} не имеет email")
            return None

        email_lower = kaiten_user.email.lower()
        bitrix_user = self._bitrix_user_map.get(email_lower)

        if bitrix_user:
            logger.info(f"Найдено соответствие для {kaiten_user.full_name} ({kaiten_user.email}) -> Bitrix ID: {bitrix_user.ID}")
            return bitrix_user
        else:
            logger.warning(f"Пользователь {kaiten_user.full_name} ({kaiten_user.email}) не найден в Bitrix24.")
            return None

    def get_user_id(self, kaiten_user: KaitenUser) -> Optional[str]:
        """
        Возвращает ID пользователя Bitrix24 для пользователя Kaiten.
        
        Args:
            kaiten_user: Объект пользователя Kaiten.
            
        Returns:
            ID пользователя Bitrix24 или None, если соответствие не найдено.
        """
        bitrix_user = self.transform(kaiten_user)
        return bitrix_user.ID if bitrix_user else None

    def kaiten_to_bitrix_data(self, kaiten_user: KaitenUser) -> dict:
        """
        Преобразует пользователя Kaiten в словарь данных для API Bitrix24.
        
        Args:
            kaiten_user: Объект пользователя Kaiten.
            
        Returns:
            Словарь с данными для создания/обновления пользователя в Bitrix24.
        """
        if not kaiten_user.email:
            logger.warning(f"Пользователь {kaiten_user.full_name} не имеет email, пропускаем")
            return {}
        
        # Обработка имени пользователя
        if not kaiten_user.full_name or kaiten_user.full_name.strip() == "":
            # Если нет полного имени, используем username или email
            if kaiten_user.username:
                first_name = kaiten_user.username
                last_name = "Kaiten"
            else:
                # Извлекаем имя из email
                email_name = kaiten_user.email.split('@')[0]
                first_name = email_name
                last_name = "Kaiten"
            logger.info(f"Пользователь без имени: {kaiten_user.email} -> {first_name} {last_name}")
        else:
            # Разделяем полное имя на имя и фамилию
            full_name_parts = kaiten_user.full_name.strip().split()
            first_name = full_name_parts[0] if full_name_parts else "Пользователь"
            last_name = " ".join(full_name_parts[1:]) if len(full_name_parts) > 1 else "Kaiten"
        
        user_data = {
            "EMAIL": kaiten_user.email,
            "NAME": first_name,
            "LAST_NAME": last_name,
            "UF_DEPARTMENT": [1],  # ID=1 - подразделение "Имена" в структуре организации
            "GROUP_ID": [12],  # ID=12 - группа доступа "Имена: Сотрудники"
        }
        
        logger.debug(f"Подготовлены данные для пользователя {kaiten_user.full_name or kaiten_user.username}: {user_data}")
        return user_data
