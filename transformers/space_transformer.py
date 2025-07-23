from typing import List, Optional, Dict, Any
from models.kaiten_models import KaitenSpace
from utils.logger import logger

class SpaceTransformer:
    """
    Трансформирует и сопоставляет пространства Kaiten с рабочими группами Bitrix24.
    """

    def __init__(self, bitrix_workgroups: List[Dict], user_mapping: Dict[str, str], all_kaiten_spaces: List[KaitenSpace], kaiten_client=None):
        """
        Инициализация трансформера пространств.
        
        Args:
            bitrix_workgroups: Список существующих рабочих групп Bitrix24
            user_mapping: Маппинг Kaiten User ID -> Bitrix User ID
            all_kaiten_spaces: Все пространства Kaiten для построения иерархии
            kaiten_client: Клиент Kaiten для получения участников пространства
        """
        # Создаем карту название -> ID группы Bitrix24
        self._bitrix_group_map: Dict[str, Dict] = {
            group.get('NAME', '').lower().strip(): group 
            for group in bitrix_workgroups 
            if group.get('NAME')
        }
        
        # Маппинг пользователей для участников
        self._user_mapping = user_mapping
        
        # Создаем карту иерархии пространств Kaiten
        self._kaiten_spaces_map: Dict[str, KaitenSpace] = {
            space.uid: space for space in all_kaiten_spaces
        }
        
        # Клиент Kaiten для получения участников
        self._kaiten_client = kaiten_client
        
        logger.info(f"Карта рабочих групп Bitrix24 создана. Всего групп: {len(self._bitrix_group_map)}")
        logger.info(f"Маппинг пользователей загружен: {len(self._user_mapping)} записей")
        logger.info(f"Карта пространств Kaiten создана: {len(self._kaiten_spaces_map)} записей")

    def _build_hierarchical_name(self, kaiten_space: KaitenSpace) -> str:
        """
        Строит полное название пространства с учетом иерархии.
        
        Args:
            kaiten_space: Пространство Kaiten
            
        Returns:
            Полное название в формате "Родитель/Дочернее" или просто "Название"
        """
        names = []
        current_space = kaiten_space
        max_depth = 10  # Защита от циклических ссылок
        depth = 0
        
        # Идем вверх по иерархии, собирая названия
        while current_space and depth < max_depth:
            if current_space.title:
                names.append(current_space.title)
            
            # Ищем родительское пространство
            if current_space.parent_entity_uid:
                parent_space = self._kaiten_spaces_map.get(current_space.parent_entity_uid)
                if parent_space:
                    current_space = parent_space
                    depth += 1
                else:
                    break
            else:
                break
        
        # Переворачиваем список (от корня к листу) и соединяем через "/"
        if names:
            names.reverse()
            full_name = "/".join(names)
            
            if len(names) > 1:
                logger.debug(f"Построено иерархическое название: '{full_name}' (глубина: {len(names)})")
            
            return full_name
        else:
            return f"Space-{kaiten_space.id}"

    def find_existing_workgroup(self, kaiten_space: KaitenSpace) -> Optional[Dict]:
        """
        Ищет существующую рабочую группу в Bitrix24 по иерархическому названию пространства.

        Args:
            kaiten_space: Объект пространства Kaiten.

        Returns:
            Словарь с данными группы Bitrix24 или None, если не найдено.
        """
        # Строим полное иерархическое название
        full_name = self._build_hierarchical_name(kaiten_space)
        space_name_lower = full_name.lower().strip()
        
        existing_group = self._bitrix_group_map.get(space_name_lower)

        if existing_group:
            logger.info(f"Найдена существующая группа для '{full_name}' -> Bitrix ID: {existing_group.get('ID')}")
            return existing_group
        else:
            logger.debug(f"Группа для пространства '{full_name}' не найдена в Bitrix24")
            return None

    def kaiten_to_bitrix_workgroup_data(self, kaiten_space: KaitenSpace) -> Dict[str, Any]:
        """
        Преобразует пространство Kaiten в словарь данных для создания рабочей группы в Bitrix24.
        
        Args:
            kaiten_space: Объект пространства Kaiten.
            
        Returns:
            Словарь с данными для создания/обновления рабочей группы в Bitrix24.
        """
        # Строим полное иерархическое название
        full_name = self._build_hierarchical_name(kaiten_space)
        
        if not full_name:
            logger.warning(f"Не удалось построить название для пространства: ID {kaiten_space.id}")
            return {}

        # Подготавливаем описание
        description = ""
        if hasattr(kaiten_space, 'description') and kaiten_space.description and kaiten_space.description.strip():
            description = kaiten_space.description.strip()
        else:
            description = f"Пространство из Kaiten: {full_name}"

        workgroup_data = {
            "NAME": full_name,  # Используем полное иерархическое название
            "DESCRIPTION": description,
            'INITIATE_PERMS': 'E', # E - владелец группы и модераторы группы имеeт право на приглашение
            "PROJECT": "N",  # Это группа, а не проект
            "VISIBLE": "Y",  # Видимая для участников
            "OPENED": "N",   # Закрытая (по приглашению)
            "SUBJECT_ID": 1, # ID тематики (1 - общая)
            "AVATAR": ""     # Без аватара пока
        }
        
        logger.debug(f"Подготовлены данные для группы '{full_name}': {workgroup_data}")
        return workgroup_data

    async def get_space_members_bitrix_ids_async(self, kaiten_space: KaitenSpace) -> List[int]:
        """
        Асинхронно получает список ID пользователей Bitrix24 для участников пространства Kaiten.
        
        Args:
            kaiten_space: Объект пространства Kaiten.
            
        Returns:
            Список ID пользователей в Bitrix24.
        """
        if not self._kaiten_client:
            logger.debug(f"KaitenClient не предоставлен, пропускаем получение участников для пространства '{kaiten_space.title}'")
            return []
        
        try:
            # Получаем участников пространства из Kaiten API
            space_members = await self._kaiten_client.get_space_members(kaiten_space.id)
        except Exception as e:
            logger.warning(f"Ошибка получения участников пространства '{kaiten_space.title}': {e}")
            return []
        
        if not space_members:
            logger.debug(f"Участники пространства '{kaiten_space.title}' не найдены или пусты")
            return []
        
        # Сопоставляем участников Kaiten с пользователями Bitrix24
        bitrix_member_ids = []
        for member in space_members:
            # Ищем пользователя в маппинге по ID
            kaiten_user_id = str(member.id)
            if kaiten_user_id in self._user_mapping:
                bitrix_user_id = int(self._user_mapping[kaiten_user_id])
                bitrix_member_ids.append(bitrix_user_id)
                logger.debug(f"Участник {member.full_name or member.email} (Kaiten ID: {kaiten_user_id}) сопоставлен с Bitrix ID: {bitrix_user_id}")
            else:
                logger.warning(f"Участник пространства {member.full_name or member.email} (Kaiten ID: {kaiten_user_id}) не найден в маппинге пользователей")
        
        logger.info(f"Для пространства '{kaiten_space.title}' найдено {len(bitrix_member_ids)} участников из {len(space_members)} в Kaiten")
        return bitrix_member_ids

    def get_space_members_bitrix_ids(self, kaiten_space: KaitenSpace) -> List[int]:
        """
        Получает список ID пользователей Bitrix24 для участников пространства Kaiten.
        
        ПРИМЕЧАНИЕ: Синхронная версия возвращает пустой список. 
        Используйте get_space_members_bitrix_ids_async для реального получения участников.
        
        Args:
            kaiten_space: Объект пространства Kaiten.
            
        Returns:
            Пустой список (используйте асинхронную версию).
        """
        logger.debug(f"Вызван синхронный метод для пространства '{kaiten_space.title}'. Используйте асинхронную версию для получения участников")
        return []

    def get_space_owner_bitrix_id(self, kaiten_space: KaitenSpace) -> Optional[int]:
        """
        Получает ID владельца пространства в Bitrix24 на основе автора.
        
        Args:
            kaiten_space: Объект пространства Kaiten.
            
        Returns:
            ID владельца в Bitrix24 или None.
        """
        # Используем author_uid как владельца, если доступно
        if not hasattr(kaiten_space, 'author_uid') or not kaiten_space.author_uid:
            logger.debug(f"Пространство '{kaiten_space.title}' не имеет автора")
            return None

        # author_uid это строка UUID, а не числовой ID, поэтому пока не можем найти в маппинге
        # TODO: Добавить сопоставление UUID пользователей с их ID
        logger.debug(f"Автор пространства '{kaiten_space.title}': {kaiten_space.author_uid} (пока не сопоставляется)")
        return None
