import httpx
import json
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any
import time # Added for caching

from config.settings import settings
from models.kaiten_models import KaitenSpace, KaitenUser, KaitenBoard, KaitenCard, KaitenSpaceMember, KaitenColumn, KaitenLane
from models.simple_kaiten_models import SimpleKaitenCard
from utils.logger import get_logger

logger = get_logger(__name__)

class KaitenClient:
    """
    Асинхронный клиент для взаимодействия с Kaiten API.
    """
    def __init__(self):
        self.base_url = settings.kaiten_base_url
        self.api_token = settings.kaiten_api_token
        self.headers = {
            'Authorization': f'Bearer {self.api_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # Кеш для пользовательских свойств
        self._properties_cache_file = Path(__file__).parent.parent / "mappings" / "custom_properties.json"
        self._properties_cache: Optional[Dict] = None

    async def _request(self, method: str, endpoint: str, **kwargs) -> Optional[dict]:
        """
        Выполняет асинхронный HTTP-запрос к Kaiten API.
        """
        async with httpx.AsyncClient(base_url=self.base_url, headers=self.headers) as client:
            try:
                response = await client.request(method, endpoint, **kwargs)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                logger.error(f"Ошибка ответа API Kaiten: {e.response.status_code} - {e.response.text}")
                return None
            except httpx.RequestError as e:
                logger.error(f"Ошибка запроса к Kaiten API: {e}")
                return None

    async def get_spaces(self) -> List[KaitenSpace]:
        """
        Получает список всех пространств.
        """
        endpoint = "/api/v1/spaces"
        logger.info("Запрос списка пространств из Kaiten...")
        data = await self._request("GET", endpoint)
        if data:
            logger.success(f"Получено {len(data)} пространств.")
            return [KaitenSpace(**item) for item in data]
        return []

    async def get_users(self, limit: int = 100) -> List[KaitenUser]:
        """Получить список всех пользователей с поддержкой пагинации"""
        users = []
        offset = 0
        page = 1
        max_pages = 3  # Ограничиваем максимум 3 страницами (~300 пользователей)
        
        while page <= max_pages:
            try:
                params = {
                    'limit': limit,
                    'offset': offset
                }
                
                logger.info(f"Запрос пользователей: страница {page}, лимит {limit}, смещение {offset}")
                response = await self._request('GET', '/api/v1/users', params=params)
                
                # API Kaiten возвращает массив пользователей напрямую, а не обернутый в объект
                if not response or not isinstance(response, list):
                    logger.warning(f"Некорректный ответ API на странице {page}: {response}")
                    break
                
                page_users = response  # Изменено: response уже является массивом
                logger.info(f"Страница {page}: получено {len(page_users)} пользователей")
                
                # Если получили пустой массив, значит больше пользователей нет
                if not page_users:
                    logger.info("Получен пустой массив пользователей, завершаем пагинацию")
                    break
                
                # Обрабатываем всех пользователей
                for user_data in page_users:
                    try:
                        user = KaitenUser(**user_data)
                        users.append(user)
                        # Показываем первых 3 пользователей для проверки
                        if len(users) <= 3:
                            logger.debug(f"Пользователь #{len(users)}: {user.email}")
                    except Exception as e:
                        logger.debug(f"Ошибка валидации пользователя {user_data.get('id')}: {e}")
                
                # Если получили меньше пользователей чем лимит, значит это последняя страница
                if len(page_users) < limit:
                    logger.info(f"Получено {len(page_users)} < {limit}, это последняя страница")
                    break
                
                # Переходим к следующей странице
                offset += limit
                page += 1
                
            except Exception as e:
                logger.error(f"Ошибка при получении пользователей на странице {page}: {e}")
                break
        
        logger.success(f"Итого загружено {len(users)} пользователей за {page-1} страниц")
        return users
        
    async def get_boards(self, space_id: int) -> List[KaitenBoard]:
        """
        Получает доски в указанном пространстве.
        """
        endpoint = f"/api/v1/spaces/{space_id}/boards"
        logger.info(f"Запрос досок для пространства {space_id}...")
        data = await self._request("GET", endpoint)
        if data:
            logger.success(f"Получено {len(data)} досок для пространства {space_id}.")
            return [KaitenBoard(**item) for item in data]
        return []

    async def get_cards(self, board_id: int) -> List[KaitenCard]:
        """
        Получает карточки на указанной доске.
        """
        endpoint = f"/api/v1/boards/{board_id}/cards"
        logger.info(f"Запрос карточек для доски {board_id}...")
        data = await self._request("GET", endpoint)
        if data:
            logger.success(f"Получено {len(data)} карточек для доски {board_id}.")
            return [KaitenCard(**item) for item in data]
        return []

    async def get_card_by_id(self, card_id: int) -> Optional[SimpleKaitenCard]:
        """
        Получает карточку по ID (упрощенная модель) с полной информацией включая описание.
        """
        endpoint = f"/api/v1/cards/{card_id}"
        logger.debug(f"Запрос полной карточки {card_id}...")
        data = await self._request("GET", endpoint)
        if data:
            logger.debug(f"Получена полная карточка {card_id}.")
            return SimpleKaitenCard(**data)
        return None

    async def get_cards_by_ids(self, card_ids: List[int]) -> List[SimpleKaitenCard]:
        """
        Получает карточки по списку ID (упрощенная модель).
        """
        cards = []
        for card_id in card_ids:
            card = await self.get_card_by_id(card_id)
            if card:
                cards.append(card)
        return cards

    async def get_space_members(self, space_id: int) -> List[KaitenSpaceMember]:
        """
        Получает список участников пространства.
        
        Args:
            space_id: ID пространства
            
        Returns:
            Список пользователей - участников пространства
        """
        # Пробуем разные возможные endpoints для получения участников пространства
        possible_endpoints = [
            f"/api/v1/spaces/{space_id}/members",
            f"/api/v1/spaces/{space_id}/users",  
            f"/api/v1/spaces/{space_id}/participants"
        ]
        
        for endpoint in possible_endpoints:
            logger.info(f"Пробую получить участников пространства {space_id} через {endpoint}...")
            data = await self._request("GET", endpoint)
            
            if data is not None:
                if isinstance(data, list):
                    # Если получили список пользователей напрямую
                    logger.success(f"Получено {len(data)} участников пространства {space_id} через {endpoint}")
                    try:
                        return [KaitenSpaceMember(**user_data) for user_data in data]
                    except Exception as e:
                        logger.warning(f"Ошибка валидации участников пространства {space_id}: {e}")
                        continue
                elif isinstance(data, dict) and 'users' in data:
                    # Если данные обернуты в объект
                    users = data['users']
                    logger.success(f"Получено {len(users)} участников пространства {space_id} через {endpoint}")
                    try:
                        return [KaitenSpaceMember(**user_data) for user_data in users]
                    except Exception as e:
                        logger.warning(f"Ошибка валидации участников пространства {space_id}: {e}")
                        continue
                elif isinstance(data, dict) and 'members' in data:
                    # Если данные обернуты как members
                    members = data['members']
                    logger.success(f"Получено {len(members)} участников пространства {space_id} через {endpoint}")
                    try:
                        return [KaitenSpaceMember(**user_data) for user_data in members]
                    except Exception as e:
                        logger.warning(f"Ошибка валидации участников пространства {space_id}: {e}")
                        continue
                else:
                    logger.warning(f"Неожиданная структура ответа от {endpoint}: {data}")
                    continue
        
        # Если ни один endpoint не сработал, возвращаем пустой список
        logger.warning(f"Не удалось получить участников пространства {space_id} ни через один из endpoints")
        return []

    async def get_board_columns(self, board_id: int) -> List[KaitenColumn]:
        """
        Получает колонки указанной доски через получение полной информации о доске.
        """
        endpoint = f"/api/v1/boards/{board_id}"
        logger.info(f"Запрос доски {board_id} для получения колонок...")
        data = await self._request("GET", endpoint)
        if data and 'columns' in data:
            columns_data = data['columns']
            logger.success(f"Получено {len(columns_data)} колонок для доски {board_id}.")
            return [KaitenColumn(**item) for item in columns_data]
        elif data:
            logger.warning(f"Доска {board_id} найдена, но колонки отсутствуют в ответе")
            return []
        else:
            logger.warning(f"Доска {board_id} не найдена")
            return []

    async def get_board_lanes(self, board_id: int) -> List[KaitenLane]:
        """
        Получает lanes (подколонки) указанной доски.
        """
        endpoint = f"/api/v1/lanes"
        params = {"board_id": board_id}
        logger.info(f"Запрос lanes для доски {board_id}...")
        data = await self._request("GET", endpoint, params=params)
        if data and isinstance(data, list):
            logger.success(f"Получено {len(data)} lanes для доски {board_id}.")
            return [KaitenLane(**item) for item in data]
        elif data and 'lanes' in data:
            lanes_data = data['lanes']
            logger.success(f"Получено {len(lanes_data)} lanes для доски {board_id}.")
            return [KaitenLane(**item) for item in lanes_data]
        else:
            logger.warning(f"Lanes для доски {board_id} не найдены")
            return []

    async def get_board_info(self, board_id: int) -> Optional[dict]:
        """
        Получает полную информацию о доске.
        """
        endpoint = f"/api/v1/boards/{board_id}"
        logger.info(f"Запрос полной информации о доске {board_id}...")
        data = await self._request("GET", endpoint)
        if data:
            logger.success(f"Получена информация о доске {board_id}.")
            return data
        else:
            logger.warning(f"Информация о доске {board_id} не найдена")
            return None

    async def get_board_subcolumns(self, board_id: int) -> List[dict]:
        """
        Получает подколонки (subcolumns) указанной доски.
        """
        endpoint = f"/api/v1/subcolumns"
        params = {"board_id": board_id}
        logger.info(f"Запрос подколонок для доски {board_id}...")
        data = await self._request("GET", endpoint, params=params)
        if data and isinstance(data, list):
            logger.success(f"Получено {len(data)} подколонок для доски {board_id}.")
            return data
        elif data and 'subcolumns' in data:
            subcolumns_data = data['subcolumns']
            logger.success(f"Получено {len(subcolumns_data)} подколонок для доски {board_id}.")
            return subcolumns_data
        else:
            logger.warning(f"Подколонки для доски {board_id} не найдены")
            return []

    async def get_card_checklists(self, card_id: int) -> List[dict]:
        """
        Получает все чек-листы указанной карточки.
        
        Args:
            card_id: ID карточки Kaiten
            
        Returns:
            Список чек-листов карточки
        """
        try:
            # Получаем полную информацию о карточке
            logger.debug(f"Получаем полную информацию о карточке {card_id} для поиска чек-листов...")
            card_data = await self._request("GET", f"/api/v1/cards/{card_id}")
            
            if not card_data:
                logger.debug(f"Не удалось получить данные карточки {card_id}")
                return []
            
            # Ищем поля, связанные с чек-листами
            checklists = []
            
            # Ищем поле checklists в данных карточки
            if 'checklists' in card_data and card_data['checklists']:
                checklists = card_data['checklists']
                logger.debug(f"Найдено поле checklists с {len(checklists)} чек-листами")
            # Дополнительно проверяем parent_checklist_ids для случая, если чек-листы хранятся отдельно
            elif 'parent_checklist_ids' in card_data and card_data['parent_checklist_ids']:
                checklist_ids = card_data['parent_checklist_ids']
                logger.debug(f"Найдено поле parent_checklist_ids: {checklist_ids}")
                for checklist_id in checklist_ids:
                    try:
                        checklist_data = await self._request("GET", f"/api/v1/checklists/{checklist_id}")
                        if checklist_data:
                            checklists.append(checklist_data)
                    except Exception as e:
                        logger.debug(f"Ошибка получения чек-листа {checklist_id}: {e}")
            
            if checklists:
                logger.debug(f"Найдено {len(checklists)} чек-листов для карточки {card_id}")
                return checklists
            else:
                logger.debug(f"Чек-листы для карточки {card_id} не найдены")
                return []
                
        except Exception as e:
            logger.debug(f"Ошибка при получении чек-листов карточки {card_id}: {e}")
            return []

    async def get_card_comments(self, card_id: int) -> List[Dict[str, Any]]:
        """
        Получает комментарии к карточке.
        
        Args:
            card_id: ID карточки в Kaiten
            
        Returns:
            Список комментариев карточки
        """
        try:
            endpoint = f"/api/v1/cards/{card_id}/comments"
            logger.debug(f"Запрос комментариев для карточки {card_id}...")
            data = await self._request("GET", endpoint)
            
            if data and isinstance(data, list):
                logger.debug(f"Найдено {len(data)} комментариев для карточки {card_id}")
                return data
            else:
                logger.debug(f"Комментарии для карточки {card_id} не найдены")
                return []
                
        except Exception as e:
            logger.debug(f"Ошибка при получении комментариев карточки {card_id}: {e}")
            return []

    async def get_card_files(self, card_id: int) -> List[Dict[str, Any]]:
        """
        Получает файлы карточки.
        
        Args:
            card_id: ID карточки в Kaiten
            
        Returns:
            Список файлов карточки
        """
        try:
            endpoint = f"/api/v1/cards/{card_id}/files"
            logger.debug(f"Запрос файлов для карточки {card_id}...")
            data = await self._request("GET", endpoint)
            
            if data and isinstance(data, list):
                logger.debug(f"Найдено {len(data)} файлов для карточки {card_id}")
                return data
            else:
                logger.debug(f"Файлы для карточки {card_id} не найдены")
                return []
                
        except Exception as e:
            logger.debug(f"Ошибка при получении файлов карточки {card_id}: {e}")
            return []

    async def download_file(self, file_url: str) -> Optional[bytes]:
        """
        Скачивает файл по URL.
        
        Args:
            file_url: URL файла для скачивания
            
        Returns:
            Содержимое файла в байтах или None при ошибке
        """
        try:
            async with httpx.AsyncClient(headers={'Authorization': f'Bearer {self.api_token}'}) as client:
                logger.debug(f"Скачивание файла: {file_url}")
                response = await client.get(file_url)
                response.raise_for_status()
                logger.debug(f"Файл успешно скачан, размер: {len(response.content)} байт")
                return response.content
        except Exception as e:
            logger.error(f"Ошибка скачивания файла {file_url}: {e}")
            return None

    def _load_properties_cache(self) -> Dict:
        """
        Загружает кеш пользовательских свойств из файла.
        
        Returns:
            Словарь с кешем или пустой словарь если файл не существует
        """
        if self._properties_cache is not None:
            return self._properties_cache
        
        try:
            if self._properties_cache_file.exists():
                with open(self._properties_cache_file, 'r', encoding='utf-8') as f:
                    self._properties_cache = json.load(f)
                    logger.debug(f"Загружен кеш пользовательских свойств: {len(self._properties_cache.get('properties', {}))} полей")
            else:
                self._properties_cache = {
                    "created_at": datetime.now().isoformat(),
                    "description": "Кеш пользовательских свойств Kaiten",
                    "properties": {},  # {property_id: property_info}
                    "values": {}       # {property_id: [values_list]}
                }
                logger.debug("Создан новый кеш пользовательских свойств")
            
            return self._properties_cache
            
        except Exception as e:
            logger.error(f"Ошибка загрузки кеша свойств: {e}")
            return {
                "created_at": datetime.now().isoformat(),
                "description": "Кеш пользовательских свойств Kaiten",
                "properties": {},
                "values": {}
            }

    def _save_properties_cache(self) -> bool:
        """
        Сохраняет кеш пользовательских свойств в файл.
        
        Returns:
            True в случае успеха
        """
        try:
            # Создаем директорию если её нет
            self._properties_cache_file.parent.mkdir(exist_ok=True)
            
            if self._properties_cache:
                self._properties_cache["last_updated"] = datetime.now().isoformat()
                
                with open(self._properties_cache_file, 'w', encoding='utf-8') as f:
                    json.dump(self._properties_cache, f, ensure_ascii=False, indent=2)
                
                logger.debug(f"Кеш пользовательских свойств сохранен: {len(self._properties_cache.get('properties', {}))} полей")
                return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения кеша свойств: {e}")
            
        return False

    async def get_custom_properties(self) -> List[Dict[str, Any]]:
        """
        Получает список всех пользовательских свойств компании.
        Использует кеш для ускорения работы.
        
        Returns:
            Список пользовательских свойств
        """
        try:
            # Загружаем кеш
            cache = self._load_properties_cache()
            
            # Если кеш пустой, делаем запрос к API
            if not cache.get('properties'):
                logger.debug("Кеш пустой, загружаем свойства из API...")
                endpoint = "/api/latest/company/custom-properties"
                data = await self._request("GET", endpoint)
                
                if data and isinstance(data, list):
                    logger.debug(f"Получено {len(data)} пользовательских свойств из API")
                    
                    # Сохраняем в кеш
                    for prop in data:
                        prop_id = str(prop.get('id'))
                        if prop_id:
                            cache['properties'][prop_id] = prop
                    
                    self._save_properties_cache()
                    return data
                else:
                    logger.debug("Пользовательские свойства не найдены в API")
                    return []
            else:
                # Возвращаем данные из кеша
                properties_list = list(cache['properties'].values())
                logger.debug(f"Возвращено {len(properties_list)} пользовательских свойств из кеша")
                return properties_list
                
        except Exception as e:
            logger.debug(f"Ошибка при получении пользовательских свойств: {e}")
            return []

    async def get_custom_property(self, property_id: int) -> Optional[Dict[str, Any]]:
        """
        Получает информацию о конкретном пользовательском свойстве.
        Использует кеш для ускорения работы.
        
        Args:
            property_id: ID пользовательского свойства
            
        Returns:
            Информация о свойстве или None
        """
        try:
            # Загружаем кеш
            cache = self._load_properties_cache()
            prop_id_str = str(property_id)
            
            # Проверяем кеш
            if prop_id_str in cache.get('properties', {}):
                prop = cache['properties'][prop_id_str]
                logger.debug(f"Найдено свойство {property_id} в кеше: {prop.get('name', 'N/A')}")
                return prop
            
            # Если нет в кеше, получаем все свойства (это обновит кеш)
            all_properties = await self.get_custom_properties()
            
            # Ищем в полученных данных
            for prop in all_properties:
                if prop.get('id') == property_id:
                    logger.debug(f"Найдено свойство {property_id}: {prop.get('name', 'N/A')}")
                    return prop
            
            logger.debug(f"Свойство {property_id} не найдено")
            return None
                
        except Exception as e:
            logger.debug(f"Ошибка при получении свойства {property_id}: {e}")
            return None

    async def get_custom_property_select_values(self, property_id: int) -> List[Dict[str, Any]]:
        """
        Получает список возможных значений для свойства типа "select".
        Использует кеш для ускорения работы.
        
        Args:
            property_id: ID пользовательского свойства
            
        Returns:
            Список значений для выбора
        """
        try:
            # Загружаем кеш
            cache = self._load_properties_cache()
            prop_id_str = str(property_id)
            
            # Проверяем кеш значений
            if prop_id_str in cache.get('values', {}):
                values = cache['values'][prop_id_str]
                logger.debug(f"Возвращено {len(values)} значений для свойства {property_id} из кеша")
                return values
            
            # Если нет в кеше, делаем запрос к API
            logger.debug(f"Загружаем значения для свойства {property_id} из API...")
            endpoint = f"/api/latest/company/custom-properties/{property_id}/select-values"
            data = await self._request("GET", endpoint)
            
            if data and isinstance(data, list):
                logger.debug(f"Получено {len(data)} значений для свойства {property_id} из API")
                
                # Сохраняем в кеш
                cache['values'][prop_id_str] = data
                self._save_properties_cache()
                
                return data
            else:
                logger.debug(f"Значения для свойства {property_id} не найдены в API")
                return []
                
        except Exception as e:
            logger.debug(f"Ошибка при получении значений свойства {property_id}: {e}")
            return []

    async def get_space_users_with_roles(self, space_id: int) -> List[Dict[str, Any]]:
        """
        Получает список пользователей пространства с их ролями и правами доступа.
        
        Args:
            space_id: ID пространства
            
        Returns:
            Список пользователей с ролями (включая администраторов и редакторов)
        """
        endpoint = f"/api/v1/spaces/{space_id}/users"
        logger.info(f"Получение пользователей с ролями для пространства {space_id}...")
        data = await self._request("GET", endpoint)
        
        if data and isinstance(data, list):
            logger.success(f"Получено {len(data)} пользователей с ролями для пространства {space_id}")
            return data
        else:
            logger.warning(f"Пользователи с ролями для пространства {space_id} не найдены")
            return []

    async def get_space_administrators(self, space_id: int) -> List[Dict[str, Any]]:
        """
        Получает список администраторов пространства (space_role_id = 3).
        
        Args:
            space_id: ID пространства
            
        Returns:
            Список администраторов пространства
        """
        users = await self.get_space_users_with_roles(space_id)
        administrators = [user for user in users if user.get('space_role_id') == 3]
        
        logger.info(f"Найдено {len(administrators)} администраторов в пространстве {space_id}")
        return administrators

    async def get_space_access_groups(self, space_id: int) -> List[Dict[str, Any]]:
        """
        Получает список групп доступа для указанного пространства.
        
        Args:
            space_id: ID пространства
            
        Returns:
            Список групп доступа пространства
        """
        try:
            # Пробуем возможные endpoints для получения групп доступа пространства
            possible_endpoints = [
                f"/api/v1/spaces/{space_id}/groups",
                f"/api/v1/spaces/{space_id}/access-groups",
                f"/api/v1/spaces/{space_id}/group-access"
            ]
            
            for endpoint in possible_endpoints:
                logger.debug(f"Пробую получить группы доступа пространства {space_id} через {endpoint}...")
                data = await self._request("GET", endpoint)
                
                if data is not None:
                    if isinstance(data, list):
                        logger.success(f"Получено {len(data)} групп доступа для пространства {space_id} через {endpoint}")
                        return data
                    elif isinstance(data, dict) and 'groups' in data:
                        groups = data['groups']
                        logger.success(f"Получено {len(groups)} групп доступа для пространства {space_id} через {endpoint}")
                        return groups
                    else:
                        logger.debug(f"Неожиданная структура ответа от {endpoint}: {data}")
                        continue
            
            # Если прямые endpoints не сработали, пробуем через API /api/latest/groups
            logger.debug(f"Пробую получить группы доступа через общий endpoint...")
            endpoint = "/api/latest/groups"
            data = await self._request("GET", endpoint)
            
            if data and isinstance(data, list):
                # Фильтруем группы, которые имеют доступ к указанному пространству
                space_groups = []
                for group in data:
                    # Проверяем есть ли в группе информация о доступе к пространствам
                    if self._group_has_space_access(group, space_id):
                        space_groups.append(group)
                
                if space_groups:
                    logger.success(f"Найдено {len(space_groups)} групп доступа для пространства {space_id} через фильтрацию")
                    return space_groups
            
            logger.debug(f"Группы доступа для пространства {space_id} не найдены")
            return []
                
        except Exception as e:
            logger.debug(f"Ошибка при получении групп доступа пространства {space_id}: {e}")
            return []

    def _group_has_space_access(self, group: Dict[str, Any], space_id: int) -> bool:
        """
        Проверяет имеет ли группа доступ к указанному пространству.
        
        Args:
            group: Данные группы
            space_id: ID пространства
            
        Returns:
            True если группа имеет доступ к пространству
        """
        try:
            # Возможные поля где может храниться информация о доступе к пространствам
            possible_fields = ['spaces', 'space_ids', 'accessible_spaces', 'space_access']
            
            for field in possible_fields:
                if field in group and group[field]:
                    spaces_data = group[field]
                    # Проверяем разные форматы хранения
                    if isinstance(spaces_data, list):
                        # Список ID пространств или объектов
                        for item in spaces_data:
                            if isinstance(item, int) and item == space_id:
                                return True
                            elif isinstance(item, dict) and item.get('id') == space_id:
                                return True
                            elif isinstance(item, dict) and item.get('space_id') == space_id:
                                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Ошибка проверки доступа группы к пространству: {e}")
            return False

    async def get_group_members(self, group_id: int) -> List[Dict[str, Any]]:
        """
        Получает список участников группы доступа.
        
        Args:
            group_id: ID группы доступа
            
        Returns:
            Список участников группы
        """
        try:
            # Пробуем endpoints согласно документации Kaiten
            possible_configs = [
                {"endpoint": "/group-users/get-list-of-group-users", "params": {"group_id": group_id}},
                {"endpoint": f"/api/latest/group-users/get-list-of-group-users", "params": {"group_id": group_id}},
                {"endpoint": f"/api/latest/groups/{group_id}/members", "params": None}, 
                {"endpoint": f"/api/latest/groups/{group_id}/users", "params": None},
                {"endpoint": f"/api/v1/groups/{group_id}/members", "params": None},
                {"endpoint": f"/api/v1/groups/{group_id}/users", "params": None}
            ]
            
            for config in possible_configs:
                endpoint = config["endpoint"]
                params = config["params"]
                logger.debug(f"Пробую получить участников группы {group_id} через {endpoint}...")
                data = await self._request("GET", endpoint, params=params)
                
                if data is not None:
                    if isinstance(data, list):
                        logger.debug(f"Получено {len(data)} участников группы {group_id} через {endpoint}")
                        return data
                    elif isinstance(data, dict) and 'members' in data:
                        members = data['members']
                        logger.debug(f"Получено {len(members)} участников группы {group_id} через {endpoint}")
                        return members
                    elif isinstance(data, dict) and 'users' in data:
                        users = data['users']
                        logger.debug(f"Получено {len(users)} участников группы {group_id} через {endpoint}")
                        return users
                    else:
                        logger.debug(f"Неожиданная структура ответа от {endpoint}: {data}")
                        continue
            
            logger.debug(f"Участники группы {group_id} не найдены")
            return []
                
        except Exception as e:
            logger.debug(f"Ошибка при получении участников группы {group_id}: {e}")
            return []

    async def get_all_space_users_including_groups(self, space_id: int) -> List[Dict[str, Any]]:
        """
        Получает ВСЕХ пользователей пространства включая пользователей из групп доступа.
        
        Args:
            space_id: ID пространства
            
        Returns:
            Список всех уникальных пользователей с доступом к пространству
        """
        try:
            all_users = {}  # Используем словарь для автоматического удаления дубликатов по ID
            
            # 1. Получаем пользователей с ролями (администраторы, редакторы и некоторые участники)
            logger.info(f"🔍 Получаем пользователей с ролями пространства {space_id}...")
            users_with_roles = await self.get_space_users_with_roles(space_id)
            
            for user in users_with_roles:
                user_id = user.get('id')
                if user_id:
                    all_users[user_id] = {
                        **user,
                        'access_type': 'roles',  # Помечаем как пользователя с ролью
                        'source': 'roles'
                    }
            
            logger.info(f"📋 Найдено {len(users_with_roles)} пользователей с ролями")
            
            # 2. Получаем всех участников пространства (включая обычных участников)
            logger.info(f"🔍 Получаем всех участников пространства {space_id}...")
            try:
                space_members = await self.get_space_members(space_id)
                
                for member in space_members:
                    # Обрабатываем как объект KaitenSpaceMember или словарь
                    if hasattr(member, 'id'):
                        user_id = member.id
                        user_data = {
                            'id': member.id,
                            'full_name': member.full_name,
                            'email': member.email,
                            'space_role_id': getattr(member, 'space_role_id', None)
                        }
                    else:
                        user_id = member.get('id')
                        user_data = member
                    
                    if user_id:
                        # Если пользователь уже есть из ролей, объединяем информацию
                        if user_id in all_users:
                            existing_access = all_users[user_id].get('access_type', 'roles')
                            all_users[user_id].update(user_data)
                            all_users[user_id]['access_type'] = 'both' if existing_access == 'roles' else 'members'
                            all_users[user_id]['source'] = 'both'
                        else:
                            # Новый пользователь только из участников
                            all_users[user_id] = {
                                **user_data,
                                'access_type': 'members',
                                'source': 'members'
                            }
                
                logger.info(f"📋 Найдено {len(space_members)} участников пространства")
                
            except Exception as e:
                logger.warning(f"⚠️ Ошибка получения участников пространства {space_id}: {e}")
                logger.info("Продолжаем только с пользователями с ролями")
            
            # 3. Получаем группы доступа пространства
            logger.info(f"🔍 Получаем группы доступа пространства {space_id}...")
            access_groups = await self.get_space_access_groups(space_id)
            
            if access_groups:
                logger.info(f"📋 Найдено {len(access_groups)} групп доступа для пространства")
                
                # 4. Получаем участников каждой группы доступа
                for group in access_groups:
                    group_id = group.get('id')
                    group_name = group.get('name', f'ID {group_id}')
                    
                    if group_id:
                        logger.info(f"👥 Получаем участников группы '{group_name}' (ID: {group_id})...")
                        group_members = await self.get_group_members(group_id)
                        
                        for member in group_members:
                            user_id = member.get('id')
                            if user_id:
                                # Если пользователь уже есть, обновляем информацию о доступе
                                if user_id in all_users:
                                    # Пользователь имеет доступ через несколько источников
                                    existing_access = all_users[user_id].get('access_type', 'roles')
                                    all_users[user_id]['access_type'] = 'groups_and_direct' if existing_access in ['roles', 'members', 'both'] else 'groups'
                                    all_users[user_id]['groups'] = all_users[user_id].get('groups', []) + [group_name]
                                else:
                                    # Новый пользователь только через группу
                                    all_users[user_id] = {
                                        **member,
                                        'access_type': 'groups',
                                        'source': 'groups',
                                        'groups': [group_name]
                                    }
                        
                        logger.info(f"✅ Обработано {len(group_members)} участников группы '{group_name}'")
                        
            else:
                logger.info("📋 Группы доступа не найдены для пространства")
            
            # Возвращаем всех уникальных пользователей
            unique_users = list(all_users.values())
            
            # Подсчитываем статистику доступа
            roles_count = len([u for u in unique_users if u.get('access_type') == 'roles'])
            members_count = len([u for u in unique_users if u.get('access_type') == 'members'])
            both_count = len([u for u in unique_users if u.get('access_type') == 'both'])
            groups_count = len([u for u in unique_users if u.get('access_type') == 'groups'])
            groups_and_direct_count = len([u for u in unique_users if u.get('access_type') == 'groups_and_direct'])
            
            logger.success(f"✅ Всего найдено {len(unique_users)} уникальных пользователей:")
            logger.info(f"   📋 Только с ролями: {roles_count}")
            logger.info(f"   👥 Только участники: {members_count}")
            logger.info(f"   🔗 И роли, и участники: {both_count}")
            logger.info(f"   👑 Только через группы: {groups_count}")
            logger.info(f"   🌟 Группы + прямой доступ: {groups_and_direct_count}")
            
            return unique_users
            
        except Exception as e:
            logger.error(f"Ошибка при получении всех пользователей пространства {space_id}: {e}")
            return []

    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ГРУППАМИ ДОСТУПА ==========
    
    async def get_groups_cache(self) -> Dict[str, Any]:
        """
        Получает и кеширует информацию о всех группах доступа с их пользователями и сущностями.
        
        Returns:
            Словарь с информацией о группах: {group_id: {name, users, entities}}
        """
        try:
            cache_file = Path("mappings/groups_cache.json")
            
            # Проверяем существует ли кеш и не старше ли он 24 часов
            if cache_file.exists():
                cache_time = cache_file.stat().st_mtime
                current_time = time.time()
                if current_time - cache_time < 24 * 3600:  # 24 часа
                    try:
                        with open(cache_file, 'r', encoding='utf-8') as f:
                            cached_data = json.load(f)
                        logger.info(f"📂 Загружен кеш групп: {len(cached_data)} записей")
                        return cached_data
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка чтения кеша групп: {e}")
            
            # Получаем все группы
            logger.info("🔍 Получаем все группы доступа из Kaiten...")
            all_groups = await self.get_all_groups()
            
            groups_cache = {}
            
            for group in all_groups:
                group_id = group.get('id')
                group_uid = group.get('uid')
                group_name = group.get('name', f'Group {group_id}')
                
                if not group_uid:
                    logger.warning(f"Группа '{group_name}' не имеет UID, пропускаем")
                    continue
                
                logger.info(f"📋 Обрабатываем группу '{group_name}' (UID: {group_uid})")
                
                # Получаем пользователей группы по UID
                group_users = await self.get_group_users(group_uid)
                
                # Получаем сущности группы по UID
                group_entities = await self.get_group_entities(group_uid)
                
                groups_cache[group_uid] = {
                    'id': group_id,
                    'uid': group_uid,
                    'name': group_name,
                    'users': group_users,
                    'entities': group_entities
                }
                
                logger.info(f"✅ Группа '{group_name}': пользователей={len(group_users)}, сущностей={len(group_entities)}")
            
            # Сохраняем кеш
            cache_file.parent.mkdir(exist_ok=True)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(groups_cache, f, ensure_ascii=False, indent=2)
            
            logger.success(f"💾 Кеш групп сохранен: {len(groups_cache)} групп")
            return groups_cache
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения кеша групп: {e}")
            return {}

    async def get_space_users_via_groups(self, space_id: int) -> List[Dict[str, Any]]:
        """
        Получает пользователей пространства через группы доступа.
        
        Args:
            space_id: ID пространства
            
        Returns:
            Список пользователей, имеющих доступ к пространству через группы
        """
        try:
            # Получаем UID пространства по его ID
            space_uid = await self.get_space_uid_by_id(space_id)
            if not space_uid:
                logger.warning(f"Не удалось получить UID для пространства {space_id}")
                return []
            
            # Получаем кеш групп
            groups_cache = await self.get_groups_cache()
            
            space_users_via_groups = []
            
            # Ищем группы, которые имеют доступ к нашему пространству
            for group_uid, group_data in groups_cache.items():
                group_name = group_data.get('name', f'Group {group_uid}')
                entities = group_data.get('entities', [])
                users = group_data.get('users', [])
                
                # Проверяем есть ли наше пространство среди сущностей группы
                has_space_access = False
                for entity in entities:
                    if isinstance(entity, dict):
                        entity_uid = entity.get('uid')
                        entity_type = entity.get('entity_type')
                        
                        # Проверяем совпадение UID и что это пространство
                        if entity_uid == space_uid and entity_type == 'space':
                            has_space_access = True
                            logger.info(f"✅ Группа '{group_name}' имеет доступ к пространству {space_id} (через UID {space_uid})")
                            break
                
                if has_space_access:
                    # Добавляем всех пользователей этой группы
                    for user in users:
                        user_with_group_info = user.copy()
                        user_with_group_info['access_type'] = 'groups'
                        user_with_group_info['group_name'] = group_name
                        user_with_group_info['group_uid'] = group_uid
                        space_users_via_groups.append(user_with_group_info)
            
            logger.info(f"👥 Найдено {len(space_users_via_groups)} пользователей пространства {space_id} через группы доступа")
            return space_users_via_groups
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения пользователей пространства {space_id} через группы: {e}")
            return []

    async def get_all_groups(self) -> List[Dict[str, Any]]:
        """
        Получает список всех групп доступа компании.
        
        Returns:
            Список всех групп доступа
        """
        try:
            # Правильный endpoint для получения групп компании
            endpoint = "/api/latest/company/groups"
            logger.info(f"🔍 Получаем группы компании через {endpoint}...")
            data = await self._request("GET", endpoint)
            
            if data is not None:
                if isinstance(data, list):
                    logger.success(f"✅ Найдено {len(data)} групп доступа через {endpoint}")
                    return data
                else:
                    logger.debug(f"Неожиданная структура ответа от {endpoint}: {data}")
                    return []
            
            logger.warning("❌ Группы доступа не найдены")
            return []
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении групп доступа: {e}")
            return []

    async def get_group_users(self, group_uid: str) -> List[Dict[str, Any]]:
        """
        Получает список пользователей группы по UID группы.
        
        Args:
            group_uid: UID группы (не ID!)
            
        Returns:
            Список пользователей группы
        """
        try:
            endpoint = f"/api/latest/groups/{group_uid}/users"
            logger.info(f"👥 Получаем пользователей группы {group_uid}...")
            data = await self._request("GET", endpoint)
            
            if data is not None:
                if isinstance(data, list):
                    logger.success(f"✅ Найдено {len(data)} пользователей в группе {group_uid}")
                    return data
                else:
                    logger.debug(f"Неожиданная структура ответа от {endpoint}: {data}")
                    return []
            
            logger.warning(f"❌ Пользователи группы {group_uid} не найдены")
            return []
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении пользователей группы {group_uid}: {e}")
            return []

    async def get_group_entities(self, group_uid: str) -> List[Dict[str, Any]]:
        """
        Получает список сущностей (entities) группы по UID группы.
        Это пространства, к которым у группы есть доступ.
        
        Args:
            group_uid: UID группы (не ID!)
            
        Returns:
            Список сущностей группы (пространства)
        """
        try:
            endpoint = f"/api/latest/company/groups/{group_uid}/entities"
            logger.info(f"📂 Получаем сущности группы {group_uid}...")
            data = await self._request("GET", endpoint)
            
            if data is not None:
                if isinstance(data, list):
                    logger.success(f"✅ Найдено {len(data)} сущностей для группы {group_uid}")
                    return data
                else:
                    logger.debug(f"Неожиданная структура ответа от {endpoint}: {data}")
                    return []
            
            logger.warning(f"❌ Сущности группы {group_uid} не найдены")
            return []
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении сущностей группы {group_uid}: {e}")
            return []

    async def get_tree_entity_roles(self) -> List[Dict[str, Any]]:
        """
        Получает список ролей для древовидных сущностей.
        
        Returns:
            Список ролей сущностей
        """
        try:
            # Пробуем разные endpoints из документации
            possible_endpoints = [
                "/api/latest/tree-entity-roles",
                "/api/v1/tree-entity-roles", 
                "/tree-entity-roles",
            ]
            
            for endpoint in possible_endpoints:
                logger.info(f"🌳 Пробуем получить роли сущностей через {endpoint}...")
                data = await self._request("GET", endpoint)
                
                if data is not None:
                    if isinstance(data, list):
                        logger.success(f"✅ Найдено {len(data)} ролей сущностей через {endpoint}")
                        return data
                    elif isinstance(data, dict) and 'roles' in data:
                        roles = data['roles']
                        logger.success(f"✅ Найдено {len(roles)} ролей сущностей через {endpoint}")
                        return roles
                    else:
                        logger.debug(f"Неожиданная структура ответа от {endpoint}: {data}")
                        continue
            
            logger.warning("❌ Роли сущностей не найдены ни через один endpoint")
            return []
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении ролей сущностей: {e}")
            return []

    async def get_tree_entities(self) -> List[Dict[str, Any]]:
        """
        Получает список древовидных сущностей.
        
        Returns:
            Список древовидных сущностей (предположительно пространства)
        """
        try:
            # Пробуем разные endpoints из документации
            possible_endpoints = [
                "/api/latest/tree-entities",
                "/api/v1/tree-entities",
                "/tree-entities",
            ]
            
            for endpoint in possible_endpoints:
                logger.info(f"🌳 Пробуем получить древовидные сущности через {endpoint}...")
                data = await self._request("GET", endpoint)
                
                if data is not None:
                    if isinstance(data, list):
                        logger.success(f"✅ Найдено {len(data)} древовидных сущностей через {endpoint}")
                        return data
                    elif isinstance(data, dict) and 'entities' in data:
                        entities = data['entities']
                        logger.success(f"✅ Найдено {len(entities)} древовидных сущностей через {endpoint}")
                        return entities
                    else:
                        logger.debug(f"Неожиданная структура ответа от {endpoint}: {data}")
                        continue
            
            logger.warning("❌ Древовидные сущности не найдены ни через один endpoint")
            return []
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении древовидных сущностей: {e}")
            return []

    async def find_group_by_name(self, group_name: str) -> Optional[Dict[str, Any]]:
        """
        Находит группу по названию.
        
        Args:
            group_name: Название группы для поиска
            
        Returns:
            Данные группы или None если не найдена
        """
        try:
            all_groups = await self.get_all_groups()
            
            for group in all_groups:
                if group.get('name') == group_name:
                    logger.success(f"✅ Найдена группа '{group_name}' с ID {group.get('id')}")
                    return group
            
            logger.warning(f"❌ Группа '{group_name}' не найдена")
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка поиска группы '{group_name}': {e}")
            return None

    async def get_space_uid_by_id(self, space_id: int) -> Optional[str]:
        """
        Получает UID пространства по его ID.
        
        Args:
            space_id: ID пространства
            
        Returns:
            UID пространства или None если не найдено
        """
        try:
            # Получаем информацию о пространстве по ID
            endpoint = f"/api/latest/spaces/{space_id}"
            data = await self._request("GET", endpoint)
            
            if data and isinstance(data, dict):
                space_uid = data.get('uid')
                if space_uid:
                    logger.debug(f"Найден UID {space_uid} для пространства ID {space_id}")
                    return space_uid
            
            logger.warning(f"UID для пространства ID {space_id} не найден")
            return None
                
        except Exception as e:
            logger.error(f"Ошибка получения UID для пространства ID {space_id}: {e}")
            return None
