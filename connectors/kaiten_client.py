import httpx
import json
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Any

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
