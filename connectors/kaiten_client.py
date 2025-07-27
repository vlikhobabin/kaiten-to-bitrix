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
        """Получение всех пространств из Kaiten"""
        logger.debug("Запрос списка пространств из Kaiten...")
        endpoint = "/api/v1/spaces"
        data = await self._request("GET", endpoint)
        if data:
            logger.success(f"Получено {len(data)} пространств.")
            return [KaitenSpace(**item) for item in data]
        return []

    async def get_users(self, limit: int = 50) -> List[KaitenUser]:
        """
        Получение всех пользователей из Kaiten с пагинацией.
        Убираем is_archived пользователей.
        """
        users = []
        page = 0
        
        while True:
            offset = page * limit
            logger.debug(f"Запрос пользователей: страница {page}, лимит {limit}, смещение {offset}")
            
            endpoint = f"users?limit={limit}&offset={offset}"
            result = await self._request("GET", endpoint)
            
            if not result:
                break
            
            # Фильтруем архивированных пользователей на уровне сырых данных
            active_user_data = [user_data for user_data in result if not user_data.get('is_archived', False)]
            page_users = [KaitenUser(**user_data) for user_data in active_user_data]
            
            if not page_users:
                logger.debug("Получен пустой массив пользователей, завершаем пагинацию")
                break
            
            users.extend(page_users)
            
            archived_count = len(result) - len(page_users)
            if archived_count > 0:
                logger.debug(f"Страница {page}: исключено {archived_count} архивированных пользователей")
            
            page += 1
            
            # Прерываем если получили меньше запрошенного лимита
            if len(page_users) < limit:
                logger.debug(f"Получено {len(page_users)} < {limit}, это последняя страница")
                break
        
        logger.info(f"Загружено {len(users)} активных пользователей из Kaiten")
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
                    logger.debug(f"Загружен кеш пользовательских свойств: {len((self._properties_cache or {}).get('properties', {}))} полей")
            else:
                self._properties_cache = {
                    "created_at": datetime.now().isoformat(),
                    "description": "Кеш пользовательских свойств Kaiten",
                    "properties": {},  # {property_id: property_info}
                    "values": {}       # {property_id: [values_list]}
                }
                logger.debug("Создан новый кеш пользовательских свойств")
            
            return self._properties_cache or {}
            
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
        Получает пользователей пространства с их ролями и правами доступа.
        
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

    async def get_all_space_users_including_groups(self, space_id: int) -> List[Dict[str, Any]]:
        """
        Получает ВСЕХ пользователей пространства включая пользователей из групп доступа.
        
        Args:
            space_id: ID пространства
            
        Returns:
            Список всех уникальных пользователей с доступом к пространству
        """
        try:
            logger.debug(f"Получаем пользователей с ролями пространства {space_id}...")
            
            all_users = {}  # Используем словарь для автоматического удаления дубликатов по ID
            
            # 1. Получаем пользователей с ролями (администраторы, редакторы)
            users_with_roles = await self.get_space_users_with_roles(space_id)
            
            for user in users_with_roles:
                user_id = user.get('id')
                if user_id:
                    all_users[user_id] = {
                        **user,
                        'access_type': 'roles',
                        'source': 'roles'
                    }
            
            logger.debug(f"Найдено {len(users_with_roles)} пользователей с ролями")
            
            # 2. Получаем всех участников пространства (включая только участников без ролей)
            logger.debug(f"Получаем всех участников пространства {space_id}...")
            space_members = await self.get_space_members(space_id)
            
            if space_members:
                for member in space_members:
                    # Convert KaitenSpaceMember to dict for processing
                    member_dict = member.model_dump() if hasattr(member, 'model_dump') else member.__dict__
                    user_id = member_dict.get('id')
                    if user_id:
                        # Если пользователь уже есть, обновляем информацию о доступе
                        if user_id in all_users:
                            all_users[user_id]['access_type'] = 'both'
                            # Объединяем данные, приоритет у пользователей с ролями
                            all_users[user_id].update({
                                k: v for k, v in member_dict.items() 
                                if k not in all_users[user_id] or all_users[user_id][k] is None
                            })
                        else:
                            # Новый пользователь только как участник
                            all_users[user_id] = {
                                **member_dict,
                                'access_type': 'members',
                                'source': 'members'
                            }
                
                logger.debug(f"Найдено {len(space_members)} участников пространства")
            else:
                # Если не удалось получить участников через API, продолжаем только с пользователями с ролями
                logger.debug("Продолжаем только с пользователями с ролями")
            
            # 3. Получаем пользователей из групп доступа
            logger.debug(f"Получаем группы доступа пространства {space_id}...")
            space_users_via_groups = await self.get_space_users_via_groups(space_id)
            
            if space_users_via_groups:
                logger.debug(f"Найдено {len(space_users_via_groups)} пользователей через группы доступа")
                
                for user in space_users_via_groups:
                    user_id = user.get('id')
                    if user_id:
                        # Если пользователь уже есть, обновляем информацию о доступе
                        if user_id in all_users:
                            current_access = all_users[user_id]['access_type']
                            if current_access == 'roles':
                                all_users[user_id]['access_type'] = 'groups_and_direct'
                            elif current_access == 'members':
                                all_users[user_id]['access_type'] = 'groups_and_direct'
                            elif current_access == 'both':
                                all_users[user_id]['access_type'] = 'groups_and_direct'
                            
                            # Добавляем информацию о группах
                            existing_groups = all_users[user_id].get('groups', [])
                            new_group = user.get('group_name')
                            if new_group and new_group not in existing_groups:
                                all_users[user_id]['groups'] = existing_groups + [new_group]
                        else:
                            # Новый пользователь только через группу
                            all_users[user_id] = {
                                **user,
                                'access_type': 'groups',
                                'source': 'groups',
                                'groups': [user.get('group_name', 'Unknown Group')]
                            }
            else:
                logger.debug("Группы доступа не найдены для пространства")
            
            # Возвращаем всех уникальных пользователей
            result = list(all_users.values())
            
            # Статистика по типам доступа
            roles_count = len([u for u in result if u.get('access_type') == 'roles'])
            members_count = len([u for u in result if u.get('access_type') == 'members'])
            both_count = len([u for u in result if u.get('access_type') == 'both'])
            groups_count = len([u for u in result if u.get('access_type') == 'groups'])
            groups_and_direct_count = len([u for u in result if u.get('access_type') == 'groups_and_direct'])
            
            logger.debug(f"   Только с ролями: {roles_count}")
            logger.debug(f"   Только участники: {members_count}")
            logger.debug(f"   И роли, и участники: {both_count}")
            logger.debug(f"   Только через группы: {groups_count}")
            logger.debug(f"   Группы + прямой доступ: {groups_and_direct_count}")
            
            return result
                
        except Exception as e:
            logger.error(f"Ошибка при получении всех пользователей пространства {space_id}: {e}")
            return []

    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ГРУППАМИ ДОСТУПА ==========
    
    def _is_cache_valid(self, cache_file: Path, max_age_hours: int = 24) -> bool:
        """Проверяет актуальность кеша"""
        if not cache_file.exists():
            return False
        
        # Проверяем возраст файла
        file_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
        age_hours = file_age.total_seconds() / 3600
        
        if age_hours <= max_age_hours:
            logger.debug(f"Кеш актуален (возраст: {age_hours:.1f} часов)")
            return True
        else:
            logger.debug(f"Кеш устарел (возраст: {age_hours:.1f} часов)")
            return False

    async def _get_all_groups_from_api(self) -> List[Dict[str, Any]]:
        """
        Получает список всех групп доступа напрямую через API (без кеша).
        Используется для обновления кеша во избежание циклических зависимостей.
        
        Returns:
            Список всех групп доступа с базовой информацией (id, uid, name)
        """
        try:
            endpoint = "/api/latest/company/groups"
            logger.info(f"🔍 Получаем группы компании через API: {endpoint}...")
            data = await self._request("GET", endpoint)
            
            if data is not None:
                if isinstance(data, list):
                    logger.success(f"✅ Найдено {len(data)} групп доступа через API")
                    return data
                else:
                    logger.debug(f"Неожиданная структура ответа от {endpoint}: {data}")
                    return []
            
            logger.warning("❌ Группы доступа не найдены")
            return []
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении групп доступа: {e}")
            return []

    async def get_groups_cache(self) -> Dict[str, Any]:
        """
        Получает и кеширует информацию о всех группах доступа с их пользователями и сущностями.
        
        Returns:
            Словарь с информацией о группах: {group_uid: {id, uid, name, users, entities}}
        """
        try:
            cache_file = Path("mappings/groups_cache.json")
            
            # Проверяем актуальность кеша
            if self._is_cache_valid(cache_file, max_age_hours=24):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cached_data = json.load(f)
                    logger.success(f"📂 Загружен кеш групп: {len(cached_data)} записей")
                    return cached_data
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка чтения кеша групп: {e}")
            
            # Получаем все группы напрямую через API
            logger.info("🔍 Обновляем кеш групп...")
            all_groups = await self._get_all_groups_from_api()
            
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
    
    async def get_all_groups(self) -> List[Dict[str, Any]]:
        """
        Получает список всех групп доступа компании.
        Использует кеш если он актуален, иначе запрашивает через API.
        
        Returns:
            Список всех групп доступа с базовой информацией (id, uid, name)
        """
        try:
            cache_file = Path("mappings/groups_cache.json")
            
            # Сначала проверяем кеш
            if self._is_cache_valid(cache_file, max_age_hours=24):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cached_data = json.load(f)
                    
                    # Извлекаем базовую информацию о группах из кеша
                    groups_list = []
                    for group_uid, group_data in cached_data.items():
                        groups_list.append({
                            'id': group_data.get('id'),
                            'uid': group_data.get('uid'),
                            'name': group_data.get('name', '')
                        })
                    
                    logger.success(f"📂 Группы загружены из кеша: {len(groups_list)} записей")
                    return groups_list
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка чтения кеша групп: {e}")
            
            # Если кеш недоступен или устарел - запрашиваем через API
            return await self._get_all_groups_from_api()
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении групп доступа: {e}")
            return []

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

    async def get_group_users(self, group_uid: str) -> List[Dict[str, Any]]:
        """
        Получает список пользователей группы по UID группы.
        
        Args:
            group_uid: UID группы (не ID!)
            
        Returns:
            Список пользователей группы
        """
        logger.debug(f"Получаем пользователей группы {group_uid}...")
        try:
            endpoint = f"/api/latest/groups/{group_uid}/users"
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
        Получает список сущностей (пространств/досок), к которым имеет доступ группа.
        
        Args:
            group_uid: UID группы
            
        Returns:
            Список сущностей с их ролями
        """
        logger.debug(f"Получаем сущности группы {group_uid}...")
        try:
            endpoint = f"/api/latest/company/groups/{group_uid}/entities"
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
