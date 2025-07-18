import httpx
from typing import List, Optional

from config.settings import settings
from models.kaiten_models import KaitenSpace, KaitenUser, KaitenBoard, KaitenCard, KaitenSpaceMember, KaitenColumn, KaitenLane
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
