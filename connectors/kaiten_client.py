import httpx
from typing import List, Optional

from config.settings import settings
from models.kaiten_models import KaitenSpace, KaitenUser, KaitenBoard, KaitenCard
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
