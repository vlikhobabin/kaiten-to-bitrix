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

    async def get_users(self) -> List[KaitenUser]:
        """
        Получает список всех пользователей.
        """
        endpoint = "/api/v1/users"
        logger.info("Запрос списка пользователей из Kaiten...")
        data = await self._request("GET", endpoint)
        if data:
            logger.success(f"Получено {len(data)} пользователей.")
            return [KaitenUser(**item) for item in data]
        return []
        
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
