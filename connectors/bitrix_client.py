import httpx
from typing import Optional, Dict, Any, List

from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)

class BitrixClient:
    """
    Асинхронный клиент для взаимодействия с Bitrix24 REST API.
    """
    def __init__(self):
        # Webhook URL уже содержит и базовый путь, и токен
        self.webhook_url = settings.bitrix_webhook_url

    async def _request(self, method: str, api_method: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Выполняет асинхронный HTTP-запрос к Bitrix24 API.
        
        :param method: HTTP метод ('GET', 'POST', etc.)
        :param api_method: Метод Bitrix24 API (e.g., 'sonet_group.user.add')
        :param params: Параметры запроса
        """
        url = f"{self.webhook_url.rstrip('/')}/{api_method}"
        
        async with httpx.AsyncClient() as client:
            try:
                if method.upper() == 'POST':
                    response = await client.post(url, json=params)
                else:
                    response = await client.get(url, params=params)
                
                response.raise_for_status()
                data = response.json()

                if 'error' in data:
                    logger.error(f"Ошибка API Bitrix24: {data.get('error_description', 'Неизвестная ошибка')}")
                    return None
                
                return data.get('result')
            
            except httpx.HTTPStatusError as e:
                logger.error(f"Ошибка ответа API Bitrix24: {e.response.status_code} - {e.response.text}")
                return None
            except httpx.RequestError as e:
                logger.error(f"Ошибка запроса к Bitrix24 API: {e}")
                return None

    async def add_user_to_workgroup(self, group_id: int, user_id: int) -> bool:
        """
        Добавляет пользователя в рабочую группу.
        
        :param group_id: ID рабочей группы
        :param user_id: ID пользователя
        :return: True в случае успеха, иначе False
        """
        api_method = 'sonet_group.user.add'
        params = {
            'GROUP_ID': group_id,
            'USER_ID': user_id
        }
        logger.info(f"Добавление пользователя {user_id} в группу {group_id} в Bitrix24...")
        result = await self._request('POST', api_method, params)
        
        if result:
            logger.success(f"Пользователь {user_id} успешно добавлен в группу {group_id}.")
            return True
        return False

    async def create_workgroup(self, name: str, description: str, owner_id: int, user_ids: List[int]) -> Optional[int]:
        """
        Создает новую рабочую группу (проект).
        
        :param name: Название группы
        :param description: Описание группы
        :param owner_id: ID владельца группы
        :param user_ids: Список ID участников
        :return: ID созданной группы или None
        """
        api_method = 'sonet_group.create'
        params = {
            'NAME': name,
            'DESCRIPTION': description,
            'OWNER_ID': owner_id,
            'USER_IDS': user_ids,
            'VISIBLE': 'Y',
            'OPENED': 'N', # N - по приглашению
            'PROJECT': 'Y' # Y - это проект, а не группа
        }
        logger.info(f"Создание рабочей группы '{name}' в Bitrix24...")
        result = await self._request('POST', api_method, params)
        if result:
            group_id = int(result)
            logger.success(f"Рабочая группа '{name}' успешно создана с ID {group_id}.")
            return group_id
        return None

    async def create_task(self, title: str, description: str, responsible_id: int, group_id: int, **kwargs) -> Optional[int]:
        """
        Создает новую задачу.
        
        :param title: Название задачи
        :param description: Описание задачи
        :param responsible_id: ID ответственного
        :param group_id: ID проекта (рабочей группы)
        :param kwargs: Другие параметры задачи (tags, priority, etc.)
        :return: ID созданной задачи или None
        """
        api_method = 'tasks.task.add'
        params = {
            'fields': {
                'TITLE': title,
                'DESCRIPTION': description,
                'RESPONSIBLE_ID': responsible_id,
                'GROUP_ID': group_id,
                **kwargs # Добавляем остальные поля, если они есть
            }
        }
        logger.info(f"Создание задачи '{title}' в Bitrix24...")
        result = await self._request('POST', api_method, params)
        if result and 'task' in result:
            task_id = result['task']['id']
            logger.success(f"Задача '{title}' успешно создана с ID {task_id}.")
            return task_id
        return None

    async def get_workgroup_list(self) -> List[Dict[str, Any]]:
        """
        Получает список рабочих групп.
        """
        api_method = 'sonet_group.get'
        logger.info("Запрос списка рабочих групп из Bitrix24...")
        result = await self._request('GET', api_method)
        if result:
            logger.success(f"Получено {len(result)} рабочих групп.")
            return result
        return []
        
    async def get_task_list(self, group_id: int) -> List[Dict[str, Any]]:
        """
        Получает список задач в проекте.
        """
        api_method = 'tasks.task.list'
        params = {
            'filter': {'GROUP_ID': group_id},
            'select': ['ID', 'TITLE', 'STATUS']
        }
        logger.info(f"Запрос задач для проекта {group_id} в Bitrix24...")
        result = await self._request('GET', api_method, params)
        if result and 'tasks' in result:
            logger.success(f"Получено {len(result['tasks'])} задач.")
            return result['tasks']
        return []
