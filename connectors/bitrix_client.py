import httpx
from typing import Optional, Dict, Any, List

from config.settings import settings
from utils.logger import get_logger
from models.bitrix_models import BitrixUser

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

    async def create_workgroup(self, group_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Создает новую рабочую группу (проект).
        
        :param group_data: Словарь с данными группы (NAME, DESCRIPTION, OWNER_ID, USER_IDS и т.д.)
        :return: Словарь с данными созданной группы или None
        """
        api_method = 'sonet_group.create'
        # Устанавливаем значения по умолчанию для обязательных полей
        params = {
            'VISIBLE': 'Y',
            'OPENED': 'N',  # N - по приглашению
            'PROJECT': 'Y',  # Y - это проект, а не группа
            **group_data  # Объединяем с переданными данными
        }
        
        group_name = group_data.get('NAME', 'Без названия')
        logger.info(f"Создание рабочей группы '{group_name}' в Bitrix24...")
        result = await self._request('POST', api_method, params)
        if result:
            # result может быть как числом (ID), так и объектом
            if isinstance(result, (int, str)):
                group_id = str(result)
                logger.success(f"Рабочая группа '{group_name}' успешно создана с ID {group_id}.")
                return {"ID": group_id}
            elif isinstance(result, dict) and "ID" in result:
                logger.success(f"Рабочая группа '{group_name}' успешно создана с ID {result['ID']}.")
                return result
            else:
                logger.success(f"Рабочая группа '{group_name}' успешно создана.")
                return result
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

    async def get_workgroup_users(self, group_id: int) -> List[Dict[str, Any]]:
        """
        Получает список пользователей рабочей группы.
        """
        api_method = 'sonet_group.user.get'
        params = {'ID': group_id}
        logger.info(f"Запрос пользователей для группы {group_id} из Bitrix24...")
        result = await self._request('GET', api_method, params)
        if result:
            logger.success(f"Получено {len(result)} пользователей для группы {group_id}.")
            return result
        return []

    async def create_user(self, user_data: dict) -> Optional[int]:
        """
        Создает нового пользователя в Bitrix24.

        Args:
            user_data: Словарь с данными пользователя (EMAIL, NAME, LAST_NAME и т.д.).

        Returns:
            ID созданного пользователя или None в случае ошибки.
        """
        try:
            email = user_data.get("EMAIL")
            logger.info(f"Отправка запроса на создание пользователя с email: {email}...")
            
            # Используем user.add для создания/приглашения пользователя
            created_user_id = await self._request('POST', 'user.add', user_data)

            if created_user_id and isinstance(created_user_id, int):
                logger.success(f"Пользователь с email {email} успешно создан с ID: {created_user_id}")
                return created_user_id
            else:
                logger.error(f"Не удалось создать пользователя с email {email}. Ответ API: {created_user_id}")
                return None
        except Exception as e:
            logger.error(f"Исключение при создании пользователя в Bitrix24: {e}")
            return None

    async def update_user(self, user_id: str, user_data: dict) -> bool:
        """
        Обновляет данные существующего пользователя в Bitrix24.

        Args:
            user_id: ID пользователя для обновления.
            user_data: Словарь с новыми данными пользователя (NAME, LAST_NAME и т.д.).

        Returns:
            True в случае успеха, иначе False.
        """
        try:
            email = user_data.get("EMAIL", "неизвестный")
            logger.info(f"Отправка запроса на обновление пользователя ID {user_id} (email: {email})...")
            
            # Используем user.update для обновления данных пользователя
            # Передаем ID пользователя в параметрах вместе с данными для обновления
            update_params = {"ID": user_id, **user_data}
            result = await self._request('POST', 'user.update', update_params)

            if result is not None:
                logger.success(f"Пользователь ID {user_id} (email: {email}) успешно обновлен")
                return True
            else:
                logger.error(f"Не удалось обновить пользователя ID {user_id} (email: {email})")
                return False
        except Exception as e:
            logger.error(f"Исключение при обновлении пользователя ID {user_id} в Bitrix24: {e}")
            return False

    async def get_users(self, params: Optional[dict] = None) -> list[BitrixUser]:
        """
        Получает список пользователей из Bitrix24.

        Args:
            params: Параметры для фильтрации.

        Returns:
            Список объектов BitrixUser.
        """
        if params is None:
            params = {}
        try:
            logger.info(f"Запрос пользователей из Bitrix24 с фильтром {params}...")
            # Метод user.get возвращает список словарей
            users_data = await self._request('GET', 'user.get', {"filter": params})
            
            # Преобразуем словари в объекты BitrixUser
            users = [BitrixUser.model_validate(u) for u in users_data]
            
            logger.success(f"Получено {len(users)} пользователей.")
            return users
        except Exception as e:
            logger.error(f"Ошибка при получении пользователей из Bitrix24: {e}")
            return []
