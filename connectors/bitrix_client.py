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
            'PROJECT': 'N',  # N - это группа, а не проект
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

    async def update_task(self, task_id: int, **kwargs) -> bool:
        """
        Обновляет существующую задачу.
        
        :param task_id: ID задачи для обновления
        :param kwargs: Поля задачи для обновления (title, description, responsible_id, etc.)
        :return: True в случае успеха, иначе False
        """
        api_method = 'tasks.task.update'
        params = {
            'taskId': task_id,
            'fields': kwargs
        }
        
        task_title = kwargs.get('TITLE', f'ID {task_id}')
        logger.info(f"Обновление задачи '{task_title}' (ID: {task_id}) в Bitrix24...")
        result = await self._request('POST', api_method, params)
        if result:
            logger.success(f"Задача '{task_title}' (ID: {task_id}) успешно обновлена.")
            return True
        else:
            logger.error(f"Не удалось обновить задачу '{task_title}' (ID: {task_id})")
            return False

    async def get_workgroup_list(self) -> List[Dict[str, Any]]:
        """
        Получает список всех рабочих групп из Bitrix24 с поддержкой пагинации.
        """
        all_groups = []
        page = 1
        max_pages = 5  # До 250 групп (5 страниц по 50)
        
        try:
            logger.info("📥 Запрос всех рабочих групп из Bitrix24 с пагинацией...")
            
            while page <= max_pages:
                # Формула для start: start = (N-1) * 50, где N — номер страницы
                start = (page - 1) * 50
                
                request_params = {
                    'start': start
                }
                
                logger.info(f"  Страница {page}: start={start}")
                
                # Метод sonet_group.get возвращает список словарей
                groups_data = await self._request('GET', 'sonet_group.get', request_params)
                
                # Проверяем что получили корректные данные
                if not groups_data or not isinstance(groups_data, list):
                    logger.warning(f"  Некорректный ответ на странице {page}: {groups_data}")
                    break
                
                # Если получили пустой список, значит больше групп нет
                if not groups_data:
                    logger.info(f"  Страница {page}: пустой результат, завершаем пагинацию")
                    break
                
                logger.info(f"  Страница {page}: получено {len(groups_data)} групп")
                
                # Добавляем группы к общему списку
                all_groups.extend(groups_data)
                
                # Если получили меньше 50 групп, это последняя страница
                if len(groups_data) < 50:
                    logger.info(f"  Получено {len(groups_data)} < 50, это последняя страница")
                    break
                
                page += 1
            
            logger.success(f"✅ Получено {len(all_groups)} рабочих групп из {page-1} страниц")
            return all_groups
            
        except Exception as e:
            logger.error(f"Ошибка при получении рабочих групп из Bitrix24: {e}")
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

    async def find_user_by_email(self, email: str) -> Optional[BitrixUser]:
        """Найти пользователя по email"""
        try:
            response = await self._request('GET', 'user.get', {
                'filter': {'EMAIL': email}
            })
            
            if response and isinstance(response, list) and response:
                user_data = response[0]  # Берем первого найденного
                return BitrixUser(**user_data)
            
            return None
            
        except Exception as e:
            logger.warning(f"Ошибка поиска пользователя по email {email}: {e}")
            return None

    async def create_user(self, user_data: dict) -> Optional[BitrixUser]:
        """Создать нового пользователя в Bitrix24"""
        try:
            response = await self._request('POST', 'user.add', user_data)
            
            if response:
                # response уже является user_id
                user_id = response
                full_user = await self.get_user(user_id)
                if full_user:
                    logger.success(f"Создан пользователь {user_data.get('EMAIL')} (ID: {user_id})")
                    return full_user
            
            logger.error(f"Ошибка создания пользователя {user_data.get('EMAIL', 'unknown')}")
            return None
            
        except Exception as e:
            error_msg = str(e)
            
            # Если пользователь уже существует, попытаемся найти его
            if "уже существует" in error_msg or "already exists" in error_msg.lower():
                existing_user = await self.find_user_by_email(user_data.get('EMAIL'))
                if existing_user:
                    logger.info(f"Найден существующий пользователь {existing_user.EMAIL} (ID: {existing_user.ID})")
                    return existing_user
                else:
                    logger.error(f"Не удалось найти пользователя {user_data.get('EMAIL')}")
            else:
                logger.error(f"Ошибка создания пользователя {user_data.get('EMAIL', 'unknown')}: {e}")
            
            return None

    async def update_user(self, user_id: str, user_data: dict) -> Optional[BitrixUser]:
        """Обновить данные пользователя в Bitrix24"""
        try:
            # Добавляем ID пользователя к данным для обновления
            update_data = {'ID': user_id, **user_data}
            
            response = await self._request('POST', 'user.update', update_data)
            
            if response is True:
                # Получаем обновленную информацию о пользователе
                updated_user = await self.get_user(user_id)
                if updated_user:
                    return updated_user
            
            logger.error(f"Ошибка обновления пользователя ID {user_id}")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка обновления пользователя ID {user_id}: {e}")
            return None

    async def get_users(self, params: Optional[dict] = None) -> list[BitrixUser]:
        """
        Получает список всех пользователей из Bitrix24 с поддержкой пагинации.

        Args:
            params: Дополнительные параметры (не используется для совместимости).

        Returns:
            Список объектов BitrixUser.
        """
        all_users = []
        page = 1
        max_pages = 4  # До 200 пользователей (4 страницы по 50)
        
        try:
            logger.info("📥 Запрос всех пользователей из Bitrix24 с пагинацией...")
            
            while page <= max_pages:
                # Формула для start: start = (N-1) * 50, где N — номер страницы
                start = (page - 1) * 50
                
                request_params = {
                    'ADMIN_MODE': 'True',  # Режим администратора для получения всех пользователей
                    'start': start
                }
                
                logger.info(f"  Страница {page}: start={start}")
                
            # Метод user.get возвращает список словарей
                users_data = await self._request('GET', 'user.get', request_params)
                
                # Проверяем что получили корректные данные
                if not users_data or not isinstance(users_data, list):
                    logger.warning(f"  Некорректный ответ на странице {page}: {users_data}")
                    break
                
                # Если получили пустой список, значит больше пользователей нет
                if not users_data:
                    logger.info(f"  Страница {page}: пустой результат, завершаем пагинацию")
                    break
                
                logger.info(f"  Страница {page}: получено {len(users_data)} пользователей")
                
                # Добавляем пользователей к общему списку
                all_users.extend(users_data)
                
                # Если получили меньше 50 пользователей, это последняя страница
                if len(users_data) < 50:
                    logger.info(f"  Получено {len(users_data)} < 50, это последняя страница")
                    break
                
                page += 1
            
            # Преобразуем словари в объекты BitrixUser
            users = [BitrixUser.model_validate(u) for u in all_users]
            
            logger.success(f"✅ Получено {len(users)} пользователей из {page-1} страниц")
            return users
            
        except Exception as e:
            logger.error(f"Ошибка при получении пользователей из Bitrix24: {e}")
            return []

    async def get_user(self, user_id: int) -> Optional[BitrixUser]:
        """Получить информацию о пользователе по ID"""
        try:
            response = await self._request('GET', 'user.get', {'ID': user_id})
            
            if response and isinstance(response, list) and response:
                user_data = response[0]
                return BitrixUser(**user_data)
            
            return None
            
        except Exception as e:
            logger.warning(f"Ошибка получения пользователя с ID {user_id}: {e}")
            return None

    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С СТАДИЯМИ ЗАДАЧ ==========
    
    async def get_task_stages(self, entity_id: int) -> List[Dict[str, Any]]:
        """
        Получает список стадий канбана для группы.
        
        :param entity_id: ID группы (рабочей группы)
        :return: Список стадий
        """
        api_method = 'task.stages.get'
        params = {'entityId': entity_id}
        logger.info(f"Запрос стадий канбана для группы {entity_id}...")
        result = await self._request('GET', api_method, params)
        if result:
            logger.success(f"Получено {len(result)} стадий для группы {entity_id}.")
            return result
        return []

    async def create_task_stage(self, entity_id: int, title: str, sort: int = 100, 
                               color: str = "0066CC") -> Optional[Dict[str, Any]]:
        """
        Создает новую стадию канбана для группы.
        
        :param entity_id: ID группы (рабочей группы)
        :param title: Название стадии
        :param sort: Порядок сортировки (по умолчанию 100)
        :param color: Цвет стадии в HEX формате (по умолчанию синий)
        :return: Данные созданной стадии или None
        """
        api_method = 'task.stages.add'
        params = {
            'fields': {
                'TITLE': title,
                'SORT': sort,
                'COLOR': color,
                'ENTITY_ID': entity_id,
                'ENTITY_TYPE': 'GROUP'  # Указываем что это стадии для группы
            }
        }
        logger.info(f"Создание стадии '{title}' для группы {entity_id}...")
        result = await self._request('POST', api_method, params)
        if result:
            logger.success(f"Стадия '{title}' успешно создана для группы {entity_id}.")
            return result
        return None

    async def update_task_stage(self, stage_id: int, fields: Dict[str, Any]) -> bool:
        """
        Обновляет существующую стадию канбана.
        
        :param stage_id: ID стадии
        :param fields: Поля для обновления
        :return: True в случае успеха, иначе False
        """
        api_method = 'task.stages.update'
        params = {
            'id': stage_id,
            'fields': fields
        }
        logger.info(f"Обновление стадии {stage_id}...")
        result = await self._request('POST', api_method, params)
        if result:
            logger.success(f"Стадия {stage_id} успешно обновлена.")
            return True
        return False

    async def delete_task_stage(self, stage_id: int) -> bool:
        """
        Удаляет стадию канбана.
        
        :param stage_id: ID стадии
        :return: True в случае успеха, иначе False
        """
        api_method = 'task.stages.delete'
        params = {'id': stage_id}
        logger.info(f"Удаление стадии {stage_id}...")
        result = await self._request('POST', api_method, params)
        if result:
            logger.success(f"Стадия {stage_id} успешно удалена.")
            return True
        return False

    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ЧЕК-ЛИСТАМИ ЗАДАЧ ==========
    


    async def add_checklist_item(self, task_id: int, title: str, is_complete: bool = False, 
                                checklist_id: int = None) -> Optional[int]:
        """
        Добавляет элемент в чек-лист задачи.
        
        :param task_id: ID задачи
        :param title: Текст элемента чек-листа
        :param is_complete: Выполнен ли элемент (по умолчанию False)
        :param checklist_id: ID чек-листа (опционально)
        :return: ID созданного элемента или None
        """
        api_method = 'task.checklistitem.add'  # Исправленный метод
        # Правильная структура с полем TITLE
        params = {
            'taskId': task_id,
            'fields': {
                'TITLE': title,
                'IS_COMPLETE': is_complete
            }
        }
        
        if checklist_id:
            params['fields']['parentId'] = checklist_id
        
        logger.debug(f"Добавление элемента '{title}' в чек-лист задачи {task_id}...")
        result = await self._request('POST', api_method, params)
        if result:
            item_id = result
            logger.debug(f"Элемент чек-листа '{title}' создан с ID {item_id}")
            return item_id
        else:
            logger.warning(f"Не удалось создать элемент чек-листа '{title}' для задачи {task_id}")
            return None

    async def get_task_checklists(self, task_id: int) -> List[Dict[str, Any]]:
        """
        Получает чек-листы задачи.
        
        :param task_id: ID задачи
        :return: Список чек-листов задачи
        """
        api_method = 'task.checklistitem.getlist'  # Исправленный метод
        params = {'taskId': task_id}  # Исправленный параметр
        logger.debug(f"Запрос чек-листов для задачи {task_id}...")
        result = await self._request('GET', api_method, params)
        if result:
            logger.debug(f"Получено {len(result)} элементов чек-листов для задачи {task_id}")
            return result
        return []

    async def delete_checklist_item(self, item_id: int) -> bool:
        """
        Удаляет элемент чек-листа.
        
        :param item_id: ID элемента чек-листа
        :return: True в случае успеха, иначе False
        """
        api_method = 'task.checklistitem.delete'  # Исправленный метод
        params = {'itemId': item_id}  # Правильный параметр
        logger.debug(f"Удаление элемента чек-листа {item_id}...")
        result = await self._request('POST', api_method, params)
        if result:
            logger.debug(f"Элемент чек-листа {item_id} удален")
            return True
        else:
            logger.warning(f"Не удалось удалить элемент чек-листа {item_id}")
            return False

    async def clear_task_checklists(self, task_id: int) -> bool:
        """
        Очищает все чек-листы задачи.
        
        :param task_id: ID задачи
        :return: True в случае успеха
        """
        try:
            # Получаем все элементы чек-листов
            items = await self.get_task_checklists(task_id)
            
            if not items:
                logger.debug(f"У задачи {task_id} нет чек-листов для очистки")
                return True
            
            logger.debug(f"Очистка {len(items)} элементов чек-листов задачи {task_id}...")
            
            # Удаляем все элементы
            deleted_count = 0
            for item in items:
                item_id = item.get('ID') or item.get('id')
                if item_id and await self.delete_checklist_item(int(item_id)):
                    deleted_count += 1
            
            logger.debug(f"Удалено {deleted_count} из {len(items)} элементов чек-листов")
            return True
            
        except Exception as e:
            logger.warning(f"Ошибка очистки чек-листов задачи {task_id}: {e}")
            return False
