import httpx
from typing import Optional, Dict, Any, List, Union

from config.settings import settings
from utils.logger import get_logger
from models.bitrix_models import BitrixUser

logger = get_logger(__name__)

class BitrixClient:
    """
    Асинхронный клиент для взаимодействия с Bitrix24 REST API.
    """
    def __init__(self):
        """Инициализация клиента Bitrix24"""
        # Кэширование для производительности
        self._group_storage_cache = {}  # {group_id: storage_id}
        self._group_folder_cache = {}   # {storage_id: folder_id}
        
        # Загружаем настройки из env
        self.webhook_url = settings.bitrix_webhook_url
        if not self.webhook_url:
            raise ValueError("BITRIX_WEBHOOK_URL не настроен в переменных окружения")
            
        # Извлекаем базовый URL для формирования ссылок на файлы
        self.base_url = self._extract_base_url(self.webhook_url)

    def _extract_base_url(self, webhook_url: str) -> str:
        """
        Извлекает базовый URL Bitrix24 из webhook URL.
        
        Args:
            webhook_url: URL вебхука (например: https://domain/rest/1/webhook_code/)
            
        Returns:
            Базовый URL (например: https://domain)
        """
        import re
        # Паттерн для извлечения базового URL из webhook
        # Формат webhook: https://domain/rest/1/webhook_code/
        match = re.match(r'(https?://[^/]+)', webhook_url)
        if match:
            return match.group(1)
        else:
            # Fallback на случай нестандартного формата
            return webhook_url.split('/rest/')[0] if '/rest/' in webhook_url else webhook_url.rstrip('/')

    def get_file_url(self, file_id: str) -> str:
        """
        Формирует URL для просмотра файла в Bitrix24.
        
        Args:
            file_id: ID файла (может содержать префикс 'n')
            
        Returns:
            URL для просмотра файла
        """
        # Убираем префикс 'n' если есть
        clean_file_id = file_id.replace('n', '') if file_id.startswith('n') else file_id
        return f"{self.base_url}/bitrix/tools/disk/focus.php?objectId={clean_file_id}&cmd=show&action=showObjectInGrid&ncc=1"

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

    async def _request_form(self, method: str, api_method: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Выполняет асинхронный HTTP-запрос к Bitrix24 API с передачей данных как form data.
        Специально для пользовательских полей с массивами.
        
        :param method: HTTP метод ('GET', 'POST', etc.)
        :param api_method: Метод Bitrix24 API
        :param params: Параметры запроса
        """
        url = f"{self.webhook_url.rstrip('/')}/{api_method}"
        
        async with httpx.AsyncClient() as client:
            try:
                if method.upper() == 'POST':
                    # Отправляем данные как form data вместо JSON
                    response = await client.post(url, data=params)
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

    async def add_user_to_workgroup_as_moderator(self, group_id: int, user_id: int) -> bool:
        """
        Добавляет пользователя в рабочую группу как помощника руководителя (модератора).
        
        :param group_id: ID рабочей группы
        :param user_id: ID пользователя
        :return: True в случае успеха, иначе False
        """
        api_method = 'sonet_group.user.add'
        params = {
            'GROUP_ID': group_id,
            'USER_ID': user_id,
            'ROLE': 'M'  # M = Moderator (помощник руководителя), A = Admin, E = Employee
        }
        logger.info(f"Добавление пользователя {user_id} в группу {group_id} как помощника руководителя...")
        result = await self._request('POST', api_method, params)
        
        if result:
            logger.success(f"Пользователь {user_id} назначен помощником руководителя группы {group_id}.")
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
            'INITIATE_PERMS': 'E', # E - владелец группы и модераторы группы имеeт право на приглашение
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

    async def create_workgroup_with_features(self, group_data: Dict[str, Any], enabled_features: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """
        Создает рабочую группу с настройкой возможностей (фичей).
        
        Args:
            group_data: Данные группы для создания
            enabled_features: Список включенных возможностей ['tasks', 'files', 'calendar', 'chat', 'landing_knowledge']
            
        Returns:
            Словарь с данными созданной группы или None при ошибке
        """
        try:
            # Настройки возможностей по умолчанию (только нужные включены)
            default_features = ['tasks', 'files', 'calendar', 'chat', 'landing_knowledge']
            features_to_enable = enabled_features if enabled_features else default_features
            
            # Все доступные возможности из HTML формы
            all_features = {
                'tasks': 'Задачи',
                'calendar': 'Календарь', 
                'files': 'Диск',
                'chat': 'Чат',
                'forum': 'Обсуждения',
                'blog': 'Сообщения',
                'photo': 'Фотогалерея',
                'group_lists': 'Списки',
                'marketplace': 'Маркет',
                'landing_knowledge': 'База знаний',
                'search': 'Поиск'
            }
            
            # Подготавливаем данные с возможностями
            enhanced_data = group_data.copy()
            
            # Включаем только нужные возможности
            for feature_name in all_features.keys():
                param_name = f"{feature_name}_active"
                if feature_name in features_to_enable:
                    enhanced_data[param_name] = "Y"
                    logger.debug(f"✅ Включаем возможность: {all_features[feature_name]} ({feature_name})")
                else:
                    enhanced_data[param_name] = "N"
                    logger.debug(f"❌ Отключаем возможность: {all_features[feature_name]} ({feature_name})")
                
                # Добавляем пустое кастомное название (как в HTML форме)
                name_param = f"{feature_name}_name"
                enhanced_data[name_param] = ""
            
            # Поиск всегда включен (как hidden поле в HTML)
            enhanced_data['search_active'] = "Y"
            enhanced_data['search_name'] = ""
            
            logger.info(f"🎯 Создаем группу с {len(features_to_enable)} включенными возможностями...")
            logger.info(f"📋 Включенные возможности: {[all_features[f] for f in features_to_enable]}")
            
            # Создаем группу обычным методом
            result = await self.create_workgroup(enhanced_data)
            
            return result
                
        except Exception as e:
            logger.error(f"Ошибка создания группы с возможностями: {e}")
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
        logger.debug(f"Создание задачи '{title}' в Bitrix24...")
        result = await self._request('POST', api_method, params)
        if result and 'task' in result:
            task_id = result['task']['id']
            logger.debug(f"Задача '{title}' успешно создана с ID {task_id}.")
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
        logger.debug(f"Обновление задачи '{task_title}' (ID: {task_id}) в Bitrix24...")
        result = await self._request('POST', api_method, params)
        if result:
            logger.debug(f"Задача '{task_title}' (ID: {task_id}) успешно обновлена.")
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
            if isinstance(result, list):
                logger.success(f"Получено {len(result)} пользователей для группы {group_id}.")
                return result
            else:
                logger.warning(f"Неожиданный тип ответа для группы {group_id}: {type(result)}")
                return []
        return []

    async def find_user_by_email(self, email: str) -> Optional[BitrixUser]:
        """Найти пользователя по email"""
        try:
            response = await self._request('GET', 'user.get', {
                'filter': {'EMAIL': email}
            })
            
            if response and isinstance(response, list) and response:
                user_data = response[0]  # type: ignore  # Берем первого найденного
                return BitrixUser.model_validate(user_data)
            
            return None
            
        except Exception as e:
            logger.warning(f"Ошибка поиска пользователя по email {email}: {e}")
            return None

    async def create_user(self, user_data: dict) -> Optional[BitrixUser]:
        """Создать нового пользователя в Bitrix24"""
        try:
            response = await self._request('POST', 'user.add', user_data)
            
            if response:
                # response может быть ID (число) или объектом
                if isinstance(response, (int, str)):
                    user_id = int(response)
                    full_user = await self.get_user(user_id)
                    if full_user:
                        logger.success(f"Создан пользователь {user_data.get('EMAIL')} (ID: {user_id})")
                        return full_user
                elif isinstance(response, dict) and 'ID' in response:
                    user_id = int(response['ID'])
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
                email = user_data.get('EMAIL')
                if email:
                    existing_user = await self.find_user_by_email(email)
                    if existing_user:
                        logger.info(f"Найден существующий пользователь {existing_user.EMAIL} (ID: {existing_user.ID})")
                        return existing_user
                    else:
                        logger.error(f"Не удалось найти пользователя {email}")
            else:
                logger.error(f"Ошибка создания пользователя {user_data.get('EMAIL', 'unknown')}: {e}")
            
            return None

    async def update_user(self, user_id: Union[str, int], user_data: dict) -> Optional[BitrixUser]:
        """Обновить данные пользователя в Bitrix24"""
        try:
            # Добавляем ID пользователя к данным для обновления
            update_data = {'ID': str(user_id), **user_data}
            
            response = await self._request('POST', 'user.update', update_data)
            
            if response is True:
                # Получаем обновленную информацию о пользователе
                updated_user = await self.get_user(int(user_id))
                if updated_user:
                    return updated_user
            
            logger.error(f"Ошибка обновления пользователя ID {user_id}")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка обновления пользователя ID {user_id}: {e}")
            return None

    async def get_users(self, params: Optional[dict] = None) -> List[BitrixUser]:
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
                user_data = response[0]  # type: ignore
                return BitrixUser.model_validate(user_data)
            
            return None
            
        except Exception as e:
            logger.warning(f"Ошибка получения пользователя с ID {user_id}: {e}")
            return None

    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С СТАДИЯМИ ЗАДАЧ ==========
    
    async def get_task_stages(self, entity_id: int) -> Dict[str, Dict[str, Any]]:
        """
        Получает список стадий канбана для группы.
        
        :param entity_id: ID группы (рабочей группы)
        :return: Словарь стадий в формате {stage_id: stage_data}
        """
        api_method = 'task.stages.get'
        params = {
            'entityId': entity_id,
            'isAdmin': True  # Запрашиваем с правами администратора
        }
        logger.debug(f"Запрос стадий канбана для группы {entity_id}...")
        result = await self._request('GET', api_method, params)
        
        if result:
            # API возвращает словарь стадий в формате {"stage_id": {stage_data}}
            if isinstance(result, dict):
                logger.debug(f"Получено {len(result)} стадий для группы {entity_id}.")
                return result
            else:
                logger.warning(f"Неожиданный тип ответа для стадий группы {entity_id}: {type(result)}")
                return {}
        return {}

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
            },
            'isAdmin': True  # Создаем с правами администратора
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
            'fields': fields,
            'isAdmin': True  # Обновляем с правами администратора
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
        params = {
            'id': stage_id,
            'isAdmin': True  # Удаляем с правами администратора
        }
        logger.info(f"Удаление стадии {stage_id}...")
        result = await self._request('POST', api_method, params)
        if result:
            logger.success(f"Стадия {stage_id} успешно удалена.")
            return True
        return False

    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ЧЕК-ЛИСТАМИ ЗАДАЧ ==========
    
    async def create_checklist_group(self, task_id: int, title: str) -> Optional[int]:
        """
        Создает группу чек-листа с названием.
        
        :param task_id: ID задачи
        :param title: Название группы чек-листа
        :return: ID созданной группы или None
        """
        api_method = 'task.checklistitem.add'
        # Группа чек-листа создается с PARENT_ID = 0
        params = {
            'taskId': task_id,
            'fields': {
                'TITLE': title,
                'PARENT_ID': 0,  # 0 означает, что это группа (корневой элемент)
                'IS_COMPLETE': False,
                'SORT_INDEX': '10'
            }
        }
        
        logger.debug(f"Создание группы чек-листа '{title}' для задачи {task_id}...")
        result = await self._request('POST', api_method, params)
        if result:
            # result может быть числом или объектом
            if isinstance(result, (int, str)):
                group_id = int(result)
                logger.debug(f"Группа чек-листа '{title}' создана с ID {group_id}")
                return group_id
            elif isinstance(result, dict) and 'ID' in result:
                group_id = int(result['ID'])
                logger.debug(f"Группа чек-листа '{title}' создана с ID {group_id}")
                return group_id
            else:
                logger.warning(f"Неожиданный ответ при создании группы чек-листа: {result}")
                return None
        else:
            logger.warning(f"Не удалось создать группу чек-листа '{title}' для задачи {task_id}")
            return None

    async def add_checklist_item(self, task_id: int, title: str, is_complete: bool = False, 
                                parent_id: Optional[int] = None) -> Optional[int]:
        """
        Добавляет элемент в чек-лист задачи.
        
        :param task_id: ID задачи
        :param title: Текст элемента чек-листа
        :param is_complete: Выполнен ли элемент (по умолчанию False)
        :param parent_id: ID родительского элемента (для группы)
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
        
        if parent_id:
            params['fields']['PARENT_ID'] = parent_id
        
        logger.debug(f"Добавление элемента '{title}' в чек-лист задачи {task_id}...")
        result = await self._request('POST', api_method, params)
        if result:
            # result может быть числом или объектом
            if isinstance(result, (int, str)):
                item_id = int(result)
                logger.debug(f"Элемент чек-листа '{title}' создан с ID {item_id}")
                return item_id
            elif isinstance(result, dict) and 'ID' in result:
                item_id = int(result['ID'])
                logger.debug(f"Элемент чек-листа '{title}' создан с ID {item_id}")
                return item_id
            else:
                logger.warning(f"Неожиданный ответ при создании элемента чек-листа: {result}")
                return None
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
            if isinstance(result, list):
                logger.debug(f"Получено {len(result)} элементов чек-листов для задачи {task_id}")
                return result
            else:
                logger.warning(f"Неожиданный тип ответа для чек-листов задачи {task_id}: {type(result)}")
                return []
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
            errors_count = 0
            
            for item in items:
                item_id = item.get('ID') or item.get('id')
                if item_id:
                    try:
                        # Попытка удаления элемента
                        async with httpx.AsyncClient() as client:
                            url = f"{self.webhook_url.rstrip('/')}/task.checklistitem.delete"
                            params = {'itemId': int(item_id)}
                            response = await client.post(url, json=params)
                            
                            # Если удаление прошло успешно или элемент уже не существует
                            if response.status_code == 200:
                                result = response.json()
                                if result.get('result') or 'error' not in result:
                                    deleted_count += 1
                                else:
                                    # Игнорируем ошибки о несуществующих элементах
                                    errors_count += 1
                            else:
                                errors_count += 1
                                
                    except Exception as e:
                        errors_count += 1
                        # Игнорируем ошибки удаления - элементы могут уже не существовать
                        continue
            
            if errors_count > 0:
                logger.debug(f"При очистке возникло {errors_count} ошибок (возможно, элементы уже удалены)")
            
            logger.debug(f"Процесс очистки завершен для задачи {task_id}")
            return True
            
        except Exception as e:
            logger.warning(f"Ошибка очистки чек-листов задачи {task_id}: {e}")
            return False

    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С КОММЕНТАРИЯМИ ЗАДАЧ ==========
    
    async def add_task_comment(self, task_id: int, text: str, author_id: int, created_date: Optional[str] = None) -> Optional[int]:
        """
        Добавляет комментарий к задаче.
        
        :param task_id: ID задачи
        :param text: Текст комментария
        :param author_id: ID автора комментария в Bitrix24
        :param created_date: Дата создания комментария в формате ISO (опционально)
        :return: ID созданного комментария или None
        """
        api_method = 'task.commentitem.add'
        params = {
            'taskId': task_id,
            'fields': {
                'POST_MESSAGE': text,
                'AUTHOR_ID': author_id
            }
        }
        
        # Если указана дата создания, добавляем её
        if created_date:
            params['fields']['POST_DATE'] = created_date
        
        logger.debug(f"Добавление комментария к задаче {task_id} от пользователя {author_id}...")
        result = await self._request('POST', api_method, params)
        if result:
            # result может быть числом или объектом
            if isinstance(result, (int, str)):
                comment_id = int(result)
                logger.debug(f"Комментарий добавлен с ID {comment_id}")
                return comment_id
            elif isinstance(result, dict) and 'ID' in result:
                comment_id = int(result['ID'])
                logger.debug(f"Комментарий добавлен с ID {comment_id}")
                return comment_id
            else:
                logger.warning(f"Неожиданный ответ при создании комментария: {result}")
                return None
        else:
            logger.warning(f"Не удалось добавить комментарий к задаче {task_id}")
            return None

    async def get_task_comments(self, task_id: int) -> List[Dict[str, Any]]:
        """
        Получает комментарии задачи.
        
        :param task_id: ID задачи
        :return: Список комментариев задачи
        """
        api_method = 'task.commentitem.getlist'
        params = {'taskId': task_id}
        logger.debug(f"Запрос комментариев для задачи {task_id}...")
        result = await self._request('GET', api_method, params)
        if result:
            if isinstance(result, list):
                logger.debug(f"Получено {len(result)} комментариев для задачи {task_id}")
                return result
            else:
                logger.warning(f"Неожиданный тип ответа для комментариев задачи {task_id}: {type(result)}")
                return []
        return []

    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ФАЙЛАМИ ==========
    
    async def get_group_storage(self, group_id: int) -> Optional[int]:
        """
        Получает ID хранилища диска для указанной группы/проекта.
        
        :param group_id: ID группы в Bitrix24
        :return: ID хранилища группы или None
        """
        try:
            # Проверяем кэш
            if group_id in self._group_storage_cache:
                logger.debug(f"Хранилище группы {group_id} найдено в кэше: {self._group_storage_cache[group_id]}")
                return self._group_storage_cache[group_id]
            
            logger.debug(f"Поиск хранилища для группы {group_id}...")
            
            # Получаем список всех хранилищ
            storages = await self._request('GET', 'disk.storage.getlist')
            if not storages:
                logger.error("❌ Не удалось получить список хранилищ")
                return None
            
            # Убеждаемся что storages - это список
            if not isinstance(storages, list):
                logger.error(f"❌ Неожиданный тип списка хранилищ: {type(storages)}")
                return None
            
            # Ищем хранилище группы (ENTITY_TYPE = 'group')
            group_storage = None
            for storage in storages:
                if isinstance(storage, dict):
                    if (storage.get('ENTITY_TYPE') == 'group' and
                        int(storage.get('ENTITY_ID', 0)) == group_id):
                        group_storage = storage
                        break
            
            if not group_storage:
                logger.error(f"❌ Хранилище для группы {group_id} не найдено")
                # Выводим доступные хранилища групп для отладки
                group_storages = [s for s in storages if isinstance(s, dict) and s.get('ENTITY_TYPE') == 'group']
                if group_storages:
                    logger.debug(f"Доступные хранилища групп: {[(s.get('ENTITY_ID'), s.get('NAME')) for s in group_storages[:5]]}")
                return None
            
            storage_id = int(group_storage['ID'])  # type: ignore
            storage_name = group_storage.get('NAME', 'unknown')
            
            # Сохраняем в кэш
            self._group_storage_cache[group_id] = storage_id
            
            logger.success(f"✅ Найдено хранилище группы {group_id}: '{storage_name}' (ID: {storage_id})")
            return storage_id
            
        except Exception as e:
            logger.error(f"❌ Ошибка при поиске хранилища группы {group_id}: {e}")
            return None

    async def find_file_in_folder(self, folder_id: int, filename: str) -> Optional[str]:
        """
        Ищет файл по имени в указанной папке.
        
        :param folder_id: ID папки для поиска
        :param filename: Имя файла для поиска
        :return: ID найденного файла с префиксом 'n' или None
        """
        try:
            logger.debug(f"Поиск файла '{filename}' в папке {folder_id}...")
            
            # Получаем содержимое папки
            folder_children = await self._request('GET', 'disk.folder.getchildren', {'id': folder_id})
            
            if not folder_children:
                logger.debug(f"Папка {folder_id} пуста или недоступна")
                return None
            
            # Убеждаемся что folder_children - это список
            if not isinstance(folder_children, list):
                logger.warning(f"Неожиданный тип содержимого папки {folder_id}: {type(folder_children)}")
                return None
            
            # Ищем файл по имени (точное совпадение или с timestamp)
            for item in folder_children:
                if not isinstance(item, dict):
                    continue
                
                item_name = item.get('NAME', '')
                item_type = item.get('TYPE', '')
                item_id = item.get('ID', '')
                
                # Проверяем только файлы (не папки)
                if item_type == 'file' and item_id:
                    # Точное совпадение имени
                    if item_name == filename:
                        file_id_with_prefix = f"n{item_id}"
                        logger.debug(f"✅ Найден файл '{filename}' с ID {item_id} (для комментариев: {file_id_with_prefix})")
                        return file_id_with_prefix
                    
                    # Проверяем нормализованное имя (Bitrix24 может изменять символы)
                    import re
                    # Нормализуем исходное имя как это делает Bitrix24
                    # Экранированное подчеркивание \_ преобразуется в __
                    normalized_filename = filename.replace('\\_', '__')
                    if item_name == normalized_filename:
                        file_id_with_prefix = f"n{item_id}"
                        logger.debug(f"✅ Найден нормализованный файл '{item_name}' для '{filename}' с ID {item_id}")
                        return file_id_with_prefix
                    
                    # Проверяем файлы с timestamp (для случаев когда файл был переименован)
                    # Формат: original_name_timestamp.ext или modified_name_timestamp.ext
                    if filename.count('.') >= 1:  # Есть расширение
                        base_name, ext = filename.rsplit('.', 1)
                        
                        # Нормализуем имя файла (убираем проблемные символы для поиска)
                        normalized_base = re.sub(r'[\\/_]+', '_', base_name)
                        
                        # Ищем паттерн: normalized_base_[timestamp].ext
                        pattern1 = f"^{re.escape(normalized_base)}_\\d+\\.{re.escape(ext)}$"
                        if re.match(pattern1, item_name):
                            file_id_with_prefix = f"n{item_id}"
                            logger.debug(f"✅ Найден файл с timestamp '{item_name}' для '{filename}' с ID {item_id}")
                            return file_id_with_prefix
                        
                        # Дополнительно проверяем оригинальное имя
                        pattern2 = f"^{re.escape(base_name)}_\\d+\\.{re.escape(ext)}$"
                        if re.match(pattern2, item_name):
                            file_id_with_prefix = f"n{item_id}"
                            logger.debug(f"✅ Найден файл с timestamp '{item_name}' для '{filename}' с ID {item_id}")
                            return file_id_with_prefix
            
            logger.debug(f"Файл '{filename}' не найден в папке {folder_id}")
            return None
            
        except Exception as e:
            logger.warning(f"Ошибка поиска файла '{filename}' в папке {folder_id}: {e}")
            return None

    async def get_or_create_kaiten_folder(self, storage_id: int) -> Optional[int]:
        """
        Находит или создает служебную папку "Перенос из Kaiten" на указанном диске.
        
        :param storage_id: ID хранилища диска
        :return: ID папки "Перенос из Kaiten" или None
        """
        try:
            # Проверяем кэш
            if storage_id in self._group_folder_cache:
                logger.debug(f"Папка 'Перенос из Kaiten' найдена в кэше для хранилища {storage_id}: {self._group_folder_cache[storage_id]}")
                return self._group_folder_cache[storage_id]
            
            folder_name = "Перенос из Kaiten"
            
            # Получаем информацию о хранилище для получения ROOT_OBJECT_ID
            logger.debug(f"Поиск папки '{folder_name}' в хранилище {storage_id}...")
            storage_info = await self._request('GET', 'disk.storage.get', {'id': storage_id})
            
            if not storage_info or 'ROOT_OBJECT_ID' not in storage_info:
                logger.warning(f"⚠️ Не удалось получить информацию о хранилище {storage_id}")
                return None
            
            root_object_id = storage_info['ROOT_OBJECT_ID']
            logger.debug(f"ROOT_OBJECT_ID хранилища {storage_id}: {root_object_id}")
            
            # Получаем содержимое корневой папки
            storage_children = await self._request('GET', 'disk.folder.getchildren', {'id': root_object_id})
            
            # Ищем существующую папку
            if storage_children and isinstance(storage_children, list):
                for item in storage_children:
                    if isinstance(item, dict) and (item.get('TYPE') == 'folder' and 
                        item.get('NAME') == folder_name):
                        folder_id = item.get('ID')
                        logger.debug(f"✅ Найдена существующая папка '{folder_name}' с ID: {folder_id}")
                        
                        # Сохраняем в кэш
                        self._group_folder_cache[storage_id] = folder_id
                        return folder_id
            
            # Папка не найдена, создаем новую
            logger.info(f"📁 Создаем служебную папку '{folder_name}' в хранилище {storage_id}...")
            create_params = {
                'id': root_object_id,  # Используем ROOT_OBJECT_ID
                'data': {
                    'NAME': folder_name
                }
            }
            
            result = await self._request('POST', 'disk.folder.addsubfolder', create_params)
            
            if result and 'ID' in result:
                folder_id = result['ID']
                
                # Сохраняем в кэш
                self._group_folder_cache[storage_id] = folder_id
                
                logger.success(f"✅ Создана папка '{folder_name}' с ID: {folder_id}")
                return folder_id
            else:
                logger.error(f"❌ Не удалось создать папку '{folder_name}': {result}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка при работе с папкой '{folder_name}': {e}")
            return None

    async def get_or_create_task_folder(self, storage_id: int, task_id: int) -> Optional[int]:
        """
        Находит или создает папку задачи внутри "Перенос из Kaiten".
        
        :param storage_id: ID хранилища диска
        :param task_id: ID задачи Bitrix24
        :return: ID папки задачи или None
        """
        try:
            # Сначала получаем основную папку "Перенос из Kaiten"
            kaiten_folder_id = await self.get_or_create_kaiten_folder(storage_id)
            if not kaiten_folder_id:
                logger.error("❌ Не удалось получить папку 'Перенос из Kaiten'")
                return None
            
            task_folder_name = str(task_id)
            
            # Получаем содержимое папки "Перенос из Kaiten"
            kaiten_folder_children = await self._request('GET', 'disk.folder.getchildren', {'id': kaiten_folder_id})
            
            # Ищем существующую папку задачи
            if kaiten_folder_children and isinstance(kaiten_folder_children, list):
                for item in kaiten_folder_children:
                    if isinstance(item, dict) and (item.get('TYPE') == 'folder' and 
                        item.get('NAME') == task_folder_name):
                        task_folder_id = item.get('ID')
                        logger.debug(f"✅ Найдена папка задачи {task_id} с ID: {task_folder_id}")
                        return task_folder_id
            
            # Папка задачи не найдена, создаем новую
            logger.debug(f"📁 Создаем папку для задачи {task_id} в 'Перенос из Kaiten'...")
            create_params = {
                'id': kaiten_folder_id,
                'data': {
                    'NAME': task_folder_name
                }
            }
            
            result = await self._request('POST', 'disk.folder.addsubfolder', create_params)
            
            if result and 'ID' in result:
                task_folder_id = result['ID']
                logger.success(f"✅ Создана папка задачи {task_id} с ID: {task_folder_id}")
                return task_folder_id
            else:
                logger.error(f"❌ Не удалось создать папку задачи {task_id}: {result}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка при работе с папкой задачи {task_id}: {e}")
            return None

    async def upload_file(self, file_content: bytes, filename: str, group_id: int, task_id: Optional[int] = None) -> Optional[str]:
        """
        Загружает файл в Bitrix24 через disk.folder.uploadfile в служебную папку группы.
        Если файл уже существует, возвращает ID существующего файла.
        
        :param file_content: Содержимое файла в байтах
        :param filename: Имя файла
        :param group_id: ID группы в Bitrix24
        :param task_id: ID задачи Bitrix24 (опционально, для создания подпапки)
        :return: ID файла с префиксом 'n' или None
        """
        try:
            logger.debug(f"Проверяем/загружаем файл '{filename}' размером {len(file_content)} байт для группы {group_id}...")
            
            # Получаем хранилище группы
            storage_id = await self.get_group_storage(group_id)
            if not storage_id:
                logger.error(f"❌ Не удалось найти хранилище для группы {group_id}")
                return None
            
            # Определяем целевую папку в зависимости от наличия task_id
            if task_id:
                # Создаем папку задачи в "Перенос из Kaiten\{task_id}\"
                target_folder_id = await self.get_or_create_task_folder(storage_id, task_id)
                if not target_folder_id:
                    logger.error(f"❌ Не удалось получить/создать папку задачи {task_id}")
                    return None
                logger.debug(f"Используем папку задачи {task_id} (ID: {target_folder_id})")
            else:
                # Используем общую папку "Перенос из Kaiten"
                target_folder_id = await self.get_or_create_kaiten_folder(storage_id)
                if not target_folder_id:
                    logger.error("❌ Не удалось получить/создать папку 'Перенос из Kaiten'")
                    return None
                logger.debug(f"Используем общую папку 'Перенос из Kaiten' (ID: {target_folder_id})")
            
            # Сначала проверяем, существует ли уже такой файл
            existing_file_id = await self.find_file_in_folder(target_folder_id, filename)
            if existing_file_id:
                logger.debug(f"Файл '{filename}' уже существует в Bitrix24 с ID: {existing_file_id.replace('n', '')} (используем существующий)")
                return existing_file_id
            
            # Файла нет, загружаем новый
            import base64
            import time
            from pathlib import Path
            
            # Кодируем файл в base64 для API Bitrix24
            file_base64 = base64.b64encode(file_content).decode('utf-8')
            
            # Пробуем загрузить файл с исходным именем
            original_filename = filename
            
            for attempt in range(3):  # Максимум 3 попытки
                # Генерируем уникальное имя при необходимости
                if attempt > 0:
                    # Добавляем временную метку к имени файла
                    file_path = Path(original_filename)
                    timestamp = int(time.time())
                    unique_filename = f"{file_path.stem}_{timestamp}{file_path.suffix}"
                else:
                    unique_filename = original_filename
                
                # Загружаем файл в целевую папку
                upload_params = {
                    'id': target_folder_id,  # Используем целевую папку (общую или задачи)
                    'data': {
                        'NAME': unique_filename
                    },
                    'fileContent': file_base64
                }
                
                folder_path = f"Перенос из Kaiten\\{task_id}" if task_id else "Перенос из Kaiten"
                logger.debug(f"Попытка {attempt + 1}: загружаем файл '{unique_filename}' в папку '{folder_path}' группы {group_id} (ID: {target_folder_id})")
                result = await self._request('POST', 'disk.folder.uploadfile', upload_params)
                
                if result and 'ID' in result:
                    file_id = result['ID']
                    # Возвращаем ID с префиксом 'n' как требует API для комментариев
                    file_id_with_prefix = f"n{file_id}"
                    logger.debug(f"Файл '{unique_filename}' успешно загружен в группу {group_id} с ID: {file_id} (для комментариев: {file_id_with_prefix})")
                    return file_id_with_prefix
                else:
                    if attempt < 2:  # Не последняя попытка
                        logger.debug(f"   ⚠️ Не удалось загрузить с именем '{unique_filename}', пробуем с уникальным именем...")
                        continue
                    else:
                        logger.error(f"❌ Неожиданный ответ при загрузке файла после {attempt + 1} попыток: {result}")
                        return None
            
            return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки файла '{filename}' в группу {group_id}: {e}")
            return None

    async def add_task_comment_with_file(self, task_id: int, text: str, author_id: int, 
                                       file_id: Optional[str] = None, created_date: Optional[str] = None) -> Optional[int]:
        """
        Добавляет комментарий к задаче с прикрепленным файлом.
        
        :param task_id: ID задачи
        :param text: Текст комментария
        :param author_id: ID автора комментария в Bitrix24
        :param file_id: ID файла в Bitrix24 Drive с префиксом 'n' (опционально)
        :param created_date: Дата создания комментария в формате ISO (опционально)
        :return: ID созданного комментария или None
        """
        api_method = 'task.commentitem.add'
        params = {
            'taskId': task_id,
            'fields': {
                'POST_MESSAGE': text,
                'AUTHOR_ID': author_id
            }
        }
        
        # Добавляем файл, если он указан
        if file_id:
            # file_id уже содержит префикс 'n', добавляем его в массив
            params['fields']['UF_FORUM_MESSAGE_DOC'] = [file_id]
            logger.debug(f"Прикрепляем файл {file_id} к комментарию задачи {task_id}")
        
        # Дата создания комментария (API может игнорировать)
        if created_date:
            params['fields']['CREATED_DATE'] = created_date
        
        logger.debug(f"Создание комментария{'с файлом' if file_id else ''} для задачи {task_id}...")
        result = await self._request('POST', api_method, params)
        
        if result:
            # API может вернуть как число, так и объект
            if isinstance(result, (int, str)):
                comment_id = int(result)
            elif isinstance(result, dict) and 'result' in result:
                comment_id = int(result['result'])
            else:
                # Неожиданный тип ответа
                logger.warning(f"⚠️ Неожиданный ответ при создании комментария: {result}")
                return None
            
            if comment_id:
                logger.debug(f"✅ Комментарий создан с ID: {comment_id}")
                return comment_id
            else:
                logger.warning(f"⚠️ Неожиданный ответ при создании комментария: {result}")
                return None
        else:
            logger.error(f"❌ Не удалось создать комментарий для задачи {task_id}")
            return None

    async def download_file(self, file_id: str) -> Optional[bytes]:
        """
        Скачивает файл из Bitrix24 по его ID.
        
        Args:
            file_id: ID файла в Bitrix24 (может содержать префикс 'n')
            
        Returns:
            Содержимое файла в байтах или None при ошибке
        """
        try:
            # Убираем префикс 'n' если есть
            clean_file_id = file_id.replace('n', '') if file_id.startswith('n') else file_id
            
            # Получаем информацию о файле для получения download_url
            file_info = await self._request('GET', 'disk.file.get', {'id': clean_file_id})
            
            if not file_info or 'DOWNLOAD_URL' not in file_info:
                logger.error(f"❌ Не удалось получить download_url для файла {file_id}")
                return None
            
            download_url = file_info['DOWNLOAD_URL']
            file_name = file_info.get('NAME', 'unknown')
            
            logger.debug(f"Скачивание файла '{file_name}' по URL: {download_url}")
            
            # Скачиваем файл
            async with httpx.AsyncClient() as client:
                response = await client.get(download_url)
                response.raise_for_status()
                
                file_data = response.content
                logger.debug(f"Скачано {len(file_data)} байт")
                
                return file_data
                
        except Exception as e:
            logger.error(f"Ошибка скачивания файла {file_id}: {e}")
            return None

    async def remove_user_from_workgroup(self, group_id: int, user_id: int) -> bool:
        """
        Удаляет пользователя из рабочей группы.
        
        :param group_id: ID рабочей группы
        :param user_id: ID пользователя
        :return: True в случае успеха, иначе False
        """
        api_method = 'sonet_group.user.delete'
        params = {
            'GROUP_ID': group_id,
            'USER_ID': user_id
        }
        logger.info(f"Удаление пользователя {user_id} из группы {group_id} в Bitrix24...")
        result = await self._request('POST', api_method, params)
        
        if result:
            logger.success(f"Пользователь {user_id} успешно удален из группы {group_id}.")
            return True
        return False

    async def update_workgroup_user_role(self, group_id: int, user_id: int, role: str) -> bool:
        """
        Обновляет роль пользователя в рабочей группе.
        
        :param group_id: ID рабочей группы
        :param user_id: ID пользователя
        :param role: Роль пользователя (A = Admin, E = Employee/Moderator, K = Member)
        :return: True в случае успеха, иначе False
        """
        api_method = 'sonet_group.user.update'
        params = {
            'GROUP_ID': group_id,
            'USER_ID': user_id,
            'ROLE': role
        }
        logger.info(f"Обновление роли пользователя {user_id} в группе {group_id} на '{role}'...")
        result = await self._request('POST', api_method, params)
        
        if result:
            logger.success(f"Роль пользователя {user_id} в группе {group_id} изменена на '{role}'.")
            return True
        else:
            logger.error(f"Ошибка обновления роли пользователя {user_id} в группе {group_id}")
            return False

    async def get_workgroup_users_with_roles(self, group_id: int) -> Dict[str, List[str]]:
        """
        Получает список пользователей рабочей группы, сгруппированных по ролям.
        
        :param group_id: ID рабочей группы
        :return: Словарь с ролями и списками ID пользователей
        """
        api_method = 'sonet_group.user.get'
        params = {'ID': group_id}
        logger.info(f"Запрос пользователей с ролями для группы {group_id} из Bitrix24...")
        result = await self._request('GET', api_method, params)
        
        roles = {
            'owner': [],      # A = Administrator (владелец)
            'moderators': [], # M = Moderator (помощник руководителя)
            'members': []     # K = Member (обычный участник)
        }
        
        if result and isinstance(result, list):
            for user in result:
                if isinstance(user, dict):
                    user_id = user.get('USER_ID')
                    role = user.get('ROLE', 'K')  # По умолчанию обычный участник
                    
                    if user_id:
                        if role == 'A':
                            roles['owner'].append(str(user_id))
                        elif role == 'M':
                            roles['moderators'].append(str(user_id))
                        else:  # K или любая другая роль
                            roles['members'].append(str(user_id))
            
            logger.success(f"Получены участники группы {group_id}: владельцев={len(roles['owner'])}, помощников={len(roles['moderators'])}, участников={len(roles['members'])}")
        else:
            logger.warning(f"Не удалось получить участников группы {group_id}")
        
        return roles

    async def clear_workgroup_members(self, group_id: int) -> Dict[str, int]:
        """
        Удаляет всех участников из рабочей группы (кроме владельца).
        
        :param group_id: ID рабочей группы
        :return: Статистика: {"removed": count, "errors": count}
        """
        stats = {"removed": 0, "errors": 0}
        
        try:
            # Получаем текущих участников группы с ролями
            current_roles = await self.get_workgroup_users_with_roles(group_id)
            
            # Удаляем всех кроме владельцев (A = Administrator) 
            to_remove = current_roles['moderators'] + current_roles['members']
            logger.info(f"🔍 Текущие роли в группе {group_id}: владельцев={len(current_roles['owner'])}, помощников={len(current_roles['moderators'])}, участников={len(current_roles['members'])}")
            logger.info(f"🔍 Сохраняем владельцев: {current_roles['owner']}")
            logger.info(f"🗑️ Удаляем: помощников={len(current_roles['moderators'])}, участников={len(current_roles['members'])}")
            
            if to_remove:
                logger.info(f"🧹 Очищаем группу {group_id}: удаляем {len(to_remove)} участников...")
                
                for user_id in to_remove:
                    try:
                        success = await self.remove_user_from_workgroup(group_id, int(user_id))
                        if success:
                            stats["removed"] += 1
                        else:
                            logger.warning(f"⚠️ Не удалось удалить пользователя {user_id} из группы {group_id}")
                            stats["errors"] += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка удаления пользователя {user_id}: {e}")
                        stats["errors"] += 1
                
                logger.success(f"✅ Очистка группы {group_id} завершена: удалено={stats['removed']}, ошибок={stats['errors']}")
            else:
                logger.info(f"ℹ️ Группа {group_id} уже пуста (только владельцы)")
                
        except Exception as e:
            logger.error(f"💥 Ошибка очистки группы {group_id}: {e}")
            stats["errors"] += 1
        
        return stats

    async def set_workgroup_owner(self, group_id: int, user_id: int) -> bool:
        """
        Устанавливает нового владельца рабочей группы.
        
        :param group_id: ID рабочей группы
        :param user_id: ID нового владельца
        :return: True в случае успеха, иначе False
        """
        api_method = 'sonet_group.setowner'
        params = {
            'GROUP_ID': group_id,
            'USER_ID': user_id
        }
        logger.info(f"Смена владельца группы {group_id} на пользователя {user_id}...")
        result = await self._request('POST', api_method, params)
        
        if result:
            logger.success(f"Владелец группы {group_id} изменен на пользователя {user_id}.")
            return True
        else:
            logger.error(f"Ошибка смены владельца группы {group_id}")
            return False

    async def get_workgroup_detailed_info(self, group_id: int) -> Optional[Dict]:
        """
        Получает подробную информацию о рабочей группе включая фичи.
        
        Args:
            group_id: ID группы в Bitrix24
            
        Returns:
            Словарь с подробной информацией о группе или None при ошибке
        """
        try:
            api_method = "sonet_group.get"
            params = {
                "ID": group_id
            }
            
            logger.info(f"Запрос подробной информации о группе {group_id} из Bitrix24...")
            
            response = await self._request('GET', api_method, params)
            
            if response and 'result' in response:
                group_info = response['result']
                logger.success(f"Получена подробная информация о группе {group_id}")
                
                # Логируем все доступные поля для анализа
                logger.info(f"Доступные поля группы {group_id}: {list(group_info.keys())}")
                
                # Логируем фичи для анализа если есть
                if 'FEATURES' in group_info:
                    logger.info(f"Фичи группы {group_id}: {len(group_info['FEATURES'])} элементов")
                    for feature in group_info['FEATURES']:
                        logger.debug(f"  - {feature.get('name', 'Unknown')}: active={feature.get('active', False)}")
                else:
                    logger.warning(f"Фичи не найдены в информации о группе {group_id}")
                
                return group_info
            else:
                logger.error(f"Некорректный ответ при получении информации о группе {group_id}: {response}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка получения информации о группе {group_id}: {e}")
            return None

    async def get_task_custom_fields(self, task_id: int) -> Dict[str, Any]:
        """
        Получает пользовательские поля задачи через task.item.getdata.
        
        Args:
            task_id: ID задачи в Bitrix24
            
        Returns:
            Словарь с пользовательскими полями или пустой словарь
        """
        try:
            result = await self._request('POST', 'task.item.getdata', {'id': task_id})
            
            if result:
                # Фильтруем только UF_ поля
                custom_fields = {}
                for key, value in result.items():
                    if key.startswith('UF_') and value not in [False, None]:
                        custom_fields[key] = value
                return custom_fields
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Ошибка получения пользовательских полей задачи {task_id}: {e}")
            return {}

    async def set_task_custom_field(self, task_id: int, field_name: str, value: Any) -> bool:
        """
        Устанавливает значение пользовательского поля для задачи.
        ИСПРАВЛЕНО: Использует правильный формат для списочных полей Bitrix24.
        
        Args:
            task_id: ID задачи в Bitrix24
            field_name: Имя пользовательского поля (например, UF_KAITEN_PROJECT_365518)
            value: Значение поля (может быть строкой, числом, списком для множественных полей)
            
        Returns:
            True в случае успеха
        """
        try:
            # Получаем текущие значения поля
            current_fields = await self.get_task_custom_fields(task_id)
            current_value = current_fields.get(field_name, [])
            
            # Определяем новые значения
            if isinstance(value, list):
                new_values = value
            else:
                # Для одиночного значения проверяем, не добавлено ли оно уже
                if isinstance(current_value, list):
                    if value not in current_value:
                        new_values = current_value + [value]
                    else:
                        logger.debug(f"Значение {value} уже присутствует в поле {field_name}")
                        return True
                else:
                    new_values = [value] if not current_value else [current_value, value]
            
            # Формируем параметры для обновления задачи в правильном формате для массивов
            params = {'taskId': task_id}
            
            # Добавляем значения в формате fields[field_name][index] = value
            for i, val in enumerate(new_values):
                params[f'fields[{field_name}][{i}]'] = val
            
            # Для пользовательских полей используем специальный запрос без JSON
            result = await self._request_form('POST', 'tasks.task.update', params)
            
            if result is not None:
                logger.debug(f"Установлено значение поля {field_name} для задачи {task_id}: {new_values}")
                return True
            else:
                logger.warning(f"Не удалось установить поле {field_name} для задачи {task_id}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка установки поля {field_name} для задачи {task_id}: {e}")
            return False

    async def set_task_custom_fields(self, task_id: int, fields_data: Dict[str, Any]) -> bool:
        """
        Устанавливает несколько пользовательских полей для задачи одним запросом.
        ИСПРАВЛЕНО: Использует правильный формат для списочных полей Bitrix24.
        
        Args:
            task_id: ID задачи в Bitrix24
            fields_data: Словарь с данными полей {field_name: value}
            
        Returns:
            True в случае успеха
        """
        try:
            if not fields_data:
                return True
            
            # Получаем текущие значения всех полей
            current_fields = await self.get_task_custom_fields(task_id)
            
            # Формируем параметры для обновления задачи в правильном формате для массивов
            params = {'taskId': task_id}
            
            for field_name, value in fields_data.items():
                current_value = current_fields.get(field_name, [])
                
                # Определяем новые значения для каждого поля
                if isinstance(value, list):
                    new_values = value
                else:
                    # Для одиночного значения проверяем, не добавлено ли оно уже
                    if isinstance(current_value, list):
                        if value not in current_value:
                            new_values = current_value + [value]
                        else:
                            logger.debug(f"Значение {value} уже присутствует в поле {field_name}")
                            new_values = current_value  # Оставляем как есть
                    else:
                        new_values = [value] if not current_value else [current_value, value]
                
                # Добавляем значения в формате fields[field_name][index] = value
                for i, val in enumerate(new_values):
                    params[f'fields[{field_name}][{i}]'] = val
            
            # Для пользовательских полей используем специальный запрос без JSON
            result = await self._request_form('POST', 'tasks.task.update', params)
            
            if result is not None:
                logger.debug(f"Установлены пользовательские поля для задачи {task_id}: {list(fields_data.keys())}")
                return True
            else:
                logger.warning(f"Не удалось установить поля для задачи {task_id}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка установки полей для задачи {task_id}: {e}")
            return False

    async def get_user_fields_for_tasks(self) -> List[Dict[str, Any]]:
        """
        Получает список всех пользовательских полей для задач.
        
        Returns:
            Список пользовательских полей для сущности TASKS_TASK
        """
        try:
            # Получаем все пользовательские поля
            result = await self._request('GET', 'task.item.userfield.getlist')
            
            if result and isinstance(result, list):
                logger.debug(f"Получено {len(result)} пользовательских полей для задач")
                return result
            else:
                logger.warning("Не удалось получить пользовательские поля для задач")
                return []
                
        except Exception as e:
            logger.error(f"Ошибка получения пользовательских полей: {e}")
            return []

    async def find_user_field_by_xml_id(self, xml_id: str) -> Optional[Dict[str, Any]]:
        """
        Ищет пользовательское поле по XML_ID.
        
        Args:
            xml_id: XML_ID поля (ID из Kaiten)
            
        Returns:
            Информация о поле или None
        """
        try:
            fields = await self.get_user_fields_for_tasks()
            
            for field in fields:
                if field.get('XML_ID') == xml_id:
                    logger.debug(f"Найдено поле по XML_ID {xml_id}: {field.get('FIELD_NAME')}")
                    return field
            
            logger.debug(f"Поле с XML_ID {xml_id} не найдено")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка поиска поля по XML_ID {xml_id}: {e}")
            return None


