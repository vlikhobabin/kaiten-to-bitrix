"""
Мигратор пространств Kaiten в группы Bitrix24.
Логика: 
1. НЕ переносим доски
2. Переносим только конечные пространства (без дочерних) или пространства 2-го уровня
3. Исключаем пространства из списка исключений
"""

import asyncio
import json
import subprocess
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from connectors.kaiten_client import KaitenClient
from connectors.bitrix_client import BitrixClient
from models.kaiten_models import KaitenSpace
from config.space_exclusions import is_space_excluded, get_excluded_spaces
from utils.logger import get_logger

logger = get_logger(__name__)

class SpaceMigrator:
    """
    Мигратор пространств из Kaiten в группы Bitrix24.
    Логика: переносим пространства, а не доски.
    """
    
    def __init__(self):
        self.kaiten_client = KaitenClient()
        self.bitrix_client = BitrixClient()
        self.user_mapping: Dict[str, str] = {}
        self.space_mapping: Dict[str, str] = {}
        self.spaces_hierarchy: Dict[str, KaitenSpace] = {}
        
        # Настройки для вызова удаленного скрипта возможностей
        self.enable_features_update = True  # По умолчанию включено
        self.ssh_config = self._load_ssh_config()

    def _load_ssh_config(self) -> Dict[str, str]:
        """
        Загружает SSH конфигурацию для вызова удаленного скрипта возможностей.
        """
        try:
            # Ищем файл конфигурации
            env_file = None
            for filename in ['.env', 'env.txt']:
                if os.path.exists(filename):
                    env_file = filename
                    break
            
            if not env_file:
                logger.warning("⚠️ Файл конфигурации SSH не найден. Автоматическая установка возможностей отключена.")
                return {}
            
            config = {}
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip().strip('"').strip("'")
            
            required_keys = ['SSH_HOST', 'SSH_USER', 'SSH_KEY_PATH_PUTTY']
            missing_keys = [key for key in required_keys if key not in config]
            
            if missing_keys:
                logger.warning(f"⚠️ Отсутствуют SSH параметры: {missing_keys}. Автоматическая установка возможностей отключена.")
                return {}
            
            return config
            
        except Exception as e:
            logger.warning(f"⚠️ Ошибка загрузки SSH конфигурации: {e}. Автоматическая установка возможностей отключена.")
            return {}

    async def set_group_features_via_ssh(self, group_id: int, features: Optional[List[str]] = None) -> bool:
        """
        Устанавливает возможности группы через вызов удаленного скрипта update_group_features.py.
        
        Args:
            group_id: ID группы в Bitrix24
            features: Список возможностей для установки (None = стандартные)
            
        Returns:
            True если установка прошла успешно
        """
        if not self.enable_features_update or not self.ssh_config:
            logger.debug(f"Автоматическая установка возможностей отключена для группы {group_id}")
            return True  # Не считаем ошибкой
        
        try:
            # Подготавливаем команду
            remote_script = "/root/update_group_features.py"
            
            if features:
                features_str = ",".join(features)
                ssh_command = f"python3 {remote_script} --update-group {group_id} --features {features_str}"
            else:
                ssh_command = f"python3 {remote_script} --update-group {group_id}"
            
            # Формируем команду plink
            plink_command = [
                "plink.exe",
                "-batch",
                "-i", self.ssh_config['SSH_KEY_PATH_PUTTY'],
                f"{self.ssh_config['SSH_USER']}@{self.ssh_config['SSH_HOST']}",
                ssh_command
            ]
            
            logger.debug(f"🔧 Установка возможностей для группы {group_id} через SSH...")
            
            # Выполняем команду
            result = subprocess.run(
                plink_command,
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8'
            )
            
            if result.returncode == 0:
                logger.info(f"✅ Возможности группы {group_id} установлены успешно")
                # Логируем вывод для отладки
                if result.stdout.strip():
                    logger.debug(f"SSH output: {result.stdout.strip()}")
                return True
            else:
                logger.warning(f"⚠️ Ошибка установки возможностей для группы {group_id}: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.warning(f"⚠️ Таймаут при установке возможностей для группы {group_id}")
            return False
        except FileNotFoundError:
            logger.warning(f"⚠️ plink.exe не найден. Убедитесь, что PuTTY установлен и добавлен в PATH")
            return False
        except Exception as e:
            logger.warning(f"⚠️ Ошибка вызова SSH для группы {group_id}: {e}")
            return False

    async def load_user_mapping(self) -> bool:
        """Загружает маппинг пользователей из файла"""
        try:
            mapping_file = Path(__file__).parent.parent / "mappings" / "user_mapping.json"
            
            if not mapping_file.exists():
                logger.error("❌ Не найден файл маппинга пользователей. Запустите сначала миграцию пользователей!")
                return False
            
            with open(mapping_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.user_mapping = data.get('mapping', {})
            
            logger.info(f"📥 Загружен маппинг пользователей: {len(self.user_mapping)} записей")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки маппинга пользователей: {e}")
            return False

    async def build_spaces_hierarchy(self) -> bool:
        """Строит полную иерархию пространств"""
        try:
            logger.info("📥 Получение иерархии пространств из Kaiten...")
            spaces = await self.kaiten_client.get_spaces()
            
            if not spaces:
                logger.error("❌ Не удалось получить пространства из Kaiten")
                return False
            
            # Создаем словарь пространств по UID для быстрого поиска
            for space in spaces:
                self.spaces_hierarchy[space.uid] = space
            
            logger.info(f"📊 Загружено {len(spaces)} пространств в иерархию")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка построения иерархии пространств: {e}")
            return False

    def get_root_spaces(self) -> List[KaitenSpace]:
        """Получает корневые пространства (без родителей)"""
        root_spaces = []
        for space in self.spaces_hierarchy.values():
            if not space.parent_entity_uid:
                root_spaces.append(space)
        return root_spaces

    def get_child_spaces(self, parent_space: KaitenSpace) -> List[KaitenSpace]:
        """Получает дочерние пространства для указанного родителя"""
        child_spaces = []
        for space in self.spaces_hierarchy.values():
            if space.parent_entity_uid == parent_space.uid:
                child_spaces.append(space)
        return child_spaces

    def is_space_in_excluded_tree(self, space: KaitenSpace) -> bool:
        """
        Проверяет, находится ли пространство в дереве исключенных пространств.
        Проходит вверх по иерархии до корня и проверяет каждый уровень.
        """
        current_space = space
        max_depth = 10  # Защита от циклов
        depth = 0
        
        while current_space and depth < max_depth:
            # Проверяем текущее пространство
            if is_space_excluded(current_space.title):
                return True
            
            # Идем к родителю
            if current_space.parent_entity_uid:
                current_space = self.spaces_hierarchy.get(current_space.parent_entity_uid)
                depth += 1
            else:
                break
                
        return False

    def get_space_level(self, space: KaitenSpace) -> int:
        """
        Определяет уровень пространства в иерархии (1 = корневое, 2 = дочернее корневого, и т.д.)
        """
        level = 1
        current_space = space
        max_depth = 10
        
        while current_space.parent_entity_uid and level < max_depth:
            current_space = self.spaces_hierarchy.get(current_space.parent_entity_uid)
            if current_space:
                level += 1
            else:
                break
        
        return level

    def determine_admin_source_space(self, space: KaitenSpace) -> Optional[KaitenSpace]:
        """
        Определяет пространство, из которого нужно брать администраторов для назначения руководителя группы.
        
        Args:
            space: Пространство, для которого создается группа
            
        Returns:
            Пространство-источник администраторов или None
        """
        level = self.get_space_level(space)
        
        if level == 1:
            # Корневое пространство - берем администраторов из него самого
            logger.debug(f"Корневое пространство '{space.title}' - берем администраторов из него самого")
            return space
        elif level == 2:
            # Пространство 2-го уровня - берем администраторов из родительского
            if space.parent_entity_uid:
                parent_space = self.spaces_hierarchy.get(space.parent_entity_uid)
                if parent_space:
                    logger.debug(f"Пространство 2-го уровня '{space.title}' - берем администраторов из родительского '{parent_space.title}'")
                    return parent_space
                else:
                    logger.warning(f"Родительское пространство для '{space.title}' не найдено в иерархии")
            else:
                logger.warning(f"Пространство '{space.title}' не имеет родителя, но определено как 2-го уровня")
        else:
            logger.warning(f"Пространство '{space.title}' уровня {level} не должно мигрироваться")
        
        return None

    async def get_space_administrators_bitrix_ids(self, space: KaitenSpace) -> Tuple[Optional[str], List[str]]:
        """
        Получает ID администраторов пространства в формате Bitrix24.
        
        Args:
            space: Пространство для получения администраторов
            
        Returns:
            Кортеж (owner_id, moderator_ids):
            - owner_id: ID первого администратора (руководитель группы) или None
            - moderator_ids: Список ID остальных администраторов (помощники руководителя)
        """
        try:
            # Определяем пространство-источник администраторов
            admin_source_space = self.determine_admin_source_space(space)
            if not admin_source_space:
                logger.warning(f"Не удалось определить источник администраторов для пространства '{space.title}'")
                return None, []
            
            # Получаем администраторов пространства
            administrators = await self.kaiten_client.get_space_administrators(admin_source_space.id)
            
            if not administrators:
                logger.warning(f"Администраторы не найдены в пространстве '{admin_source_space.title}'")
                return None, []
            
            # Преобразуем в ID Bitrix24
            admin_bitrix_ids = []
            for admin in administrators:
                kaiten_id = str(admin['id'])
                bitrix_id = self.user_mapping.get(kaiten_id)
                
                if bitrix_id:
                    admin_bitrix_ids.append(bitrix_id)
                    logger.debug(f"Администратор {admin['full_name']} (Kaiten: {kaiten_id}) -> Bitrix: {bitrix_id}")
                else:
                    logger.warning(f"Администратор {admin['full_name']} (Kaiten: {kaiten_id}) не найден в маппинге пользователей")
            
            if admin_bitrix_ids:
                owner_id = admin_bitrix_ids[0]  # Первый администратор = руководитель
                moderator_ids = admin_bitrix_ids[1:]  # Остальные = помощники
                
                logger.info(f"Для пространства '{space.title}' из '{admin_source_space.title}': руководитель={owner_id}, помощников={len(moderator_ids)}")
                return owner_id, moderator_ids
            else:
                logger.warning(f"Ни один администратор пространства '{admin_source_space.title}' не найден в маппинге пользователей")
                return None, []
                
        except Exception as e:
            logger.error(f"Ошибка получения администраторов для пространства '{space.title}': {e}")
            return None, []

    async def get_space_roles_bitrix_ids(self, space: KaitenSpace) -> Tuple[Optional[str], List[str]]:
        """
        Получает ID владельца и модераторов для группы с учетом администраторов дочернего пространства.
        
        Args:
            space: Пространство для получения ролей
            
        Returns:
            Кортеж (owner_id, moderator_ids):
            - owner_id: ID владельца группы (первый администратор соответствующего пространства)
            - moderator_ids: Список ID модераторов (остальные администраторы + администраторы дочернего)
        """
        try:
            level = self.get_space_level(space)
            all_moderator_ids = []
            owner_id = None
            
            if level == 1:
                # Корневое пространство - берем администраторов из него самого
                logger.debug(f"Корневое пространство '{space.title}' - определяем роли")
                admins = await self.kaiten_client.get_space_administrators(space.id)
                
                # Преобразуем в ID Bitrix24
                admin_bitrix_ids = []
                for admin in admins:
                    kaiten_id = str(admin['id'])
                    bitrix_id = self.user_mapping.get(kaiten_id)
                    if bitrix_id:
                        admin_bitrix_ids.append(bitrix_id)
                
                if admin_bitrix_ids:
                    owner_id = admin_bitrix_ids[0]  # Первый = владелец
                    all_moderator_ids = admin_bitrix_ids[1:]  # Остальные = модераторы
                    
            elif level == 2:
                # Пространство 2-го уровня - владелец из родительского, модераторы из обоих
                logger.debug(f"Пространство 2-го уровня '{space.title}' - определяем роли из родительского и дочернего")
                
                # 1. Получаем администраторов родительского пространства
                admin_source_space = self.determine_admin_source_space(space)
                if admin_source_space:
                    parent_admins = await self.kaiten_client.get_space_administrators(admin_source_space.id)
                    
                    parent_admin_bitrix_ids = []
                    for admin in parent_admins:
                        kaiten_id = str(admin['id'])
                        bitrix_id = self.user_mapping.get(kaiten_id)
                        if bitrix_id:
                            parent_admin_bitrix_ids.append(bitrix_id)
                    
                    if parent_admin_bitrix_ids:
                        owner_id = parent_admin_bitrix_ids[0]  # Первый администратор родительского = владелец
                        all_moderator_ids.extend(parent_admin_bitrix_ids[1:])  # Остальные = модераторы
                
                # 2. Получаем администраторов дочернего пространства (все становятся модераторами)
                child_admins = await self.kaiten_client.get_space_administrators(space.id)
                
                for admin in child_admins:
                    kaiten_id = str(admin['id'])
                    bitrix_id = self.user_mapping.get(kaiten_id)
                    if bitrix_id and bitrix_id != owner_id:  # Исключаем владельца, если он совпадает
                        all_moderator_ids.append(bitrix_id)
                        
                logger.info(f"Для пространства '{space.title}': владелец из родительского={owner_id}, модераторов={len(all_moderator_ids)} (родительские + дочерние администраторы)")
            
            else:
                logger.warning(f"Пространство '{space.title}' уровня {level} не должно мигрироваться")
                return None, []
            
            # Удаляем дубликаты из модераторов
            unique_moderator_ids = list(dict.fromkeys(all_moderator_ids))  # Сохраняет порядок
            
            logger.info(f"Роли для пространства '{space.title}': владелец={owner_id}, модераторов={len(unique_moderator_ids)}")
            return owner_id, unique_moderator_ids
                
        except Exception as e:
            logger.error(f"Ошибка определения ролей для пространства '{space.title}': {e}")
            return None, []

    def get_spaces_to_migrate(self) -> List[KaitenSpace]:
        """
        Определяет какие пространства нужно мигрировать согласно новой логике:
        1. Конечные пространства (без дочерних) любого уровня
        2. Пространства строго 2-го уровня (независимо от наличия дочерних)
        3. НЕ переносим: пространства 1-го уровня с дочерними, пространства глубже 2-го уровня
        4. Исключаем пространства из списка исключений
        """
        spaces_to_migrate = []
        
        logger.info("🔍 Анализ пространств для миграции...")
        logger.info(f"📋 Исключенные пространства: {get_excluded_spaces()}")
        
        for space in self.spaces_hierarchy.values():
            # Пропускаем пространства из исключенного дерева
            if self.is_space_in_excluded_tree(space):
                logger.debug(f"⏭️ Пропускаем пространство '{space.title}' (в исключенном дереве)")
                continue
            
            # Определяем уровень пространства
            level = self.get_space_level(space)
            
            # Получаем дочерние пространства
            child_spaces = self.get_child_spaces(space)
            
            # Логика отбора:
            if level == 1 and child_spaces:
                # Пространство 1-го уровня с дочерними - НЕ переносим
                logger.debug(f"⏭️ Пропускаем пространство 1-го уровня с дочерними: '{space.title}'")
                continue
            elif level == 2:
                # Пространство 2-го уровня - переносим всегда
                spaces_to_migrate.append(space)
                logger.debug(f"✅ Пространство 2-го уровня: '{space.title}'")
            elif level > 2:
                # Пространство глубже 2-го уровня - НЕ переносим
                logger.debug(f"⏭️ Пропускаем пространство {level}-го уровня: '{space.title}'")
                continue
            elif level == 1 and not child_spaces:
                # Конечное пространство 1-го уровня - переносим
                spaces_to_migrate.append(space)
                logger.debug(f"✅ Конечное пространство 1-го уровня: '{space.title}'")
            elif not child_spaces:
                # Любое другое конечное пространство - переносим
                spaces_to_migrate.append(space)
                logger.debug(f"✅ Конечное пространство {level}-го уровня: '{space.title}'")
        
        logger.info(f"📊 Найдено {len(spaces_to_migrate)} пространств для миграции")
        return spaces_to_migrate

    def build_space_path(self, space: KaitenSpace) -> str:
        """
        Строит полный иерархический путь для пространства.
        """
        path_parts = []
        current_space = space
        max_depth = 10
        depth = 0
        
        # Идем вверх по иерархии, собирая названия
        while current_space and depth < max_depth:
            path_parts.insert(0, current_space.title)
            
            # Ищем родительское пространство
            if current_space.parent_entity_uid:
                current_space = self.spaces_hierarchy.get(current_space.parent_entity_uid)
                depth += 1
            else:
                break
        
        return "/".join(path_parts)

    async def get_space_members_bitrix_ids(self, space_id: int) -> List[str]:
        """
        Получает ID участников пространства в формате Bitrix24.
        Для дочерних пространств (2-го уровня) объединяет участников родительского и дочернего пространств.
        """
        try:
            # Находим пространство в иерархии
            target_space = None
            for space in self.spaces_hierarchy.values():
                if space.id == space_id:
                    target_space = space
                    break
            
            if not target_space:
                logger.error(f"Пространство {space_id} не найдено в иерархии")
                return []
            
            # Определяем уровень пространства
            level = self.get_space_level(target_space)
            
            all_bitrix_ids = set()  # Используем set для исключения дубликатов
            
            if level == 2:
                # Для пространства 2-го уровня объединяем участников родительского и дочернего
                logger.info(f"🔗 Пространство 2-го уровня '{target_space.title}' - объединяем участников родительского и дочернего")
                
                # Получаем всех пользователей дочернего пространства с ролями
                child_users = await self.kaiten_client.get_space_users_with_roles(space_id)
                
                # Разделяем на администраторов и остальных
                child_admins = [user for user in child_users if user.get('space_role_id') == 3]
                child_others = [user for user in child_users if user.get('space_role_id') != 3]
                
                logger.info(f"👥 Пользователей дочернего пространства: {len(child_others)} (редакторы+участники) + {len(child_admins)} (администраторы)")
                
                # Добавляем всех пользователей дочернего пространства (включая администраторов)
                for user in child_users:
                    kaiten_id = str(user['id'])
                    bitrix_id = self.user_mapping.get(kaiten_id)
                    if bitrix_id:
                        all_bitrix_ids.add(bitrix_id)
                
                # Получаем всех пользователей родительского пространства (редакторы + участники)
                if target_space.parent_entity_uid:
                    parent_space = self.spaces_hierarchy.get(target_space.parent_entity_uid)
                    if parent_space:
                        # Получаем всех пользователей с ролями
                        parent_users = await self.kaiten_client.get_space_users_with_roles(parent_space.id)
                        
                        # Исключаем только администраторов (space_role_id == 3)
                        parent_members = [user for user in parent_users if user.get('space_role_id') != 3]
                        logger.info(f"👥 Пользователей родительского пространства '{parent_space.title}': {len(parent_members)} (исключены администраторы)")
                        
                        for member in parent_members:
                            kaiten_id = str(member['id'])
                            bitrix_id = self.user_mapping.get(kaiten_id)
                            if bitrix_id:
                                all_bitrix_ids.add(bitrix_id)
                    else:
                        logger.warning(f"Родительское пространство не найдено для {target_space.title}")
            else:
                # Для остальных пространств - получаем всех пользователей с ролями
                logger.info(f"📍 Пространство {level}-го уровня '{target_space.title}' - берем всех пользователей с ролями")
                space_users = await self.kaiten_client.get_space_users_with_roles(space_id)
                
                for user in space_users:
                    kaiten_id = str(user['id'])
                    bitrix_id = self.user_mapping.get(kaiten_id)
                    if bitrix_id:
                        all_bitrix_ids.add(bitrix_id)
                    else:
                        logger.warning(f"⚠️ Пользователь {user.get('full_name', 'Unknown')} (ID: {kaiten_id}) не найден в маппинге")
            
            result = list(all_bitrix_ids)
            logger.info(f"👥 Итого найдено {len(result)} уникальных участников для пространства {space_id}")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка получения участников пространства {space_id}: {e}")
            return []

    async def list_available_spaces(self, verbose: bool = False) -> bool:
        """
        Выводит список всех доступных пространств для миграции.
        """
        logger.info("📋 СПИСОК ДОСТУПНЫХ ПРОСТРАНСТВ ДЛЯ МИГРАЦИИ")
        logger.info("=" * 80)
        
        try:
            # Строим иерархию пространств
            if not await self.build_spaces_hierarchy():
                return False
            
            # Получаем пространства для миграции
            spaces_to_migrate = self.get_spaces_to_migrate()
            
            if not spaces_to_migrate:
                logger.warning("❌ Не найдено пространств для миграции")
                return False
            
            logger.info(f"🎯 Найдено {len(spaces_to_migrate)} пространств для миграции:")
            logger.info("")
            
            # Сортируем пространства по пути для удобства просмотра
            spaces_with_paths = [(space, self.build_space_path(space)) for space in spaces_to_migrate]
            spaces_with_paths.sort(key=lambda x: x[1])
            
            for i, (space, path) in enumerate(spaces_with_paths, 1):
                logger.info(f"{i:3d}. {space.id:8d} {path}")
            
            logger.info("=" * 80)
            logger.info("💡 Для миграции конкретного пространства используйте:")
            logger.info("   python scripts/space_migration.py --space-id <ID>")
            logger.info("")
            logger.info("💡 Для миграции первых N пространств используйте:")
            logger.info("   python scripts/space_migration.py --limit <N>")
            logger.info("")
            logger.info("💡 Для миграции всех доступных пространств используйте:")
            logger.info("   python scripts/space_migration.py")
            
            return True
            
        except Exception as e:
            logger.error(f"💥 Ошибка при получении списка пространств: {e}")
            return False

    async def migrate_spaces(self, limit: Optional[int] = None, space_id: Optional[int] = None) -> Dict:
        """
        Выполняет миграцию пространств из Kaiten в группы Bitrix24.
        
        Args:
            limit: Максимальное количество пространств для миграции (None = все)
            space_id: ID конкретного пространства для миграции (None = все пространства)
            
        Returns:
            Словарь со статистикой миграции
        """
        logger.info("🚀 НАЧИНАЕМ МИГРАЦИЮ ПРОСТРАНСТВ ИЗ KAITEN В BITRIX24")
        logger.info("🔄 ЛОГИКА: Переносим пространства (НЕ доски)")
        logger.info("=" * 80)
        
        # Проверка взаимоисключающих параметров
        if limit and space_id:
            logger.warning("⚠️ Параметры --limit и --space-id взаимоисключающие. Используется --space-id")
            limit = None
            
        if space_id:
            logger.info(f"🎯 Режим: миграция конкретного пространства ID {space_id}")
        elif limit:
            logger.info(f"🔢 Режим: миграция первых {limit} пространств")
        else:
            logger.info("🔄 Режим: миграция ВСЕХ подходящих пространств")
        
        stats = {
            "processed": 0,
            "created": 0,
            "updated": 0,
            "errors": 0,
            "spaces_migrated": 0,
            "members_added": 0,
            "members_removed": 0
        }
        
        try:
            # Загружаем маппинг пользователей
            if not await self.load_user_mapping():
                return stats
            
            # Строим иерархию пространств
            if not await self.build_spaces_hierarchy():
                return stats
            
            # Получаем пространства для миграции
            if space_id:
                # Режим конкретного пространства
                target_space = None
                for space in self.spaces_hierarchy.values():
                    if space.id == space_id:
                        target_space = space
                        break
                
                if not target_space:
                    logger.error(f"❌ Пространство с ID {space_id} не найдено в Kaiten!")
                    stats["errors"] += 1
                    return stats
                
                spaces_to_migrate = [target_space]
            else:
                # Режим автоматического определения пространств
                spaces_to_migrate = self.get_spaces_to_migrate()
            
            # Применяем лимит если указан
            if limit:
                spaces_to_migrate = spaces_to_migrate[:limit]
                logger.info(f"🔢 Ограничение: будет обработано {len(spaces_to_migrate)} пространств")
            
            # Получаем существующие группы из Bitrix24
            logger.info("📥 Получение существующих рабочих групп из Bitrix24...")
            existing_groups = await self.bitrix_client.get_workgroup_list()
            groups_map = {group['NAME']: group for group in existing_groups}
            logger.info(f"📊 Найдено {len(existing_groups)} существующих рабочих групп в Bitrix24")
            
            # Обрабатываем каждое пространство
            for i, space in enumerate(spaces_to_migrate, 1):
                try:
                    stats["processed"] += 1
                    
                    # Формируем название группы
                    group_name = self.build_space_path(space)
                    
                    logger.info(f"🔄 [{i}/{len(spaces_to_migrate)}] Обрабатываем пространство: '{group_name}'")
                    
                    # Получаем роли с учетом администраторов дочернего пространства
                    owner_id, moderator_ids = await self.get_space_roles_bitrix_ids(space)
                    
                    # Проверяем существует ли группа
                    if group_name in groups_map:
                        logger.info(f"♻️ Группа '{group_name}' уже существует, обновляем владельца и участников...")
                        group_id = str(groups_map[group_name]['ID'])
                        stats["updated"] += 1
                        
                        # Изменяем владельца группы на правильного администратора
                        if owner_id:
                            await self.bitrix_client.set_workgroup_owner(int(group_id), int(owner_id))
                        
                        # Обновляем возможности группы до наших стандартов
                        logger.info(f"🎯 Обновляем возможности группы '{group_name}' до стандартного набора...")
                        enabled_features = ['tasks', 'files', 'calendar', 'chat', 'landing_knowledge', 'search']
                        
                        # Попытка через API (может не сработать из-за ограничений webhook)
                        api_features_updated = await self.bitrix_client.update_workgroup_features(int(group_id), enabled_features)
                        
                        # Установка возможностей через прямой доступ к БД (основной способ)
                        ssh_features_updated = await self.set_group_features_via_ssh(int(group_id), enabled_features)
                        
                        if ssh_features_updated:
                            logger.info(f"✅ Возможности группы обновлены через БД: Задачи, Диск, Календарь, Чат, База знаний")
                        elif api_features_updated:
                            logger.info(f"✅ Возможности группы обновлены через API: Задачи, Диск, Календарь, Чат, База знаний")
                        else:
                            logger.warning(f"⚠️ Не удалось обновить возможности группы '{group_name}'. Установите их вручную.")
                        
                        # Очищаем всех участников существующей группы (кроме владельца)
                        clear_stats = await self.bitrix_client.clear_workgroup_members(int(group_id))
                        stats["members_removed"] += clear_stats["removed"]
                        if clear_stats["errors"] > 0:
                            stats["errors"] += clear_stats["errors"]
                        
                    else:
                        # Создаем новую группу
                        logger.info(f"➕ Создание новой группы '{group_name}'...")
                        
                        group_data = {
                            'NAME': group_name,
                            'DESCRIPTION': f"Пространство из Kaiten: {space.title}",
                            'VISIBLE': 'Y',
                            'OPENED': 'N',  # N - закрытая группа (по приглашению)
                            'PROJECT': 'N'  # N - обычная группа, не проект (чтобы не включались все возможности автоматически)
                        }
                        
                        # Назначаем руководителя группы, если найден
                        if owner_id:
                            group_data['OWNER_ID'] = owner_id
                            logger.info(f"👑 Назначаем руководителя группы: пользователь {owner_id}")
                        
                        # Создаем группу без возможностей (установим их потом через БД)
                        group_result = await self.bitrix_client.create_workgroup(group_data)
                        
                        if group_result:
                            # Извлекаем ID из результата
                            if isinstance(group_result, dict) and 'ID' in group_result:
                                group_id = str(group_result['ID'])
                            else:
                                group_id = str(group_result)
                            
                            logger.success(f"✅ Создана группа '{group_name}' с ID: {group_id}")
                            
                            # Устанавливаем возможности для новой группы
                            enabled_features = ['tasks', 'files', 'calendar', 'chat', 'landing_knowledge', 'search']
                            ssh_features_updated = await self.set_group_features_via_ssh(int(group_id), enabled_features)
                            
                            if ssh_features_updated:
                                logger.info(f"🎯 Возможности установлены: Задачи, Диск, Календарь, Чат, База знаний")
                            else:
                                logger.warning(f"⚠️ Не удалось автоматически установить возможности для группы '{group_name}'. Установите их вручную.")
                            
                            stats["created"] += 1
                            groups_map[group_name] = {'ID': group_id, 'NAME': group_name}
                        else:
                            logger.error(f"❌ Ошибка создания группы '{group_name}'")
                            stats["errors"] += 1
                            continue
                    
                    # Сохраняем маппинг пространства -> группы
                    self.space_mapping[str(space.id)] = str(group_id)
                    stats["spaces_migrated"] += 1
                    
                    # Получаем участников пространства и добавляем их в группу
                    space_members = await self.get_space_members_bitrix_ids(space.id)
                    if space_members:
                        # Исключаем администраторов из списка обычных участников
                        admin_ids = []
                        if owner_id:
                            admin_ids.append(owner_id)
                        admin_ids.extend(moderator_ids)
                        
                        regular_members = [user_id for user_id in space_members if user_id not in admin_ids]
                        
                        logger.info(f"👥 Всего участников пространства: {len(space_members)}")
                        logger.info(f"👑 Владелец группы: {owner_id}")
                        logger.info(f"👔 Модераторов: {len(moderator_ids)}")
                        logger.info(f"👥 Обычных участников: {len(regular_members)}")
                        
                        # Добавляем всех участников в группу с правильными ролями
                        add_stats = await self.add_members_to_group(
                            group_id, space, owner_id, moderator_ids, regular_members
                        )
                        
                        # Обновляем общую статистику
                        stats["members_added"] += add_stats["added"]
                        if add_stats["errors"] > 0:
                            stats["errors"] += add_stats["errors"]
                    else:
                        logger.warning(f"⚠️ Нет участников для добавления в группу '{group_name}'")
                    
                except Exception as e:
                    logger.error(f"💥 Ошибка обработки пространства '{space.title}': {e}")
                    stats["errors"] += 1
            
            # Сохраняем маппинг пространств
            await self._save_space_mapping(stats)
            
            # Выводим финальный отчет
            await self._print_final_report(stats)
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка миграции пространств: {e}")
            stats["errors"] += 1
        
        return stats

    async def add_members_to_group(self, group_id: str, space: KaitenSpace, 
                                  owner_id: Optional[str], moderator_ids: List[str], 
                                  member_ids: List[str]) -> Dict[str, int]:
        """
        Добавляет участников в группу с правильными ролями.
        
        Args:
            group_id: ID группы в Bitrix24
            space: Пространство Kaiten
            owner_id: ID владельца группы (уже назначен)
            moderator_ids: Список ID модераторов (администраторы кроме первого)
            member_ids: Список ID обычных участников
            
        Returns:
            Статистика операций: {"added": count, "errors": count}
        """
        stats = {"added": 0, "errors": 0}
        
        try:
            # 1. Добавляем модераторов (администраторы кроме владельца)
            if moderator_ids:
                logger.info(f"👔 Добавляем {len(moderator_ids)} модераторов...")
                for moderator_id in moderator_ids:
                    try:
                        # Сначала добавляем как обычного участника
                        success = await self.bitrix_client.add_user_to_workgroup(int(group_id), int(moderator_id))
                        if success:
                            # Затем меняем роль на модератора (E)
                            role_success = await self.bitrix_client.update_workgroup_user_role(int(group_id), int(moderator_id), 'E')
                            if role_success:
                                stats["added"] += 1
                                logger.info(f"✅ Пользователь {moderator_id} добавлен как модератор")
                            else:
                                logger.warning(f"⚠️ Пользователь {moderator_id} добавлен, но не удалось назначить роль модератора")
                                stats["added"] += 1  # Все равно считаем как добавленного
                        else:
                            logger.warning(f"⚠️ Не удалось добавить модератора {moderator_id}")
                            stats["errors"] += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка добавления модератора {moderator_id}: {e}")
                        stats["errors"] += 1
            
            # 2. Добавляем обычных участников
            if member_ids:
                logger.info(f"👥 Добавляем {len(member_ids)} обычных участников...")
                for member_id in member_ids:
                    try:
                        success = await self.bitrix_client.add_user_to_workgroup(int(group_id), int(member_id))
                        if success:
                            stats["added"] += 1
                        else:
                            logger.warning(f"⚠️ Не удалось добавить участника {member_id}")
                            stats["errors"] += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка добавления участника {member_id}: {e}")
                        stats["errors"] += 1
            
            total_target = len(moderator_ids) + len(member_ids) + (1 if owner_id else 0)
            logger.success(f"✅ Добавлено участников в группу '{space.title}': {stats['added']} из {total_target} (владелец: {1 if owner_id else 0}, модераторы: {len(moderator_ids)}, участники: {len(member_ids)})")
            
        except Exception as e:
            logger.error(f"💥 Ошибка добавления участников в группу {group_id}: {e}")
            stats["errors"] += 1
        
        return stats

    async def _save_space_mapping(self, stats: Dict):
        """Сохраняет/обновляет маппинг пространств в файл"""
        mapping_file = Path(__file__).parent.parent / "mappings" / "space_mapping.json"
        mapping_file.parent.mkdir(exist_ok=True)
        
        # Если файл существует, загружаем и объединяем данные
        existing_mapping = {}
        existing_stats = {"processed": 0, "created": 0, "updated": 0, "errors": 0, "spaces_migrated": 0, "members_added": 0, "members_removed": 0}
        
        if mapping_file.exists():
            try:
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                    existing_mapping = existing_data.get("mapping", {})
                    existing_stats = existing_data.get("stats", existing_stats)
                logger.info(f"📂 Загружен существующий маппинг пространств: {len(existing_mapping)} записей")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка загрузки существующего маппинга пространств: {e}")
        
        # Объединяем маппинги (новые данные имеют приоритет)
        combined_mapping = {**existing_mapping, **self.space_mapping}
        
        # Объединяем статистику
        combined_stats = {}
        for key in existing_stats.keys():
            combined_stats[key] = existing_stats.get(key, 0) + stats.get(key, 0)
        
        mapping_data = {
            "created_at": datetime.now().isoformat(),
            "description": "Маппинг ID пространств Kaiten -> рабочих групп Bitrix24",
            "migration_logic": "Переносим пространства, НЕ доски. Конечные пространства или 2-й уровень.",
            "excluded_spaces": get_excluded_spaces(),
            "stats": combined_stats,
            "mapping": combined_mapping
        }
        
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Маппинг пространств сохранен/обновлен в файл: {mapping_file}")

    async def _print_final_report(self, stats: Dict):
        """Выводит финальный отчет миграции"""
        logger.info("🎉 МИГРАЦИЯ ПРОСТРАНСТВ ЗАВЕРШЕНА")
        logger.info("=" * 80)
        
        logger.info("📋 КРАТКАЯ СВОДКА:")
        logger.info(f"  ✅ Обработано пространств: {stats['processed']}")
        logger.info(f"  ➕ Создано групп: {stats['created']}")
        logger.info(f"  🔄 Обновлено групп: {stats['updated']}")
        logger.info(f"  📋 Пространств мигрировано: {stats['spaces_migrated']}")
        logger.info(f"  👥 Участников добавлено: {stats['members_added']}")
        logger.info(f"  🗑️ Участников удалено: {stats['members_removed']}")
        logger.info(f"  ❌ Ошибок: {stats['errors']}")
        logger.info("=" * 80)
        
        if stats["errors"] > 0:
            logger.error("❌ Миграция пространств завершена с ошибками")
        else:
            logger.success("✅ Миграция пространств завершена успешно!") 