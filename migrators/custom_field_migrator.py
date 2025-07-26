"""
Migrator для пользовательских полей Kaiten -> Bitrix24.
Локально получает данные из Kaiten, отправляет на VPS для создания полей через SQL.
"""
import json
import time
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from connectors.kaiten_client import KaitenClient
from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class CustomFieldMigrator:
    """
    Migrator для создания пользовательских полей в Bitrix24.
    Использует двухэтапный процесс: локально получает данные, на VPS создает поля.
    """
    
    def __init__(self, kaiten_client: KaitenClient):
        self.kaiten_client = kaiten_client
        
        # Пути к файлам
        self.local_json_file = Path(__file__).parent.parent / "mappings" / "custom_fields_data.json"
        self.local_mapping_file = Path(__file__).parent.parent / "mappings" / "custom_fields_mapping.json" 
        self.vps_script_path = "/root/kaiten-vps-scripts/create_custom_fields_on_vps.py"
        self.vps_json_path = "/root/kaiten-to-bitrix/mappings/custom_fields_data.json"
        self.vps_mapping_path = "/root/kaiten-to-bitrix/mappings/custom_fields_mapping.json"
        
        # Файлы мониторинга на VPS
        self.vps_progress_log = "/root/kaiten-to-bitrix/logs/custom-fields-in-progress.log"
        self.vps_completed_log = "/root/kaiten-to-bitrix/logs/custom-fields-app.log"
    
    async def migrate_all_custom_fields(self) -> Dict[str, Any]:
        """
        Выполняет полную миграцию пользовательских полей.
        
        Returns:
            Результат миграции с маппингом полей
        """
        try:
            logger.info("🚀 Начинаем миграцию пользовательских полей")
            
            # Этап 1: Получаем данные из Kaiten API
            logger.info("📥 Этап 1: Получение данных из Kaiten...")
            kaiten_data = await self._fetch_kaiten_data()
            
            if not kaiten_data.get('fields'):
                logger.warning("⚠️ Нет пользовательских полей для миграции")
                return {'success': False, 'error': 'No fields found'}
            
            # Этап 2: Отправляем данные и скрипт на VPS
            logger.info("📤 Этап 2: Отправка данных на VPS...")
            upload_success = await self._upload_to_vps(kaiten_data)
            
            if not upload_success:
                return {'success': False, 'error': 'Failed to upload to VPS'}
            
            # Этап 3: Запускаем обработку на VPS
            logger.info("⚙️ Этап 3: Запуск обработки на VPS...")
            execution_success = await self._execute_on_vps()
            
            if not execution_success:
                return {'success': False, 'error': 'VPS execution failed'}
            
            # Этап 4: Ждем завершения и получаем результат
            logger.info("⏳ Этап 4: Ожидание завершения...")
            result = await self._wait_and_download_result()
            
            if result['success']:
                logger.success("✅ Миграция пользовательских полей завершена успешно!")
            else:
                logger.error(f"❌ Миграция завершилась с ошибкой: {result.get('error', 'Unknown')}")
            
            return result
            
        except Exception as e:
            logger.error(f"💥 Критическая ошибка миграции: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _fetch_kaiten_data(self) -> Dict[str, Any]:
        """
        Получает все данные пользовательских полей из Kaiten API.
        
        Returns:
            Словарь с полями и их значениями
        """
        try:
            # Получаем все пользовательские поля
            logger.debug("Получение списка пользовательских полей...")
            kaiten_fields = await self.kaiten_client.get_custom_properties()
            
            if not kaiten_fields:
                logger.warning("Пользовательские поля не найдены в Kaiten")
                return {'fields': {}, 'total_fields': 0}
            
            logger.info(f"Найдено {len(kaiten_fields)} пользовательских полей")
            
            # Получаем значения для каждого поля типа select
            fields_data = {}
            
            for field in kaiten_fields:
                field_id = str(field.get('id', ''))
                field_type = field.get('type', '')
                
                logger.debug(f"Обрабатываем поле '{field.get('name', 'N/A')}' (ID: {field_id}, тип: {field_type})")
                
                field_data = {
                    'field_info': field,
                    'values': []
                }
                
                # Получаем значения для списков
                if field_type in ['select', 'multi_select']:
                    logger.debug(f"Получение значений для поля {field_id}...")
                    values = await self.kaiten_client.get_custom_property_select_values(int(field_id))
                    field_data['values'] = values or []
                    logger.debug(f"Получено {len(field_data['values'])} значений")
                
                fields_data[field_id] = field_data
            
            # Сохраняем данные локально
            result_data = {
                'created_at': datetime.now().isoformat(),
                'total_fields': len(kaiten_fields),
                'fields': fields_data
            }
            
            # Создаем директорию если её нет
            self.local_json_file.parent.mkdir(exist_ok=True)
            
            with open(self.local_json_file, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2)
            
            logger.success(f"✅ Данные Kaiten сохранены локально: {len(kaiten_fields)} полей, {sum(len(data['values']) for data in fields_data.values())} значений")
            
            return result_data
            
        except Exception as e:
            logger.error(f"Ошибка получения данных из Kaiten: {e}")
            return {'fields': {}, 'total_fields': 0, 'error': str(e)}
    
    async def _upload_to_vps(self, kaiten_data: Dict[str, Any]) -> bool:
        """
        Отправляет JSON данные и скрипт на VPS.
        
        Args:
            kaiten_data: Данные полей из Kaiten
            
        Returns:
            True в случае успеха
        """
        try:
            if not settings.ssh_host or not settings.ssh_key_path:
                logger.error("❌ SSH настройки не настроены")
                return False
            
            # Создаем директории на VPS
            logger.debug("Создание директорий на VPS...")
            create_dirs_cmd = [
                "ssh", "-i", settings.ssh_key_path,
                f"{settings.ssh_user}@{settings.ssh_host}",
                "mkdir -p /root/kaiten-to-bitrix/mappings /root/kaiten-to-bitrix/logs /root/kaiten-to-bitrix/scripts"
            ]
            
            result = subprocess.run(create_dirs_cmd, capture_output=True, timeout=10)
            if result.returncode != 0:
                logger.warning(f"⚠️ Не удалось создать директории: {result.stderr.decode()}")
            
            # Отправляем JSON файл с данными
            logger.debug("Отправка JSON данных на VPS...")
            upload_cmd = [
                "scp", "-i", settings.ssh_key_path,
                str(self.local_json_file),
                f"{settings.ssh_user}@{settings.ssh_host}:{self.vps_json_path}"
            ]
            
            result = subprocess.run(upload_cmd, capture_output=True, timeout=30)
            if result.returncode != 0:
                logger.error(f"❌ Не удалось отправить JSON: {result.stderr.decode()}")
                return False
            
            # Отправляем скрипт обработки
            vps_script_local = Path(__file__).parent.parent / "scripts" / "create_custom_fields_on_vps.py"
            if vps_script_local.exists():
                logger.debug("Отправка скрипта обработки на VPS...")
                script_cmd = [
                    "scp", "-i", settings.ssh_key_path,
                    str(vps_script_local),
                    f"{settings.ssh_user}@{settings.ssh_host}:{self.vps_script_path}"
                ]
                
                result = subprocess.run(script_cmd, capture_output=True, timeout=30)
                if result.returncode != 0:
                    logger.warning(f"⚠️ Не удалось отправить скрипт: {result.stderr.decode()}")
            
            logger.success("✅ Данные успешно отправлены на VPS")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки на VPS: {e}")
            return False
    
    async def _execute_on_vps(self) -> bool:
        """
        Запускает скрипт обработки на VPS.
        
        Returns:
            True если запуск прошел успешно
        """
        try:
            if not settings.ssh_host or not settings.ssh_key_path:
                logger.error("❌ SSH настройки не настроены")
                return False
            
            # Удаляем старые лог файлы
            cleanup_cmd = [
                "ssh", "-i", settings.ssh_key_path,
                f"{settings.ssh_user}@{settings.ssh_host}",
                f"rm -f {self.vps_progress_log} {self.vps_completed_log}"
            ]
            
            subprocess.run(cleanup_cmd, capture_output=True, timeout=10)
            
            # Запускаем скрипт в фоне
            logger.debug("Запуск скрипта создания полей на VPS...")
            execute_cmd = [
                "ssh", "-i", settings.ssh_key_path,
                f"{settings.ssh_user}@{settings.ssh_host}",
                f"cd /root/kaiten-to-bitrix && python3 {self.vps_script_path} > /dev/null 2>&1 &"
            ]
            
            result = subprocess.run(execute_cmd, capture_output=True, timeout=15)
            
            if result.returncode == 0:
                logger.success("✅ Скрипт запущен на VPS")
                return True
            else:
                logger.error(f"❌ Не удалось запустить скрипт: {result.stderr.decode()}")
                return False
            
        except Exception as e:
            logger.error(f"Ошибка запуска на VPS: {e}")
            return False
    
    async def _wait_and_download_result(self) -> Dict[str, Any]:
        """
        Ждет завершения обработки на VPS и скачивает результат.
        
        Returns:
            Результат обработки
        """
        try:
            max_attempts = 5
            wait_interval = 10  # секунд
            
            for attempt in range(max_attempts):
                logger.info(f"⏳ Проверка завершения ({attempt + 1}/{max_attempts})...")
                
                # Проверяем наличие файла завершения
                check_cmd = [
                    "ssh", "-i", settings.ssh_key_path,
                    f"{settings.ssh_user}@{settings.ssh_host}",
                    f"test -f {self.vps_completed_log} && echo 'COMPLETED' || echo 'IN_PROGRESS'"
                ]
                
                result = subprocess.run(check_cmd, capture_output=True, timeout=10)
                
                if result.returncode == 0 and 'COMPLETED' in result.stdout.decode():
                    logger.success("✅ Обработка на VPS завершена!")
                    return await self._download_results()
                
                if attempt < max_attempts - 1:
                    logger.debug(f"💤 Ждем {wait_interval} секунд...")
                    time.sleep(wait_interval)
            
            # Таймаут - скачиваем лог ошибки
            logger.error("❌ Таймаут ожидания завершения VPS скрипта")
            return await self._download_error_log()
            
        except Exception as e:
            logger.error(f"Ошибка ожидания результата: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _download_results(self) -> Dict[str, Any]:
        """
        Скачивает результаты обработки с VPS.
        
        Returns:
            Результат с маппингом полей
        """
        try:
            # Скачиваем лог выполнения
            local_log = Path(__file__).parent.parent / "logs" / "custom_fields_vps.log"
            local_log.parent.mkdir(exist_ok=True)
            
            download_log_cmd = [
                "scp", "-i", settings.ssh_key_path,
                f"{settings.ssh_user}@{settings.ssh_host}:{self.vps_completed_log}",
                str(local_log)
            ]
            
            subprocess.run(download_log_cmd, capture_output=True, timeout=30)
            
            # Скачиваем обновленный маппинг
            download_mapping_cmd = [
                "scp", "-i", settings.ssh_key_path,
                f"{settings.ssh_user}@{settings.ssh_host}:{self.vps_mapping_path}",
                str(self.local_mapping_file)
            ]
            
            result = subprocess.run(download_mapping_cmd, capture_output=True, timeout=30)
            
            if result.returncode == 0:
                # Удаляем файлы с VPS
                cleanup_cmd = [
                    "ssh", "-i", settings.ssh_key_path,
                    f"{settings.ssh_user}@{settings.ssh_host}",
                    f"rm -f {self.vps_completed_log} {self.vps_mapping_path}"
                ]
                subprocess.run(cleanup_cmd, capture_output=True, timeout=10)
                
                # Читаем результат
                if self.local_mapping_file.exists():
                    with open(self.local_mapping_file, 'r', encoding='utf-8') as f:
                        mapping_data = json.load(f)
                    
                    logger.success(f"✅ Результат получен: {len(mapping_data.get('fields', {}))} полей обработано")
                    return {
                        'success': True,
                        'mapping': mapping_data,
                        'log_file': str(local_log)
                    }
                else:
                    return {'success': False, 'error': 'Mapping file not found'}
            else:
                logger.error(f"❌ Не удалось скачать результат: {result.stderr.decode()}")
                return {'success': False, 'error': 'Download failed'}
            
        except Exception as e:
            logger.error(f"Ошибка скачивания результата: {e}")
            return {'success': False, 'error': str(e)}
    
    async def _download_error_log(self) -> Dict[str, Any]:
        """
        Скачивает лог ошибки при неуспешном выполнении.
        
        Returns:
            Результат с информацией об ошибке
        """
        try:
            local_error_log = Path(__file__).parent.parent / "logs" / "custom_fields_error.log"
            local_error_log.parent.mkdir(exist_ok=True)
            
            # Пытаемся скачать лог выполнения
            download_cmd = [
                "scp", "-i", settings.ssh_key_path,
                f"{settings.ssh_user}@{settings.ssh_host}:{self.vps_progress_log}",
                str(local_error_log)
            ]
            
            subprocess.run(download_cmd, capture_output=True, timeout=30)
            
            # Удаляем файл с VPS
            cleanup_cmd = [
                "ssh", "-i", settings.ssh_key_path,
                f"{settings.ssh_user}@{settings.ssh_host}",
                f"rm -f {self.vps_progress_log}"
            ]
            subprocess.run(cleanup_cmd, capture_output=True, timeout=10)
            
            logger.error(f"❌ Лог ошибки сохранен: {local_error_log}")
            
            return {
                'success': False,
                'error': 'VPS script timeout or error',
                'error_log': str(local_error_log)
            }
            
        except Exception as e:
            logger.error(f"Ошибка скачивания лога ошибки: {e}")
            return {'success': False, 'error': f'Error log download failed: {e}'} 