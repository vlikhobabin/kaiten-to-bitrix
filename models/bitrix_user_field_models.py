from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from datetime import datetime


@dataclass
class BitrixUserField:
    """
    Модель пользовательского поля Bitrix24 (таблица b_user_field).
    """
    id: Optional[int] = None
    entity_id: str = "TASKS_TASK"  # Для задач
    field_name: str = ""  # UF_PREFIX_NAME
    user_type_id: str = "enumeration"  # Для списков
    xml_id: Optional[str] = None  # ID поля из Kaiten
    sort: int = 100
    multiple: str = "N"  # Y/N
    mandatory: str = "N"  # Y/N  
    show_filter: str = "S"  # N/I/E/S
    show_in_list: str = "Y"  # Y/N
    edit_in_list: str = "Y"  # Y/N
    is_searchable: str = "Y"  # Y/N
    settings: Optional[str] = None  # PHP сериализованный массив
    
    def __post_init__(self):
        """Автоматически устанавливает настройки по умолчанию для списков."""
        if not self.settings and self.user_type_id == "enumeration":
            # Стандартные настройки для списков (PHP сериализация)
            self.settings = 'a:4:{s:7:"DISPLAY";s:4:"LIST";s:11:"LIST_HEIGHT";i:1;s:16:"CAPTION_NO_VALUE";s:0:"";s:13:"SHOW_NO_VALUE";s:1:"Y";}'


@dataclass  
class BitrixUserFieldEnum:
    """
    Модель значения пользовательского поля-списка (таблица b_user_field_enum).
    """
    id: Optional[int] = None
    user_field_id: int = 0
    value: str = ""
    is_default: str = "N"  # Y/N - значение по умолчанию (было def_)
    sort: int = 500
    xml_id: Optional[str] = None  # ID значения из Kaiten


@dataclass
class BitrixUserFieldLang:
    """
    Модель языкового описания пользовательского поля (таблица b_user_field_lang).
    """
    user_field_id: int = 0
    language_id: str = "ru"  # ru/en
    edit_form_label: str = ""
    list_column_label: str = ""
    list_filter_label: str = ""
    error_message: str = ""
    help_message: str = ""


@dataclass
class CustomFieldMapping:
    """
    Модель маппинга пользовательского поля между Kaiten и Bitrix.
    """
    kaiten_field_id: str
    kaiten_field_name: str
    bitrix_field_id: Optional[int] = None
    bitrix_field_name: str = ""
    field_type: str = "enumeration"
    created_at: Optional[datetime] = None
    
    # Маппинг значений: {kaiten_value_id: bitrix_enum_id}
    values_mapping: Dict[str, int] = None
    
    def __post_init__(self):
        if self.values_mapping is None:
            self.values_mapping = {}
        if self.created_at is None:
            self.created_at = datetime.now()


@dataclass
class CustomFieldsMigrationResult:
    """
    Результат миграции пользовательских полей.
    """
    total_fields: int = 0
    created_fields: int = 0
    updated_fields: int = 0
    total_values: int = 0
    created_values: int = 0
    updated_values: int = 0
    
    # Детальная информация
    field_mappings: List[CustomFieldMapping] = None
    errors: List[str] = None
    
    def __post_init__(self):
        if self.field_mappings is None:
            self.field_mappings = []
        if self.errors is None:
            self.errors = []
    
    @property
    def success_rate(self) -> float:
        """Процент успешно созданных полей."""
        if self.total_fields == 0:
            return 0.0
        return (self.created_fields / self.total_fields) * 100
    
    def add_error(self, error: str):
        """Добавляет ошибку в результат."""
        self.errors.append(error)
    
    def add_field_mapping(self, mapping: CustomFieldMapping):
        """Добавляет маппинг поля в результат."""
        self.field_mappings.append(mapping)


@dataclass
class BitrixCustomFieldsConfig:
    """
    Конфигурация для создания пользовательских полей в Bitrix.
    """
    # Параметры по умолчанию
    entity_id: str = "TASKS_TASK"
    field_prefix: str = "UF_KAITEN_"
    default_sort_increment: int = 100
    
    # Настройки создания
    create_lang_versions: bool = True
    default_languages: List[str] = None
    
    # Настройки маппинга
    mapping_cache_file: str = "mappings/custom_fields_mapping.json"
    
    def __post_init__(self):
        if self.default_languages is None:
            self.default_languages = ["ru", "en"] 