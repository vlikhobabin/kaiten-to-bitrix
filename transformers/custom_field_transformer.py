"""
Transformer для преобразования пользовательских полей из Kaiten в Bitrix24.
"""
import re
from typing import Dict, Any, List, Optional
from datetime import datetime

from transformers.base_transformer import BaseTransformer
from models.bitrix_user_field_models import (
    BitrixUserField, BitrixUserFieldEnum, BitrixUserFieldLang,
    CustomFieldMapping, BitrixCustomFieldsConfig
)
from utils.logger import get_logger

logger = get_logger(__name__)


class CustomFieldTransformer(BaseTransformer):
    """
    Трансформер для пользовательских полей Kaiten -> Bitrix24.
    """
    
    def __init__(self, config: Optional[BitrixCustomFieldsConfig] = None):
        super().__init__()
        self.config = config or BitrixCustomFieldsConfig()
    
    def kaiten_to_bitrix_field(self, kaiten_field: Dict[str, Any], sort: int = 100) -> BitrixUserField:
        """
        Преобразует поле Kaiten в модель пользовательского поля Bitrix.
        
        Args:
            kaiten_field: Данные поля из Kaiten API
            sort: Порядок сортировки для поля
            
        Returns:
            Модель пользовательского поля Bitrix
        """
        kaiten_id = str(kaiten_field.get('id', ''))
        kaiten_name = kaiten_field.get('name', f'Поле_{kaiten_id}')
        
        # Создаем имя поля для Bitrix
        field_name = self._generate_field_name(kaiten_name, kaiten_id)
        
        # Определяем тип поля
        field_type = self._map_field_type(kaiten_field.get('type', 'select'))
        
        bitrix_field = BitrixUserField(
            entity_id=self.config.entity_id,
            field_name=field_name,
            user_type_id=field_type,
            xml_id=kaiten_id,
            sort=sort,
            multiple=self._is_multiple_field(kaiten_field),
            mandatory="N",  # По умолчанию необязательные
            show_filter="S",  # Показывать в расширенном фильтре
            show_in_list="Y",
            edit_in_list="Y", 
            is_searchable="Y"
        )
        
        logger.debug(f"Преобразовано поле Kaiten {kaiten_id} -> Bitrix {field_name}")
        return bitrix_field
    
    def kaiten_to_bitrix_field_values(self, kaiten_values: List[Dict[str, Any]], 
                                     bitrix_field_id: int) -> List[BitrixUserFieldEnum]:
        """
        Преобразует значения поля Kaiten в модели значений Bitrix.
        
        Args:
            kaiten_values: Список значений из Kaiten API
            bitrix_field_id: ID созданного поля в Bitrix
            
        Returns:
            Список моделей значений Bitrix
        """
        bitrix_values = []
        
        for i, kaiten_value in enumerate(kaiten_values):
            kaiten_value_id = str(kaiten_value.get('id', ''))
            value_text = kaiten_value.get('value', f'Значение_{kaiten_value_id}')
            
            bitrix_value = BitrixUserFieldEnum(
                user_field_id=bitrix_field_id,
                value=value_text,
                def_="N",  # По умолчанию не выбрано
                sort=(i + 1) * 100,  # Сортировка 100, 200, 300...
                xml_id=kaiten_value_id
            )
            
            bitrix_values.append(bitrix_value)
        
        logger.debug(f"Преобразовано {len(bitrix_values)} значений для поля {bitrix_field_id}")
        return bitrix_values
    
    def kaiten_to_bitrix_field_lang(self, kaiten_field: Dict[str, Any], 
                                   bitrix_field_id: int) -> List[BitrixUserFieldLang]:
        """
        Создает языковые версии для пользовательского поля.
        
        Args:
            kaiten_field: Данные поля из Kaiten
            bitrix_field_id: ID созданного поля в Bitrix
            
        Returns:
            Список языковых версий
        """
        lang_versions = []
        field_name = kaiten_field.get('name', 'Пользовательское поле')
        
        for lang_id in self.config.default_languages:
            # Переводим название для английского языка (простейший вариант)
            if lang_id == "en":
                translated_name = self._translate_to_english(field_name)
            else:
                translated_name = field_name
            
            lang_version = BitrixUserFieldLang(
                user_field_id=bitrix_field_id,
                language_id=lang_id,
                edit_form_label=translated_name,
                list_column_label="",
                list_filter_label="",
                error_message="",
                help_message=""
            )
            
            lang_versions.append(lang_version)
        
        return lang_versions
    
    def create_field_mapping(self, kaiten_field: Dict[str, Any], 
                           bitrix_field: BitrixUserField,
                           kaiten_values: List[Dict[str, Any]] = None,
                           bitrix_values: List[BitrixUserFieldEnum] = None) -> CustomFieldMapping:
        """
        Создает маппинг между полем Kaiten и полем Bitrix.
        
        Args:
            kaiten_field: Поле Kaiten
            bitrix_field: Поле Bitrix
            kaiten_values: Значения Kaiten (опционально)
            bitrix_values: Значения Bitrix (опционально)
            
        Returns:
            Маппинг полей
        """
        mapping = CustomFieldMapping(
            kaiten_field_id=str(kaiten_field.get('id', '')),
            kaiten_field_name=kaiten_field.get('name', ''),
            bitrix_field_id=bitrix_field.id,
            bitrix_field_name=bitrix_field.field_name,
            field_type=bitrix_field.user_type_id
        )
        
        # Создаем маппинг значений если предоставлены
        if kaiten_values and bitrix_values:
            # Создаем словарь для быстрого поиска по XML_ID
            bitrix_values_by_xml = {v.xml_id: v.id for v in bitrix_values if v.xml_id}
            
            for kaiten_val in kaiten_values:
                kaiten_val_id = str(kaiten_val.get('id', ''))
                if kaiten_val_id in bitrix_values_by_xml:
                    mapping.values_mapping[kaiten_val_id] = bitrix_values_by_xml[kaiten_val_id]
        
        return mapping
    
    def _generate_field_name(self, kaiten_name: str, kaiten_id: str) -> str:
        """
        Генерирует имя поля для Bitrix на основе названия из Kaiten.
        
        Args:
            kaiten_name: Название поля в Kaiten
            kaiten_id: ID поля в Kaiten
            
        Returns:
            Имя поля для Bitrix (UF_KAITEN_...)
        """
        # Очищаем название от спецсимволов и приводим к ASCII
        clean_name = re.sub(r'[^a-zA-Zа-яА-Я0-9_]', '_', kaiten_name)
        
        # Транслитерация русских букв
        translit_map = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
            'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
            'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
            'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
            'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
        }
        
        transliterated = ''
        for char in clean_name.lower():
            transliterated += translit_map.get(char, char)
        
        # Ограничиваем длину и добавляем префикс
        max_name_length = 50 - len(self.config.field_prefix) - len(kaiten_id) - 1
        short_name = transliterated[:max_name_length].rstrip('_')
        
        return f"{self.config.field_prefix}{short_name}_{kaiten_id}".upper()
    
    def _map_field_type(self, kaiten_type: str) -> str:
        """
        Маппинг типов полей Kaiten в типы Bitrix.
        
        Args:
            kaiten_type: Тип поля в Kaiten
            
        Returns:
            Тип поля в Bitrix
        """
        type_mapping = {
            'select': 'enumeration',
            'multi_select': 'enumeration',  # Для мульти-селекта тоже enumeration
            'text': 'string',
            'number': 'integer',
            'date': 'date',
            'datetime': 'datetime',
            'boolean': 'boolean',
            'file': 'file'
        }
        
        return type_mapping.get(kaiten_type, 'enumeration')
    
    def _is_multiple_field(self, kaiten_field: Dict[str, Any]) -> str:
        """
        Определяет, является ли поле множественным.
        
        Args:
            kaiten_field: Поле Kaiten
            
        Returns:
            'Y' для множественных полей, 'N' для одиночных
        """
        field_type = kaiten_field.get('type', '')
        multi_select = kaiten_field.get('multi_select', False)
        
        return "Y" if (field_type == 'multi_select' or multi_select) else "N"
    
    def _translate_to_english(self, russian_text: str) -> str:
        """
        Простой перевод русских названий полей на английский.
        
        Args:
            russian_text: Русский текст
            
        Returns:
            Английский перевод
        """
        # Простой словарь для перевода часто встречающихся слов
        translations = {
            'проект': 'Project',
            'название': 'Name', 
            'тип': 'Type',
            'статус': 'Status',
            'приоритет': 'Priority',
            'категория': 'Category',
            'договор': 'Contract',
            'причина': 'Reason',
            'отмена': 'Cancellation',
            'дата': 'Date',
            'время': 'Time',
            'автор': 'Author',
            'исполнитель': 'Assignee',
            'комментарий': 'Comment',
            'описание': 'Description'
        }
        
        # Простая замена известных слов
        result = russian_text
        for ru_word, en_word in translations.items():
            result = result.replace(ru_word, en_word)
            result = result.replace(ru_word.capitalize(), en_word)
        
        return result if result != russian_text else f"Custom Field ({russian_text})" 