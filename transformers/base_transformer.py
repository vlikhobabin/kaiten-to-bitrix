from abc import ABC, abstractmethod
from typing import Any, Dict

class BaseTransformer(ABC):
    """
    Абстрактный базовый класс для всех трансформеров данных.
    """

    @abstractmethod
    def transform(self, data: Any, **kwargs) -> Dict[str, Any]:
        """
        Преобразует данные из одной модели в словарь, 
        готовый для отправки в API другой системы.
        
        :param data: Объект исходной модели (например, KaitenSpace).
        :param kwargs: Дополнительные параметры, необходимые для трансформации.
        :return: Словарь с данными для целевой системы.
        """
        pass
