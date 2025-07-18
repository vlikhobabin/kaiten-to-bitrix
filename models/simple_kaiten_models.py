"""
Упрощенные модели Kaiten для работы с ограниченными данными API.
Используются когда полные данные недоступны или содержат неполную информацию.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


class SimpleKaitenUser(BaseModel):
    """Упрощенная модель пользователя Kaiten"""
    id: int
    uid: Optional[str] = None
    full_name: Optional[str] = None
    email: Optional[str] = None
    username: Optional[str] = None
    activated: Optional[bool] = None


class SimpleKaitenCardType(BaseModel):
    """Упрощенная модель типа карточки Kaiten"""
    id: int
    name: Optional[str] = None
    color: Optional[int] = None
    letter: Optional[str] = None
    archived: Optional[bool] = None


class SimpleKaitenColumn(BaseModel):
    """Упрощенная модель колонки Kaiten"""
    id: int
    uid: Optional[str] = None
    title: Optional[str] = None
    type: Optional[int] = None
    sort_order: Optional[float] = None


class SimpleKaitenLane(BaseModel):
    """Упрощенная модель дорожки Kaiten"""
    id: int
    title: Optional[str] = None
    sort_order: Optional[float] = None
    condition: Optional[int] = None


class SimpleKaitenBoard(BaseModel):
    """Упрощенная модель доски Kaiten"""
    id: int
    title: Optional[str] = None
    space_id: Optional[int] = None
    description: Optional[str] = None
    type: Optional[int] = None


class SimpleKaitenTag(BaseModel):
    """Упрощенная модель тега Kaiten"""
    id: int
    name: Optional[str] = None
    color: Optional[int] = None


class SimpleKaitenCard(BaseModel):
    """
    Упрощенная модель карточки Kaiten для работы с API данными.
    Содержит только основные поля, необходимые для миграции.
    """
    id: int
    uid: Optional[str] = None
    title: str
    archived: Optional[bool] = False
    due_date: Optional[datetime] = None
    
    # Основные поля для миграции
    board_id: Optional[int] = None
    column_id: Optional[int] = None
    lane_id: Optional[int] = None
    owner_id: Optional[int] = None
    type_id: Optional[int] = None
    
    # Временные поля
    created: Optional[datetime] = None
    updated: Optional[datetime] = None
    last_moved_at: Optional[datetime] = None
    
    # Связанные объекты (упрощенные)
    owner: Optional[SimpleKaitenUser] = None
    type: Optional[SimpleKaitenCardType] = None
    board: Optional[SimpleKaitenBoard] = None
    column: Optional[SimpleKaitenColumn] = None
    lane: Optional[SimpleKaitenLane] = None
    tags: Optional[List[SimpleKaitenTag]] = []
    members: Optional[List[SimpleKaitenUser]] = []
    
    # Дополнительные поля
    description: Optional[str] = None
    description_filled: Optional[bool] = False
    
    # Статистика
    comments_total: Optional[int] = 0
    children_count: Optional[int] = 0
    parents_count: Optional[int] = 0 