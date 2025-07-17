from __future__ import annotations
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator
from datetime import datetime


class KaitenBaseModel(BaseModel):
    id: int
    uid: str
    created: datetime
    updated: datetime


class KaitenUser(BaseModel):
    id: int
    uid: str
    full_name: str
    email: str
    username: str
    activated: bool
    locked: bool
    delete_requested_at: Optional[str] = None
    company_id: int
    user_id: int
    permissions: int
    own_permissions: int
    role: int
    lng: str = 'ru'
    timezone: str = 'UTC'
    theme: str = 'auto'
    virtual: bool = False
    email_blocked: Optional[str] = None
    email_blocked_reason: Optional[str] = None
    default_space_id: Optional[int] = None
    email_frequency: int = 2
    email_settings: Optional[Dict[str, Any]] = None
    slack_id: Optional[str] = None
    slack_settings: Optional[Dict[str, Any]] = None
    notification_settings: Optional[Dict[str, Any]] = None
    notification_enabled_channels: Optional[List[str]] = None
    slack_private_channel_id: Optional[str] = None
    telegram_sd_bot_enabled: bool = False
    invite_last_sent_at: Optional[str] = None
    apps_permissions: int = 5
    external: bool = False
    last_request_date: Optional[str] = None
    last_request_method: Optional[str] = None
    work_time_settings: Optional[Dict[str, Any]] = None
    personal_settings: Optional[Dict[str, Any]] = None
    created: Optional[str] = None
    updated: Optional[str] = None
    avatar_initials_url: Optional[str] = None
    avatar_uploaded_url: Optional[str] = None
    initials: Optional[str] = None
    avatar_type: Optional[int] = None
    ui_version: int = 2

    @validator('email')
    def email_must_be_valid(cls, v):
        if not v or '@' not in v:
            raise ValueError('Email must be valid')
        return v


class KaitenTag(BaseModel):
    id: int
    name: str
    color: int


class KaitenSpace(KaitenBaseModel):
    title: str
    external_id: Optional[str] = None
    company_id: int
    sort_order: float
    path: str
    parent_entity_uid: Optional[str] = None
    entity_type: str
    access: str
    archived: bool
    for_everyone_access_role_id: Optional[str] = None
    work_calendar_id: Optional[int] = None
    author_uid: Optional[str] = None
    description: Optional[str] = None  # Добавляем описание если есть


class KaitenCardType(KaitenBaseModel):
    name: str
    color: int
    letter: str
    company_id: Optional[int] = None
    description_template: Optional[str] = None
    archived: bool
    properties: Optional[dict] = None
    author_uid: Optional[str] = None
    suggest_fields: bool


class KaitenColumn(BaseModel):
    id: int
    uid: str
    title: str
    sort_order: float
    col_count: int
    type: int
    board_id: int
    column_id: Optional[int] = None
    external_id: Optional[str] = None
    rules: int
    pause_sla: bool


class KaitenLane(BaseModel):
    id: int
    title: str
    sort_order: float
    board_id: int
    condition: int
    external_id: Optional[str] = None
    default_card_type_id: Optional[int] = None


class KaitenBoard(KaitenBaseModel):
    title: str
    cell_wip_limits: Optional[dict] = None
    external_id: Optional[str] = None
    default_card_type_id: Optional[int] = None
    description: Optional[str] = None
    email_key: str
    move_parents_to_done: bool
    default_tags: Optional[List[str]] = None
    first_image_is_cover: bool
    reset_lane_spent_time: bool
    backward_moves_enabled: bool
    hide_done_policies: bool
    hide_done_policies_in_done_column: bool
    automove_cards: bool
    auto_assign_enabled: bool
    card_properties: Optional[dict] = None
    import_uid: Optional[str] = None
    locked: Optional[bool] = None
    columns: List[KaitenColumn]
    lanes: List[KaitenLane]
    space_id: int
    type: int


class KaitenCardFile(KaitenBaseModel):
    id: int
    url: str
    name: str
    type: int
    size: int
    mime_type: Optional[str] = None
    deleted: bool
    card_id: int
    external: bool
    author_id: int
    comment_id: Optional[int] = None
    sort_order: float
    card_cover: bool
    created: datetime
    updated: datetime
    uid: str
    custom_property_id: Optional[int] = None

class KaitenSpaceMember(KaitenBaseModel):
    """Модель для участника пространства (отличается от обычного пользователя)"""
    # Обязательные поля
    id: int
    uid: str
    email: str
    username: str
    full_name: str
    
    # Опциональные поля для участников пространства
    locked: Optional[bool] = None
    company_id: Optional[int] = None
    user_id: Optional[int] = None
    permissions: Optional[List[str]] = None
    own_permissions: Optional[List[str]] = None
    role: Optional[str] = None
    activated: Optional[bool] = None
    blocked: Optional[bool] = None
    space_role_id: Optional[int] = None
    
    # Дополнительные поля участников
    invite_requested_at: Optional[datetime] = None
    delete_requested_at: Optional[datetime] = None


class KaitenCard(KaitenBaseModel):
    archived: bool
    title: str
    asap: bool
    due_date: Optional[datetime] = None
    sort_order: float
    fifo_order: Optional[float] = None
    state: int
    condition: int
    expires_later: bool
    parents_count: int
    children_count: int
    children_done: int
    has_blocked_children: bool
    goals_total: int
    goals_done: int
    time_spent_sum: int
    time_blocked_sum: int
    children_number_properties_sum: Optional[dict] = None
    parent_checklist_ids: Optional[List[int]] = None
    parents_ids: Optional[List[int]] = None
    children_ids: Optional[List[int]] = None
    blocking_card: bool
    blocked: bool
    size: Optional[float] = None
    size_unit: Optional[str] = None
    size_text: Optional[str] = None
    due_date_time_present: bool
    board_id: int
    column_id: int
    lane_id: int
    owner_id: int
    type_id: int
    version: int
    updater_id: int
    completed_on_time: Optional[bool] = None
    completed_at: Optional[datetime] = None
    last_moved_at: datetime
    lane_changed_at: datetime
    column_changed_at: datetime
    first_moved_to_in_progress_at: Optional[datetime] = None
    last_moved_to_done_at: Optional[datetime] = None
    sprint_id: Optional[int] = None
    external_id: Optional[str] = None
    comments_total: int
    comment_last_added_at: Optional[datetime] = None
    properties: Optional[dict] = None
    counters_recalculated_at: datetime
    service_id: Optional[int] = None
    sd_new_comment: bool
    public: bool
    share_id: Optional[str] = None
    share_settings: Optional[dict] = None

    description_filled: bool
    estimate_workload: int
    tag_ids: Optional[List[int]] = None
    locked: Optional[bool] = None
    source: Optional[str] = None
    owner: KaitenUser
    type: KaitenCardType
    board: KaitenBoard
    column: KaitenColumn
    lane: KaitenLane
    tags: List[KaitenTag]
    members: List[KaitenUser]
    files: List[KaitenCardFile]
