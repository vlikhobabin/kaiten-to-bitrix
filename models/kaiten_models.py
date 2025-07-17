from __future__ import annotations
from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class KaitenBaseModel(BaseModel):
    id: int
    uid: str
    created: datetime
    updated: datetime


class KaitenUser(KaitenBaseModel):
    full_name: str
    email: str
    username: str
    avatar_initials_url: Optional[str] = None
    avatar_uploaded_url: Optional[str] = None
    initials: str
    avatar_type: int
    lng: str
    timezone: str
    theme: str
    activated: bool
    virtual: bool
    email_blocked: Optional[bool] = None
    email_blocked_reason: Optional[str] = None
    delete_requested_at: Optional[datetime] = None


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


class KaitenCardFile(BaseModel):
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
