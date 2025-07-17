from pydantic import BaseModel, Field
from typing import Optional


class BitrixUser(BaseModel):
    ID: str  # В Bitrix24 API ID возвращается как строка
    EMAIL: Optional[str] = None
    NAME: Optional[str] = None
    LAST_NAME: Optional[str] = None

class BitrixWorkgroup(BaseModel):
    ID: str = Field(..., alias='id')  # ID как строка
    NAME: str = Field(..., alias='name')

class BitrixTask(BaseModel):
    ID: str = Field(..., alias='id')  # ID как строка
    TITLE: str = Field(..., alias='title')
    DESCRIPTION: Optional[str] = Field(None, alias='description')
    GROUP_ID: str = Field(..., alias='groupId')  # ID как строка
    CREATED_BY: str = Field(..., alias='createdBy')  # ID как строка
    RESPONSIBLE_ID: str = Field(..., alias='responsibleId')  # ID как строка
