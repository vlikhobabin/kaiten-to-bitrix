import pytest
import json
import httpx
from unittest.mock import AsyncMock, MagicMock

from connectors.kaiten_client import KaitenClient
from models.kaiten_models import KaitenSpace, KaitenUser

# Определяем путь к файлам с тестовыми данными
SAMPLES_DIR = "project-doc/data_samples/kaiten"

@pytest.fixture
def mock_httpx_client(mocker):
    """
    Фикстура для мокирования httpx.AsyncClient.
    """
    mock_client = MagicMock()
    mocker.patch('httpx.AsyncClient', return_value=mock_client)
    
    # Чтобы использовать `async with`, мокируем и эти методы
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = AsyncMock()
    
    return mock_client

@pytest.mark.asyncio
async def test_get_spaces_success(mock_httpx_client):
    """
    Тест успешного получения списка пространств.
    """
    # 1. Подготовка
    # Загружаем тестовые данные из файла
    with open(f"{SAMPLES_DIR}/spaces.json", 'r', encoding='utf-8') as f:
        mock_spaces_data = json.load(f)

    # Настраиваем мок-ответ
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_spaces_data
    
    mock_httpx_client.request = AsyncMock(return_value=mock_response)

    # 2. Выполнение
    client = KaitenClient()
    spaces = await client.get_spaces()

    # 3. Проверка
    # Проверяем, что был вызван правильный эндпоинт
    mock_httpx_client.request.assert_called_once_with("GET", "/api/v1/spaces")
    
    # Проверяем, что результат - это список объектов KaitenSpace
    assert isinstance(spaces, list)
    assert len(spaces) == len(mock_spaces_data)
    assert all(isinstance(s, KaitenSpace) for s in spaces)
    assert spaces[0].title == mock_spaces_data[0]['title']

@pytest.mark.asyncio
async def test_get_users_success(mock_httpx_client):
    """
    Тест успешного получения списка пользователей.
    """
    # 1. Подготовка
    with open(f"{SAMPLES_DIR}/users.json", 'r', encoding='utf-8') as f:
        mock_users_data = json.load(f)
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = mock_users_data
    
    mock_httpx_client.request = AsyncMock(return_value=mock_response)

    # 2. Выполнение
    client = KaitenClient()
    users = await client.get_users()

    # 3. Проверка
    mock_httpx_client.request.assert_called_once_with("GET", "/api/v1/users")
    assert isinstance(users, list)
    assert len(users) == len(mock_users_data)
    assert all(isinstance(u, KaitenUser) for u in users)
    assert users[0].full_name == mock_users_data[0]['full_name']

@pytest.mark.asyncio
async def test_api_error_handling(mock_httpx_client):
    """
    Тест обработки ошибки API (например, 500 Internal Server Error).
    """
    # 1. Подготовка
    # Настраиваем мок-ответ с ошибкой
    mock_httpx_client.request = AsyncMock(side_effect=httpx.HTTPStatusError(
        "Server Error", 
        request=MagicMock(), 
        response=MagicMock(status_code=500, text="Internal Server Error")
    ))

    # 2. Выполнение
    client = KaitenClient()
    result = await client.get_spaces()

    # 3. Проверка
    # Убеждаемся, что метод вернул пустой список и залогировал ошибку
    assert result == []
    # (проверка логов требует более сложной настройки, пока опускаем)
