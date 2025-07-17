import pytest
from datetime import datetime
from typing import List

from models.kaiten_models import KaitenUser
from models.bitrix_models import BitrixUser
from transformers.user_transformer import UserTransformer

@pytest.fixture
def mock_bitrix_users() -> List[BitrixUser]:
    """Фикстура, имитирующая список пользователей из Bitrix24."""
    return [
        BitrixUser(ID="101", EMAIL="test.user@example.com", NAME="Test", LAST_NAME="User"),
        BitrixUser(ID="102", EMAIL="another.user@example.com", NAME="Another", LAST_NAME="User"),
        BitrixUser(ID="103", EMAIL="CAPS.USER@EXAMPLE.COM", NAME="Caps", LAST_NAME="User"),
    ]

@pytest.fixture
def kaiten_user_found() -> KaitenUser:
    """Фикстура для пользователя Kaiten, который есть в Bitrix24."""
    return KaitenUser(
        id=1, uid="uid1", created=datetime.now(), updated=datetime.now(),
        full_name="Test User", email="test.user@example.com", username="testuser",
        initials="TU", avatar_type=2, lng="ru", timezone="UTC", theme="auto",
        activated=True, virtual=False
    )

@pytest.fixture
def kaiten_user_not_found() -> KaitenUser:
    """Фикстура для пользователя Kaiten, которого нет в Bitrix24."""
    return KaitenUser(
        id=2, uid="uid2", created=datetime.now(), updated=datetime.now(),
        full_name="Ghost User", email="ghost@example.com", username="ghostuser",
        initials="GU", avatar_type=2, lng="ru", timezone="UTC", theme="auto",
        activated=True, virtual=False
    )

@pytest.fixture
def kaiten_user_case_insensitive() -> KaitenUser:
    """Фикстура для проверки нечувствительности к регистру email."""
    return KaitenUser(
        id=3, uid="uid3", created=datetime.now(), updated=datetime.now(),
        full_name="Caps User", email="caps.user@example.com", username="capsuser",
        initials="CU", avatar_type=2, lng="ru", timezone="UTC", theme="auto",
        activated=True, virtual=False
    )

def test_user_transformer_initialization(mock_bitrix_users):
    """Тест, что карта пользователей для сопоставления создается правильно."""
    transformer = UserTransformer(mock_bitrix_users)
    assert len(transformer._bitrix_user_map) == 3
    # Проверяем, что email приводятся к нижнему регистру
    assert "test.user@example.com" in transformer._bitrix_user_map
    assert "caps.user@example.com" in transformer._bitrix_user_map

def test_user_found(mock_bitrix_users, kaiten_user_found):
    """Тест успешного нахождения пользователя."""
    transformer = UserTransformer(mock_bitrix_users)
    bitrix_user = transformer.transform(kaiten_user_found)
    assert bitrix_user is not None
    assert bitrix_user.ID == "101"
    assert bitrix_user.EMAIL == "test.user@example.com"

def test_user_not_found(mock_bitrix_users, kaiten_user_not_found):
    """Тест, когда пользователь не найден в Bitrix24."""
    transformer = UserTransformer(mock_bitrix_users)
    bitrix_user = transformer.transform(kaiten_user_not_found)
    assert bitrix_user is None

def test_user_email_case_insensitive(mock_bitrix_users, kaiten_user_case_insensitive):
    """Тест на нечувствительность к регистру email при поиске."""
    transformer = UserTransformer(mock_bitrix_users)
    bitrix_user = transformer.transform(kaiten_user_case_insensitive)
    assert bitrix_user is not None
    assert bitrix_user.ID == "103"

def test_get_user_id_method(mock_bitrix_users, kaiten_user_found):
    """Тест нового метода get_user_id."""
    transformer = UserTransformer(mock_bitrix_users)
    user_id = transformer.get_user_id(kaiten_user_found)
    assert user_id == "101"

def test_get_user_id_not_found(mock_bitrix_users, kaiten_user_not_found):
    """Тест метода get_user_id когда пользователь не найден."""
    transformer = UserTransformer(mock_bitrix_users)
    user_id = transformer.get_user_id(kaiten_user_not_found)
    assert user_id is None

def test_kaiten_to_bitrix_data_conversion(kaiten_user_found):
    """Тест преобразования данных пользователя Kaiten в формат Bitrix24."""
    transformer = UserTransformer([])  # Пустой список для этого теста
    
    bitrix_data = transformer.kaiten_to_bitrix_data(kaiten_user_found)
    
    # Проверяем, что все нужные поля присутствуют
    assert bitrix_data["EMAIL"] == "test.user@example.com"
    assert bitrix_data["NAME"] == "Test"
    assert bitrix_data["LAST_NAME"] == "User"
    assert bitrix_data["UF_DEPARTMENT"] == [1]  # Подразделение "Имена"
    assert bitrix_data["GROUP_ID"] == [12]  # Группа доступа "Имена: Сотрудники"

def test_kaiten_to_bitrix_data_single_name():
    """Тест преобразования пользователя с одним словом в имени."""
    kaiten_user = KaitenUser(
        id=5, uid="uid5", created=datetime.now(), updated=datetime.now(),
        full_name="Вася", email="vasya@example.com", username="vasya",
        initials="В", avatar_type=2, lng="ru", timezone="UTC", theme="auto",
        activated=True, virtual=False
    )
    
    transformer = UserTransformer([])
    bitrix_data = transformer.kaiten_to_bitrix_data(kaiten_user)
    
    assert bitrix_data["NAME"] == "Вася"
    assert bitrix_data["LAST_NAME"] == "Kaiten"
    assert bitrix_data["EMAIL"] == "vasya@example.com"
    assert bitrix_data["UF_DEPARTMENT"] == [1]  # Подразделение "Имена"
    assert bitrix_data["GROUP_ID"] == [12]  # Группа доступа "Имена: Сотрудники"

def test_kaiten_to_bitrix_data_no_email():
    """Тест обработки пользователя без email."""
    kaiten_user = KaitenUser(
        id=6, uid="uid6", created=datetime.now(), updated=datetime.now(),
        full_name="No Email User", email="", username="noemail",
        initials="NE", avatar_type=2, lng="ru", timezone="UTC", theme="auto",
        activated=True, virtual=False
    )
    
    transformer = UserTransformer([])
    bitrix_data = transformer.kaiten_to_bitrix_data(kaiten_user)
    
    # Должен вернуть пустой словарь для пользователя без email
    assert bitrix_data == {}

def test_kaiten_to_bitrix_data_multiple_names():
    """Тест преобразования пользователя с несколькими словами в имени."""
    kaiten_user = KaitenUser(
        id=7, uid="uid7", created=datetime.now(), updated=datetime.now(),
        full_name="Иван Петрович Сидоров", email="ivan@example.com", username="ivan",
        initials="ИПС", avatar_type=2, lng="ru", timezone="UTC", theme="auto",
        activated=True, virtual=False
    )
    
    transformer = UserTransformer([])
    bitrix_data = transformer.kaiten_to_bitrix_data(kaiten_user)
    
    assert bitrix_data["NAME"] == "Иван"
    assert bitrix_data["LAST_NAME"] == "Петрович Сидоров"
    assert bitrix_data["EMAIL"] == "ivan@example.com"
    assert bitrix_data["UF_DEPARTMENT"] == [1]  # Подразделение "Имена"
    assert bitrix_data["GROUP_ID"] == [12]  # Группа доступа "Имена: Сотрудники"
