import pytest

def test_initial_setup():
    """
    Простой тест для проверки, что pytest работает.
    """
    assert True == True

@pytest.mark.skip(reason="Example of a skipped test")
def test_example_skip():
    assert 1 == 2 