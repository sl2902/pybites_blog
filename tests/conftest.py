import sys
import os
import pytest

@pytest.fixture
def test_db_params():
    return {
        "host": "test_host",
        "database": "test_db",
        "user": "test_user",
        "password": "test_pwd",
        "port": "5432",
    }
