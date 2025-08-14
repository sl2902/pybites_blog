import pytest
from db.supabase_client import SupabaseConnector
import psycopg2


@pytest.fixture
def mock_conn(mocker):
    mock_conn = mocker.MagicMock()
    mocker.patch("psycopg2.connect", return_value=mock_conn)
    return mock_conn

@pytest.fixture
def db(test_db_params):
    return SupabaseConnector(test_db_params)

def test_connect_calls_psycopg2_connect(db, mock_conn, mocker, test_db_params):
    db.connect()
    psycopg2.connect.assert_called_once_with(**test_db_params)
    assert db.conn is mock_conn

def test_execute_runs_query(db, mock_conn):
    mock_cursor = mock_conn.cursor.return_value
    db.execute("SELECT 1")
    mock_cursor.execute.assert_called_once_with("SELECT 1")

    # test with params
    db.execute("SELECT 2 WHERE val=%s", (9,))
    assert mock_cursor.execute.call_args_list[-1][0][0] == "SELECT 2 WHERE val=%s"
    assert mock_cursor.execute.call_args_list[-1][0][1] == (9,)

def test_executemany_runs_query(db, mock_conn):
    mock_cursor = mock_conn.cursor.return_value
    data = [(1,), (2,)]
    db.executemany("INSERT INTO t VALUES (%s)", data)
    mock_cursor.executemany.assert_called_once_with("INSERT INTO t VALUES (%s)", data)

def test_fetchall_returns_data(db, mock_conn):
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [(1,)]
    result = db.fetchall("SELECT * FROM tbl")
    assert result == [(1,)]
    mock_cursor.close.assert_called_once()

def test_fetchone_returns_data(db, mock_conn):
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchone.return_value = (99,)
    result = db.fetchone("SELECT 42")
    assert result == (99,)
    mock_cursor.close.assert_called_once()

def test_commit_and_rollback(db, mock_conn):
    db.connect()
    mock_conn.closed = False
    db.commit()
    mock_conn.commit.assert_called_once()
    db.rollback()
    mock_conn.rollback.assert_called_once()

def test_close(db, mock_conn):
    db.connect()
    mock_conn.closed = False
    db.close()
    mock_conn.close.assert_called_once()
    assert db.conn is None

def test_transaction_success(db, mock_conn):
    with db.transaction() as conn:
        assert conn == mock_conn
    mock_conn.commit.assert_called_once()

def test_transaction_exception_rolls_back(db, mock_conn):
    with pytest.raises(ValueError):
        with db.transaction():
            raise ValueError("fail")
    mock_conn.rollback.assert_called_once()

def test_context_manager_commit_calls(mock_conn, test_db_params):
    db = SupabaseConnector(test_db_params)
    db.conn = mock_conn
    mock_conn.closed = False
    with db:
        pass  # no exception triggers commit
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()

def test_context_manager_rollback_calls(mock_conn, test_db_params):
    db = SupabaseConnector(test_db_params)
    db.conn = mock_conn
    mock_conn.closed = False
    with pytest.raises(Exception):
        with db:
            raise Exception("fail")
    mock_conn.rollback.assert_called_once()
    mock_conn.close.assert_called_once()
