import pytest
from src.db.duckdb_client import DuckDBConnector
import duckdb

@pytest.fixture
def db():
    connector = DuckDBConnector()
    yield connector
    connector.close()

def test_connect_and_close(db):
    conn = db.connect()
    assert isinstance(conn, duckdb.DuckDBPyConnection)
    db.close()
    assert db.conn is None

def test_execute_and_fetchall(db):
    db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
    db.execute("INSERT INTO test VALUES (?, ?)", (1, 'Alice'))
    rows = db.fetchall("SELECT * FROM test")
    assert rows == [(1, 'Alice')]

def test_executemany_and_fetchall(db):
    db.execute("CREATE TABLE test2 (id INTEGER, value VARCHAR)")
    data = [(1, 'a'), (2, 'b')]
    db.executemany("INSERT INTO test2 VALUES (?, ?)", data)
    rows = db.fetchall("SELECT * FROM test2 ORDER BY id")
    assert rows == data

def test_executemany_no_params(db):
    db.execute("CREATE TABLE test3 (id INTEGER)")
    with pytest.raises(duckdb.duckdb.InvalidInputException):
        db.executemany("INSERT INTO test3 VALUES (?)", None)

def test_transaction_commit_and_rollback():
    db = DuckDBConnector()
    db.execute("CREATE TABLE t4 (i INTEGER)")
    try:
        with db.transaction() as conn:
            conn.execute("INSERT INTO t4 VALUES (100)")
            raise Exception("fail")
    except Exception:
        pass
    # insert must be rolled back
    assert db.fetchall("SELECT * FROM t4") == []
    db.close()
