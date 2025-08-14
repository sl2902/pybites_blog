import pytest
from src import bronze_tables

@pytest.fixture(autouse=True)
def patch_bronze_deps(mocker):
    mock_db = mocker.patch('src.bronze_tables.db')
    mock_logger = mocker.patch('src.bronze_tables.logger')
    yield (mock_db, mock_logger)

# Happy path test

def test_create_bronze_table_happy(mocker):
    db = bronze_tables.db
    db.transaction.return_value.__enter__.return_value = None
    db.fetchall.return_value = [(8,)]
    
    create_q = 'CREATE TABLE ...'
    bronze_tables.create_bronze_table(create_q, 'tab', s3_path='s3://bucket/', partition_key=['year','month'])
    # Should run several execute() calls
    assert db.execute.call_count > 0
    db.fetchall.assert_any_call('SELECT COUNT(*) FROM tab')

# Error case

def test_create_bronze_table_exception(mocker):
    db = bronze_tables.db
    db.transaction.return_value.__enter__.return_value = None
    db.execute.side_effect = Exception('SQL error')
    create_q = 'CREATE TABLE ...'
    with pytest.raises(Exception):
        bronze_tables.create_bronze_table(create_q, 'tab', s3_path='s3://')
    bronze_tables.logger.error.assert_any_call('Error creating bronze table: SQL error')
