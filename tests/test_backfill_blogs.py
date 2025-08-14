import pytest
from src import backfill_blogs

# --- Mocks for db, blog_parser, logger, etc ---
@pytest.fixture(autouse=True)
def auto_patch_dependencies(mocker):
    # Patch the singleton instances that are created at import time
    mock_db = mocker.patch('src.backfill_blogs.db')
    mock_parser = mocker.patch('src.backfill_blogs.pybites_blog_parser')
    mock_logger = mocker.patch('src.backfill_blogs.logger')
    yield (mock_db, mock_parser, mock_logger)

# --- create_url_table ---
def test_create_url_table_runs_queries(mocker):
    db = backfill_blogs.db
    db.execute.reset_mock()
    backfill_blogs.create_url_table('mytable')
    db.execute.assert_called()  # at least once; not checking sql here

# --- upsert_urls: covers warnings and happy path ---
def test_upsert_urls_warns_on_empty(mocker):
    db = backfill_blogs.db
    logger = backfill_blogs.logger
    backfill_blogs.upsert_urls('tab', [])
    logger.warning.assert_called_once()
    db.execute.assert_not_called()

def test_upsert_urls_successful(mocker):
    db = backfill_blogs.db
    db.transaction.return_value.__enter__.return_value = None
    backfill_blogs.upsert_urls('tab', [('http://foo', None)])
    # should execute many times (at least for each SQL action)
    assert db.execute.call_count > 0
    db.executemany.assert_called_once()

# --- check_table_data ---
def test_check_table_data_happy_path(mocker):
    db = backfill_blogs.db
    logger = backfill_blogs.logger
    db.fetchall.side_effect = [[(42,)], [(2023, 1, 7), (2023, 2, 3)]]
    backfill_blogs.check_table_data('mytab')
    logger.info.assert_any_call('Total rows in mytab: 42')
    logger.info.assert_any_call("Distribution of data from mytab:")

# --- query_sitemap_url ---
def test_query_sitemap_url_returns_results(mocker):
    db = backfill_blogs.db
    db.fetchall.return_value = [('url1',), ('url2',)]
    out = backfill_blogs.query_sitemap_url(2024, 8)
    assert out == [('url1',), ('url2',)]
    db.fetchall.assert_called_once()

# --- parse_and_write_blog_month (main workflow) ---
def test_parse_and_write_blog_month_skips_if_no_urls(mocker):
    mocker.patch('src.backfill_blogs.query_sitemap_url', return_value=[])
    logger = backfill_blogs.logger
    out = backfill_blogs.parse_and_write_blog_month(2000, 1)
    logger.info.assert_any_call("No blogs for the period 2000-01")
    assert out == 0

# --- test deduplication ---
def test_upsert_urls_with_duplicates(mocker):
    db = backfill_blogs.db
    db.transaction.return_value.__enter__.return_value = None
    params = [
        ('http://foo', None),
        ('http://foo', None),  # duplicate
        ('http://bar', None)
    ]
    backfill_blogs.upsert_urls('tab', params)
    # Should still call executemany once with all params provided (actual deduplication is left to SQL)
    db.executemany.assert_called_once_with(
        mocker.ANY,  # SQL string
        params
    )

def test_parse_and_write_blog_month_happy(mocker):
    urls = [('http://post1',), ('http://post2',)]
    mocker.patch('src.backfill_blogs.query_sitemap_url', return_value=urls)
    parser = backfill_blogs.pybites_blog_parser
    parser.parse_url.side_effect = lambda u: {'url': u}
    parser.convert_json_to_pyarrow.return_value = 'table_obj'
    parser.write_to_s3.return_value = None
    logger = backfill_blogs.logger
    out = backfill_blogs.parse_and_write_blog_month(2025, 5)
    assert out == 2
    parser.parse_url.assert_any_call('http://post1')
    parser.parse_url.assert_any_call('http://post2')
    parser.convert_json_to_pyarrow.assert_called()
    parser.write_to_s3.assert_called()
    logger.info.assert_any_call('Total number of pages to write to s3 2')
