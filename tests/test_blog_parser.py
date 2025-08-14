import pytest
from src.pybites_site.blog_parser import PyBitesBlogParser
from bs4 import BeautifulSoup
import types

# --- list_pages: mock requests.get ---
def test_list_pages(mocker):
    parser = PyBitesBlogParser()
    xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <sitemapindex>
          <sitemap><loc>https://pybit.es/articles/foo</loc></sitemap>
          <sitemap><loc>https://pybit.es/articles/bar</loc></sitemap>
        </sitemapindex>'''
    mock_response = mocker.Mock()
    mock_response.content = xml
    mock_response.raise_for_status = mocker.Mock()
    mocker.patch('src.pybites_site.blog_parser.requests.get', return_value=mock_response)
    out = parser.list_pages('fake-url')
    assert out == ["https://pybit.es/articles/foo", "https://pybit.es/articles/bar"]
    parser.close()

# --- fetch_html: mock Selenium driver ---
def test_fetch_html(mocker):
    parser = PyBitesBlogParser()
    html = "<html><body><h1>Test</h1></body></html>"
    mocker.patch.object(parser.driver, 'get')
    mocker.patch.object(type(parser.driver), 'page_source', new=property(lambda self: html))
    soup = parser.fetch_html('http://dummy')
    assert isinstance(soup, BeautifulSoup)
    assert soup.h1.text == "Test"
    parser.close()

# --- parse_article: just test with fake soup ---
def test_parse_article_fields():
    parser = PyBitesBlogParser()
    ld_json = '{"@graph":[{"@type":"WebPage","url":"url-v","name":"thetitle","datePublished": "2023-01-02T12:11:10Z","dateModified":"2024-02-03T10:09:08Z"},{"@type": "Person","name": "Bob"}]}'
    html = f'''
        <html><script type="application/ld+json" class="rank-math-schema">{ld_json}</script>
        <div class="entry-category-header default-max-width">python,tdd</div>
        <div class="entry-content">
            <a href="/foo">foo</a> Bar
            <div>Content Line 1</div>
        </div>
        </html>
        '''
    soup = BeautifulSoup(html, 'html.parser')
    result = parser.parse_article(soup)
    assert result["url"] == "url-v"
    assert result["title"] == "thetitle"
    assert result["author"] == "Bob"
    assert result["tags"] == ["python", "tdd"]
    assert any(cl["link"] == "/foo" for cl in result["content_links"])
    assert any("Content Line 1" in c for c in result["content"])
    assert result["year"] == 2024
    assert result["month"] == 2
    parser.close()

# --- create_s3_bucket: mock boto3 ---
def test_create_s3_bucket_creates(mocker):
    parser = PyBitesBlogParser()
    mock_s3 = mocker.Mock()
    mocker.patch('src.pybites_site.blog_parser.boto3.client', return_value=mock_s3)
    
    # head_bucket will raise ClientError with code 404 (not found)
    e = Exception()
    e.response = {"Error": {"Code": "404"}}
    mock_s3.head_bucket.side_effect = e
    mock_s3.create_bucket.return_value = None
    from src.pybites_site.blog_parser import ClientError
    mocker.patch('src.pybites_site.blog_parser.ClientError', Exception)
    
    # Should not raise
    assert parser.create_s3_bucket("bucketname", "us-west-2")
    parser.close()

# --- convert_json_to_pyarrow: smoke test (pyarrow import is real) ---
def test_pyarrow_conversion():
    parser = PyBitesBlogParser()
    data = [{"url": "x", "title": "y", "date_published": None, "date_modified": None,
             "author": "joe", "tags": [], "content_links": [], "content": [], "year": 2024, "month": 8}]
    table = parser.convert_json_to_pyarrow(data)
    assert hasattr(table, 'to_pandas')
    parser.close()
