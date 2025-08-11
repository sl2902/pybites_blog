"""Parse the pybites blog for historical posts and save them on AWS S3"""
import json
import asyncio
import requests
import bs4
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from typing import Any, Dict, List, Tuple, Union
from dateutil import parser
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from io import BytesIO

base_url = 'https://pybit.es/articles/'
# url = "https://pybit.es/post-sitemap1.xml"
url = "https://pybit.es/articles/from-sql-to-sqlmodel-a-cleaner-way-to-work-with-databases-in-python/"

EXCLUSION = ["png", "jpeg", "jpg"]
BUCKET_NAME = "pybites-blog"
S3_PATH = f"s3://{BUCKET_NAME}/raw/"

schema = pa.schema([
    ("url", pa.string()),
    ("title", pa.string()),
    ("date_published", pa.timestamp("ms")),
    ("date_modified", pa.timestamp("ms")),
    ("author", pa.string()),
    ("tags", pa.list_(pa.string())),
    ("content_links", pa.list_(pa.struct([
        ("text", pa.string()),
        ("link", pa.string()),
    ]))),
    ("content", pa.list_(pa.string())),
    ("year", pa.int32()),
    ("month", pa.int32()),
])

class PyBitesBlogParser:

    def __init__(self, headless: bool = True):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        self.driver = webdriver.Chrome(options=chrome_options)
    
    def list_pages(self, url: str) -> List[str]:
        """Parse sitemap index to list all pages"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "xml")
        return [loc.get_text() for loc in soup.find_all("loc")]

    
    def fetch_html(self, url: str) -> BeautifulSoup:
        """Use Selenium to fetch and return page soup"""
        self.driver.get(url)
        return BeautifulSoup(self.driver.page_source, "html.parser")

    def parse_article(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract article metadata and content from the page"""
        ld_json_tag = soup.find("script", {"type": "application/ld+json", "class": "rank-math-schema"})
        ld_tags = {}
        if ld_json_tag:
            data = json.loads(ld_json_tag.string)
            for node in data.get("@graph", []):
                if node.get("@type") == "WebPage":
                    ld_tags["url"] = node.get("url")
                    ld_tags["title"] = node.get("name")
                    ld_tags["date_published"] = node.get("datePublished")
                    ld_tags["date_modified"] = node.get("dateModified")
                if node.get("@type") == "Person":
                    ld_tags["author"] = node.get("name")

        tags_el = soup.find('div', {"class": "entry-category-header default-max-width"})
        tags = tags_el.get_text(strip=True).split(',') if tags_el else []

        content = soup.find('div', {'class': "entry-content"})
        content_links = [
            {
                'text': a.get_text(strip=True),
                'link': a.get('href'),
            }
            for a in content.find_all('a', href=True)
        ] if content else []

        page_content = [
            line.get_text().strip()
            for line in content.find_all()
            if line.get_text().strip()
        ] if content else []

        return {
            "url": ld_tags.get("url"),
            "title": ld_tags.get("title"),
            "date_published": parser.isoparse(ld_tags.get("date_published")) if ld_tags.get("date_published") else None,
            "date_modified": parser.isoparse(ld_tags.get("date_modified")) if ld_tags.get("date_modified") else None,
            "author": ld_tags.get("author")["text"] if isinstance(ld_tags.get("author"), dict) else ld_tags.get("author"),
            "tags": tags,
            "content_links": content_links,
            "content": page_content,
            "year": parser.isoparse(ld_tags.get("date_modified")).year if ld_tags.get("date_modified") else None,
            "month": parser.isoparse(ld_tags.get("date_modified")).month if ld_tags.get("date_modified") else None,
        }
    
    def parse_url(self, url: str) -> Dict[str, Any]:
        """Convenience method to fetch and parse a single article"""
        soup = self.fetch_html(url)
        return self.parse_article(soup)

    def parse_site_map_index(self, sitemap_url: str) -> List[Tuple[Union[str, datetime]]]:
        """Parse the sitemap index to fetch all the urls"""
        self.driver.get(sitemap_url)
        soup_site_map = BeautifulSoup(self.driver.page_source, 'html.parser')
        table = soup_site_map.find('table', {'id': 'sitemap'})

        urls = []
        for trow in table.find_all('tr'):
            tmp = []
            for ele in trow.find_all('td'):
                link = ele.find('a')
                if link:
                    tmp.append(link.get('href'))
                else:
                    if "-" in ele.text.strip():
                        tmp.append(parser.parse(ele.text.strip()))
            urls.append(tuple(tmp))
        return urls
    
    def create_s3_bucket(self, bucket_name: str, region_name: str ="us-west-2") -> bool:
        """Create S3 bucket if not available"""
        s3 = boto3.client("s3", region_name=region_name)
        try:
            s3.head_bucket(Bucket=bucket_name)
            logger.warning(f"Bucket {bucket_name} exists")
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                if region_name != "us-east-1":
                    logger.info(f"Creating S3 bucket {bucket_name}")
                    s3.create_bucket(
                        Bucket=bucket_name, 
                        CreateBucketConfiguration={
                            'LocationConstraint': region_name
                        }
                    )
                else:
                    s3.create_bucket(
                        Bucket=bucket_name
                    )
            else:
                raise
        return True
    
    def convert_json_to_pyarrow(self, data: Dict[str, Any], schema=None):
        """Convert the parsed page in json to pyarrow table"""
        return pa.Table.from_pylist(data, schema=schema)
    
    def write_to_s3(self, table: pa.Table, s3_path: str) -> None:
        """Write pyarrow table as parquet file and store it on S3"""
        ds.write_dataset(
            data=table,
            base_dir=s3_path,
            format="parquet",
            partitioning=["year", "month"],
            existing_data_behavior="overwrite_or_ignore",
        )
    
    def close(self):
        self.driver.quit()


if __name__ == "__main__":
    pybites_blog_parser = PyBitesBlogParser()
    # print(pybites_blog_parser.parse_url(url))
    # for url in pybites_blog_parser.list_pages(url):
    #     if not(url == base_url or any(True for ex in EXCLUSION if url.endswith(ex))):
    #         print(url)

    # pybites_blog_parser.create_s3_bucket(BUCKET_NAME)
    # logger.info(f"Parse {url}")
    # data = pybites_blog_parser.parse_url(url)
    # logger.info("Convert json to pyarrow")
    # table = pybites_blog_parser.convert_json_to_pyarrow(data, schema=schema)
    # logger.info("Write to s3")
    # pybites_blog_parser.write_to_s3(table, S3_PATH)

    print(pybites_blog_parser.parse_site_map_index("https://pybit.es/post-sitemap1.xml"))