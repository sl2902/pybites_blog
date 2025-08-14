"""Backfill historical blog pages"""
from duckdb.duckdb import Error, CatalogException, ParserException
from db.duckdb_client import DuckDBConnector, enable_aws_for_database
from pybites_site.blog_parser import (
 PyBitesBlogParser,
 base_url,
 EXCLUSION,
 BUCKET_NAME,
 S3_PATH,
)
from loguru import logger
from typing import Any, List, Tuple, Union
from datetime import datetime
import pyarrow.parquet as pq

db = DuckDBConnector('pybites.db')
try:
    enable_aws_for_database(db, region='us-west-2', logger=logger)
except Exception as e:
    logger.error(f"Error enabling AWS for DuckDB: {e}")
    raise
pybites_blog_parser = PyBitesBlogParser()

sitemap_urls_table = "sitemap_urls"

def create_url_table(table_name: str):
    """Create table to load URLs into given DuckDB table"""
    qry = f"""
            create sequence if not exists {table_name}_seq;
            create table if not exists {table_name} (
                id int primary key default nextval('{table_name}_seq'),
                url text,
                last_modified timestamp
            );
    """
    try:
        db.execute(qry)
        logger.info(f"Table '{table_name}' created or already exists.")
    except Error as e:
        logger.error(f"DuckDB error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error creating {table_name}: {e}")
        raise

def upsert_urls(table_name: str, params: List[Tuple[Union[str, datetime]]]):
    """Bulk upsert URLs into the given DuckDB table"""
    with db.transaction():
        qry = f"""
                create or replace temporary table tmp_sitemap_url (
                    url text,
                    last_modified timestamp
                )
            """
        if not params:
            logger.warning("No data to upsert")
            return
        
        db.execute(qry)
        logger.info("Created temporary table for upserts")
        
        logger.info(f"Preparing to upsert {len(params)} URLs")

        qry = f"""
                insert into tmp_sitemap_url (url, last_modified)
                values (?, ?)
            """
        try:
            db.executemany(qry, params)
        except Error as e:
            logger.error(f"DuckDB error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error inserting urls into {table_name}: {e}")
            raise

        qry = f"""
                insert into {table_name} (url, last_modified)
                select
                    t.url,
                    t.last_modified
                from tmp_sitemap_url t
                left join {table_name} main
                on t.url = main.url
                where
                    main.url is null or t.last_modified <> main.last_modified
            """
        try:
            db.execute(qry)
            logger.info(f"URLs upserted successfully into '{table_name}'")
        except Error as e:
            logger.error(f"DuckDB error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating {table_name}: {e}")
            raise


def check_table_data(table_name: str):
    """Check the data in the table"""
    count_qry = f"select count(*) as total_rows from {table_name};"
    sample_qry = f"select * from {table_name} limit 5;"
    dist_qry = f"""
                select
                    extract(year from last_modified) as year,
                    extract(month from last_modified) as month,
                    count(*) as count
                from {table_name}
                group by 1, 2
                order by 3 desc;
            """
    try:
        total_rows = db.fetchall(count_qry)
        logger.info(f"Total rows in {table_name}: {total_rows[0][0]}")
        
        sample_data = db.fetchall(dist_qry)
        logger.info(f"Distribution of data from {table_name}:")
        logger.info(f"{sample_data}")
            
    except Error as e:
        logger.error(f"DuckDB error checking data: {e}")
    except Exception as e:
        logger.error(f"Unexpected error checking data: {e}")

def query_sitemap_url(year: int, month: int) -> List[Tuple]:
    """Query the sitemap index table to fetch all urls for a given year and month"""
    qry = f"""
            select
                url
            from
                {sitemap_urls_table}
            where
                extract(year from last_modified) = {year}
            and
                extract(month from last_modified) = {month}
        """
    return db.fetchall(qry)

def parse_and_write_blog_month(year: int, month: int) -> int:
    """Parse all the blog pages for a given year and month and write to s3"""
    urls = [url[0] for url in query_sitemap_url(year, month)]
    if not urls:
        logger.info(f"No blogs for the period {year}-{month:02d}")
        return 0
    blogs = []
    for url in urls:
        if not(url == base_url or 
               any(True for ex in EXCLUSION if url.endswith(ex))):
            logger.info(f"Parsing page {url}")
            blogs.append(pybites_blog_parser.parse_url(url))
        
    
    logger.info(f"Total number of pages to write to s3 {len(blogs)}")
    table = pybites_blog_parser.convert_json_to_pyarrow(blogs)
    pybites_blog_parser.write_to_s3(table, S3_PATH)
    logger.info(f"Successfully write blog pages for {year}-{month:02d}")
    return len(blogs)

def test_s3_read():
    """Test reading the Parquet file from S3 using DuckDB"""
    try:
        logger.info("Testing DuckDB S3 read functionality...")
        
        with db.transaction():
            # Create a view from the S3 data
            db.execute(f"CREATE OR REPLACE VIEW s3_blogs AS SELECT * FROM read_parquet('{S3_PATH}2021/6/*.parquet')")
            count = db.fetchall("SELECT COUNT(*) FROM s3_blogs")
            logger.info(f"DuckDB can read S3 data: {count[0][0]} rows")
            
            # Show sample data
            sample_data = db.fetchall("SELECT * FROM s3_blogs LIMIT 3")
            logger.info("Sample data from S3:")
            for i, row in enumerate(sample_data):
                logger.info(f"  Row {i}: {row}")
            
    except Exception as e:
        logger.error(f"Error reading from S3: {e}")
        raise


if __name__ == "__main__":
    urllist = pybites_blog_parser.parse_site_map_index("https://pybit.es/post-sitemap1.xml")
    filtered_urls = [url for url in urllist if len(url) > 1]
    create_url_table(sitemap_urls_table)
    upsert_urls(sitemap_urls_table, filtered_urls)
    # check_table_data(sitemap_urls_table)
    blog_counter = 0
    for year in range(2021, 2026):
        for month in range(1, 13):
            n_blogs = parse_and_write_blog_month(year, month)
            blog_counter += n_blogs
    logger.info(f"Total number of blogs parsed {blog_counter}")
    
    # Test reading from S3
    # logger.info("Testing S3 read functionality...")
    # test_s3_read()