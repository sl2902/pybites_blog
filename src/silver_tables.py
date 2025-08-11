"""Build silver tables in DuckDB with transformations"""
import os
from db.duckdb_client import DuckDBConnector, enable_aws_for_duckdb
from loguru import logger
from typing import Any, List, Tuple
from datetime import date, datetime, timedelta
from pybites_site.blog_parser import (
 PyBitesBlogParser,
 BUCKET_NAME,
 S3_PATH,
)

db = DuckDBConnector('pybites.db')
try:
    enable_aws_for_duckdb(db, region='us-west-2', logger=logger)
except Exception as e:
    logger.error(f"Error enabling AWS for DuckDB: {e}")
    raise

def create_silver_table(bronze_table_name: str, silver_table_name: str):
    """Create silver table with transformations from bronze table"""
    try:
        logger.info(f"Creating silver table '{silver_table_name}' with transformed fields...")
        
        with db.transaction():
            # Create silver table with transformations
            create_silver_qry = f"""
                create table if not exists {silver_table_name}(
                    row_id uuid default uuid(),
                    url text,
                    domain text,
                    category text,
                    url_title text,
                    date_published timestamp,
                    date_modified timestamp,
                    days_between_published_modified int,
                    title text,
                    author text,
                    tags text[],
                    content_links struct(text text, link text)[],
                    content text[],
                    content_paragraphs bigint,
                    total_content_words bigint,
                    year int,
                    month int
                )
                """

            db.execute(create_silver_qry)        
    except Exception as e:
        logger.error(f"Error in creating silver table: {e}")
        raise

def backfill_silver_table(bronze_table_name: str, silver_table_name: str, year: int, month: int):
    """Perform one-time backfill. Ensure idempotency"""
    current_year = datetime.now().year
    current_month = datetime.now().month
    next_month = datetime.now().month + 1
    days = (date(current_year, next_month, 1) - timedelta(days=1)).day
    start_date = f"{year}-{month:02d}-01 00:00:00"
    end_date = f"{current_year}-{current_month:02d}-{days} 23:59:59"

    try:
       logger.info(f"Backfilling silver table for period from {year}-{month:02d} to {current_year}-{current_month:02}")

       with db.transaction():
            logger.info(f"Delete any previous data to avoid duplication")
            qry = f"""
                    delete from {silver_table_name}
                    where date_modified between '{start_date}' and '{end_date}'
                """
            rows = db.execute(qry).fetchall()[0][0]
            logger.info(f"Deleted {rows} rows from {silver_table_name}")

            logger.info(f"Insert incremental records into silver table")
            qry = f"""
                    with bronze_table as (
                        select
                            *,
                            row_number() over(partition by url order by date_modified desc) as rn
                        from
                            {bronze_table_name}
                        where
                            date_modified between '{start_date}' and '{end_date}'
                    )
                    insert into {silver_table_name} (
                        url, domain, category, url_title, date_published, date_modified, days_between_published_modified, title,
                        author, tags, content_links, content, content_paragraphs, total_content_words, year, month
                    )
                    select
                        url,
                        split_part(url, '/', 1) || '//' || split_part(url, '/', 3) as domain,
                        split_part(url, '/', 4) as category,
                        split_part(url, '/', -2) as url_title,
                        date_published,
                        date_modified,
                        date_diff('day', date_published, date_modified) as days_between_published_modified,
                        title,
                        author,
                        tags,
                        content_links,
                        content,
                        array_length(content) as content_paragraphs,
                        coalesce(
                        (select sum(array_length(regexp_split_to_array(p, '\\s+')))
                        from unnest(content) as t(p)),
                        0
                        ) as total_content_words,
                        extract(year from date_modified) as year,
                        extract(month from date_modified) as month
                    from
                        bronze_table
                    where
                        rn = 1;
                """
            db.execute(qry)
            result = db.fetchall(f"select count(*) from {silver_table_name}")
            logger.info(f"Number of rows inserted {result[0][0]}")
    except Exception as e:
        logger.error(f"Error in incremental silver table update: {e}")
        raise

if __name__ == "__main__":
    bronze_table = "bronze_pybites_blogs"
    silver_table = "silver_pybites_blogs"
    
    # Create silver table with all transformations
    # db.execute(f"drop table if exists {silver_table}")
    create_silver_table(bronze_table, silver_table)
    
    backfill_silver_table(bronze_table, silver_table, 2021, 1)

    qry = f"""

        select
          url,
          domain,
          category,
          url_title,
          days_between_published_modified,
          content_paragraphs,
          total_content_words
        from
         {silver_table}
        limit 10;
    """
    result = db.fetchall(qry)
    logger.info(f"{result}")

        
