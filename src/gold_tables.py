"""Build gold tables in DuckDB for the charts"""
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

def create_blog_trends_table(silver_table_name: str, author_table_name: str):
    """Create author table"""
    try:
        logger.info(f"Creating author table '{author_table_name}'")
        
        with db.transaction():
            qry = f"""
                    create table if not exists {author_table_name} (
                        select
                            author,
                            extract(year from date_published) as year,
                            extract(month from date_published) as month
                        from
                            {silver_table_name}

                    )
                """
            db.execute(qry)
    
    except Exception as e:
        logger.error(f"Error in creating author table: {e}")
        raise

def create_authors_list(silver_table_name: str, author_list: str):
    """Create authors list table"""
    try:
        logger.info(f"Create authors list table '{author_list}'")

        with db.transaction():
            qry = f"""
                    create table if not exists {author_list} as (
                        select
                            author
                        from
                            {silver_table_name}
                        group by
                            author
                        order by
                            author
                    )
                """
            db.execute(qry)
    except Exception as e:
        logger.error(f"Error in creating author list table: {e}")
        raise

def create_blogs_trends_view(silver_table_name: str, blogs_trends_table_name: str):
    """Create author table"""
    try:
        logger.info(f"Creating author table '{blogs_trends_table_name}'")
        
        with db.transaction():
            qry = f"""
                    create table if not exists {blogs_trends_table_name} (
                        uuid,
                        url,
                        author,
                        year,
                        month
                    )
                """
            db.execute(qry)
    
    except Exception as e:
        logger.error(f"Error in creating author table: {e}")
        raise

if __name__ == "__main__":
    author_list = "author_list"
    silver_table_name = "silver_pybites_blogs"

    create_authors_list(silver_table_name, author_list)