"""Build silver tables in DuckDB with transformations"""
import os
import argparse
from db.duckdb_client import DuckDBConnector, enable_aws_for_database
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
    enable_aws_for_database(db, region='us-west-2', logger=logger)
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

def backfill_silver_table(bronze_table_name: str, silver_table_name: str, start_date: str, end_date: str):
    """Perform one-time backfill. Ensure idempotency"""
    # current_year = datetime.now().year
    # current_month = datetime.now().month
    # next_month = datetime.now().month + 1
    # days = (date(current_year, next_month, 1) - timedelta(days=1)).day
    # start_date = f"{year}-{month:02d}-01 00:00:00"
    # end_date = f"{current_year}-{current_month:02d}-{days} 23:59:59"

    try:
       logger.info(f"Backfilling silver table for period from {start_date} to {end_date}")

       with db.transaction():
            logger.info(f"Delete any previous data to avoid duplication")
            qry = f"""
                    delete from {silver_table_name}
                    where date_modified between '{start_date}' and '{end_date}'
                """
            rows = db.execute(qry).fetchall()[0][0]
            logger.info(f"Deleted {rows} rows from {silver_table_name}")

            logger.info(f"Insert backfill records into silver table")
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
        logger.error(f"Error in backfilling silver table: {e}")
        raise

def create_content_links_table(silver_table_name: str, silver_content_links_table: str):
    """Create content links table"""
    try:
        logger.info(f"Create content links table {silver_content_links_table}")


        with db.transaction():
            qry = f"""
                create table if not exists {silver_content_links_table} (
                    row_id uuid default uuid(),
                    url text,
                    alias text,
                    link text,
                    date_modified timestamp
                )
            """
            db.execute(qry)
    except Exception as e:
        logger.error(f"Error in creating silver content links table: {e}")
        raise

def backfill_content_links_table(silver_table_name: str, silver_content_links_table: str, start_date: str, end_date: str):
    """Perform one-time backfill for the content links table. Ensure idempotency"""
    # current_year = datetime.now().year
    # current_month = datetime.now().month
    # next_month = datetime.now().month + 1
    # days = (date(current_year, next_month, 1) - timedelta(days=1)).day
    # start_date = f"{year}-{month:02d}-01 00:00:00"
    # end_date = f"{current_year}-{current_month:02d}-{days} 23:59:59"

    try:
       logger.info(f"Backfilling silver table for period from {start_date} to {end_date}")

       with db.transaction():
            logger.info(f"Delete any previous data to avoid duplication")
            qry = f"""
                    delete from {silver_content_links_table}
                    where date_modified between '{start_date}' and '{end_date}'
                """
            rows = db.execute(qry).fetchall()[0][0]
            logger.info(f"Deleted {rows} rows from {silver_content_links_table}")

            logger.info(f"Insert backfill records into silver content links table")
            qry = f"""
                    insert into {silver_content_links_table} (
                        url, alias, link, date_modified
                    )
                    with base as (
                    select
                        url,
                        unnest(content_links) as t,
                        date_modified
                    from
                        {silver_table_name}
                    where
                        date_modified between '{start_date}' and '{end_date}'
                    )
                    select
                        url,
                        t.text as alias,
                        t.link as link,
                        date_modified
                    from
                        base
                """
            
            db.execute(qry)
            result = db.fetchall(f"""
                                 select count(*) from {silver_content_links_table} 
                                 where date_modified between '{start_date}' and '{end_date}'
                                 """
                                 )
            logger.info(f"Number of rows inserted {result[0][0]}")        
    except Exception as e:
        logger.error(f"Error in backfilling silver content links table: {e}")
        raise

def run_silver_pipeline():
    """Run the steps in the pipeline"""
    bronze_table = "bronze_pybites_blogs"
    silver_table = "silver_pybites_blogs"
    silver_content_links_table = "silver_content_links"

    parser = argparse.ArgumentParser(description="Populate Pybites silver tables")
    parser.add_argument(
        "--start-year",
        type=int,
        required=True,
        help="Enter the 4 digit starting year, based on last modidied date, from which to start loading",
    )
    parser.add_argument(
        "--start-month",
        type=int,
        required=True,
        help="Enter the digit starting month, based on last modidied date, from which to start loading",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=datetime.now().year,
        help="Enter the optional 4 digit ending year, based on last modidied date, to end loading",
    )
    parser.add_argument(
        "--end-month",
        type=int,
        default=datetime.now().month,
        help="Enter the digit ending month, based on last modidied date, from which to end loading",
    )
    args = parser.parse_args()
    # earliet last modified date year is 2021
    if args.start_year < 2021:
        logger.error(f"Invalid start year {args.start_year}. The oldest last modified date is 2021")
        return
    
    if not (0 < args.start_month < 13 and 0 < args.end_month < 13):
        logger.error(f"Invalid start month {args.start_month} and/or invalid end month {args.end_month}. Valid range [1-12] inclusive")
        return
    
    if args.end_year and args.end_year > datetime.now().year:
        logger.error(f"Invalid end year {args.end_year}. It cannot be greater than current year")
        return
    
    if args.start_year > args.end_year:
        logger.error(f"start_year {args.start_year} cannot be greater than end_year {args.end_year}")
        return
    
    if args.start_year == args.end_year and args.start_month > args.end_month:
        logger.error(f"start_month {args.start_month} cannot be greater than end_month {args.end_month} for the same period {args.start_year}")
        return
    
    if args.end_month > datetime.now().month:
        logger.error(f"No data available for future months in the given period {args.end_year}")
        return
    
    current_year = args.end_year
    current_month = args.end_month

    next_month = current_month + 1
    days = (date(current_year, next_month, 1) - timedelta(days=1)).day
    start_date = f"{args.start_year}-{args.start_month:02d}-01 00:00:00"
    end_date = f"{current_year}-{current_month:02d}-{days} 23:59:59"
    
    # Create silver table with all transformations
    # db.execute(f"drop table if exists {silver_table}")
    create_silver_table(bronze_table, silver_table)
    backfill_silver_table(bronze_table, silver_table, start_date, end_date)

    create_content_links_table(silver_table, silver_content_links_table)
    backfill_content_links_table(silver_table, silver_content_links_table, start_date, end_date)

if __name__ == "__main__":
    run_silver_pipeline()
    # qry = f"""

    #     select
    #       url,
    #       domain,
    #       category,
    #       url_title,
    #       days_between_published_modified,
    #       content_paragraphs,
    #       total_content_words
    #     from
    #      {silver_table}
    #     limit 10;
    # """
    # result = db.fetchall(qry)
    # logger.info(f"{result}")

        
