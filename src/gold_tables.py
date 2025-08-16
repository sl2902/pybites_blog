"""Build gold tables in Supbase for Streamlit charts"""
import os
import json
import io
import re
import asyncio
import httpx
from urllib.parse import urljoin
import pandas as pd
from db.duckdb_client import DuckDBConnector, enable_aws_for_database
from db.supabase_client import SupabaseConnector
from psycopg2.extras import execute_values
from loguru import logger
from typing import Any, List, Tuple
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

params = {
     "host": os.getenv("SUPABASE_HOST"),
     "database": os.getenv("SUPABASE_DB"),
     "user": os.getenv("SUPABASE_USER"),
     "password": os.getenv("SUPABASE_PWD"),
     "port": os.getenv("SUPABASE_PORT"),
    #  "pool_mode": os.getenv("SUPABASE_POOLMODE")
}

supabase_db = SupabaseConnector(params)

duckdb_db = DuckDBConnector("pybites.db")
try:
    enable_aws_for_database(duckdb_db, region='us-west-2', logger=logger)
except Exception as e:
    logger.error(f"Error enabling AWS for Duckdb Postgres: {e}")
    raise

def fetch_silver_blogs_results(silver_table_name: str) -> List[Tuple]:
    """Fetch the results from silver_pybites_blogs"""
    try:
        logger.info(f"Fetching results from {silver_table_name}")

        with duckdb_db.transaction():
            qry = f"select * from {silver_table_name}"

            result = duckdb_db.execute(qry).fetchall()
            logger.info(f"Number of records fetched from {silver_table_name} is {len(result)}")
    except Exception as e:
        logger.error(f"Error in creating '{gold_table_name}': {e}")
        raise
    return result

def create_gold_table(gold_table_name: str):
    """Create gold table which matched the duckdb silver table schem"""
    try:
        logger.info(f"Create Supabase '{gold_table_name}'")
    
        with supabase_db.transaction():
            qry = f"""
                    create table if not exists {gold_table_name}(
                        row_id uuid,
                        url varchar(255),
                        domain varchar(255),
                        category varchar(255),
                        url_title varchar(255),
                        date_published timestamp,
                        date_modified timestamp,
                        days_between_published_modified int,
                        title varchar(255),
                        author varchar(255),
                        tags text[],
                        content_links jsonb,
                        content text[],
                        content_paragraphs bigint,
                        total_content_words bigint,
                        year int,
                        month int
                )
                """
            cursor = supabase_db.execute(qry)

    except Exception as e:
        logger.error(f"Error in creating '{gold_table_name}': {e}")
        raise

def copy_silver_blogs_table(
        silver_table_name: str, 
        gold_table_name: str,
        year: int, 
        month: int
    ):
    """Copy the Duckdb silver_pybites_blogs into Supabase gold_pybites_blogs"""
    current_year = datetime.now().year
    current_month = datetime.now().month
    next_month = datetime.now().month + 1
    days = (date(current_year, next_month, 1) - timedelta(days=1)).day
    start_date = f"{year}-{month:02d}-01 00:00:00"
    end_date = f"{current_year}-{current_month:02d}-{days} 23:59:59"

    try:
        logger.info(f"Delete {gold_table_name} for period {start_date} to {end_date}")
        
        with supabase_db.transaction():
            qry = f"""
                    delete from {gold_table_name}
                    where date_modified between '{start_date}' and '{end_date}'
                """
            cur = supabase_db.execute(qry)
            rows_deleted = cur.rowcount
            logger.info(f"Deleted {rows_deleted} rows from {gold_table_name}")

        results = fetch_silver_blogs_results(silver_table_name)

        # fetch column names from supabase
        cur = supabase_db.execute(f"select * from {gold_table_name} limit 0")
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(results, columns=columns)
        # convert uuid to str for postgres
        df["row_id"] = df["row_id"].astype(str)

        # conversion for array/struct from duckdb to postgres
        # if "tags" in df.columns:
        #     df["tags"] = df["tags"].apply(lambda x: json.dumps(x) if x is not None else None)
        if "content_links" in df.columns:
            df["content_links"] = df["content_links"].apply(lambda x: json.dumps(x) if x is not None else None)
        # if "content" in df.columns:
        #     df["content"] = df["content"] = df["content"].apply(lambda x: json.dumps(x) if x is not None else None)
        
        with supabase_db.transaction() as conn:
            cursor = conn.cursor()
            data_tuples = [tuple(row) for row in df.values]

            # placeholders = ",".join(["%s"] * len(columns))
            insert_query = f"""
                insert into {gold_table_name} ({",".join(columns)}) 
                values %s
            """

            execute_values(
                cursor,
                insert_query,
                data_tuples,
                template=None,
                page_size=1000
            )
            
            logger.info(f"Successfully inserted {len(data_tuples)} rows using execute_values")
            cursor.close()
            
            # the copy_from approach fails as the content field has unescaped newlines which
            # results in new fields
            # write the df to buffer
            # buf = io.StringIO()
            # df.to_csv(buf, index=False, header=False, na_rep='\\N', escapechar='\\', quoting=1)
            # buf.seek(0)
            # first_few_lines = []
            # for i, line in enumerate(buf):
            #     if i < 5:  # Check first 5 lines
            #         first_few_lines.append(line.strip())
            #     else:
            #         break
                
            # logger.info(f"Expected columns: {len(columns)}")
            # logger.info(f"Column names: {columns}")

            # for i, line in enumerate(first_few_lines):
            #     col_count = len(line.split(',')) if line else 0
            #     logger.info(f"Line {i+1}: {col_count} columns - {line[:100]}...")

            # buf.seek(0)

            # logger.info(f"Bulk inserting {len(df)} rows into {gold_table_name}")
            # try:
            #     cursor.copy_from(
            #         buf, 
            #         gold_table_name, 
            #         sep=",", 
            #         null="\\N", 
            #         columns=columns,
            #     )
            #     logger.info(f"Successfully inserted {len(df)} rows")
            # except Exception as copy_error:
            #     logger.error(f"Error during copy_from: {copy_error}")
            #     raise
            # finally:
            #     cursor.close()

        # result = supabase_db.fetchall(f"select count(*) from {gold_table_name}")[0].get("count")
        # logger.info(f"Number of records copied into {gold_table_name} is {result}")
    
    except Exception as e:
        logger.error(f"Error in creating '{gold_table_name}': {e}")
        raise


def create_authors_list(db, silver_table_name: str, author_list: str):
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

def create_content_links_table(gold_content_links_table: str):
    """Create gold table to store broken/working content links"""
    try:
        logger.info(f"Create gold content links table")

        with supabase_db.transaction():
            qry = f"""
                    create table if not exists {gold_content_links_table} (
                        row_id uuid,
                        url text,
                        link text,
                        link_status varchar(100),
                        date_modified timestamp
                    )
                """
            supabase_db.execute(qry)
    
    except Exception as e:
        logger.error(f"Error in creating content links table: {e}")
        raise 
            

async def check_broken_links(url: str, link: str = None, timeout: int = 30) -> bool:
    """Check whether given link is broken or not"""
    headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8"
    }
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, follow_redirects=True, timeout=timeout)
            resp.raise_for_status()
    except httpx.TimeoutException:
        return f'timeout {timeout} sec'
    except Exception as e:
        # link is broken
        if link:
            return 'internal_broken'
        return 'external_broken'
    if link:
        return 'internal_working'
    return 'external_working'

async def check_content_links(content_links_table: str, year: int, month: int) -> List[Tuple]:
    """Query the content links table to identify broken links"""
    current_year = datetime.now().year
    current_month = datetime.now().month
    next_month = datetime.now().month + 1
    days = (date(current_year, next_month, 1) - timedelta(days=1)).day
    start_date = f"{year}-{month:02d}-01 00:00:00"
    end_date = f"{current_year}-{current_month:02d}-{days} 23:59:59"

    try:
        logger.info(f"Querying {content_links_table} for period from {start_date} to {end_date}")

        qry = f"""
            select
                row_id,
                url,
                link,
                date_modified
            from
                {content_links_table}
            where
                date_modified between '{start_date}' and '{end_date}'
        """

        results = duckdb_db.fetchall(qry)
        logger.info(f"Fetched {len(results)} content links")
        all_valid_links = []

        logger.info(f"Checking link status for {len(results)} links")
        async def gather_links(rec: Tuple) -> Tuple:
            row_id, url, link, date_modified = str(rec[0]), rec[1], rec[2], rec[3]
            link_status = None
            try:
                if 'http' in link:
                    # a slight modification to exclude valid links but 
                    # which are embedded incorrectly
                    # is_valid_link = re.match(r'(https?:\/\/[^)]+)', link)
                    is_valid_link = re.match(r'(^https?:\/\/[^)]+)', link)
                    if is_valid_link:
                        link_status = await check_broken_links(is_valid_link.group(1))
                    else:
                        # malformed internal links
                        internal_url = urljoin(url, link)
                        link_status = await check_broken_links(internal_url, link)
                elif 'mail' in link:
                    is_valid_link = re.match(r'(mailto:[^)]+)', link)
                    if is_valid_link:
                        link_status = "mail_link"
                else:
                    # internal link
                    internal_url = urljoin(url, link)
                    if not link.startswith("#"):
                        link_status = await check_broken_links(internal_url, link)
                    else:
                        link_status = "internal_working"
                    # logger.info(f"Article in {url} doesn't have a valid link {link}")
            except Exception as e:
                logger.error(f"Regex matching failed for url {url} and link {link}")
                link_status = "parse_error"
            return (row_id, url, link, link_status, date_modified)
    
        tasks = [gather_links(rec) for rec in results]
        all_valid_links = await asyncio.gather(*tasks)
        return all_valid_links

    except Exception as e:
        logger.error(f"Error querying table {content_links_table}: {e}")
        raise

    return all_valid_links

def copy_content_links(silver_content_links_table: str, gold_content_links_table: str, year: int, month: int):
    """Load content links to Supabase gold layer"""
    current_year = datetime.now().year
    current_month = datetime.now().month
    next_month = datetime.now().month + 1
    days = (date(current_year, next_month, 1) - timedelta(days=1)).day
    start_date = f"{year}-{month:02d}-01 00:00:00"
    end_date = f"{current_year}-{current_month:02d}-{days} 23:59:59"

    try:
        logger.info(f"Delete {gold_content_links_table} for period {start_date} to {end_date}")
        
        with supabase_db.transaction():
            qry = f"""
                    delete from {gold_content_links_table}
                    where date_modified between '{start_date}' and '{end_date}'
                """
            cur = supabase_db.execute(qry)
            rows_deleted = cur.rowcount
            logger.info(f"Deleted {rows_deleted} rows from {gold_content_links_table}")

        results = asyncio.run(check_content_links(silver_content_links_table, year, month))

        with supabase_db.transaction() as conn:
            cursor = conn.cursor()

            insert_query = f"""
                insert into {gold_content_links_table} (row_id, url, link, link_status, date_modified) 
                values %s
            """

            execute_values(
                cursor,
                insert_query,
                results,
                template=None,
                page_size=1000
            )
            
            cursor.execute(f"select count(*) from {gold_content_links_table}")
            n_rows = cursor.fetchone()[0]
            logger.info(f"Successfully inserted {n_rows} rows using execute_values")
            cursor.close()

    except Exception as e:
        logger.error(f"Error copying content links into {gold_content_links_table}: {e}")
        raise



if __name__ == "__main__":
    author_list = "author_list"
    silver_table_name = "silver_pybites_blogs"
    gold_table_name = "gold_pybites_blogs"

    silver_content_links_table = "silver_content_links"
    gold_content_links_table = "gold_content_links"

    # supabase_db.execute(f"drop table {gold_table_name}")

    create_authors_list(duckdb_db, silver_table_name, author_list)
    create_gold_table(gold_table_name)
    copy_silver_blogs_table(silver_table_name, gold_table_name, 2021, 1)

    # supabase_db.execute(f"drop table {gold_content_links_table}")
    create_content_links_table(gold_content_links_table)
    # df = pd.DataFrame(asyncio.run(check_content_links(silver_content_links_table, 2021, 1)))
    # print(df[3].value_counts())
    copy_content_links(silver_content_links_table, gold_content_links_table, 2021, 1)
