"""Build gold tables in Supbase for Streamlit charts"""
import os
import json
import io
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

if __name__ == "__main__":
    author_list = "author_list"
    silver_table_name = "silver_pybites_blogs"
    gold_table_name = "gold_pybites_blogs"

    # supabase_db.execute(f"drop table {gold_table_name}")

    create_authors_list(duckdb_db, silver_table_name, author_list)
    create_gold_table(gold_table_name)
    copy_silver_blogs_table(silver_table_name, gold_table_name, 2021, 1)