"""Build bronze tables in Duckdb"""
import os
from db.duckdb_client import DuckDBConnector, enable_aws_for_database
from loguru import logger
from typing import Any, List, Tuple
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

def create_bronze_table(create_query: str, table_name: str, s3_path: str = None, partition_key: List[str] = None):
    """Create Idempotent table"""
    try:
        logger.info("Creating bronze table from S3 Parquet files...")
        # replaced the dataset api with read_parquet, which improves performance by ~ 40%
        # dataset = ds.dataset(s3_path, format="parquet", partitioning=partition_key)
        # table_data = dataset.to_table().to_pylist()
        # params = []
        
        # # Convert each dictionary to tuple in correct order
        # for row in table_data:
        #     row_tuple = (
        #         row['url'],
        #         row['title'], 
        #         row['date_published'],
        #         row['date_modified'],
        #         row['author'],
        #         row['tags'],
        #         row['content_links'],
        #         row['content'],
        #         row['year'],
        #         row['month']
        #     )
        #     params.append(row_tuple)
        
        # logger.info(f"Converted {len(params)} rows to tuples")

        with db.transaction():
            db.execute(create_query)
            logger.info(f"Created table {table_name}")

            qry = f"""
                    create or replace temporary table tmp_table as
                    select * from {table_name} where 1=0
            """
            db.execute(qry)
            logger.info("Created temporary table inheriting schema from main table")


            # qry = f"""
            #         insert into tmp_table (url, title, date_published, date_modified, author, tags, content_links, content, year, month)
            #         values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            # """
            # db.executemany(qry, params)
            qry = f"""
                    insert into tmp_table
                    select * from read_parquet('{s3_path}*/*/*.parquet', hive_partitioning=true)
                """
            db.execute(qry)
            logger.info("Loaded dataset into tmp_table")

            qry = f"""
                    insert into {table_name}
                    select t.* from tmp_table t
                    left join {table_name} main on t.url = main.url
                    where main.url is null or t.date_modified <> main.date_modified
                """
            db.execute(qry)
            logger.info(f"Loaded dataset into table {table_name}")
            
            # Verify the data
            count = db.fetchall(f"SELECT COUNT(*) FROM {table_name}")
            logger.info(f"Table {table_name} now contains {count[0][0]} rows")
            # rec = db.fetchall(f"SELECT * FROM {table_name}")
            # logger.info(f"{rec}")
    except Exception as e:
        logger.error(f"Error creating bronze table: {e}")
        raise

def run_bronze_pipeline():
    """Run the steps in a pipeline"""
    table_name = "bronze_pybites_blogs"
    create_query = f"""
                    create table if not exists {table_name} (
                        url text,
                        title text,
                        date_published timestamp,
                        date_modified timestamp,
                        author text,
                        tags text[],
                        content_links struct(text text, link text)[],
                        content text[]
                    );
                """
    create_bronze_table(create_query, table_name, S3_PATH, ["year", "month"])


if __name__ == "__main__":
    run_bronze_pipeline()
    # qry = f"""
    #         with base as (
    #         select
    #          *,
    #          count(*) over(partition by url) cc,
    #          row_number() over(partition by url order by date_modified desc) rr
    #         from
    #          {table_name}
    #         )
    #         select
    #         url,
    #         title,
    #         date_modified
    #         from
    #         base
    #         where cc > 1
    #     """
    # result = db.fetchall(qry)
    # logger.info(result)