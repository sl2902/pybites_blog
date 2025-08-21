"""Chunk and load pybites blogs into Milvus Vector db"""
import os
import argparse
import sys
import json
import asyncio
from datetime import date, datetime, timedelta
from typing import List, Dict, Tuple, Generator, Any, Optional
from loguru import logger
from rag_system.rag_client import (
    milvus_hybrid_service,
)
import tiktoken
from db.supabase_client import SupabaseConnector
from openai_services.openai_client import openai_service
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

def chunk_blogs(
        blog: List[str], 
        chunk_size: int, 
        overlap_size: int, 
        encoding_name: str = "cl100k_base"
    ) -> List[str]:
    """Chunk blog content based on `chunk_size` and `overlap_size`"""
    merged_blog = " ".join(line for line in blog)

    enc = tiktoken.get_encoding(encoding_name)
    start, end = 0, len(merged_blog)
    tokens = enc.encode(merged_blog)

    chunks = []
    while start < end:
        chunk_token = tokens[start: start+chunk_size]
        chunk_text = enc.decode(chunk_token)
        if len(chunk_text) > 0:
            chunks.append(chunk_text)
        start += chunk_size - overlap_size
    return chunks

async def create_document_chunks_from_blog(
        blog: List[Tuple], 
        chunk_size: int = 400, 
        overlap_size: int = 50
    ) -> List[Dict[str, Any]]:
    """Create a document suitable for Milvus db consumption"""
    document = {}
    # blog[12] contains the blog content
    chunks =  chunk_blogs(blog[12], chunk_size, overlap_size)
    embeddings = await asyncio.gather(*(get_embedding(chunk) for chunk in chunks))
    logger.info(f"Generated {len(embeddings)} chunks for blog title {blog[8]}")
    return [
            {
            "id": f"{blog[0]}_{id}",
            "content": chunks[id],
            "dense_vector": embeddings[id],
            "metadata": json.dumps({
                "row_id": f"{blog[0]}",
                "url": blog[1],
                "date_published": str(blog[5]),
                "date_modified": str(blog[6]),
                "title": blog[8],
                "author": blog[9],
                "tags": blog[10],
            })
        }
        for id in range(len(chunks))
    ]

async def get_embedding(chunk: str):
    """Get embedding from OpenAI for a chunk of a blog"""
    return await openai_service.get_embedding(chunk)

async def ingest_documents_to_milvus(document: List[Dict[str, Any]]) -> Optional[bool]:
    """Upsert document to Milvus to create dense vector"""
    try:
        if not document:
            logger.warning("No document found for dense vector ingestion")
            return None
        await asyncio.to_thread(milvus_hybrid_service.upsert_documents, document)
    except Exception as e:
        logger.error(f"Failed to ingest dense vector emebddings: {e}")
        return False
    return True

async def ingest_documents_batch_to_milvus(documents: List[Dict[str, Any]]) -> bool:
    """Batch upsert documents to Milvus"""
    try:
        if not documents:
            logger.warning("No documents found for batch ingestion")
            return False
        
        await asyncio.to_thread(milvus_hybrid_service.upsert_documents, documents)
        logger.info(f"Successfully batch ingested {len(documents)} document chunks to Milvus")
        return True
    except Exception as e:
        logger.error(f"Failed to batch ingest documents: {e}")
        return False

async def run_pipeline_memory_efficient(
    table_name: str,
    concurrent_blogs: int = 2,
    batch_size: int = 5
):
    """Memory-efficient pipeline that processes and inserts in smaller batches"""
    try:
        logger.info(f"Starting memory-efficient pipeline for table {table_name}")
        
        blogs = await asyncio.to_thread(supabase_db.fetchall, f"select * from {table_name}")
        logger.info(f"Fetched {len(blogs)} blogs from database")

        semaphore = asyncio.Semaphore(concurrent_blogs)
        
        # Tracking variables
        total_chunks_processed = 0
        blogs_processed = 0
        blogs_failed = 0
        blogs_skipped = 0
        processed_blog_ids = set()
        failed_blog_ids = []
        
        async def process_and_insert_blog(blog):
            nonlocal total_chunks_processed, blogs_processed, blogs_failed, blogs_skipped
            blog_id = blog[0]  # Assuming blog[0] is the ID
            blog_title = blog[8]
            
            async with semaphore:
                try:
                    # Check if blog content exists and is not empty
                    if not blog[12]:
                        logger.warning(f"Blog ID {blog_id} '{blog_title}': Empty or missing content")
                        blogs_skipped += 1
                        return False
                    
                    if not ' '.join(blog[12]).strip():
                        logger.warning(f"⚠ Blog ID {blog_id} '{blog_title}': Content is only whitespace")
                        blogs_skipped += 1
                        return False
                    
                    document_chunks = await create_document_chunks_from_blog(blog)
                    
                    if not document_chunks:
                        logger.warning(f"Blog ID {blog_id} '{blog_title}': No chunks generated")
                        blogs_skipped += 1
                        return False
                    
                    if len(document_chunks) == 0:
                        logger.warning(f"Blog ID {blog_id} '{blog_title}': Empty chunks list")
                        blogs_skipped += 1
                        return False
                    
                    success = await ingest_documents_batch_to_milvus(document_chunks)
                    if success:
                        total_chunks_processed += len(document_chunks)
                        blogs_processed += 1
                        processed_blog_ids.add(blog_id)
                        logger.info(f"Blog ID {blog_id} '{blog_title}': {len(document_chunks)} chunks (Processed: {blogs_processed})")
                        return True
                    else:
                        blogs_failed += 1
                        failed_blog_ids.append((blog_id, blog_title))
                        logger.error(f"Blog ID {blog_id} '{blog_title}': Failed to insert to Milvus")
                        return False
                        
                except Exception as e:
                    blogs_failed += 1
                    failed_blog_ids.append((blog_id, blog_title))
                    logger.error(f"Blog ID {blog_id} '{blog_title}': Exception during processing: {e}")
                    return False

        # Process blogs in smaller groups to maintain memory efficiency
        for i in range(0, len(blogs), batch_size):
            group = blogs[i:i + batch_size]
            group_start = i + 1
            group_end = min(i + batch_size, len(blogs))
            logger.info(f"Processing group {i//batch_size + 1}/{(len(blogs) + batch_size - 1)//batch_size} (blogs {group_start}-{group_end})")
            
            tasks = [process_and_insert_blog(blog) for blog in group]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Log any exceptions that occurred
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    blog_id = group[j][0] if len(group[j]) > 0 else "Unknown"
                    logger.error(f"Blog ID {blog_id}: Unhandled exception: {result}")
                    blogs_failed += 1
            
            # Small pause between groups
            await asyncio.sleep(1)

        # Final summary
        logger.info("=" * 60)
        logger.info("PIPELINE SUMMARY:")
        logger.info(f"Total blogs fetched from DB: {len(blogs)}")
        logger.info(f"Blogs successfully processed: {blogs_processed}")
        logger.info(f"Blogs failed: {blogs_failed}")
        logger.info(f"Blogs skipped (empty content): {blogs_skipped}")
        logger.info(f"Total chunks processed: {total_chunks_processed}")
        logger.info(f"Expected blogs to process: {len(blogs) - blogs_skipped}")
        logger.info(f"Actual blogs processed: {blogs_processed}")
        
        if failed_blog_ids:
            logger.error(f"Failed blog IDs: {failed_blog_ids}")
        
        if blogs_processed + blogs_failed + blogs_skipped != len(blogs):
            missing_count = len(blogs) - (blogs_processed + blogs_failed + blogs_skipped)
            logger.warning(f"DISCREPANCY: {missing_count} blogs unaccounted for!")
            
            # Find missing blog IDs
            all_blog_ids = {blog[0] for blog in blogs}
            accounted_ids = processed_blog_ids.union({bid for bid, _ in failed_blog_ids})
            missing_ids = all_blog_ids - accounted_ids
            if missing_ids:
                logger.warning(f"Missing blog IDs: {list(missing_ids)}")
        
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error in memory-efficient pipeline: {e}")
        raise


async def run_pipeline():
    """Run the steps in the pipeline"""
    table_name = "gold_pybites_blogs"
    parser = argparse.ArgumentParser(description="Populate Pybites RAG DB")
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

    try:
        logger.info(f"Query table {table_name} for period from {start_date} to {end_date}")

        blogs = await asyncio.to_thread(supabase_db.fetchall, f"""
                                        select * from {table_name}
                                        where date_modified between '{start_date}' and '{end_date}'
                                        """
                                        )
        chunks_processed = 0
        for blog in blogs:
            document = await create_document_chunks_from_blog(blog)
            status = await ingest_documents_to_milvus(document)
            if status is None:
                continue
            elif not status:
                return
            chunks_processed += len(document)
            logger.info(f"Successfully ingested blog '{blog[8]}' to Milvus")
    except Exception as e:
        logger.error(f"Error in run_pipeline() method: {e}")
        raise
    logger.info(f"Total chunks processed {chunks_processed}")
    logger.info(f"Complete ingesting {len(blogs)} to Milvus")

if __name__ == "__main__":
    asyncio.run(run_pipeline())
    # asyncio.run(run_pipeline(table_name))
    # asyncio.run(run_pipeline_memory_efficient(table_name))
    # milvus_hybrid_service.get_distinct_row_id_count()