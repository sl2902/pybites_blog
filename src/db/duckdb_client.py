"""DuckDB connector"""
import duckdb
from contextlib import contextmanager
from typing import Any, List, Tuple, Union
from datetime import datetime
import os

class DuckDBConnector:
    def __init__(self, db_path: str =":memory:"):
        """
        Initialize DuckDB connection
        """
        self.db_path = db_path
        self.conn = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Create DuckDB connection"""
        if self.conn is None:
            self.conn = duckdb.connect(self.db_path)
        return self.conn
    
    def execute(self, query, params=None) -> duckdb.DuckDBPyRelation:
        """Execute a query against DuckDB"""
        if not self.conn:
            self.connect()
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)
    
    def executemany(self, query: str, params: List[Tuple[Any, ...]]) -> duckdb.DuckDBPyRelation:
        """Execute a bulk query against DuckDB"""
        if not self.conn:
            self.connect()
        if params:
            return self.conn.executemany(query, params)
        raise duckdb.duckdb.InvalidInputException("params is None")
    
    def fetchall(self, query, params=None):
        """Fetch all the rows"""
        return self.execute(query, params).fetchall()
    
    def rollback(self):
        """Rollback changes on exception"""
        if self.conn:
            try:
                self.conn.rollback()
            except duckdb.duckdb.TransactionException:
                pass
    
    def commit(self):
        """Commit changes (DuckDB auto-commits most operations)"""
        if self.conn:
            self.conn.commit()
    
    def close(self):
        """Close the DuckDB connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    @contextmanager
    def transaction(self):
        """Provide a transaction context manager"""
        if not self.conn:
            self.connect()
        try:
            self.conn.execute("BEGIN TRANSACTION")
            yield self.conn
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise

    @contextmanager
    def cursor(self):
        """Provide a context manager for cursor-like usage"""
        try:
            if not self.conn:
                self.connect()
            yield self.conn
        finally:
            self.close()

def enable_aws_for_duckdb(db, region='us-west-2', logger=None):
    """Enable AWS S3 access for DuckDB connection"""
    db.execute("INSTALL httpfs;")
    db.execute("LOAD httpfs;")
    db.execute(f"SET s3_region='{region}';")

    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    session_token = os.getenv('AWS_SESSION_TOKEN')

    if access_key and secret_key:
        db.execute(f"SET s3_access_key_id='{access_key}';")
        db.execute(f"SET s3_secret_access_key='{secret_key}';")
        if session_token:
            db.execute(f"SET s3_session_token='{session_token}';")
        if logger:
            logger.info("Using AWS SSO temporary credentials from environment")
    else:
        if logger:
            logger.error("No AWS credentials found")
        raise Exception("No AWS credentials found")
