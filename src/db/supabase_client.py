"""Supbase connector"""
import os
import psycopg2
import psycopg2.extras
import streamlit as st
from typing import Any, Optional, List, Dict, Tuple
from contextlib import contextmanager
from datetime import datetime
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

class SupabaseConnector:
    def __init__(self, params: Optional[Dict[str, str]] = None):

        self.conn = None
        if params:
            self.params = params
        else:
            self.params = {
                "host": st.secrets["supabase"]["host"],
                "database": st.secrets["supabase"]["database"],
                "user": st.secrets["supabase"]["user"],
                "password": st.secrets["supabase"]["password"],
                "port": st.secrets["supabase"]["port"],
            }
    
    def connect(self) -> psycopg2.extensions.connection:
        """Create Supabase connection"""
        if not self.conn or self.conn.closed:
            try:
                self.conn = psycopg2.connect(**self.params)
            except psycopg2.OperationalError:
                raise
        return self.conn
    
    def execute(self, query: str, params: Optional[Tuple] = None) -> psycopg2.extensions.cursor:
        """Execute a query against Supabase"""
        if not self.conn or self.conn.closed:
            self.connect()
        
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor
    
    def executemany(self, query: str, params: List[Tuple[Any, ...]]) -> psycopg2.extensions.cursor:
        """Execute a bulk query against Supabase"""
        if not self.conn or self.conn.closed:
            self.connect()
        
        cursor = self.conn.cursor()
        cursor.executemany(query, params)
        return cursor
    
    def fetchall(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """Fetch all rows"""
        cursor = self.execute(query, params)
        try:
            return cursor.fetchall()
        except psycopg2.Error as e:
            raise
        finally:
            cursor.close()
    
    def fetchone(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """Fetch one row"""
        cursor = self.execute(query, params)
        try:
            return cursor.fetchone()
        except psycopg2.Error as e:
            raise
        finally:
            cursor.close()
    
    def rollback(self):
        """Rollback changes on exception"""
        if self.conn and not self.conn.closed:
            try:
                self.conn.rollback()
            except psycopg2.Error:
                raise
    
    def commit(self):
        """Commit successful transaction"""
        if self.conn and not self.conn.closed:
            self.conn.commit()
    
    def close(self):
        """Close the Supabase connection"""
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.conn = None
    
    @contextmanager
    def transaction(self):
        """Provides a transaction context manager"""
        if not self.conn or self.conn.closed:
            self.connect()
        
        try:
            yield self.conn
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()

if __name__ == "__main__":
    
    db = SupabaseConnector(params)
    conn = db.connect()
    # cursor = db.execute("select * from information_schema.tables")
    print(db.fetchall("select * from information_schema.tables"))
    

    

        
    
