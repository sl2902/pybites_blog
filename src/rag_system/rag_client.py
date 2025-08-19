"""Create a RAG client using Milvus Vector DB"""
import os
import sys
import json
from pymilvus import (
    AnnSearchRequest,
    MilvusClient,
    FieldSchema,
    DataType,
    Function,
    FunctionType,
    RRFRanker,
)
from loguru import logger
from typing import Any, List, Dict, Optional
from dotenv import load_dotenv
load_dotenv()

milvus_host = os.getenv("MILVUS_HOST")
milvus_port = os.getenv("MILVUS_PORT")
milvus_username = os.getenv("MILVUS_USERNAME")
milvus_password = os.getenv("MILVUS_PASSWORD")
milvus_collection_name = "pybites_blogs"

class MilvusService:
    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        self.dimensions = 1536
        self._connected = False
        self.ranker = RRFRanker(100)
        self.client = self._connect()
    
    def _connect(self):
        client = None
        try:
            if not self.is_connected():
                client = MilvusClient(
                        uri=f"https://{milvus_host}:{milvus_port}",
                        token=f"{milvus_username}:{milvus_password}"
                )
                self._connected = True
                logger.info(f"Connected to Milvus cloud at {milvus_host}")
            else:
                logger.info("Already connected to Milvus")
        except Exception as e:
            self._connected = False
            logger.error(f"Failed to connect to Milvus: {e}")
        return client
    
    def create_collection(self):
        if not self._connected:
            logger.warning("Cannot create collection: Milvus not connected")
            return
        if self.client.has_collection(self.collection_name):
            logger.info(f"Collection {self.collection_name} already exists")
            return
        
        schema = MilvusClient.create_schema(auto_id=False)

        schema.add_field(
            field_name="id",
            datatype=DataType.VARCHAR,
            max_length=128,
            is_primary=True
        )
        schema.add_field(
            field_name="content",
            datatype=DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True
        )
        schema.add_field(
            field_name="dense_vector",
            datatype=DataType.FLOAT_VECTOR,
            dim=self.dimensions
        )
        schema.add_field(
            field_name="sparse_vector",
            datatype=DataType.SPARSE_FLOAT_VECTOR
        )
        schema.add_field(
            field_name="metadata",
            datatype=DataType.VARCHAR,
            max_length=65535
        )

        bm25_function = Function(
            name="content_bm25_emb",
            input_field_names=["content"],
            output_field_names=["sparse_vector"],
            function_type=FunctionType.BM25
        )
        schema.add_function(bm25_function)

        index_params = self.client.prepare_index_params()
        index_params.add_index(
            field_name="dense_vector",
            index_name="dense_vec_idx",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200}
        )
        index_params.add_index(
            field_name="sparse_vector",
            index_name="sparse_vec_idx",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="BM25",
            params={
                "inverted_index_algo": "DAAT_MAXSCORE",
                "bm25_k1": 1.2,
                "bm25_b": 0.75
            }
        )

        self.client.create_collection(
            collection_name=self.collection_name,
            schema=schema,
            index_params=index_params
        )
        logger.info(f"Created collection {self.collection_name} with HNSW dense and BM25 sparse indexing")

    def upsert_documents(self, documents: List[Dict[str, Any]]):
        """Upsert documents with dense vectors using OpenAI embedding"""

        if not self._connected:
            logger.warning("Cannot insert documents: Milvus not connected")
            return
        
        if not self.client.has_collection(self.collection_name):
            self.create_collection()

        try:
            self.client.load_collection(self.collection_name)
            self.client.upsert(self.collection_name, documents)
            self.client.flush(self.collection_name)
            logger.info(f"Upserted {len(documents)} documents into Milvus")
        except Exception as e:
            logger.error(f"Failed to insert documents: {e}")
            raise
        finally:
            self.client.release_collection(self.collection_name)
    
    def get_distinct_row_ids(self) -> List[str]:
        """Get distinct row_id values from metadata"""
        if not self._connected:
            logger.warning("Cannot query: Milvus not connected")
            return []
        
        try:
            self.client.load_collection(self.collection_name)
            
            # Query all documents to get metadata
            # Note: Milvus requires a vector search, so we'll use a dummy vector
            # and set a very high limit to get all documents
            dummy_vector = [0.0] * self.dimensions
            
            results = self.client.search(
                collection_name=self.collection_name,
                data=[dummy_vector],
                anns_field="dense_vector",
                search_params={
                    "metric_type": "COSINE",
                    "params": {"ef": 100}
                },
                limit=1024,  # Milvus max limit
                output_fields=["metadata"]
            )
            
            # Extract row_ids from metadata
            row_ids = set()
            for hit in results[0]:
                try:
                    metadata = json.loads(hit.get("metadata", "{}"))
                    row_id = metadata.get("row_id")
                    if row_id:
                        row_ids.add(row_id)
                except json.JSONDecodeError:
                    continue
            
            distinct_row_ids = list(row_ids)
            logger.info(f"Found {len(distinct_row_ids)} distinct row_ids")
            return distinct_row_ids
            
        except Exception as e:
            logger.error(f"Failed to get distinct row_ids: {e}")
            return []
        finally:
            try:
                self.client.release_collection(self.collection_name)
            except:
                pass
    
    def get_distinct_row_id_count(self) -> int:
        """Get count of distinct row_id values"""
        return len(self.get_distinct_row_ids())
    
    def search_similarity(
            self, 
            query_text: str, 
            dense_vector: List[float],
            top_k: int = 10,
            dense_weight: float = 0.7,
            sparse_weight: float = 0.3
        ) -> List[Dict[str, Any]]:
        """Perform Hybrid Search on the document"""
        if not self._connected:
            logger.warning("Cannot search documents: Milvus not connected")
            return []
            
        if not self.client.has_collection(self.collection_name):
            return []

        try:
            self.client.load_collection(self.collection_name)

            dense_search_req = AnnSearchRequest(
                data=[dense_vector],
                anns_field="dense_vector",
                param={
                    "metric_type": "COSINE",
                    "params": {"ef": 100},
                },
                limit=top_k * 2
            )

            sparse_search_req = AnnSearchRequest(
                data=[query_text],
                anns_field="sparse_vector",
                param={
                    "metric_type": "BM25",
                    "params": {}
                },
                limit=top_k * 2
            )

            search_results = self.client.hybrid_search(
                collection_name=self.collection_name,
                reqs=[dense_search_req, sparse_search_req],
                ranker=self.ranker,
                limit=top_k,
                output_fields=["id", "content", "metadata"]
            )
            
                
            documents = []
            # Handle the search results properly - results is a list of SearchResult objects
            for search_result in search_results:  # type: ignore
                for hit in search_result:                   
                    documents.append({
                        "id": hit.get("id"),
                        "content": hit.entity.get("content"),
                        "metadata": json.loads(hit.entity.get("metadata")),
                        "score": hit.score,
                        "distance": hit.distance
                    })
            
            return documents
        except Exception as e:
            logger.error(f"Hybrid search failed: {e}")
            return []
        finally:
            self.client.release_collection(self.collection_name)
    
    def search_dense_only(self, dense_vector: List[float], top_k: int = 10) -> List[Dict[str, Any]]:
        """Perform dense vector search only"""
        if not self._connected:
            logger.warning("Cannot search: Milvus not connected")
            return []
        
        try:
            self.client.load_collection(self.collection_name)
            
            search_results = self.client.search(
                collection_name=self.collection_name,
                data=[dense_vector],
                anns_field="dense_vector",
                search_params={
                    "metric_type": "COSINE",
                    "params": {"ef": 100}
                },
                limit=top_k,
                output_fields=["id", "content", "metadata"]
            )
            
            results = []
            for hits in search_results:
                for hit in hits:
                    result = {
                        "id": hit.id,
                        "content": hit.entity.content,
                        "metadata": json.loads(hit.entity.metadata),
                        "score": hit.score,
                        "distance": hit.distance
                    }
                    results.append(result)
            
            logger.info(f"Dense search returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Dense search failed: {e}")
            return []
        finally:
            self.client.release_collection(self.collection_name)
    

    def search_sparse_only(self, query_text: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """Perform sparse vector search only"""
        if not self._connected:
            logger.warning("Cannot search: Milvus not connected")
            return []
        
        try:
            self.client.load_collection(self.collection_name)
            
            search_results = self.client.search(
                collection_name=self.collection_name,
                data=[query_text],
                anns_field="sparse_vector",
                search_params={
                    "metric_type": "BM25",
                    "params": {}
                },
                limit=top_k,
                output_fields=["id", "content", "metadata"]
            )
            
            results = []
            for hits in search_results:
                for hit in hits:
                    result = {
                        "id": hit.id,
                        "content": hit.entity.content,
                        "metadata": json.loads(hit.entity.metadata),
                        "score": hit.score,
                        "distance": hit.distance
                    }
                    results.append(result)
            
            logger.info(f"Sparse search returned {len(results)} results")
            return results
            
        except Exception as e:
            logger.error(f"Sparse search failed: {e}")
            return []
        finally:
            self.client.release_collection(self.collection_name)

    def is_connected(self) -> bool:
        """Check if Milvus connection is active"""
        return self._connected
    
milvus_hybrid_service = MilvusService(milvus_collection_name)
        



