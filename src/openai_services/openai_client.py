"""Class containing OpenAI services"""
import os
import asyncio
from openai import OpenAI
from typing import Any, List, Dict, Tuple
from loguru import logger
from dotenv import load_dotenv
load_dotenv()


class OpenAIServices:
    def __init__(self):
        logger.info(f"Initializing OpenAI service")
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = os.getenv("OPENAI_MODEL")
        self.embedding_model = os.getenv("OPENAI_EMBEDDING_MODEL")
    
    async def get_embedding(self, text: str) -> List[float]:
        """Get embedding for a text using OpenAI's embedding model"""
        try:
            response = await asyncio.to_thread(self.client.embeddings.create,
                                    input=text,
                                    model=self.embedding_model,

            )
            # logger.info("Embedding generated successfully")
        except Exception as e:
            logger.error(f"Failed to create embedding: {e}")
            raise

        return response.data[0].embedding
    
    async def get_chat_completion(self, messages: List[Dict[str, str]], context: str = "") -> str:
        """Get chat completion from OpenAI"""
        try:
            logger.info(f"Getting chat completion with {len(messages)} messages")
            if context:
                 system_message = {
                     "role": "system",
                     "content": f"You are an expert developer. Use the following context to answer the user's question: {context}"
                 }
                 message = [system_message] + messages
            response = await asyncio.to_thread(self.client.chat.completions.create,
                                               model=self.model,
                                               messages=messages,
                                               max_tokens=1000,
                                               temperature=0.2,
                                            )
            logger.info("Chat completion generated successfully")
            content = response.choices[0].message.content
        
        except Exception as e:
            logger.error(f"Failed to get chat completion: {e}")
            raise
        
        return content if content else "No response generated"

openai_service = OpenAIServices()