import os
from pymongo import MongoClient
from functools import lru_cache
import time
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB connection with optimized settings
MONGO_URI = "mongodb+srv://tigerbundle282:tTaRXh353IOL9mj2@testcookies.2elxf.mongodb.net/?retryWrites=true&w=majority&appName=Testcookies"

client = MongoClient(
    MONGO_URI,
    maxPoolSize=200,  # Optimize for concurrent connections
    connectTimeoutMS=30000,
    retryWrites=True,
    waitQueueTimeoutMS=1000,
    serverSelectionTimeoutMS=3000
)

db = client["telegram_bot"]
chat_data_collection = db["chat_data"]
served_chats_collection = db["served_chats"]
served_users_collection = db["served_users"]
quizzes_sent_collection = db["quizzes_sent"]

# Create indexes for better query performance
def setup_indexes():
    try:
        chat_data_collection.create_index("chat_id")
        served_chats_collection.create_index("chat_id", unique=True)
        served_users_collection.create_index("user_id", unique=True)
        quizzes_sent_collection.create_index([("chat_id", 1), ("date", 1)])
        logger.info("Database indexes created successfully")
    except Exception as e:
        logger.error(f"Error creating indexes: {e}")

# Cache decorator with TTL
def ttl_cache(maxsize=128, ttl=600):
    def decorator(func):
        cache = {}
        
        def wrapper(*args, **kwargs):
            key = str(args) + str(kwargs)
            if key in cache:
                result, timestamp = cache[key]
                if time.time() - timestamp < ttl:
                    return result
                else:
                    del cache[key]
            
            result = func(*args, **kwargs)
            cache[key] = (result, time.time())
            
            # Clean old entries
            current_time = time.time()
            cache_copy = dict(cache)
            for k, (_, ts) in cache_copy.items():
                if current_time - ts >= ttl:
                    del cache[k]
            
            return result
        return wrapper
    return decorator

@ttl_cache(maxsize=1000, ttl=300)  # Cache for 5 minutes
def load_chat_data(chat_id: Optional[str] = None) -> Dict:
    try:
        if chat_id:
            chat_data = chat_data_collection.find_one({"chat_id": chat_id})
            return chat_data["data"] if chat_data else {}
        return {chat["chat_id"]: chat["data"] for chat in chat_data_collection.find()}
    except Exception as e:
        logger.error(f"Error loading chat data: {e}")
        return {} if chat_id else {}

def save_chat_data(chat_id: str, data: Dict) -> None:
    try:
        chat_data_collection.update_one(
            {"chat_id": chat_id},
            {"$set": {"data": data}},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error saving chat data for chat {chat_id}: {e}")

@ttl_cache(maxsize=1, ttl=60)  # Cache for 1 minute
def get_served_chats() -> List[Dict]:
    try:
        return list(served_chats_collection.find({}, {"_id": 0, "chat_id": 1}))
    except Exception as e:
        logger.error(f"Error getting served chats: {e}")
        return []

@ttl_cache(maxsize=1, ttl=60)  # Cache for 1 minute
def get_served_users() -> List[Dict]:
    try:
        return list(served_users_collection.find({}, {"_id": 0, "user_id": 1}))
    except Exception as e:
        logger.error(f"Error getting served users: {e}")
        return []

def add_served_chat(chat_id: str) -> None:
    try:
        served_chats_collection.update_one(
            {"chat_id": chat_id},
            {"$set": {"chat_id": chat_id, "last_active": datetime.utcnow()}},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error adding served chat {chat_id}: {e}")

def add_served_user(user_id: str) -> None:
    try:
        served_users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "last_active": datetime.utcnow()}},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error adding served user {user_id}: {e}")

def get_active_quizzes():
    try:
        return chat_data_collection.find({"data.active": True})
    except Exception as e:
        logger.error(f"Error getting active quizzes: {e}")
        return []

def cleanup_old_data():
    """Periodic cleanup of old data"""
    try:
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        
        # Clean up old quiz data
        quizzes_sent_collection.delete_many({"date": {"$lt": thirty_days_ago}})
        
        # Clean up inactive chats and users
        served_chats_collection.delete_many({"last_active": {"$lt": thirty_days_ago}})
        served_users_collection.delete_many({"last_active": {"$lt": thirty_days_ago}})
        
        logger.info("Successfully cleaned up old data")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

# Initialize indexes when module is imported
setup_indexes()
