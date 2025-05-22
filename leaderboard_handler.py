import os
from pymongo import MongoClient
from typing import Dict, Any
from datetime import datetime
from typing import Tuple
from operator import itemgetter
import logging
import traceback


def log_error(func_name: str, error: Exception):
    """Helper function to log errors with traceback"""
    logger.error(
        f"Error in {func_name}: {str(error)}\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
# Setup logging first
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# MongoDB connection
MONGO_URI = "mongodb+srv://2004:2005@cluster0.6vdid.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

try:
    client = MongoClient(MONGO_URI, 
                        serverSelectionTimeoutMS=5000,
                        connectTimeoutMS=5000)
    db = client["telegram_bot"]
    leaderboard_collection = db["leaderboard"]
    
    # Test the connection
    client.server_info()
    logger.info("MongoDB connection successful")
except Exception as e:
    logger.error(f"MongoDB connection error: {str(e)}")



def get_rank_and_total(user_id: str) -> Tuple[int, int]:
    """Get user's rank and total number of users"""
    user = leaderboard_collection.find_one({"user_id": user_id})
    if not user:
        total_users = leaderboard_collection.count_documents({})
        return (total_users + 1, total_users + 1)
    
    total_users = leaderboard_collection.count_documents({})
    higher_scores = leaderboard_collection.count_documents({
        "score": {"$gt": user.get("score", 0)}
    })
    return (higher_scores + 1, total_users)

def get_user_stats(user_id: str) -> Dict[str, Any]:
    """Get comprehensive user statistics"""
    user = leaderboard_collection.find_one({"user_id": user_id})
    if not user:
        total_users = leaderboard_collection.count_documents({})
        return {
            "score": 0,
            "attempted_quizzes": 0,
            "correct_answers": 0,
            "incorrect_answers": 0,
            "accuracy": 0.0,
            "rank": 1,
            "total_users": total_users,
            "percentile": 0.0
        }
    
    # Calculate accuracy
    attempted = user.get("attempted_quizzes", 0)
    correct = user.get("correct_answers", 0)
    accuracy = (correct / attempted * 100) if attempted > 0 else 0
    
    # Get rank and total users
    rank, total_users = get_rank_and_total(user_id)
    
    # Calculate percentile
    if total_users > 1:
        # Percentile = (Number of users below you / Total number of users) * 100
        users_below = leaderboard_collection.count_documents({
            "score": {"$lt": user.get("score", 0)}
        })
        percentile = (users_below / (total_users - 1)) * 100
    else:
        percentile = 100.0  # If you're the only user
    
    return {
        "score": user.get("score", 0),
        "attempted_quizzes": attempted,
        "correct_answers": correct,
        "incorrect_answers": user.get("incorrect_answers", 0),
        "accuracy": accuracy,
        "rank": rank,
        "total_users": total_users,
        "percentile": percentile
    }

def update_user_stats(user_id: str, is_correct: bool) -> None:
    """Update user statistics when they answer a quiz"""
    update_data = {
        "$inc": {
            "attempted_quizzes": 1,
            "correct_answers": 1 if is_correct else 0,
            "incorrect_answers": 0 if is_correct else 1,
            "score": 2 if is_correct else 0  # 2 points for correct answers
        },
        "$set": {
            "last_updated": datetime.utcnow()
        }
    }
    
    # Use upsert to create document if it doesn't exist
    leaderboard_collection.update_one(
        {"user_id": user_id},
        update_data,
        upsert=True
    )



def load_leaderboard():
    leaderboard = {}
    for entry in leaderboard_collection.find():
        leaderboard[entry["user_id"]] = entry["score"]
    return leaderboard

def save_leaderboard(leaderboard):
    leaderboard_collection.delete_many({})
    for user_id, score in leaderboard.items():
        leaderboard_collection.insert_one({"user_id": user_id, "score": score})

def add_score(user_id, score):
    current_score = leaderboard_collection.find_one({"user_id": user_id})
    if current_score:
        new_score = current_score["score"] + score
        leaderboard_collection.update_one({"user_id": user_id}, {"$set": {"score": new_score}})
    else:
        leaderboard_collection.insert_one({"user_id": user_id, "score": score})

def get_top_scores(limit=20):
    """Get top scores with minimal data"""
    try:
        cursor = leaderboard_collection.find(
            {}, 
            {
                'user_id': 1, 
                'score': 1, 
                '_id': 0
            }
        ).sort('score', -1).limit(limit)
        
        top_scores = []
        for doc in cursor:
            user_id = str(doc.get('user_id'))
            if not user_id:
                continue
                
            top_scores.append({
                'user_id': user_id,
                'score': doc.get('score', 0)
            })
            
        return top_scores
        
    except Exception as e:
        logger.error(f"Error fetching top scores: {str(e)}")
        return []
        
def get_user_score(user_id):
    user = leaderboard_collection.find_one({"user_id": user_id})
    return user["score"] if user else 0
    
def update_user_score(user_id, points):
    try:
        user_scores_collection.update_one(
            {'user_id': user_id},
            {'$inc': {'score': points}},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error updating user score: {str(e)}")
