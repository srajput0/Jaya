import logging
from telegram import Update
from telegram.ext import CallbackContext
from chat_data_handler import load_chat_data, save_chat_data
from leaderboard_handler import add_score, update_user_stats
import random
import json
import os
from pymongo import MongoClient
from datetime import datetime
from telegram.error import BadRequest, TimedOut, NetworkError, RetryAfter 

logger = logging.getLogger(__name__)

# MongoDB connection
MONGO_URI = "mongodb+srv://tigerbundle282:tTaRXh353IOL9mj2@testcookies.2elxf.mongodb.net/?retryWrites=true&w=majority&appName=Testcookies"
client = MongoClient(MONGO_URI)
db = client["telegram_bot"]
quizzes_sent_collection = db["quizzes_sent"]
used_quizzesss_collection = db["used_quizzesssss"]
message_status_collection = db["message_status"]

# Add indexes for better performance
quizzes_sent_collection.create_index([("chat_id", 1), ("date", 1)])
used_quizzesss_collection.create_index([("chat_id", 1)])
message_status_collection.create_index([("chat_id", 1), ("date", 1)])


def retry_on_failure(func):
    """Decorator to retry function on transient errors"""
    def wrapper(*args, **kwargs):
        retries = 3
        while retries > 0:
            try:
                return func(*args, **kwargs)
            except (TimedOut, NetworkError, RetryAfter) as e:
                logger.warning(f"Retryable error occurred: {e}. Retrying...")
                retries -= 1
            except Exception as e:
                logger.error(f"Unrecoverable error occurred: {e}")
                break
        logger.error(f"Function {func.__name__} failed after retries.")
    return wrapper

def load_quizzes(category):
    file_path = os.path.join('quizzes', f'{category}.json')
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return json.load(f)
    else:
        logger.error(f"Quiz file for category '{category}' not found.")
        return []


def get_daily_quiz_limit(chat_type):
    """
    Set daily quiz limit based on chat type.
    :param chat_type: Type of chat ('private', 'group', or 'supergroup').
    :return: Daily quiz limit.
    """
    if chat_type == 'private':
        return 50  # Daily limit for private chats
    else:
        return 100 # Daily limit for groups/supergroups
        
def batch_get_chat_data(chat_id, today):
    """
    Batch fetch chat data to reduce database calls
    """
    return {
        'quizzes_sent': quizzes_sent_collection.find_one({"chat_id": chat_id, "date": today}),
        'message_status': message_status_collection.find_one({"chat_id": chat_id, "date": today}),
        'used_questions': used_quizzesss_collection.find_one({"chat_id": chat_id})
    }

def send_quiz_logic(context: CallbackContext, chat_id: str):
    """
    Optimized quiz sending logic with batch database operations
    """
    chat_data = load_chat_data(chat_id)
    category = chat_data.get('category')
    questions = load_quizzes(category)

    # Get chat type and daily quiz limit
    chat_type = context.bot.get_chat(chat_id).type
    logger.info(f"Chat ID: {chat_id} | Chat Type: {chat_type}")

    today = datetime.now().date().isoformat()
    
    # Batch fetch all required data
    batch_data = batch_get_chat_data(chat_id, today)
    quizzes_sent = batch_data['quizzes_sent']
    message_status = batch_data['message_status']
    used_question_ids = batch_data['used_questions']

    daily_limit = get_daily_quiz_limit(chat_type)
    
    # Initialize if no data exists
    if not quizzes_sent:
        quizzes_sent = {"chat_id": chat_id, "date": today, "count": 0}
        quizzes_sent_collection.insert_one(quizzes_sent)

    # Check daily limit
    if quizzes_sent["count"] >= daily_limit:
        if not message_status or not message_status.get("limit_reached", False):
            context.bot.send_message(
                chat_id=chat_id,
                text=f"Your daily '{chat_type}': {daily_limit} limit is reached. You will get quizzes tomorrow."
            )
            message_status_collection.update_one(
                {"chat_id": chat_id, "date": today},
                {"$set": {"limit_reached": True}},
                upsert=True
            )
        return

    # Check available questions
    if not questions:
        if not message_status or not message_status.get("no_questions", False):
            context.bot.send_message(
                chat_id=chat_id,
                text="No questions available for this category. /start choose other category"
            )
            message_status_collection.update_one(
                {"chat_id": chat_id, "date": today},
                {"$set": {"no_questions": True}},
                upsert=True
            )
        return

    # Process used questions
    used_ids = used_question_ids["used_questions"] if used_question_ids else []
    available_questions = [q for q in questions if q not in used_ids]
    
    if not available_questions:
        used_quizzesss_collection.update_one(
            {"chat_id": chat_id},
            {"$set": {"used_questions": []}},
            upsert=True
        )
        available_questions = questions
        context.bot.send_message(
            chat_id=chat_id,
            text="All quizzes have been used. Restarting with all available quizzes."
        )

    # Send quiz
    question = random.choice(available_questions)
    try:
        message = context.bot.send_poll(
            chat_id=chat_id,
            question=question['question'],
            options=question['options'],
            type='quiz',
            correct_option_id=question['correct_option_id'],
            is_anonymous=False
        )
        
        # Batch update database
        quizzes_sent_collection.update_one(
            {"chat_id": chat_id, "date": today},
            {"$inc": {"count": 1}},
            upsert=True
        )
        
        used_quizzesss_collection.update_one(
            {"chat_id": chat_id},
            {"$push": {"used_questions": question}},
            upsert=True
        )
        
        context.bot_data[message.poll.id] = {
            'chat_id': chat_id,
            'correct_option_id': question['correct_option_id']
        }
        
    except BadRequest as e:
        logger.error(f"Failed to send quiz to chat {chat_id}: {e}")
        raise
        
@retry_on_failure
def send_quiz(context: CallbackContext):
    """
    Send a quiz to the chat based on the category and daily limits.
    """
    chat_id = context.job.context['chat_id']
    send_quiz_logic(context, chat_id)

@retry_on_failure
def send_quiz_immediately(context: CallbackContext, chat_id: str):
    """
    Send a quiz immediately to the specified chat.
    """
    send_quiz_logic(context, chat_id)



def handle_poll_answer(update: Update, context: CallbackContext):
    """Handle user answers to quiz questions"""
    poll_answer = update.poll_answer
    user_id = str(poll_answer.user.id)
    selected_option = poll_answer.option_ids[0] if poll_answer.option_ids else None

    poll_id = poll_answer.poll_id
    poll_data = context.bot_data.get(poll_id)

    if not poll_data:
        return

    correct_option_id = poll_data['correct_option_id']
    is_correct = selected_option == correct_option_id

    # Update user statistics
    update_user_stats(user_id, is_correct)

