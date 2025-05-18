import logging
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember, Bot
from telegram.error import TelegramError, TimedOut, NetworkError, RetryAfter, BadRequest
from telegram.ext import (
    Updater, CommandHandler, CallbackQueryHandler, CallbackContext, 
    PollAnswerHandler, MessageHandler, Filters
)
from chat_data_handler import (
    load_chat_data, save_chat_data, add_served_chat, add_served_user, 
    get_active_quizzes
)
from quiz_handler import send_quiz, send_quiz_immediately, handle_poll_answer, load_quizzes
from admin_handler import broadcast
from leaderboard_handler import (
    get_top_scores,
    get_user_stats,
    update_user_stats,
    get_user_score
)
from datetime import datetime, timedelta
from pymongo import MongoClient
import threading
import time
from functools import wraps
from cachetools import TTLCache, cached
from typing import Optional, Dict, List, Any
from bot_logging import logger



# Configuration
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', "7466315774:AAEacukGEmh9ZXBFxwLbM9FlC2vDBRX2Avk")
ADMIN_ID = int(os.getenv('ADMIN_ID', "5050578106"))
LOG_GROUP_ID = int(os.getenv('LOG_GROUP_ID', "-1001902619247"))
MONGO_URI = os.getenv('MONGO_URI', "mongodb+srv://tigerbundle282:tTaRXh353IOL9mj2@testcookies.2elxf.mongodb.net/?retryWrites=true&w=majority&appName=Testcookies")


# Initialize MongoDB
client = MongoClient(
    MONGO_URI,
    maxPoolSize=50,
    connectTimeoutMS=30000,
    retryWrites=True
)
db = client["telegram_bot"]
quizzes_sent_collection = db["quizzes_sent"]

# Cache configurations
user_cache = TTLCache(maxsize=2000, ttl=600)
chat_cache = TTLCache(maxsize=2000, ttl=600)

# Rate limiting
RATE_LIMIT = 5
rate_limit_dict = {}

def rate_limit(func):
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        user_id = update.effective_user.id
        current_time = time.time()
        
        if user_id in rate_limit_dict:
            last_time = rate_limit_dict[user_id]
            if current_time - last_time < 1.0/RATE_LIMIT:
                return
        
        rate_limit_dict[user_id] = current_time
        return func(update, context, *args, **kwargs)
    return wrapper

def error_handler(func):
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        try:
            return func(update, context, *args, **kwargs)
        except RetryAfter as e:
            time.sleep(e.retry_after)
            return func(update, context, *args, **kwargs)
        except (TimedOut, NetworkError):
            time.sleep(1)
            return func(update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

@error_handler
def log_user_or_group(update: Update, context: CallbackContext):
    chat = update.effective_chat
    user = update.effective_user

    log_message = (
        f"{'Group' if chat.type in ['group', 'supergroup'] else 'User'} started the bot: "
        f"{chat.title if chat.type in ['group', 'supergroup'] else user.first_name} "
        f"{user.last_name or ''}\n"
        f"ID: {chat.id}\n"
        f"Link: https://t.me/{chat.username if chat.username else 'N/A'}"
    )

    logger.info(f"New user/group: {log_message}")
    context.bot.send_message(chat_id=LOG_GROUP_ID, text=log_message)


@rate_limit
@error_handler
def start_command(update: Update, context: CallbackContext):
    try:
        chat_id = str(update.effective_chat.id)
        user_id = str(update.effective_user.id)


        log_user_or_group(update, context)
        
        try:
            add_served_chat(chat_id)
            add_served_user(user_id)
        except Exception as e:
            logger.error(f"Error registering chat/user: {e}")

        keyboard = [
            [InlineKeyboardButton("Add in Your Group +", url="https://t.me/PYQ_Quizbot?startgroup=true")],
            [InlineKeyboardButton("Start PYQ Quizzes", callback_data='start_quiz')],
            [
                InlineKeyboardButton("📊 Leaderboard", callback_data='show_leaderboard'),
                InlineKeyboardButton("📈 My Stats", callback_data='show_stats')
            ],
            [InlineKeyboardButton("Commands", callback_data='show_commands')],
            [InlineKeyboardButton("Download all Edition Book", url="https://t.me/+ZSZUt_eBmmhiMDM1")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        welcome_message = (
            "*Pinnacle 7th Edition*\n\n"
            "Welcome to the Pinnacle 7th edition Quiz Bot! "
            "This is a Quiz Bot made by *Pinnacle Publication.*\n\n"
            "This can ask two Exams PYQ's.\n\n"
            "*➠ SSC *\n*➠ RRB*\n\n"
            "Choose the option for proceed further:"
        )
        
        update.message.reply_text(
            welcome_message,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
        return True
    except Exception as e:
        logger.error(f"Error in start_command: {e}")
        return False


def is_user_admin(update: Update, user_id: int):
    chat_member = update.effective_chat.get_member(user_id)
    return chat_member.status in [ChatMember.ADMINISTRATOR, ChatMember.CREATOR]

def button(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)
    query = update.callback_query
    query.answer()
    chat_id = str(query.message.chat.id)
    chat_data = load_chat_data(chat_id)

    if query.data == 'start_quiz':
        # Inline buttons for language selection
        keyboard = [
            [
                InlineKeyboardButton("Hindi", callback_data='language_hindi'),
                InlineKeyboardButton("English", callback_data='language_english')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(text="*Please select your language: [Hindi, English]*", reply_markup=reply_markup, parse_mode="Markdown")

    elif query.data.startswith('language_'):
        language = query.data.split('_')[1]
        chat_data['language'] = language
        save_chat_data(chat_id, chat_data)

        # Inline buttons for category selection based on the chosen language
        if language == 'hindi':
            keyboard = [
                [
                    InlineKeyboardButton("SSC", callback_data='category_SSCHi'),
                    InlineKeyboardButton("RRB", callback_data='category_RRBHi')
                ],
                [InlineKeyboardButton("Back", callback_data='back_to_languages')]
            ]
        elif language == 'english':
            keyboard = [
                [
                    InlineKeyboardButton("SSC", callback_data='category_SSCEn'),
                    InlineKeyboardButton("RRB", callback_data='category_RRBEn')
                ],
                [InlineKeyboardButton("Back", callback_data='back_to_languages')]
            ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(text=f"*Language selected: {language.upper()}\nPlease select your category: [SSC, RRB]*",
                                reply_markup=reply_markup, parse_mode="Markdown")

    elif query.data.startswith('category_'):
        category = query.data.split('_')[1]
        chat_data['category'] = category
        save_chat_data(chat_id, chat_data)

        # Directly start the quiz with interval selection
        keyboard = [
            [
                InlineKeyboardButton("30 sec", callback_data='interval_30'),
                InlineKeyboardButton("1 min", callback_data='interval_60'),
                InlineKeyboardButton("5 min", callback_data='interval_300')
            ],
            [
                InlineKeyboardButton("10 min", callback_data='interval_600'),
                InlineKeyboardButton("30 min", callback_data='interval_1800'),
                InlineKeyboardButton("60 min", callback_data='interval_3600')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(text=f"Category selected: {category.upper()}\n*Please select the interval for quizzes: using this command /setinterval *\nSet the interval for quizzes - [Ex. /setinterval 20] for set Custom Interval",
                                reply_markup=reply_markup, parse_mode="Markdown")

    elif query.data == 'back_to_languages':
        # Inline buttons for language selection
        keyboard = [
            [
                InlineKeyboardButton("Hindi", callback_data='language_hindi'),
                InlineKeyboardButton("English", callback_data='language_english')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(text="Please select your language:", reply_markup=reply_markup)

    elif query.data == 'back_to_categories':
        language = chat_data.get('language', 'english')
        if language == 'hindi':
            keyboard = [
                [
                    InlineKeyboardButton("SSC", callback_data='category_SSCHi'),
                    InlineKeyboardButton("RRB", callback_data='category_RRBHi')
                ],
                [InlineKeyboardButton("Back", callback_data='back_to_languages')]
            ]
        elif language == 'english':
            keyboard = [
                [
                    InlineKeyboardButton("SSC", callback_data='category_SSCEn'),
                    InlineKeyboardButton("RRB", callback_data='category_RRBEn')
                ],
                [InlineKeyboardButton("Back", callback_data='back_to_languages')]
            ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(text="Please select your category:", reply_markup=reply_markup)

    elif query.data.startswith('interval_'):
        interval = int(query.data.split('_')[1])
        chat_data = load_chat_data(chat_id)
        chat_data["interval"] = interval
        save_chat_data(chat_id, chat_data)
        
        if chat_data.get("active", False):
            query.edit_message_text(f"Quiz interval updated to {interval} seconds. Applying new interval immediately.")
            jobs = context.job_queue.jobs()
            for job in jobs:
                if job.context and job.context["chat_id"] == chat_id:
                    job.schedule_removal()
                    
            # Send the first quiz immediately and then schedule subsequent quizzes
        send_quiz_immediately(context, chat_id)
        context.job_queue.run_repeating(send_quiz, interval=interval, first=interval, context={"chat_id": chat_id, "used_questions": chat_data.get("used_questions", [])})
        query.edit_message_text(f"Quiz interval updated to {interval} seconds. Starting quiz.")
        start_quiz(update, context)
        
    elif query.data == 'show_leaderboard':
        chat_id = update.effective_chat.id
        try:
            # Send initial loading message
            loading_message = context.bot.send_message(
                chat_id=chat_id, 
                text="📊 Loading leaderboard..."
            )
            
            # Get top 10 scores
            top_scores = get_top_scores(10)
            
            if not top_scores:
                if loading_message:
                    context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)
                update.message.reply_text("🏆 No scores yet! Start playing to appear on the leaderboard.")
                return
    
            # Prepare the leaderboard message
            message = "📊 *LEADERBOARD* 📊\n\n"
            medals = ["🥇", "🥈", "🥉"]
    
            for rank, entry in enumerate(top_scores, 1):
                try:
                    user_id = entry["user_id"]
                    score = entry["score"]
                    
                    # Get user stats for accuracy and percentile
                    stats = get_user_stats(user_id)
                    accuracy = stats['accuracy']
                    percentile = stats['percentile']
                    
                    # Get user info from Telegram
                    try:
                        user = context.bot.get_chat(int(user_id))
                        username = f"@{user.username}" if user.username else f"{user.first_name} {user.last_name or ''}"
                    except:
                        username = f"User {user_id}"
    
                    # Format rank display (medal or number)
                    rank_display = medals[rank - 1] if rank <= 3 else f"{rank}."
                    
                    # Add entry to leaderboard with accuracy
                    message += (
                        f"*{rank_display} {username}*\n"
                        f" *├  Score: {score}*\n"
                        f" *├  Accuracy: {accuracy:.1f}%*\n"
                        f" *└  Top {percentile:.1f}%*\n\n"
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing leaderboard entry: {str(e)}")
                    continue
    
            # # Add timestamp
            # current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            # message += f"\n_Updated: {current_time}_"
    
            # Delete loading message and send leaderboard
            if loading_message:
                try:
                    context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)
                except:
                    pass
            keyboard = [
            [InlineKeyboardButton("Back", callback_data='back_to_main_menu')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(
                text=message,
                parse_mode="Markdown",
                reply_markup=reply_markup
                )
    
        except Exception as e:
            logger.error(f"Error showing leaderboard: {str(e)}")
            if loading_message:
                try:
                    context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)
                except:
                    pass
            update.message.reply_text(
                "❌ An error occurred while loading the leaderboard. Please try again later."
            )


    elif query.data == 'show_stats':
        try:
            user_id = str(update.effective_user.id)
            stats = get_user_stats(user_id)
            
            if not stats:
                
                query.edit_message_text("❌ Unable to fetch your stats. Please try again later.")
                return
            rank_display = f"#{stats['rank']}/{stats['total_users']}"
            message = (
                "📊 *Your Quiz Statistics* 📊\n\n"
                f"📈 *Your Rank: {rank_display}*\n"
                f"🏆 *Score*: {stats['score']} Points\n"
                f"📊 *Percentile*: {stats['percentile']:.1f}%\n"
                f"🎯 *Accuracy*: {stats['accuracy']:.1f}%\n\n"
                f"📝 *Quiz Attempts*: {stats['attempted_quizzes']}\n"
                f"✅ *Correct Answers*: {stats['correct_answers']}\n"
                f"❌ *Incorrect Answers*: {stats['incorrect_answers']}\n\n"
                )
            keyboard = [
                [InlineKeyboardButton("Back", callback_data='back_to_main_menu')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            query.edit_message_text(
                text=message,
                parse_mode="Markdown",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"Error in 'show_stats' button: {str(e)}")
            query.edit_message_text("❌ Unable to fetch stats. Please try again later.")
        
    elif query.data == 'show_commands':
        commands_description = """
        /start - Start the bot and show the main menu
        /setinterval - Set the interval for quizzes
        /stopquiz - Stop the current quiz
        /pause - Pause the current quiz
        /resume - Resume a paused quiz
        /leaderboard - Show the leaderboard
        /stats - Show your current stats
        """
        # Inline button to go back to the main menu
        keyboard = [
            [InlineKeyboardButton("Back", callback_data='back_to_main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(text=f"Available Commands:\n{commands_description}", reply_markup=reply_markup)

    elif query.data == 'back_to_main_menu':
        # Inline buttons for main menu
        keyboard = [
            [InlineKeyboardButton("Add in Your Group +", url="https://t.me/PYQ_Quizbot?startgroup=true")],
            [InlineKeyboardButton("Start PYQ Quizzes", callback_data='start_quiz')],
            [
                InlineKeyboardButton("📊 Leaderboard", callback_data='show_leaderboard'),
                InlineKeyboardButton("📈 My Stats", callback_data='show_stats')
            ],
            [InlineKeyboardButton("Commands", callback_data='show_commands')],
            [InlineKeyboardButton("Download all Edition Book", url="https://t.me/+ZSZUt_eBmmhiMDM1")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(text=
                                "*Pinnacle 7th Edition*\n\n"
                                "Welcome to the Pinnacle 7th edition Quiz Bot! "
                                "This is a Quiz Bot made by *Pinnacle Publication.*\n\n"
                                "This can ask two Exams PYQ's.\n\n"
                                "*➠ SSC *\n*➠ RRB*\n\n"
                                "Choose the option for proceed further:",
                                parse_mode="Markdown",
                                reply_markup=reply_markup
                                )


def set_interval(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)

    if not context.args or not context.args[0].isdigit():
        update.message.reply_text("Usage: /setinterval <seconds>")
        return
    
    interval = int(context.args[0])
    if interval < 10:
        update.message.reply_text("Interval must be at least 10 seconds.")
        return

    chat_data = load_chat_data(chat_id)
    chat_data["interval"] = interval
    save_chat_data(chat_id, chat_data)

    # If quiz is already running, update the interval immediately
    if chat_data.get("active", False):
        update.message.reply_text(f"Quiz interval updated to {interval} seconds. Applying new interval immediately.")
        jobs = context.job_queue.jobs()
        for job in jobs:
            if job.context and job.context["chat_id"] == chat_id:
                job.schedule_removal()
        # Send the first quiz immediately and then schedule subsequent quizzes
        send_quiz_immediately(context, chat_id)
        context.job_queue.run_repeating(send_quiz, interval=interval, first=interval, context={"chat_id": chat_id, "used_questions": chat_data.get("used_questions", [])})
    else:
        update.message.reply_text(f"Quiz interval updated to {interval} seconds.")
        start_quiz(update, context)


def start_quiz(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)
    chat_data = load_chat_data(chat_id)

    today = datetime.now().date().isoformat()  # Convert date to string
    quizzes_sent = quizzes_sent_collection.find_one({"chat_id": chat_id, "date": today})

    if chat_data.get("active", False):
        update.message.reply_text("A quiz is already running in this chat!")
        return

    interval = chat_data.get("interval", 30)  # Default interval to 30 seconds if not set
    chat_data["active"] = True
    save_chat_data(chat_id, chat_data)

    update.message.reply_text(f"Quiz started! Interval: {interval} seconds.")

    # Send the first quiz immediately
    send_quiz_immediately(context, chat_id)

    # Schedule subsequent quizzes at the specified interval
    context.job_queue.run_repeating(send_quiz, interval=interval, first=interval, context={"chat_id": chat_id, "used_questions": []})

def stop_quiz(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)
    chat_data = load_chat_data(chat_id)

    if chat_data:
        chat_data["active"] = False
        save_chat_data(chat_id, chat_data)

        jobs = context.job_queue.jobs()
        for job in jobs:
            if job.context and job.context["chat_id"] == chat_id:
                job.schedule_removal()

        update.message.reply_text("Quiz stopped successfully.")
    else:
        update.message.reply_text("No active quiz to stop.")

def pause_quiz(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)
    chat_data = load_chat_data(chat_id)

    if not chat_data.get("active", False):
        update.message.reply_text("No active quiz to pause.")
        return

    chat_data["paused"] = True
    save_chat_data(chat_id, chat_data)

    jobs = context.job_queue.jobs()
    for job in jobs:
        if job.context and job.context["chat_id"] == chat_id:
            job.schedule_removal()

    update.message.reply_text("Quiz paused successfully.")

def resume_quiz(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)
    chat_data = load_chat_data(chat_id)

    if not chat_data.get("paused", False):
        update.message.reply_text("No paused quiz to resume.")
        return

    chat_data["paused"] = False
    save_chat_data(chat_id, chat_data)

    interval = chat_data.get("interval", 30)
    context.job_queue.run_repeating(send_quiz, interval=interval, first=0, context={"chat_id": chat_id, "used_questions": []})

    update.message.reply_text("Quiz resumed successfully.")
    
def restart_active_quizzes(context: CallbackContext):
    active_quizzes = get_active_quizzes()
    for quiz in active_quizzes:
        chat_id = quiz["chat_id"]
        interval = quiz["data"].get("interval", 30)
        used_questions = quiz["data"].get("used_questions", [])

        # Check if bot is still a member of the chat
        try:
            context.bot.get_chat_member(chat_id, context.bot.id)
        except TelegramError:
            logger.warning(f"Bot is no longer a member of chat {chat_id}. Removing from active quizzes.")
            save_chat_data(chat_id, {"active": False})  # Mark chat as inactive
            continue

        logger.info(f"Restarting quiz for chat_id: {chat_id} with interval {interval} seconds.")
        context.job_queue.run_repeating(
            send_quiz,
            interval=interval,
            first=0,
            context={"chat_id": chat_id, "used_questions": used_questions}
        )
    
def check_stats(update: Update, context: CallbackContext):
    """Display user's quiz statistics"""
    user_id = str(update.effective_user.id)
    stats = get_user_stats(user_id)
    
    # Format rank with total users
    rank_display = f"#{stats['rank']}/{stats['total_users']}"
    
    # Get current time in UTC
    current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    message = (
        "📊 *Your Quiz Statistics* 📊\n\n"
        f"📈 *Your Rank: {rank_display}*\n"
        f"🏆 *Score*: {stats['score']} Points\n"
        f"📊 *Percentile*: {stats['percentile']:.1f}%\n"
        f"🎯 *Accuracy*: {stats['accuracy']:.1f}%\n\n"
        f"📝 *Quiz Attempts*: {stats['attempted_quizzes']}\n"
        f"✅ *Correct Answers*: {stats['correct_answers']}\n"
        f"❌ *Incorrect Answers*: {stats['incorrect_answers']}"
    )
    
    update.message.reply_text(message, parse_mode="Markdown")

def show_leaderboard(update: Update, context: CallbackContext):
    """Display leaderboard showing rank, score, accuracy and percentile"""
    chat_id = update.effective_chat.id
    loading_message = None
    
    try:
        # Send initial loading message
        loading_message = context.bot.send_message(
            chat_id=chat_id, 
            text="📊 Loading leaderboard..."
        )
        
        # Get top 10 scores
        top_scores = get_top_scores(10)
        
        if not top_scores:
            if loading_message:
                context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)
            update.message.reply_text("🏆 No scores yet! Start playing to appear on the leaderboard.")
            return

        # Prepare the leaderboard message
        message = "📊 *LEADERBOARD* 📊\n\n"
        medals = ["🥇", "🥈", "🥉"]

        for rank, entry in enumerate(top_scores, 1):
            try:
                user_id = entry["user_id"]
                score = entry["score"]
                
                # Get user stats for accuracy and percentile
                stats = get_user_stats(user_id)
                accuracy = stats['accuracy']
                percentile = stats['percentile']
                
                # Get user info from Telegram
                try:
                    user = context.bot.get_chat(int(user_id))
                    username = f"@{user.username}" if user.username else f"{user.first_name} {user.last_name or ''}"
                except:
                    username = f"User {user_id}"

                # Format rank display (medal or number)
                rank_display = medals[rank - 1] if rank <= 3 else f"{rank}."
                
                # Add entry to leaderboard with accuracy
                message += (
                    f"*{rank_display} {username}*\n"
                    f" *├  Score: {score}*\n"
                    f" *├  Accuracy: {accuracy:.1f}%*\n"
                    f" *└  Top {percentile:.1f}%*\n\n"
                )
                
            except Exception as e:
                logger.error(f"Error processing leaderboard entry: {str(e)}")
                continue

        # # Add timestamp
        # current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        # message += f"\n_Updated: {current_time}_"

        # Delete loading message and send leaderboard
        if loading_message:
            try:
                context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)
            except:
                pass
        
        update.message.reply_text(
            message,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )

    except Exception as e:
        logger.error(f"Error showing leaderboard: {str(e)}")
        if loading_message:
            try:
                context.bot.delete_message(chat_id=chat_id, message_id=loading_message.message_id)
            except:
                pass
        update.message.reply_text(
            "❌ An error occurred while loading the leaderboard. Please try again later."
        )


def next_quiz(update: Update, context: CallbackContext):
    chat_id = str(update.effective_chat.id)
    chat_data = load_chat_data(chat_id)
    # Check if there are any active quizzes
    if not chat_data.get("active", False):
        update.message.reply_text("No active quiz. Use /start to begin a quiz session.")
        return
    # Send the next quiz immediately
    send_quiz_immediately(context, chat_id)
    # update.message.reply_text("Next quiz has been sent!")

def cleanup_job(context: CallbackContext):
    """Modified cleanup job that preserves quiz history and chat IDs"""
    try:
        current_time = time.time()
        
        # Only clean rate limiting cache (temporary data)
        to_delete = [
            user_id for user_id, last_time in rate_limit_dict.items()
            if current_time - last_time > 600
        ]
        for user_id in to_delete:
            del rate_limit_dict[user_id]
            
        # Clear only temporary caches
        user_cache.expire()
        chat_cache.expire()
        
        logger.info("Cleanup job completed - Preserved quiz history and chat IDs")
            
    except Exception as e:
        logger.error(f"Error in cleanup job: {e}")

def remove_inactive_jobs(context: CallbackContext):
    """Remove jobs associated with inactive or expired chats."""
    jobs = context.job_queue.jobs()
    for job in jobs:
        if job.context and not is_chat_active(job.context["chat_id"]):  # Check if chat is active
            job.schedule_removal()
            logger.info(f"Removed inactive job for chat_id: {job.context['chat_id']}")


def main():
     # Initialize bot with optimized settings
    bot = Bot(TOKEN)
    updater = Updater(
        bot=bot,
        use_context=True,
        workers=4,
        request_kwargs={
            'read_timeout': 10,
            'connect_timeout': 10,
            'connect_pool_size': 4,  # Match this with workers count
            'connect_retries': 3,
            'pool_timeout': 30
        }
    )
    
    dp = updater.dispatcher
    # Store important data in bot_data

    # Add handlers with error handling
    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(CommandHandler("setinterval", set_interval))
    dp.add_handler(CommandHandler("stopquiz", stop_quiz))
    dp.add_handler(CommandHandler("pause", pause_quiz))
    dp.add_handler(CommandHandler("resume", resume_quiz))
    dp.add_handler(CommandHandler("next", next_quiz))
    dp.add_handler(CallbackQueryHandler(button))
    dp.add_handler(PollAnswerHandler(handle_poll_answer))
    dp.add_handler(CommandHandler("leaderboard", show_leaderboard))
    dp.add_handler(CommandHandler("broadcast", broadcast))
    dp.add_handler(CommandHandler("stats", check_stats))
    
    # Add error handler
    dp.add_error_handler(lambda _, context: logger.error(f"Update caused error: {context.error}"))

    # Schedule periodic cleanup
    updater.job_queue.run_repeating(cleanup_job, interval=600)  # Run every hour
    updater.job_queue.run_once(restart_active_quizzes, 0)
    updater.job_queue.run_repeating(remove_inactive_jobs, interval=600)  # Run every 1 hour

    # Start the bot with optimized polling settings
    logger.info("Starting bot...")
    updater.start_polling(
        drop_pending_updates=True,
        timeout=30,
        read_latency=0.1,
        allowed_updates=['message', 'callback_query', 'poll_answer']
    )
    logger.info("Bot started successfully!")
    updater.idle()

if __name__ == '__main__':
    main()
