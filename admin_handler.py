import logging
from telegram import Update
from telegram.ext import CallbackContext, CommandHandler
from chat_data_handler import load_chat_data, get_served_chats, get_served_users
from telegram.error import TimedOut, NetworkError, RetryAfter, BadRequest, Unauthorized

logger = logging.getLogger(__name__)

ADMIN_ID = 6697397532  # Replace with your actual Telegram user ID

def broadcast(update: Update, context: CallbackContext):
    if update.effective_user.id != ADMIN_ID:
        update.message.reply_text("You are not authorized to use this command.")
        return

    # Determine if the message is a photo or text
    if update.message.reply_to_message:
        if update.message.reply_to_message.photo:
            content_type = 'photo'
            file_id = update.message.reply_to_message.photo[-1].file_id
            text_content = update.message.reply_to_message.caption
        else:
            content_type = 'text'
            text_content = update.message.reply_to_message.text
    else:
        message = ' '.join(context.args)
        if not message:
            update.message.reply_text("Usage: /broadcast <message>")
            return
        content_type = 'text'
        text_content = message

    reply_markup = update.message.reply_to_message.reply_markup if hasattr(update.message.reply_to_message, 'reply_markup') else None

    sent_chats, sent_users = broadcast_to_all(context.bot, text_content, content_type, file_id if content_type == 'photo' else None, reply_markup, update.message)
    update.message.reply_text(f"Broadcast completed! Sent to {sent_chats} chats and {sent_users} users.")

def broadcast_to_all(bot, text_content, content_type, file_id, reply_markup, message):
    sent_chats = 0
    sent_users = 0

    for chat in get_served_chats():
        chat_id = chat["chat_id"]
        try:
            if content_type == 'photo':
                sent_message = bot.send_photo(chat_id=chat_id, photo=file_id, caption=text_content, reply_markup=reply_markup)
            else:
                sent_message = bot.send_message(chat_id=chat_id, text=text_content, reply_markup=reply_markup)
            
            if "-pin" in message.text:
                try:
                    sent_message.pin(disable_notification=True)
                except Exception as e:
                    logger.warning(f"Failed to pin message in chat {chat_id}: {e}")
                    continue
            elif "-pinloud" in message.text:
                try:
                    sent_message.pin(disable_notification=False)
                except Exception as e:
                    logger.warning(f"Failed to pin message with notification in chat {chat_id}: {e}")
                    continue
            sent_chats += 1
        except (TimedOut, NetworkError, RetryAfter, BadRequest, Unauthorized) as e:
            logger.error(f"Error broadcasting to chat {chat_id}: {e}")
            continue

    # Broadcasting to users
    for user in get_served_users():
        user_id = user["user_id"]
        try:
            if content_type == 'photo':
                bot.send_photo(chat_id=user_id, photo=file_id, caption=text_content, reply_markup=reply_markup)
            else:
                bot.send_message(chat_id=user_id, text=text_content, reply_markup=reply_markup)
            sent_users += 1
        except (TimedOut, NetworkError, RetryAfter, BadRequest, Unauthorized) as e:
            logger.error(f"Error broadcasting to user {user_id}: {e}")
            continue

    return sent_chats, sent_users
