from collections import deque
import time
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class QuizQueue:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QuizQueue, cls).__new__(cls)
            cls._instance.queue = {}  # Changed to dict to track each chat separately
            cls._instance.processing = False
        return cls._instance
    
    def add_chat(self, chat_id, interval, last_quiz_time=None):
        """
        Add a chat to the queue with its specific interval and optional last quiz time
        """
        current_time = datetime.utcnow()
        
        if last_quiz_time:
            # Calculate next quiz time based on last quiz time and interval
            time_since_last = (current_time - last_quiz_time).total_seconds()
            remaining_time = interval - (time_since_last % interval)
            next_quiz_time = current_time + timedelta(seconds=remaining_time)
        else:
            next_quiz_time = current_time + timedelta(seconds=interval)
            
        self.queue[chat_id] = {
            'chat_id': chat_id,
            'interval': interval,
            'next_quiz_time': next_quiz_time,
            'last_quiz_time': last_quiz_time or current_time
        }
        logger.info(f"Added chat {chat_id} to queue with {interval}s interval. Next quiz at {next_quiz_time}")
    
    def process_queue(self, context):
        """Process queued chats and send quizzes based on individual intervals"""
        if self.processing:
            return
            
        self.processing = True
        current_time = datetime.utcnow()
        processed_chats = []
        
        try:
            # Check each chat in the queue
            for chat_id, chat_data in list(self.queue.items()):
                if current_time >= chat_data['next_quiz_time']:
                    try:
                        from quiz_handler import send_quiz_logic
                        send_quiz_logic(context, chat_id)
                        
                        # Update last quiz time and calculate next quiz exactly
                        last_time = chat_data['next_quiz_time']  # Use scheduled time instead of current time
                        interval = chat_data['interval']
                        
                        # Calculate next quiz time based on the scheduled time
                        self.queue[chat_id]['last_quiz_time'] = last_time
                        self.queue[chat_id]['next_quiz_time'] = last_time + timedelta(seconds=interval)
                        
                        processed_chats.append(chat_id)
                        logger.info(f"Sent quiz to chat {chat_id}. Next quiz at {self.queue[chat_id]['next_quiz_time']}")
                        
                    except Exception as e:
                        logger.error(f"Failed to send quiz to chat {chat_id}: {e}")
                        if "bot is not a member" in str(e).lower():
                            self.remove_chat(chat_id)
                            logger.warning(f"Removed chat {chat_id} from queue due to bot removal")
                
            if processed_chats:
                logger.info(f"Processed {len(processed_chats)} chats in queue")
                
        finally:
            self.processing = False

    def remove_chat(self, chat_id):
        """Remove a chat from the queue"""
        if chat_id in self.queue:
            del self.queue[chat_id]
            logger.info(f"Removed chat {chat_id} from queue")
    
    def send_quiz_with_rate_limit(self, context, chat_id):
        """Send quiz with rate limiting and error handling"""
        MAX_RETRIES = 3
        RETRY_DELAY = 5
        
        for attempt in range(MAX_RETRIES):
            try:
                from quiz_handler import send_quiz_logic
                send_quiz_logic(context, chat_id)
                break
            except RetryAfter as e:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Rate limit hit for chat {chat_id}, waiting {e.retry_after}s")
                    time.sleep(e.retry_after)
                    continue
                raise
            except TimedOut:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Timeout for chat {chat_id}, retrying in {RETRY_DELAY}s")
                    time.sleep(RETRY_DELAY)
                    continue
                raise
