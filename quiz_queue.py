from collections import deque
import time
import logging
from datetime import datetime
from telegram.error import RetryAfter, TimedOut
from chat_data_handler import load_chat_data

logger = logging.getLogger(__name__)

class QuizQueue:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(QuizQueue, cls).__new__(cls)
            cls._instance.queue = deque()
            cls._instance.processing = False
        return cls._instance
    
    def add_chat(self, chat_id, interval):
        """Add a chat to the queue with its next quiz time"""
        self.queue.append({
            'chat_id': chat_id,
            'next_quiz_time': time.time() + interval
        })
        logger.info(f"Added chat {chat_id} to queue with interval {interval}")
    
    def process_queue(self, context):
        """Process queued chats and send quizzes"""
        if self.processing:
            return
            
        self.processing = True
        current_time = time.time()
        processed_count = 0
        
        try:
            while self.queue and self.queue[0]['next_quiz_time'] <= current_time:
                if processed_count >= 100:  # Process max 100 chats per batch
                    break
                    
                chat = self.queue.popleft()
                try:
                    self.send_quiz_with_rate_limit(context, chat['chat_id'])
                    processed_count += 1
                    
                    # Re-add to queue if still active
                    chat_data = load_chat_data(chat['chat_id'])
                    if chat_data.get('active', False):
                        chat['next_quiz_time'] = time.time() + chat_data.get('interval', 30)
                        self.queue.append(chat)
                        
                except Exception as e:
                    logger.error(f"Failed to process chat {chat['chat_id']}: {e}")
                    # Re-add with delay if temporary error
                    if isinstance(e, (RetryAfter, TimedOut)):
                        chat['next_quiz_time'] = time.time() + 60  # 1-minute delay
                        self.queue.append(chat)
        
        finally:
            self.processing = False
            if processed_count > 0:
                logger.info(f"Processed {processed_count} chats from queue")
    
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
