"""
Configuration management for Telegram Bot System
"""

import os
import logging
from typing import List, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

@dataclass
class Config:
    """System configuration with validation"""
    
    def __init__(self):
        # Telegram API Configuration
        self.API_ID = os.getenv('TELEGRAM_API_ID')
        self.API_HASH = os.getenv('TELEGRAM_API_HASH')
        self.PHONE_NUMBER = os.getenv('TELEGRAM_PHONE')
        
        # Bot tokens
        self.BOT_TOKENS = self._get_bot_tokens()
        self.PRIMARY_BOT_TOKEN = self.BOT_TOKENS[0] if self.BOT_TOKENS else None
        self.ADMIN_BOT_TOKEN = os.getenv('ADMIN_BOT_TOKEN')
        
        # Redis configuration
        self.REDIS_URL = os.getenv('REDIS_URL', "redis://localhost:6379")
        self.USE_REDIS = os.getenv('USE_REDIS', 'false').lower() == 'true'
        
        # System settings optimized for 100+ pairs
        self.MAX_WORKERS = int(os.getenv('MAX_WORKERS', '50'))  # Increased for 100+ pairs
        self.DEBUG_MODE = os.getenv('DEBUG_MODE', 'false').lower() == 'true'
        self.SIMILARITY_THRESHOLD = int(os.getenv('SIMILARITY_THRESHOLD', '5'))
        
        # Database settings
        self.DATABASE_PATH = os.getenv('DATABASE_PATH', 'bot.db')
        self.DATABASE_BACKUP_INTERVAL = int(os.getenv('DATABASE_BACKUP_INTERVAL', '3600'))
        
        # Performance settings optimized for high throughput with 100+ pairs
        self.MESSAGE_QUEUE_SIZE = int(os.getenv('MESSAGE_QUEUE_SIZE', '50000'))  # Massive queue for 100+ pairs
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))  # More retries for reliability
        self.RETRY_DELAY = float(os.getenv('RETRY_DELAY', '0.3'))  # Faster retries for high volume
        self.BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50'))  # Larger batches for efficiency
        
        # Connection pooling and optimization for high scale
        self.CONNECTION_POOL_SIZE = int(os.getenv('CONNECTION_POOL_SIZE', '100'))
        self.MAX_CONCURRENT_DOWNLOADS = int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '25'))  # Enhanced for media processing
        self.CHUNK_PROCESSING_SIZE = int(os.getenv('CHUNK_PROCESSING_SIZE', '10'))  # Process pairs in chunks
        
        # Rate limiting
        self.RATE_LIMIT_MESSAGES = int(os.getenv('RATE_LIMIT_MESSAGES', '30'))
        self.RATE_LIMIT_WINDOW = int(os.getenv('RATE_LIMIT_WINDOW', '60'))
        
        # Health monitoring
        self.HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', '30'))
        self.MAX_MEMORY_MB = int(os.getenv('MAX_MEMORY_MB', '512'))
        self.MAX_CPU_PERCENT = float(os.getenv('MAX_CPU_PERCENT', '80.0'))
        
        # Dashboard settings
        self.DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', '5000'))
        self.DASHBOARD_HOST = os.getenv('DASHBOARD_HOST', '0.0.0.0')
        
        # Security settings
        self.ADMIN_USER_IDS = self._parse_admin_ids()
        self.WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET', '')
        
        # Filtering settings
        self.GLOBAL_BLOCKED_WORDS = self._parse_blocked_words()
        
        # Image processing settings
        self.ENABLE_IMAGE_PROCESSING = os.getenv('ENABLE_IMAGE_PROCESSING', 'true').lower() == 'true'
        
    def _parse_blocked_words(self) -> List[str]:
        """Parse global blocked words from environment"""
        blocked_words_str = os.getenv('GLOBAL_BLOCKED_WORDS', '')
        if blocked_words_str:
            return [word.strip().lower() for word in blocked_words_str.split(',') if word.strip()]
        return []
        
    def _get_bot_tokens(self) -> List[str]:
        """Extract and validate bot tokens"""
        tokens = []
        
        # Get tokens from environment
        for i in range(1, 11):  # Support up to 10 bot tokens
            token = os.getenv(f'TELEGRAM_BOT_TOKEN_{i}')
            if token and len(token) > 10:
                tokens.append(token)
        
        # Also check for single token
        single_token = os.getenv('TELEGRAM_BOT_TOKEN')
        if single_token and len(single_token) > 10 and single_token not in tokens:
            tokens.append(single_token)
        
        return tokens
    
    def _parse_admin_ids(self) -> List[int]:
        """Parse admin user IDs from environment"""
        admin_ids_str = os.getenv('ADMIN_USER_IDS', '')
        if not admin_ids_str:
            return []
        
        try:
            return [int(uid.strip()) for uid in admin_ids_str.split(',') if uid.strip()]
        except ValueError as e:
            logger.warning(f"Invalid admin user IDs format: {e}")
            return []
    
    def validate(self) -> bool:
        """Validate configuration"""
        errors = []
        
        # Required Telegram settings
        if not self.API_ID:
            errors.append("TELEGRAM_API_ID is required")
        if not self.API_HASH:
            errors.append("TELEGRAM_API_HASH is required")
        if not self.PHONE_NUMBER:
            errors.append("TELEGRAM_PHONE is required")
        if not self.BOT_TOKENS:
            errors.append("At least one bot token is required")
        if not self.ADMIN_BOT_TOKEN:
            logger.warning("ADMIN_BOT_TOKEN not set - admin commands will not be available")
        
        # Validate numeric ranges for high-scale operation
        if self.MAX_WORKERS < 1 or self.MAX_WORKERS > 200:  # Allow more workers for 100+ pairs
            errors.append("MAX_WORKERS must be between 1 and 200")
        if self.SIMILARITY_THRESHOLD < 1 or self.SIMILARITY_THRESHOLD > 20:
            errors.append("SIMILARITY_THRESHOLD must be between 1 and 20")
        if self.MESSAGE_QUEUE_SIZE < 5000:  # Optimal queue for 100+ pairs 
            logger.warning(f"MESSAGE_QUEUE_SIZE is {self.MESSAGE_QUEUE_SIZE}, consider increasing to 50000+ for optimal 100+ pairs performance")
        
        if errors:
            for error in errors:
                logger.error(f"Configuration error: {error}")
            return False
        
        return True

def validate_environment():
    """Validate environment configuration"""
    config = Config()
    
    if not config.validate():
        raise ValueError("Invalid configuration. Check environment variables.")
    
    logger.info("Environment configuration validated successfully")
    return config

def get_config() -> Config:
    """Get validated configuration instance"""
    return validate_environment()
