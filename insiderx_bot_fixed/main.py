"""
Telegram Message Copying Bot - Production Ready
Complete multi-bot message copying system with advanced filtering
"""

import asyncio
import logging
import signal
import sys
import os
import time
from typing import Dict, List, Optional
from datetime import datetime

# Ensure proper asyncio setup for the bot system
try:
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    
    # Create and set a new event loop if needed
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Initialize async libraries safely
    import sniffio
    
except Exception as e:
    print(f"Asyncio setup warning: {e}")

from config import Config, validate_environment
from database import DatabaseManager
from bot_manager import BotManager
from health_monitor import HealthMonitor

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/bot.log', encoding='utf-8')
    ]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

class TelegramBotSystem:
    """Main system coordinator for the Telegram bot network"""
    
    def __init__(self):
        self.config = Config()
        self.db_manager = None
        self.bot_manager = None
        self.health_monitor = None
        self.running = False
        self._shutdown_event = asyncio.Event()
        self._start_time = time.time()
        
    async def initialize(self):
        """Initialize all system components"""
        try:
            logger.info("Starting Telegram Bot System initialization...")
            
            # Validate environment
            validate_environment()
            
            # Create logs directory
            os.makedirs('logs', exist_ok=True)
            
            # Initialize database
            self.db_manager = DatabaseManager(self.config.DATABASE_PATH)
            await self.db_manager.initialize()
            
            # Initialize bot manager
            self.bot_manager = BotManager(self.db_manager, self.config)
            await self.bot_manager.initialize()
            
            # Initialize health monitor
            self.health_monitor = HealthMonitor(self.bot_manager, self.db_manager)
            
            logger.info("System initialization completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize system: {e}", exc_info=True)
            raise
    
    async def start(self):
        """Start all system components"""
        try:
            self.running = True
            
            # Start components
            if self.bot_manager:
                await self.bot_manager.start()
            if self.health_monitor:
                await self.health_monitor.start()
            
            logger.info("=== Telegram Bot System Started ===")
            logger.info(f"Active bot tokens: {len(self.config.BOT_TOKENS)}")
            logger.info(f"Bot management available via Telegram commands")
            logger.info(f"Debug mode: {self.config.DEBUG_MODE}")
            
            # Wait for shutdown signal
            await self._shutdown_event.wait()
            
        except Exception as e:
            logger.error(f"Error during system operation: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Gracefully shutdown all components"""
        if not self.running:
            return
            
        logger.info("Initiating system shutdown...")
        self.running = False
        
        try:
            # Stop components in reverse order               
            if self.health_monitor:
                await self.health_monitor.stop()
                
            if self.bot_manager:
                await self.bot_manager.stop()
                
            if self.db_manager:
                await self.db_manager.close()
                
            logger.info("System shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self._shutdown_event.set()
    
    def get_uptime(self) -> int:
        """Get system uptime in seconds"""
        return int(time.time() - self._start_time)

async def main():
    """Main entry point"""
    system = None
    
    try:
        # Create and initialize system
        system = TelegramBotSystem()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, system.signal_handler)
        signal.signal(signal.SIGTERM, system.signal_handler)
        
        # Initialize and start
        await system.initialize()
        await system.start()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1
    finally:
        if system:
            await system.shutdown()
    
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except Exception as e:
        logger.error(f"Failed to start system: {e}", exc_info=True)
        sys.exit(1)
