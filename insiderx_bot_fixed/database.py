"""
Database management for Telegram Bot System
"""

import sqlite3
import json
import logging
import asyncio
import aiosqlite
import os
import shutil
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

@dataclass
class MessagePair:
    """Data class for message copying pairs"""
    id: int
    source_chat_id: int
    destination_chat_id: int
    name: str
    status: str = "active"
    assigned_bot_index: int = 0
    bot_token_id: Optional[int] = None
    filters: Dict[str, Any] = field(default_factory=dict)
    stats: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[str] = None

    def __post_init__(self):
        if not self.filters:
            self.filters = {
                "blocked_words": [],
                "remove_mentions": False,
                "mention_placeholder": "[User]",
                "preserve_replies": True,
                "sync_edits": True,
                "sync_deletes": True,
                "header_regex": "",
                "footer_regex": "",
                "watermark_enabled": False,
                "watermark_text": "",
                "min_message_length": 0,
                "max_message_length": 0,
                "allowed_media_types": ["photo", "video", "document", "audio", "voice"],
                "block_forwards": False,
                "block_links": False,
                "custom_regex_filters": []
            }

        if not self.stats:
            self.stats = {
                "messages_copied": 0,
                "messages_filtered": 0,
                "errors": 0,
                "replies_preserved": 0,
                "edits_synced": 0,
                "deletes_synced": 0,
                "mentions_removed": 0,
                "headers_removed": 0,
                "footers_removed": 0,
                "last_activity": None
            }

@dataclass
class MessageMapping:
    """Message mapping data structure"""
    id: int
    source_message_id: int
    destination_message_id: int
    pair_id: int
    bot_index: int
    source_chat_id: int
    destination_chat_id: int
    message_type: str = "text"
    has_media: bool = False
    created_at: Optional[str] = None
    last_updated: Optional[str] = None
    is_reply: bool = False
    reply_to_source_id: Optional[int] = None
    reply_to_dest_id: Optional[int] = None

class DatabaseManager:
    """Production-ready database manager with async support"""
    
    def __init__(self, db_path: str = "bot.db"):
        self.db_path = db_path
        self.backup_path = f"{db_path}.backup"
        self._connection_pool = []
        self._pool_size = 5
        
    async def initialize(self):
        """Initialize database with complete schema"""
        try:
            # Create database directory if needed
            os.makedirs(os.path.dirname(self.db_path) if os.path.dirname(self.db_path) else '.', exist_ok=True)
            
            # Initialize schema
            await self._init_schema()
            
            # Create backup
            await self._create_backup()
            
            logger.info(f"Database initialized: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def _init_schema(self):
        """Initialize complete database schema"""
        async with aiosqlite.connect(self.db_path) as conn:
            # Enable foreign keys and WAL mode
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute("PRAGMA journal_mode = WAL")
            
            # Bot tokens table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bot_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    token TEXT NOT NULL,
                    username TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_used TEXT,
                    usage_count INTEGER DEFAULT 0
                )
            ''')

            # Pairs table (updated with bot_token_id)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS pairs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id INTEGER NOT NULL,
                    destination_chat_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT DEFAULT 'active',
                    assigned_bot_index INTEGER DEFAULT 0,
                    bot_token_id INTEGER,
                    filters TEXT DEFAULT '{}',
                    stats TEXT DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_chat_id, destination_chat_id),
                    FOREIGN KEY (bot_token_id) REFERENCES bot_tokens (id)
                )
            ''')

            # Message mapping table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS message_mapping (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_message_id INTEGER NOT NULL,
                    destination_message_id INTEGER NOT NULL,
                    pair_id INTEGER NOT NULL,
                    bot_index INTEGER NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    destination_chat_id INTEGER NOT NULL,
                    message_type TEXT DEFAULT 'text',
                    has_media BOOLEAN DEFAULT FALSE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
                    is_reply BOOLEAN DEFAULT FALSE,
                    reply_to_source_id INTEGER,
                    reply_to_dest_id INTEGER,
                    UNIQUE(source_message_id, pair_id),
                    FOREIGN KEY(pair_id) REFERENCES pairs(id) ON DELETE CASCADE
                )
            ''')

            # Blocked images table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS blocked_images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phash TEXT NOT NULL,
                    pair_id INTEGER,
                    description TEXT DEFAULT '',
                    blocked_by TEXT DEFAULT '',
                    usage_count INTEGER DEFAULT 0,
                    block_scope TEXT DEFAULT 'pair',
                    similarity_threshold INTEGER DEFAULT 5,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(phash, pair_id)
                )
            ''')

            # System settings table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Bot metrics table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS bot_metrics (
                    bot_index INTEGER PRIMARY KEY,
                    messages_processed INTEGER DEFAULT 0,
                    success_rate REAL DEFAULT 1.0,
                    avg_processing_time REAL DEFAULT 1.0,
                    current_load INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    last_activity TEXT DEFAULT CURRENT_TIMESTAMP,
                    rate_limit_until TEXT DEFAULT '',
                    consecutive_failures INTEGER DEFAULT 0
                )
            ''')

            # Error logs table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS error_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    error_type TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    pair_id INTEGER,
                    bot_index INTEGER,
                    stack_trace TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # User subscriptions table for timed access management
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_subscriptions (
                    user_id INTEGER NOT NULL PRIMARY KEY,
                    expires_at TEXT NOT NULL,
                    added_by INTEGER,
                    notes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create indexes for performance
            indexes = [
                'CREATE INDEX IF NOT EXISTS idx_message_mapping_source ON message_mapping(source_message_id, pair_id)',
                'CREATE INDEX IF NOT EXISTS idx_message_mapping_dest ON message_mapping(destination_message_id, pair_id)',
                'CREATE INDEX IF NOT EXISTS idx_message_mapping_reply ON message_mapping(reply_to_source_id)',
                'CREATE INDEX IF NOT EXISTS idx_message_mapping_pair ON message_mapping(pair_id)',
                'CREATE INDEX IF NOT EXISTS idx_blocked_images_global ON blocked_images(phash, block_scope)',
                'CREATE INDEX IF NOT EXISTS idx_blocked_images_scope ON blocked_images(block_scope)',
                'CREATE INDEX IF NOT EXISTS idx_error_logs_time ON error_logs(created_at DESC)',
                'CREATE INDEX IF NOT EXISTS idx_pairs_status ON pairs(status)',
                'CREATE INDEX IF NOT EXISTS idx_pairs_bot ON pairs(assigned_bot_index)',
                'CREATE INDEX IF NOT EXISTS idx_user_subscriptions_expires ON user_subscriptions(expires_at)',
                'CREATE INDEX IF NOT EXISTS idx_user_subscriptions_user_id ON user_subscriptions(user_id)'
            ]
            
            for index_sql in indexes:
                await conn.execute(index_sql)

            # Initialize default settings
            default_settings = [
                ('system_paused', 'false'),
                ('global_blocks', '{"words": [], "patterns": []}'),
                ('max_message_size', '4096'),
                ('rate_limit_enabled', 'true'),
                ('auto_backup_enabled', 'true'),
                ('maintenance_mode', 'false')
            ]
            
            for key, value in default_settings:
                await conn.execute(
                    'INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)',
                    (key, value)
                )

            await conn.commit()
            logger.debug("Database schema initialized")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get database connection from pool"""
        conn = None
        try:
            conn = await aiosqlite.connect(self.db_path)
            await conn.execute("PRAGMA foreign_keys = ON")
            yield conn
        finally:
            if conn:
                await conn.close()
    
    async def create_pair(self, source_chat_id: int, destination_chat_id: int,
                         name: str, bot_index: int = 0, bot_token_id: Optional[int] = None) -> int:
        """Create new message pair"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute('''
                    INSERT INTO pairs (source_chat_id, destination_chat_id, name, assigned_bot_index, bot_token_id, filters, stats)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    source_chat_id, destination_chat_id, name, bot_index, bot_token_id,
                    json.dumps(MessagePair(0, 0, 0, "").filters),
                    json.dumps(MessagePair(0, 0, 0, "").stats)
                ))
                pair_id = cursor.lastrowid
                await conn.commit()
                
                logger.info(f"Created pair {pair_id}: {name} ({source_chat_id} -> {destination_chat_id})")
                return pair_id or 0
                
        except sqlite3.IntegrityError:
            raise ValueError(f"Pair already exists: {source_chat_id} -> {destination_chat_id}")
        except Exception as e:
            logger.error(f"Failed to create pair: {e}")
            raise

    async def get_pair(self, pair_id: int) -> Optional[MessagePair]:
        """Get pair by ID"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute('''
                    SELECT id, source_chat_id, destination_chat_id, name, status, 
                           assigned_bot_index, bot_token_id, filters, stats, created_at
                    FROM pairs WHERE id = ?
                ''', (pair_id,))
                row = await cursor.fetchone()
                
                if row:
                    try:
                        filters_data = json.loads(row[7]) if row[7] else {}
                    except json.JSONDecodeError:
                        filters_data = {}
                    
                    try:
                        stats_data = json.loads(row[8]) if row[8] else {}
                    except json.JSONDecodeError:
                        stats_data = {}
                    
                    return MessagePair(
                        id=row[0],
                        source_chat_id=row[1],
                        destination_chat_id=row[2],
                        name=row[3],
                        status=row[4],
                        assigned_bot_index=row[5],
                        bot_token_id=row[6],
                        filters=filters_data,
                        stats=stats_data,
                        created_at=row[9]
                    )
        except Exception as e:
            logger.error(f"Failed to get pair {pair_id}: {e}")
        return None

    async def get_pair_by_id(self, pair_id: int) -> Optional[MessagePair]:
        """Get pair by ID (alias for get_pair for compatibility)"""
        return await self.get_pair(pair_id)

    async def get_all_pairs(self) -> List[MessagePair]:
        """Get all pairs"""
        pairs = []
        try:
            async with self.get_connection() as conn:
                # Use explicit column selection to ensure correct order
                cursor = await conn.execute('''
                    SELECT id, source_chat_id, destination_chat_id, name, status, 
                           assigned_bot_index, bot_token_id, filters, stats, created_at
                    FROM pairs ORDER BY id
                ''')
                async for row in cursor:
                    try:
                        # Safely parse JSON fields with better error handling
                        filters_data = {}
                        if row[7]:
                            try:
                                filters_data = json.loads(row[7])
                            except json.JSONDecodeError as je:
                                logger.warning(f"Failed to parse filters for pair {row[0]}: {je}")
                                filters_data = {}
                        
                        stats_data = {}
                        if row[8]:
                            try:
                                stats_data = json.loads(row[8])
                            except json.JSONDecodeError as je:
                                logger.warning(f"Failed to parse stats for pair {row[0]}: {je}")
                                stats_data = {}
                        
                        pairs.append(MessagePair(
                            id=row[0],
                            source_chat_id=row[1],
                            destination_chat_id=row[2],
                            name=row[3],
                            status=row[4],
                            assigned_bot_index=row[5],
                            bot_token_id=row[6],
                            filters=filters_data,
                            stats=stats_data,
                            created_at=row[9]
                        ))
                    except Exception as pair_error:
                        logger.error(f"Failed to process pair row {row[0] if row else 'unknown'}: {pair_error}")
                        continue
                        
        except Exception as e:
            logger.error(f"Failed to get pairs: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
        return pairs

    async def update_pair(self, pair: MessagePair):
        """Update pair"""
        try:
            async with self.get_connection() as conn:
                await conn.execute('''
                    UPDATE pairs SET
                        name = ?, status = ?, assigned_bot_index = ?, bot_token_id = ?,
                        filters = ?, stats = ?
                    WHERE id = ?
                ''', (
                    pair.name, pair.status, pair.assigned_bot_index, pair.bot_token_id,
                    json.dumps(pair.filters), json.dumps(pair.stats), pair.id
                ))
                await conn.commit()
                logger.debug(f"Updated pair {pair.id}")
        except Exception as e:
            logger.error(f"Failed to update pair {pair.id}: {e}")
            raise

    async def delete_pair(self, pair_id: int):
        """Delete pair and all related data"""
        try:
            async with self.get_connection() as conn:
                # Delete related message mappings (cascade should handle this)
                await conn.execute('DELETE FROM message_mapping WHERE pair_id = ?', (pair_id,))
                # Delete the pair
                await conn.execute('DELETE FROM pairs WHERE id = ?', (pair_id,))
                await conn.commit()
                logger.info(f"Deleted pair {pair_id}")
        except Exception as e:
            logger.error(f"Failed to delete pair {pair_id}: {e}")
            raise

    async def update_pair_filter(self, pair_id: int, key: str, value: Any):
        """Update specific filter for a pair"""
        try:
            pair = await self.get_pair(pair_id)
            if not pair:
                raise ValueError(f"Pair {pair_id} not found")
            
            # Update the filter
            filters = pair.filters or {}
            filters[key] = value
            
            # Save updated filters
            async with self.get_connection() as conn:
                await conn.execute(
                    "UPDATE pairs SET filters = ? WHERE id = ?",
                    (json.dumps(filters), pair_id)
                )
                await conn.commit()
                
            logger.debug(f"Updated filter '{key}' for pair {pair_id}: {value}")
            
        except Exception as e:
            logger.error(f"Failed to update pair filter: {e}")
            raise

    async def save_message_mapping(self, mapping: MessageMapping):
        """Save message mapping"""
        try:
            async with self.get_connection() as conn:
                await conn.execute('''
                    INSERT OR REPLACE INTO message_mapping 
                    (source_message_id, destination_message_id, pair_id, bot_index,
                     source_chat_id, destination_chat_id, message_type, has_media,
                     is_reply, reply_to_source_id, reply_to_dest_id, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    mapping.source_message_id, mapping.destination_message_id,
                    mapping.pair_id, mapping.bot_index, mapping.source_chat_id,
                    mapping.destination_chat_id, mapping.message_type, mapping.has_media,
                    mapping.is_reply, mapping.reply_to_source_id, mapping.reply_to_dest_id
                ))
                await conn.commit()
        except Exception as e:
            logger.error(f"Failed to save message mapping: {e}")
            raise

    async def get_message_mapping(self, source_message_id: int, pair_id: int) -> Optional[MessageMapping]:
        """Get message mapping"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute('''
                    SELECT * FROM message_mapping 
                    WHERE source_message_id = ? AND pair_id = ?
                ''', (source_message_id, pair_id))
                row = await cursor.fetchone()
                
                if row:
                    return MessageMapping(*row)
        except Exception as e:
            logger.error(f"Failed to get message mapping: {e}")
        return None

    async def get_message_mappings_for_source_messages(self, source_message_ids: List[int]) -> List[MessageMapping]:
        """Fetch existing mappings for a batch of deleted source message IDs."""
        ids = list(dict.fromkeys(int(message_id) for message_id in source_message_ids if message_id is not None))
        if not ids:
            return []
        try:
            async with self.get_connection() as conn:
                placeholders = ",".join("?" for _ in ids)
                cursor = await conn.execute(
                    f"SELECT * FROM message_mapping WHERE source_message_id IN ({placeholders})",
                    ids,
                )
                rows = await cursor.fetchall()
                return [MessageMapping(*row) for row in rows]
        except Exception as e:
            logger.error("Failed to batch-fetch message mappings: %s", e, exc_info=True)
            raise

    async def delete_message_mapping(self, source_message_id: int, pair_id: int) -> bool:
        """Delete a source-to-destination mapping after confirmed destination deletion."""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute(
                    "DELETE FROM message_mapping WHERE source_message_id = ? AND pair_id = ?",
                    (source_message_id, pair_id),
                )
                await conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error("Failed to delete message mapping (%s, pair %s): %s", source_message_id, pair_id, e, exc_info=True)
            raise

    async def log_error(self, error_type: str, error_message: str, 
                       pair_id: Optional[int] = None, bot_index: Optional[int] = None,
                       stack_trace: Optional[str] = None):
        """Log error to database"""
        try:
            async with self.get_connection() as conn:
                await conn.execute('''
                    INSERT INTO error_logs (error_type, error_message, pair_id, bot_index, stack_trace)
                    VALUES (?, ?, ?, ?, ?)
                ''', (error_type, error_message, pair_id, bot_index, stack_trace))
                await conn.commit()
        except Exception as e:
            logger.error(f"Failed to log error: {e}")

    async def get_setting(self, key: str, default: Optional[str] = None) -> str:
        """Get system setting"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute('SELECT value FROM settings WHERE key = ?', (key,))
                row = await cursor.fetchone()
                return row[0] if row else (default or "")
        except Exception as e:
            logger.error(f"Failed to get setting {key}: {e}")
            return default or ""

    async def set_setting(self, key: str, value: str):
        """Set system setting"""
        try:
            async with self.get_connection() as conn:
                await conn.execute('''
                    INSERT OR REPLACE INTO settings (key, value, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                ''', (key, value))
                await conn.commit()
        except Exception as e:
            logger.error(f"Failed to set setting {key}: {e}")
            raise

    async def _create_backup(self):
        """Create database backup"""
        try:
            if os.path.exists(self.db_path):
                shutil.copy2(self.db_path, self.backup_path)
                logger.debug(f"Database backup created: {self.backup_path}")
        except Exception as e:
            logger.warning(f"Failed to create backup: {e}")

    async def cleanup_old_data(self, days: int = 30):
        """Clean up old data"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            async with self.get_connection() as conn:
                # Clean old error logs
                cursor = await conn.execute(
                    'DELETE FROM error_logs WHERE created_at < ?',
                    (cutoff_date,)
                )
                deleted_errors = cursor.rowcount
                
                # Clean old message mappings for inactive pairs
                cursor = await conn.execute('''
                    DELETE FROM message_mapping 
                    WHERE created_at < ? AND pair_id IN (
                        SELECT id FROM pairs WHERE status = 'inactive'
                    )
                ''', (cutoff_date,))
                deleted_mappings = cursor.rowcount
                
                await conn.commit()
                logger.info(f"Cleaned up data older than {days} days: {deleted_errors} errors, {deleted_mappings} mappings")
                
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")

    async def get_stats(self) -> Dict[str, Any]:
        """Get system statistics"""
        try:
            async with self.get_connection() as conn:
                stats = {}
                
                # Pair counts
                cursor = await conn.execute('SELECT COUNT(*) FROM pairs')
                row = await cursor.fetchone()
                stats['total_pairs'] = row[0] if row else 0
                
                cursor = await conn.execute('SELECT COUNT(*) FROM pairs WHERE status = "active"')
                row = await cursor.fetchone()
                stats['active_pairs'] = row[0] if row else 0
                
                # Message counts
                cursor = await conn.execute('SELECT COUNT(*) FROM message_mapping')
                row = await cursor.fetchone()
                stats['total_messages'] = row[0] if row else 0
                
                # Recent activity (last 24 hours)
                yesterday = (datetime.now() - timedelta(days=1)).isoformat()
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM message_mapping WHERE created_at > ?',
                    (yesterday,)
                )
                row = await cursor.fetchone()
                stats['messages_24h'] = row[0] if row else 0
                
                # Error counts
                cursor = await conn.execute('SELECT COUNT(*) FROM error_logs WHERE created_at > ?', (yesterday,))
                row = await cursor.fetchone()
                stats['errors_24h'] = row[0] if row else 0
                
                # Database size
                cursor = await conn.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
                row = await cursor.fetchone()
                db_size = row[0] if row else 0
                stats['database_size_mb'] = round(db_size / (1024 * 1024), 2)
                
                return stats
                
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {}

    async def cleanup_old_messages(self, cutoff_time: float) -> int:
        """Clean up old message mappings"""
        try:
            cutoff_date = datetime.fromtimestamp(cutoff_time).isoformat()
            async with self.get_connection() as conn:
                cursor = await conn.execute(
                    'DELETE FROM message_mapping WHERE created_at < ?',
                    (cutoff_date,)
                )
                deleted_count = cursor.rowcount
                await conn.commit()
                logger.info(f"Cleaned up {deleted_count} old message mappings")
                return deleted_count
        except Exception as e:
            logger.error(f"Failed to cleanup old messages: {e}")
            return 0

    async def cleanup_old_errors(self, cutoff_time: float) -> int:
        """Clean up old error logs"""
        try:
            cutoff_date = datetime.fromtimestamp(cutoff_time).isoformat()
            async with self.get_connection() as conn:
                cursor = await conn.execute(
                    'DELETE FROM error_logs WHERE created_at < ?',
                    (cutoff_date,)
                )
                deleted_count = cursor.rowcount
                await conn.commit()
                logger.info(f"Cleaned up {deleted_count} old error logs")
                return deleted_count
        except Exception as e:
            logger.error(f"Failed to cleanup old errors: {e}")
            return 0

    async def count_old_messages(self, cutoff_time: float) -> int:
        """Count old message mappings for preview"""
        try:
            cutoff_date = datetime.fromtimestamp(cutoff_time).isoformat()
            async with self.get_connection() as conn:
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM message_mapping WHERE created_at < ?',
                    (cutoff_date,)
                )
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Failed to count old messages: {e}")
            return 0

    async def count_old_errors(self, cutoff_time: float) -> int:
        """Count old error logs for preview"""
        try:
            cutoff_date = datetime.fromtimestamp(cutoff_time).isoformat()
            async with self.get_connection() as conn:
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM error_logs WHERE created_at < ?',
                    (cutoff_date,)
                )
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Failed to count old errors: {e}")
            return 0

    # Bot Token Management Methods
    async def save_bot_token(self, name: str, token: str, username: Optional[str] = None) -> int:
        """Save a bot token to database"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute('''
                    INSERT OR REPLACE INTO bot_tokens (name, token, username, is_active, created_at)
                    VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)
                ''', (name, token, username))
                token_id = cursor.lastrowid
                await conn.commit()
                logger.info(f"Saved bot token: {name} ({username})")
                return token_id or 0
        except Exception as e:
            logger.error(f"Failed to save bot token: {e}")
            raise

    async def get_bot_tokens(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get all bot tokens"""
        try:
            async with self.get_connection() as conn:
                query = "SELECT * FROM bot_tokens"
                if active_only:
                    query += " WHERE is_active = 1"
                query += " ORDER BY created_at DESC"
                
                cursor = await conn.execute(query)
                rows = await cursor.fetchall()
                
                tokens = []
                for row in rows:
                    tokens.append({
                        'id': row[0],
                        'name': row[1],
                        'token': row[2],
                        'username': row[3],
                        'is_active': bool(row[4]),
                        'created_at': row[5],
                        'last_used': row[6],
                        'usage_count': row[7]
                    })
                return tokens
        except Exception as e:
            logger.error(f"Failed to get bot tokens: {e}")
            return []
    
    async def toggle_bot_token(self, token_id: int) -> bool:
        """Toggle bot token active status"""
        try:
            async with self.get_connection() as conn:
                # Get current status
                cursor = await conn.execute(
                    "SELECT is_active FROM bot_tokens WHERE id = ?", (token_id,)
                )
                row = await cursor.fetchone()
                if not row:
                    return False
                
                new_status = not bool(row[0])
                cursor = await conn.execute(
                    "UPDATE bot_tokens SET is_active = ? WHERE id = ?", 
                    (new_status, token_id)
                )
                await conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to toggle bot token: {e}")
            return False
    
    async def get_bot_token_by_id(self, token_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific bot token by ID"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute(
                    "SELECT * FROM bot_tokens WHERE id = ?", (token_id,)
                )
                row = await cursor.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'name': row[1],
                        'token': row[2],
                        'username': row[3],
                        'is_active': bool(row[4]),
                        'created_at': row[5],
                        'last_used': row[6],
                        'usage_count': row[7]
                    }
                return None
        except Exception as e:
            logger.error(f"Failed to get bot token: {e}")
            return None

    async def update_bot_token_usage(self, token_id: int):
        """Update bot token usage statistics"""
        try:
            async with self.get_connection() as conn:
                await conn.execute('''
                    UPDATE bot_tokens 
                    SET usage_count = usage_count + 1, last_used = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (token_id,))
                await conn.commit()
        except Exception as e:
            logger.error(f"Failed to update bot token usage: {e}")

    async def delete_bot_token(self, token_id: int) -> bool:
        """Delete a bot token"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute(
                    "DELETE FROM bot_tokens WHERE id = ?", (token_id,)
                )
                await conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to delete bot token: {e}")
            return False

    async def toggle_bot_token_status(self, token_id: int) -> bool:
        """Toggle bot token active status"""
        try:
            async with self.get_connection() as conn:
                # Get current status
                cursor = await conn.execute(
                    "SELECT is_active FROM bot_tokens WHERE id = ?", (token_id,)
                )
                row = await cursor.fetchone()
                if not row:
                    return False
                
                new_status = not bool(row[0])
                cursor = await conn.execute(
                    "UPDATE bot_tokens SET is_active = ? WHERE id = ?", 
                    (new_status, token_id)
                )
                await conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to toggle bot token status: {e}")
            return False

    # Subscription management methods
    async def get_all_unique_destinations(self) -> List[Tuple[int, Optional[int]]]:
        """Get all unique destination_chat_id and bot_token_id from pairs"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute('''
                    SELECT DISTINCT destination_chat_id, bot_token_id
                    FROM pairs 
                    WHERE status = 'active'
                ''')
                rows = await cursor.fetchall()
                return [(int(row[0]), row[1] if row[1] is not None else None) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get unique destinations: {e}")
            return []

    async def get_bot_token_string_by_id(self, token_id: int) -> Optional[str]:
        """Get bot token string by ID"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute(
                    "SELECT token FROM bot_tokens WHERE id = ? AND is_active = 1", 
                    (token_id,)
                )
                row = await cursor.fetchone()
                return row[0] if row else None
        except Exception as e:
            logger.error(f"Failed to get bot token by ID {token_id}: {e}")
            return None

    async def add_or_update_subscription(self, user_id: int, expires_at: str, added_by: int, notes: str = "") -> bool:
        """Add or update user subscription"""
        try:
            async with self.get_connection() as conn:
                await conn.execute('''
                    INSERT OR REPLACE INTO user_subscriptions 
                    (user_id, expires_at, added_by, notes, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (user_id, expires_at, added_by, notes))
                await conn.commit()
                logger.info(f"Added/updated subscription for user {user_id}, expires at {expires_at}")
                return True
        except Exception as e:
            logger.error(f"Failed to add/update subscription for user {user_id}: {e}")
            return False

    async def renew_subscription(self, user_id: int, days: int) -> bool:
        """Extend subscription by specified days"""
        try:
            async with self.get_connection() as conn:
                # Check if subscription exists
                cursor = await conn.execute(
                    "SELECT expires_at FROM user_subscriptions WHERE user_id = ?", 
                    (user_id,)
                )
                row = await cursor.fetchone()
                if not row:
                    return False
                
                # Calculate new expiry date
                current_expires = datetime.fromisoformat(row[0])
                new_expires = current_expires + timedelta(days=days)
                
                await conn.execute('''
                    UPDATE user_subscriptions 
                    SET expires_at = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                ''', (new_expires.isoformat(), user_id))
                await conn.commit()
                logger.info(f"Renewed subscription for user {user_id} by {days} days, new expiry: {new_expires}")
                return True
        except Exception as e:
            logger.error(f"Failed to renew subscription for user {user_id}: {e}")
            return False

    async def get_expired_subscriptions(self, now: str) -> List[Tuple[int, str]]:
        """Get users with expired subscriptions"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute('''
                    SELECT user_id, expires_at FROM user_subscriptions 
                    WHERE expires_at <= ?
                ''', (now,))
                rows = await cursor.fetchall()
                return [(int(row[0]), str(row[1])) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get expired subscriptions: {e}")
            return []

    async def delete_subscription(self, user_id: int) -> bool:
        """Delete user subscription"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute(
                    "DELETE FROM user_subscriptions WHERE user_id = ?", 
                    (user_id,)
                )
                await conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Failed to delete subscription for user {user_id}: {e}")
            return False

    async def get_active_subscriptions(self) -> List[Tuple[int, str, str, str]]:
        """Get all active subscriptions with details"""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute('''
                    SELECT user_id, expires_at, added_by, notes
                    FROM user_subscriptions 
                    ORDER BY expires_at ASC
                ''')
                rows = await cursor.fetchall()
                return [(int(row[0]), str(row[1]), str(row[2]), str(row[3] or "")) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get active subscriptions: {e}")
            return []

    async def close(self):
        """Close database connections"""
        try:
            # Create final backup
            await self._create_backup()
            logger.info("Database connections closed")
        except Exception as e:
            logger.error(f"Error closing database: {e}")
