"""
Message Processor - Handles message filtering and copying logic
"""

import asyncio
import logging
import os
import re
import tempfile
import time
from typing import Dict, List, Optional, Any, Tuple
from io import BytesIO
from datetime import datetime

from telegram import (Bot, InputMediaPhoto, InputMediaVideo, InputMediaDocument,
                       InputMediaAudio, InputMediaAnimation, MessageEntity, InputFile)
from telegram.error import TelegramError, BadRequest, Forbidden, RetryAfter
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument
)
from telethon.errors import FloodWaitError

from database import DatabaseManager, MessagePair, MessageMapping
from filters import MessageFilter
from image_handler import ImageHandler
from config import Config

logger = logging.getLogger(__name__)

class MessageProcessor:
    """Advanced message processor with filtering and media handling"""
    
    def __init__(self, db_manager: DatabaseManager, config: Config):
        self.db_manager = db_manager
        self.config = config
        self.message_filter = MessageFilter(db_manager, config)
        self.image_handler = ImageHandler(db_manager, config)
        
        # Processing statistics
        self.stats = {
            'messages_processed': 0,
            'messages_copied': 0,
            'messages_filtered': 0,
            'errors': 0,
            'media_processed': 0
        }
    
    async def initialize(self):
        """Initialize message processor components"""
        await self.message_filter.initialize()
    
    async def process_new_message(self, event, pair: MessagePair, bot: Bot, bot_index: int) -> bool:
        """Process new message from source chat with enhanced formatting and media support"""
        media_info = None
        try:
            self.stats['messages_processed'] += 1

            # Telethon may redeliver updates; do not create duplicate destination messages.
            existing_mapping = await self.db_manager.get_message_mapping(event.id, pair.id)
            if existing_mapping:
                logger.debug("Skipping duplicate source update %s for pair %s", event.id, pair.id)
                return True
            
            # Check global and pair-specific word blocking first
            message_text = event.text or event.raw_text or ""
            if self.is_blocked_word(message_text, pair):
                logger.info(f"Message blocked by word filter (pair {pair.id}): {message_text[:100]}...")
                self.stats['messages_filtered'] += 1
                pair.stats['messages_filtered'] = pair.stats.get('messages_filtered', 0) + 1
                pair.stats['words_blocked'] = pair.stats.get('words_blocked', 0) + 1
                await self.db_manager.update_pair(pair)
                return True
            
            # Apply other filters
            filter_result = await self.message_filter.should_copy_message(event, pair)
            if not filter_result.should_copy:
                logger.info(f"Message filtered by {filter_result.filters_applied}: {filter_result.reason} - Content: {message_text[:100]}...")
                self.stats['messages_filtered'] += 1
                
                # Update pair stats
                pair.stats['messages_filtered'] = pair.stats.get('messages_filtered', 0) + 1
                await self.db_manager.update_pair(pair)
                return True  # Successfully filtered (not an error)
            
            # Handle replies first
            reply_to_message_id = None
            if event.is_reply and pair.filters.get("preserve_replies", True):
                reply_to_message_id = await self._find_reply_target(event, pair)
            
            # Always use Bot API for consistent sender appearance (not direct forwarding)
            # Process message content with entities (preserve original formatting)
            processed_content, processed_entities = await self._process_message_content(event, pair)
            
            # Handle media if present - download via Telethon and send via Bot API
            media_info = None
            if event.media and self._get_message_type(event) != "webpage":
                # Check image blocking before processing
                if await self.is_blocked_image(event, pair):
                    logger.debug("Message blocked: contains blocked image")
                    self.stats['messages_filtered'] += 1
                    pair.stats['messages_filtered'] = pair.stats.get('messages_filtered', 0) + 1
                    pair.stats['images_blocked'] = pair.stats.get('images_blocked', 0) + 1
                    await self.db_manager.update_pair(pair)
                    return True
                
                media_info = await self._download_and_prepare_media(event, pair, bot)
                logger.info(f"[MEDIA_DEBUG] Media info returned: {media_info is not None}, Type: {media_info.get('type') if media_info else None}")
                if media_info is None and self._get_message_type(event) != "webpage":
                    logger.error("Media preparation failed for source message %s; refusing to send a text-only fallback", event.id)
                    return False
                if media_info is False:  # Media blocked
                    self.stats['messages_filtered'] += 1
                    pair.stats['messages_filtered'] = pair.stats.get('messages_filtered', 0) + 1
                    await self.db_manager.update_pair(pair)
                    return True
            
            # For URL messages without media, ensure webpage previews are enabled
            # This is crucial for proper URL forwarding
            contains_urls = bool(processed_content and self._contains_urls(processed_content))
            if contains_urls:
                logger.info(f"Message contains URLs, will enable webpage preview: {processed_content[:200]}...")
            elif processed_content and self._contains_simple_urls(processed_content):
                contains_urls = True
                logger.info(f"Message contains simple URLs, will enable webpage preview: {processed_content[:200]}...")
            
            # Send message with full entity preservation and proper URL preview handling
            sent_message = await self._send_message(
                bot, pair.destination_chat_id, processed_content or "", 
                media_info, reply_to_message_id, processed_entities or []
            )
            
            if sent_message:
                # Save message mapping
                mapping = MessageMapping(
                    id=0,
                    source_message_id=event.id,
                    destination_message_id=sent_message.message_id,
                    pair_id=pair.id,
                    bot_index=bot_index,
                    source_chat_id=pair.source_chat_id,
                    destination_chat_id=pair.destination_chat_id,
                    message_type=self._get_message_type(event),
                    has_media=bool(event.media and self._get_message_type(event) not in ("webpage", "unknown")),
                    is_reply=bool(reply_to_message_id),
                    reply_to_source_id=event.reply_to_msg_id if event.is_reply else None,
                    reply_to_dest_id=reply_to_message_id
                )
                
                await self.db_manager.save_message_mapping(mapping)
                
                # Update statistics
                self.stats['messages_copied'] += 1
                pair.stats['messages_copied'] = pair.stats.get('messages_copied', 0) + 1
                pair.stats['last_activity'] = datetime.now().isoformat()
                
                if event.media:
                    self.stats['media_processed'] += 1
                
                if reply_to_message_id:
                    pair.stats['replies_preserved'] = pair.stats.get('replies_preserved', 0) + 1
                
                await self.db_manager.update_pair(pair)
                
                logger.debug(f"Message copied: {event.id} -> {sent_message.message_id}")
                return True
            
            return False
            
        except (RetryAfter, FloodWaitError):
            raise
        except Exception as e:
            logger.error(f"Error processing new message: {e}")
            self.stats['errors'] += 1
            pair.stats['errors'] = pair.stats.get('errors', 0) + 1
            await self.db_manager.update_pair(pair)
            return False
        finally:
            # _send_message normally cleans up media itself; this final guard also covers
            # unexpected failures before its media branch starts.
            if isinstance(media_info, dict) and media_info.get("cleanup_required"):
                leftover_path = media_info.get("file_path")
                if leftover_path and os.path.exists(leftover_path):
                    try:
                        os.unlink(leftover_path)
                    except OSError as cleanup_error:
                        logger.warning("Could not remove leftover media temp file %s: %s", leftover_path, cleanup_error)
    
    async def process_message_edit(self, event, pair: MessagePair, bot: Bot, bot_index: int) -> bool:
        """Synchronize edits in-place, including captions and replacement media."""
        temp_media_path = None
        try:
            mapping = await self.db_manager.get_message_mapping(event.id, pair.id)
            if not mapping:
                logger.debug("No mapping found for edited source message %s (pair %s)", event.id, pair.id)
                # The new-message item can still be in-flight. A retry lets it establish the mapping.
                return False

            processed_content, processed_entities = await self._process_message_content(event, pair)
            processed_content = processed_content or ""
            chat_id = mapping.destination_chat_id or pair.destination_chat_id
            message_id = mapping.destination_message_id
            source_media_type = self._get_message_type(event) if getattr(event, "media", None) else "text"
            mapping_is_webpage = mapping.message_type == "webpage" or source_media_type == "webpage"
            mapped_media = bool(mapping.has_media and not mapping_is_webpage)

            if mapped_media and getattr(event, "media", None) and source_media_type not in ("webpage", "unknown"):
                media_info = await self._download_and_prepare_media(event, pair, bot)
                if not media_info or media_info is False:
                    logger.error("Could not prepare replacement media for source message %s", event.id)
                    return False
                temp_media_path = media_info.get("file_path")
                media_type = media_info.get("type")
                if not temp_media_path or not os.path.isfile(temp_media_path):
                    logger.error("Replacement media file missing for source message %s", event.id)
                    return False

                caption = processed_content[:1024]
                caption_entities = self._validate_and_convert_entities(caption, processed_entities or []) if caption else []
                input_media_cls = {
                    "photo": InputMediaPhoto,
                    "video": InputMediaVideo,
                    "document": InputMediaDocument,
                    "audio": InputMediaAudio,
                    "animation": InputMediaAnimation,
                }.get(media_type)
                # Telegram doesn't support replacement-media edits for every message type
                # (e.g. stickers and video notes); preserve the destination and sync captions
                # where the Bot API supports them.
                if input_media_cls is not None:
                    with open(temp_media_path, "rb") as media_file:
                        kwargs = {"media": media_file, "caption": caption, "caption_entities": caption_entities}
                        if media_type == "video":
                            if media_info.get("duration") is not None:
                                kwargs["duration"] = media_info.get("duration")
                            if media_info.get("width") is not None:
                                kwargs["width"] = media_info.get("width")
                            if media_info.get("height") is not None:
                                kwargs["height"] = media_info.get("height")
                        await bot.edit_message_media(
                            chat_id=chat_id,
                            message_id=message_id,
                            media=input_media_cls(**kwargs),
                        )
                elif media_type not in ("voice", "sticker", "video_note", "unknown"):
                    logger.warning("Unsupported media edit type %s for source message %s", media_type, event.id)
                    return False
                else:
                    await bot.edit_message_caption(
                        chat_id=chat_id,
                        message_id=message_id,
                        caption=caption,
                        caption_entities=caption_entities,
                        parse_mode=None,
                    )
            elif mapped_media:
                caption = processed_content[:1024]
                caption_entities = self._validate_and_convert_entities(caption, processed_entities or []) if caption else []
                await bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption=caption,
                    caption_entities=caption_entities,
                    parse_mode=None,
                )
            else:
                if not processed_content:
                    logger.warning("Source text edit became empty after filtering; destination message %s was not changed", message_id)
                    return False
                contains_urls = self._contains_urls(processed_content)
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=processed_content[:4096],
                    entities=self._validate_and_convert_entities(processed_content[:4096], processed_entities or []),
                    disable_web_page_preview=not contains_urls,
                    parse_mode=None,
                )

            pair.stats["edits_synced"] = pair.stats.get("edits_synced", 0) + 1
            await self.db_manager.update_pair(pair)
            logger.debug("Edit synchronized in-place: source %s -> destination %s", event.id, message_id)
            return True
        except (RetryAfter, FloodWaitError):
            raise
        except BadRequest as e:
            message = str(e).lower()
            if "message is not modified" in message:
                return True
            logger.warning("Telegram rejected edit for source message %s / pair %s: %s", getattr(event, "id", None), pair.id, e)
            return False
        except RetryAfter:
            raise
        except (TelegramError, OSError) as e:
            logger.warning("Transient/API failure syncing edit for source message %s: %s", getattr(event, "id", None), e)
            return False
        except Exception as e:
            logger.error("Error processing message edit for source %s: %s", getattr(event, "id", None), e, exc_info=True)
            return False
        finally:
            if temp_media_path and os.path.exists(temp_media_path):
                try:
                    os.unlink(temp_media_path)
                except OSError as cleanup_error:
                    logger.warning("Could not clean up edit media temp file %s: %s", temp_media_path, cleanup_error)

    async def process_message_delete(self, event, pair: MessagePair, bot: Bot, bot_index: int) -> bool:
        """Delete all mapped destination copies for deleted source messages."""
        try:
            deleted_ids = list(dict.fromkeys(getattr(event, "deleted_ids", []) or []))
            if not deleted_ids:
                return True
            deleted_count = 0
            all_succeeded = True
            for source_message_id in deleted_ids:
                mapping = await self.db_manager.get_message_mapping(source_message_id, pair.id)
                if not mapping:
                    continue
                try:
                    await bot.delete_message(
                        chat_id=mapping.destination_chat_id or pair.destination_chat_id,
                        message_id=mapping.destination_message_id,
                    )
                    deleted_count += 1
                    await self.db_manager.delete_message_mapping(source_message_id, pair.id)
                except (RetryAfter, FloodWaitError):
                    raise
                except BadRequest as e:
                    error_text = str(e).lower()
                    if "message to delete not found" in error_text or "message can't be deleted because it is too old" in error_text:
                        # The destination is already absent; remove stale mapping idempotently.
                        await self.db_manager.delete_message_mapping(source_message_id, pair.id)
                        logger.debug("Destination message %s was already absent", mapping.destination_message_id)
                    else:
                        all_succeeded = False
                        logger.warning("Could not delete destination message %s for source %s: %s", mapping.destination_message_id, source_message_id, e)
                except (TelegramError, OSError) as e:
                    all_succeeded = False
                    logger.warning("Temporary deletion failure for source message %s: %s", source_message_id, e)

            if deleted_count:
                pair.stats["deletes_synced"] = pair.stats.get("deletes_synced", 0) + deleted_count
                await self.db_manager.update_pair(pair)
            return all_succeeded
        except (RetryAfter, FloodWaitError):
            raise
        except Exception as e:
            logger.error("Error processing message deletion for pair %s: %s", pair.id, e, exc_info=True)
            return False

    async def _process_message_content(self, event, pair: MessagePair) -> tuple[Optional[str], List]:
        """Process and filter message content with full entity support and formatting preservation"""
        try:
            # Use raw_text to preserve original formatting without markdown conversion
            text = event.raw_text or event.text or ""
            entities = getattr(event, 'entities', []) or []
            
            logger.debug(f"Processing message content: {text[:100]}... (entities: {len(entities)})")
            
            # Always apply text filters (mention removal, header/footer, etc.) but preserve formatting
            # First apply all text transformations
            filtered_text, processed_entities = await self.message_filter.filter_text(text, pair, entities)
            
            # Log text filtering results
            original_lines = len(text.split('\n'))
            filtered_lines = len(filtered_text.split('\n'))
            logger.info(f"Text filtering completed - Lines: {original_lines} → {filtered_lines}, Length: {len(text)} → {len(filtered_text)}")
            
            if text != filtered_text:
                logger.info(f"Text was modified by filters")
                logger.debug(f"Original: {repr(text[:200])}")
                logger.debug(f"Filtered: {repr(filtered_text[:200])}")
            else:
                logger.info(f"Text unchanged by filters")
            
            # Check length limits
            min_length = pair.filters.get("min_message_length", 0)
            max_length = pair.filters.get("max_message_length", 0)
            
            if min_length > 0 and len(filtered_text) < min_length:
                return None, []
            
            if max_length > 0 and len(filtered_text) > max_length:
                # Truncate text and adjust entities
                filtered_text = filtered_text[:max_length] + "..."
                processed_entities = [
                    e for e in processed_entities 
                    if getattr(e, 'offset', 0) + getattr(e, 'length', 0) <= max_length
                ]
            
            return filtered_text, processed_entities
            
        except Exception as e:
            logger.error(f"Error processing message content: {e}")
            # Return original text as fallback
            return event.raw_text or event.text or "", []
    
    async def _process_media(self, event, pair: MessagePair, bot: Bot) -> Optional[Any]:
        """Process media content with comprehensive type detection and web page support"""
        try:
            media = event.media
            
            # Handle web page previews
            if hasattr(media, '__class__') and 'MessageMediaWebPage' in str(media.__class__):
                # Extract webpage info
                webpage = getattr(media, 'webpage', None)
                if webpage:
                    return {
                        'type': 'webpage',
                        'url': getattr(webpage, 'url', ''),
                        'title': getattr(webpage, 'title', ''),
                        'description': getattr(webpage, 'description', ''),
                        'photo': getattr(webpage, 'photo', None),
                        'original_media': media
                    }
            
            # Check if media type is allowed
            allowed_types = pair.filters.get("allowed_media_types", [
                "photo", "video", "document", "audio", "voice", "animation", "video_note", "sticker", "webpage"
            ])
            
            media_type = self._get_media_type(media)
            if media_type not in allowed_types:
                logger.debug(f"Media type {media_type} not allowed")
                return False  # Media blocked
            
            # Special handling for images with enhanced duplicate detection
            if media_type in ["photo", "animation"] or (isinstance(media, MessageMediaDocument) and media_type == "photo"):
                if await self.image_handler.is_image_blocked(event, pair):
                    logger.debug("Image blocked as duplicate")
                    pair.stats['images_blocked'] = pair.stats.get('images_blocked', 0) + 1
                    return False
            
            # Download media with comprehensive error handling and retries
            media_data = None
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    media_data = await self._download_media(event)
                    if media_data:
                        break
                except Exception as download_error:
                    logger.warning(f"Media download attempt {attempt + 1} failed: {download_error}")
                    if attempt == max_retries - 1:
                        logger.error(f"All download attempts failed for media type {media_type}")
                        return None
            
            if not media_data:
                logger.warning("Failed to download media after all attempts")
                return None
            
            # Extract comprehensive media attributes with safe attribute access
            filename = None
            duration = None
            width = None
            height = None
            thumb = None
            
            if hasattr(media, 'document') and media.document and getattr(media.document, 'attributes', None):
                document = media.document
                attributes = getattr(document, 'attributes', [])
                
                # Safely extract filename and attributes from document
                for attr in attributes:
                    attr_type = type(attr).__name__
                    # Extract filename from DocumentAttributeFilename
                    if attr_type == 'DocumentAttributeFilename':
                        filename = getattr(attr, 'file_name', None)
                    # Extract duration from DocumentAttributeVideo or DocumentAttributeAudio
                    elif attr_type in ['DocumentAttributeVideo', 'DocumentAttributeAudio']:
                        duration = getattr(attr, 'duration', None)
                    # Extract dimensions from DocumentAttributeVideo or DocumentAttributeImageSize
                    elif attr_type in ['DocumentAttributeVideo', 'DocumentAttributeImageSize']:
                        width = getattr(attr, 'w', None)
                        height = getattr(attr, 'h', None)
                
                # Extract thumbnail safely
                if hasattr(document, 'thumbs') and getattr(document, 'thumbs', None):
                    thumbs = getattr(document, 'thumbs', [])
                    thumb = thumbs[0] if thumbs else None
            
            elif hasattr(event.media, 'photo') and getattr(event.media, 'photo', None):
                # Handle photo attributes
                photo = event.media.photo
                if hasattr(photo, 'sizes') and photo.sizes:
                    largest_size = max(photo.sizes, key=lambda s: getattr(s, 'w', 0) * getattr(s, 'h', 0))
                    width = getattr(largest_size, 'w', None)
                    height = getattr(largest_size, 'h', None)
            
            # Get MIME type safely
            mime_type = None
            if hasattr(media, 'document') and media.document:
                mime_type = getattr(media.document, 'mime_type', None)
            
            return {
                'type': media_type,
                'data': media_data,
                'filename': filename,
                'duration': duration,
                'width': width,
                'height': height,
                'thumbnail': thumb,
                'caption': event.raw_text or event.text or "",  # Use raw_text to preserve formatting
                'original_media': media,
                'mime_type': mime_type
            }
            
        except Exception as e:
            logger.error(f"Error processing media: {e}")
            return None
    
    async def _download_and_prepare_media(self, event, pair: MessagePair, bot: Bot) -> Optional[Dict]:
        """Download media via Telethon and prepare for Bot API sending"""
        import time
        start_time = time.time()
        
        try:
            # Get media type and MIME type for logging
            media_type = self._get_message_type(event)
            mime_type = None
            if hasattr(event.media, 'document') and event.media.document:
                mime_type = getattr(event.media.document, 'mime_type', 'unknown')
            
            logger.info(f"[MEDIA_DEBUG] Starting media processing - Type: {media_type}, MIME: {mime_type}, Pair: {pair.id}")
            
            if media_type == "text":
                return None
            
            # Allocate a unique path safely; tempfile.mktemp() has a filesystem race.
            temp_file = None
            download_start = time.time()
            try:
                with tempfile.NamedTemporaryFile(prefix="insiderx_source_", suffix=".media", delete=False) as temp_handle:
                    temp_file = temp_handle.name
                allocated_path = temp_file
                downloaded_path = await event.download_media(file=allocated_path)
                if downloaded_path:
                    temp_file = downloaded_path
                    if downloaded_path != allocated_path and os.path.exists(allocated_path):
                        try:
                            os.unlink(allocated_path)
                        except OSError as cleanup_error:
                            logger.debug("Unable to remove unused temp path %s: %s", allocated_path, cleanup_error)
                download_time = time.time() - download_start
                
                if not temp_file or not os.path.exists(temp_file):
                    logger.error(f"[MEDIA_DEBUG] Download failed - no file created, Pair: {pair.id}")
                    return None
                
                file_size = os.path.getsize(temp_file) if os.path.exists(temp_file) else 0
                logger.info(f"[MEDIA_DEBUG] Download completed - File: {temp_file}, Size: {file_size} bytes, Time: {download_time:.2f}s, Pair: {pair.id}")
                if file_size <= 0:
                    logger.error("Media download produced an empty file for source message %s", getattr(event, "id", None))
                    try:
                        os.unlink(temp_file)
                    except OSError as cleanup_error:
                        logger.debug("Unable to remove empty download %s: %s", temp_file, cleanup_error)
                    return None
                
                # Check if watermarking is enabled for this pair and if this is an image
                watermark_enabled = pair.filters.get("watermark_enabled", False)
                watermark_text = pair.filters.get("watermark_text") or "@InsiderX"
                
                logger.info(f"[MEDIA_DEBUG] Watermark check - Enabled: {watermark_enabled}, Text: '{watermark_text}', Type: {media_type}, Pair: {pair.id}")
                
                # Apply watermark if enabled and media is an image
                if watermark_enabled and watermark_text and media_type in ['photo', 'document']:
                    # Check if it's actually an image for documents
                    is_image = media_type == 'photo'
                    if media_type == 'document' and hasattr(event.media, 'document') and event.media.document:
                        mime_type = getattr(event.media.document, 'mime_type', '').lower()
                        is_image = mime_type.startswith('image/')
                        logger.info(f"[MEDIA_DEBUG] Document MIME check - MIME: {mime_type}, Is Image: {is_image}, Pair: {pair.id}")
                    
                    if is_image:
                        # Select an output suffix matching the source image format so that the
                        # document/photo isn't mislabeled and the watermark pass starts from the
                        # freshly downloaded, unwatermarked source bytes on every edit.
                        watermark_start = time.time()
                        watermark_suffix = ".jpg"
                        try:
                            from PIL import Image as PILImage
                            with PILImage.open(temp_file) as source_image:
                                watermark_suffix = {
                                    "JPEG": ".jpg", "JPG": ".jpg", "PNG": ".png",
                                    "WEBP": ".webp", "TIFF": ".tiff", "BMP": ".bmp",
                                }.get((source_image.format or "JPEG").upper(), ".jpg")
                        except Exception as image_probe_error:
                            logger.warning("Could not determine image format for watermark; using JPEG: %s", image_probe_error)
                        with tempfile.NamedTemporaryFile(prefix="insiderx_watermark_", suffix=watermark_suffix, delete=False) as watermark_handle:
                            watermarked_file = watermark_handle.name
                        logger.info(f"[MEDIA_DEBUG] Starting watermark - Input: {temp_file}, Output: {watermarked_file}, Text: '{watermark_text}', Pair: {pair.id}")
                        success = self.image_handler.add_text_watermark(temp_file, watermarked_file, watermark_text or "@InsiderX")
                        watermark_time = time.time() - watermark_start
                        
                        if success:
                            # Verify watermarked file exists and has content
                            if os.path.exists(watermarked_file):
                                watermarked_size = os.path.getsize(watermarked_file)
                                logger.info(f"[MEDIA_DEBUG] Watermark success - Size: {watermarked_size} bytes, Time: {watermark_time:.2f}s, Pair: {pair.id}")
                                
                                if watermarked_size > 0:
                                    # Clean up original file and use watermarked version
                                    try:
                                        os.unlink(temp_file)
                                    except Exception as cleanup_error:
                                        logger.warning(f"[MEDIA_DEBUG] Failed to cleanup original file: {cleanup_error}")
                                    temp_file = watermarked_file
                                else:
                                    logger.error(f"[MEDIA_DEBUG] Watermarked file is empty - Size: 0 bytes, Pair: {pair.id}")
                                    try:
                                        os.unlink(watermarked_file)
                                    except OSError as cleanup_error:
                                        logger.debug("Unable to remove empty watermark file %s: %s", watermarked_file, cleanup_error)
                            else:
                                logger.error(f"[MEDIA_DEBUG] Watermarked file not created - Path: {watermarked_file}, Pair: {pair.id}")
                        else:
                            logger.error(f"[MEDIA_DEBUG] Watermark failed - Time: {watermark_time:.2f}s, Pair: {pair.id}")
                            if os.path.exists(watermarked_file):
                                try:
                                    os.unlink(watermarked_file)
                                except OSError as cleanup_error:
                                    logger.warning("Failed to clean unused watermark output %s: %s", watermarked_file, cleanup_error)
                    else:
                        logger.info(f"[MEDIA_DEBUG] Skipping watermark - not an image, Type: {media_type}, MIME: {mime_type}, Pair: {pair.id}")
                else:
                    logger.info(f"[MEDIA_DEBUG] Skipping watermark - conditions not met, Pair: {pair.id}")
                
                # Extract media attributes safely
                logger.info(f"[MEDIA_DEBUG] Extracting attributes - Type: {media_type}, File: {temp_file}, Pair: {getattr(pair, 'id', 'unknown')}")
                filename = None
                duration = None
                width = None
                height = None
                
                if hasattr(event.media, 'document') and event.media.document:
                    document = event.media.document
                    if getattr(document, 'attributes', None):
                        for attr in document.attributes:
                            attr_type = type(attr).__name__
                            if attr_type == 'DocumentAttributeFilename':
                                filename = getattr(attr, 'file_name', None)
                            elif attr_type in ['DocumentAttributeVideo', 'DocumentAttributeAudio']:
                                duration = getattr(attr, 'duration', None)
                            elif attr_type in ['DocumentAttributeVideo', 'DocumentAttributeImageSize']:
                                width = getattr(attr, 'w', None)
                                height = getattr(attr, 'h', None)
                
                elif hasattr(event.media, 'photo') and event.media.photo:
                    photo = event.media.photo
                    if hasattr(photo, 'sizes') and photo.sizes:
                        largest_size = max(photo.sizes, key=lambda s: getattr(s, 'w', 0) * getattr(s, 'h', 0))
                        width = getattr(largest_size, 'w', None)
                        height = getattr(largest_size, 'h', None)
                
                # Prepare media for Bot API with file cleanup
                try:
                    file_size = os.path.getsize(temp_file) if temp_file and os.path.exists(temp_file) else 0
                    pair_id = getattr(pair, 'id', 'unknown')
                    logger.info(f"[MEDIA_DEBUG] Returning media info - Type: {media_type}, File: {temp_file}, Size: {file_size} bytes, Pair: {pair_id}")
                except Exception as size_error:
                    logger.warning(f"[MEDIA_DEBUG] Error getting file size: {size_error}")
                    file_size = 0
                    pair_id = 'unknown'
                
                return {
                    'type': media_type,
                    'file_path': temp_file,
                    'filename': filename,
                    'duration': duration,
                    'width': width,
                    'height': height,
                    'caption': event.raw_text or event.text or "",
                    'cleanup_required': True
                }
                
            except (RetryAfter, FloodWaitError):
                if temp_file and os.path.exists(temp_file):
                    try:
                        os.unlink(temp_file)
                    except OSError as cleanup_error:
                        logger.debug("Failed to remove rate-limited temp file %s: %s", temp_file, cleanup_error)
                raise
            except Exception as download_error:
                # Clean up on error
                if temp_file and os.path.exists(temp_file):
                    try:
                        os.unlink(temp_file)
                    except OSError as cleanup_error:
                        logger.debug("Failed to remove failed download temp file %s: %s", temp_file, cleanup_error)
                import traceback
                pair_id = getattr(pair, 'id', 'unknown')
                logger.error(f"[MEDIA_DEBUG] Error during media download/processing - Pair: {pair_id}, Type: {media_type}, Error: {download_error}")
                logger.error(f"[MEDIA_DEBUG] Full traceback: {traceback.format_exc()}")
                return None
                
        except (RetryAfter, FloodWaitError):
            raise
        except Exception as e:
            import traceback
            pair_id = getattr(pair, 'id', 'unknown')
            logger.error(f"[MEDIA_DEBUG] Error preparing media - Pair: {pair_id}, Error: {e}")
            logger.error(f"[MEDIA_DEBUG] Full traceback: {traceback.format_exc()}")
            return None
            
    async def _download_media(self, event) -> Optional[BytesIO]:
        """Download media from message"""
        try:
            # Create a BytesIO buffer
            buffer = BytesIO()
            
            # Download media to buffer
            await event.client.download_media(event.media, file=buffer)
            buffer.seek(0)
            
            return buffer
            
        except Exception as e:
            logger.error(f"Error downloading media: {e}")
            return None
    


    async def _send_message(self, bot: Bot, chat_id: int, content: str, 
                          media_info: Optional[Dict], reply_to_message_id: Optional[int] = None,
                          entities: Optional[List] = None):
        """Send message to destination chat with comprehensive media and formatting support"""
        import time
        send_start = time.time()
        
        try:
            logger.info(f"[SEND_DEBUG] Starting message send - Chat: {chat_id}, Content length: {len(content) if content else 0}, Media: {media_info['type'] if media_info else None}")
            
            # Validate and convert entities for proper formatting and premium emoji support  
            converted_entities = self._validate_and_convert_entities(content, entities or [])
            
            # Handle webpage preview messages
            if media_info and media_info.get('type') == 'webpage':
                # For webpage previews, send as text with web preview enabled
                if content:
                    return await bot.send_message(
                        chat_id=chat_id,
                        text=content,
                        entities=converted_entities,
                        disable_web_page_preview=False,  # Enable webpage previews
                        reply_to_message_id=reply_to_message_id
                    )
                else:
                    # If no content but has webpage info, send the URL to trigger preview
                    webpage_url = media_info.get('url', '')
                    if webpage_url:
                        return await bot.send_message(
                            chat_id=chat_id,
                            text=webpage_url,
                            disable_web_page_preview=False,
                            reply_to_message_id=reply_to_message_id
                        )
            
            if media_info and media_info.get('type') != 'webpage':
                # Send media message with downloaded file
                caption = content[:1024] if content else None
                caption_entities = self._validate_and_convert_entities(caption, entities) if caption and entities else None
                
                media_type = media_info['type']
                file_path = media_info.get('file_path')
                file_size = os.path.getsize(file_path) if file_path and os.path.exists(file_path) else 0
                
                logger.info(f"[SEND_DEBUG] Media send prep - Type: {media_type}, File: {file_path}, Size: {file_size} bytes")
                
                if not file_path or not os.path.exists(file_path):
                    logger.error(f"[SEND_DEBUG] Media file missing - Path: {file_path}, Exists: {os.path.exists(file_path) if file_path else False}")
                    return None
                
                if file_size == 0:
                    logger.error(f"[SEND_DEBUG] Media file is empty - Path: {file_path}")
                    return None
                
                try:
                    # Send based on media type with all attributes preserved
                    if media_type == 'photo':
                        logger.info(f"[SEND_DEBUG] Sending photo - Size: {file_size} bytes, Caption: {len(caption) if caption else 0} chars")
                        try:
                            with open(file_path, 'rb') as photo_file:
                                result = await bot.send_photo(
                                    chat_id=chat_id,
                                    photo=photo_file,
                                    caption=caption,
                                    caption_entities=caption_entities,
                                    reply_to_message_id=reply_to_message_id
                                )
                                send_time = time.time() - send_start
                                logger.info(f"[SEND_DEBUG] Photo sent successfully - Message ID: {result.message_id}, Time: {send_time:.2f}s")
                        except Exception as photo_error:
                            logger.error(f"[SEND_DEBUG] Photo send failed: {photo_error}")
                            raise photo_error
                    elif media_type == 'video':
                        logger.info(f"[SEND_DEBUG] Sending video - Size: {file_size} bytes, Duration: {media_info.get('duration')}")
                        try:
                            with open(file_path, 'rb') as video_file:
                                result = await bot.send_video(
                                    chat_id=chat_id,
                                    video=video_file,
                                    caption=caption,
                                    caption_entities=caption_entities,
                                    duration=media_info.get('duration'),
                                    width=media_info.get('width'),
                                    height=media_info.get('height'),
                                    reply_to_message_id=reply_to_message_id
                                )
                                send_time = time.time() - send_start
                                logger.info(f"[SEND_DEBUG] Video sent successfully - Message ID: {result.message_id}, Time: {send_time:.2f}s")
                        except Exception as video_error:
                            logger.error(f"[SEND_DEBUG] Video send failed: {video_error}")
                            raise video_error
                    elif media_type == 'animation':
                        with open(file_path, 'rb') as animation_file:
                            result = await bot.send_animation(
                                chat_id=chat_id,
                                animation=animation_file,
                                caption=caption,
                                caption_entities=caption_entities,
                                duration=media_info.get('duration'),
                                width=media_info.get('width'),
                                height=media_info.get('height'),
                                reply_to_message_id=reply_to_message_id
                            )
                    elif media_type == 'document':
                        with open(file_path, 'rb') as document_file:
                            result = await bot.send_document(
                                chat_id=chat_id,
                                document=document_file,
                                filename=media_info.get('filename'),
                                caption=caption,
                                caption_entities=caption_entities,
                                reply_to_message_id=reply_to_message_id
                            )
                    elif media_type == 'audio':
                        with open(file_path, 'rb') as audio_file:
                            result = await bot.send_audio(
                                chat_id=chat_id,
                                audio=audio_file,
                                caption=caption,
                                caption_entities=caption_entities,
                                duration=media_info.get('duration'),
                                reply_to_message_id=reply_to_message_id
                            )
                    elif media_type == 'voice':
                        with open(file_path, 'rb') as voice_file:
                            result = await bot.send_voice(
                                chat_id=chat_id,
                                voice=voice_file,
                                caption=caption,
                                caption_entities=caption_entities,
                                duration=media_info.get('duration'),
                                reply_to_message_id=reply_to_message_id
                            )
                    elif media_type == 'video_note':
                        with open(file_path, 'rb') as video_note_file:
                            result = await bot.send_video_note(
                                chat_id=chat_id,
                                video_note=video_note_file,
                                duration=media_info.get('duration'),
                                length=media_info.get('width', 240),  # Video notes are square
                                reply_to_message_id=reply_to_message_id
                            )
                    elif media_type == 'sticker':
                        with open(file_path, 'rb') as sticker_file:
                            result = await bot.send_sticker(
                                chat_id=chat_id,
                                sticker=sticker_file,
                                reply_to_message_id=reply_to_message_id
                            )
                    else:
                        result = None
                    
                    # Clean up downloaded file
                    if media_info.get('cleanup_required') and file_path and os.path.exists(file_path):
                        try:
                            os.unlink(file_path)
                        except Exception as cleanup_error:
                            logger.warning(f"Failed to cleanup temp file {file_path}: {cleanup_error}")
                    
                    return result
                    
                except Exception as send_error:
                    # Keep the file alive until the simplified BadRequest fallback is tried.
                    # process_new_message has a finally block that guarantees cleanup.
                    raise send_error

            else:
                # Send text message with enhanced formatting support and URL preview handling
                if content:
                    contains_urls = self._contains_urls(content)
                    logger.info(f"Sending text message. Contains URLs: {contains_urls}, Content: {content[:200]}...")
                    
                    # For messages with URLs, ensure webpage preview is enabled
                    # disable_web_page_preview=False means preview is ENABLED
                    # disable_web_page_preview=True means preview is DISABLED
                    disable_preview = not contains_urls  # False if URLs present (preview enabled), True if no URLs (preview disabled)
                    
                    logger.info(f"Setting disable_web_page_preview={disable_preview} for message with URLs={contains_urls}")
                    
                    # Send as text message (never as media) to ensure formatting is preserved and URLs work properly
                    return await bot.send_message(
                        chat_id=chat_id,
                        text=content,
                        entities=converted_entities,
                        disable_web_page_preview=disable_preview,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode=None  # Use entities instead of parse_mode to preserve original formatting
                    )
            
            return None
            
        except RetryAfter:
            raise
        except Forbidden as e:
            logger.error(f"Bot forbidden in chat {chat_id}: {e}")
            return None
        except BadRequest as e:
            logger.error(f"Bad request sending message to chat {chat_id}: {e}")
            
            # Check if it's a "Chat not found" error - this usually means:
            # 1. The bot is not a member of the destination chat
            # 2. The chat ID is incorrect
            # 3. The chat was deleted or made private
            if "Chat not found" in str(e):
                logger.error(f"CHAT ACCESS ERROR for pair with destination {chat_id}:")
                logger.error(f"  - Bot might not be a member of the destination chat")
                logger.error(f"  - Chat ID might be incorrect: {chat_id}")
                logger.error(f"  - Chat might be private or deleted")
                logger.error(f"  - Solution: Add the bot to the destination chat or check the chat ID")
                return None
            
            logger.warning(f"Bad request sending message, trying fallback: {e}")
            # Comprehensive fallback strategy
            try:
                if media_info and media_info.get('type') != 'webpage':
                    caption = content[:1024] if content else None
                    media_type = media_info['type']
                    
                    # Retry once using the on-disk path with minimal optional arguments.
                    fallback_path = media_info.get('file_path')
                    if not fallback_path or not os.path.isfile(fallback_path):
                        return None
                    if media_type == 'photo':
                        with open(fallback_path, 'rb') as media_file:
                            return await bot.send_photo(chat_id=chat_id, photo=media_file, caption=caption, reply_to_message_id=reply_to_message_id)
                    elif media_type == 'video':
                        with open(fallback_path, 'rb') as media_file:
                            return await bot.send_video(chat_id=chat_id, video=media_file, caption=caption, reply_to_message_id=reply_to_message_id)
                    elif media_type == 'animation':
                        with open(fallback_path, 'rb') as media_file:
                            return await bot.send_animation(chat_id=chat_id, animation=media_file, caption=caption, reply_to_message_id=reply_to_message_id)
                    elif media_type == 'document':
                        with open(fallback_path, 'rb') as media_file:
                            return await bot.send_document(chat_id=chat_id, document=media_file, caption=caption, reply_to_message_id=reply_to_message_id)
                    elif media_type == 'audio':
                        with open(fallback_path, 'rb') as media_file:
                            return await bot.send_audio(chat_id=chat_id, audio=media_file, caption=caption, reply_to_message_id=reply_to_message_id)
                    elif media_type == 'voice':
                        with open(fallback_path, 'rb') as media_file:
                            return await bot.send_voice(chat_id=chat_id, voice=media_file, reply_to_message_id=reply_to_message_id)
                    elif media_type == 'video_note':
                        with open(fallback_path, 'rb') as media_file:
                            return await bot.send_video_note(chat_id=chat_id, video_note=media_file, reply_to_message_id=reply_to_message_id)
                    elif media_type == 'sticker':
                        with open(fallback_path, 'rb') as media_file:
                            return await bot.send_sticker(chat_id=chat_id, sticker=media_file, reply_to_message_id=reply_to_message_id)
                else:
                    # Final fallback: plain text without entities, check for URLs
                    contains_urls = self._contains_urls(content)
                    disable_preview = not contains_urls
                    logger.info(f"Fallback: Setting disable_web_page_preview={disable_preview} for URLs={contains_urls}")
                    
                    # Ensure fallback also sends as text message with URL preview capability
                    return await bot.send_message(
                        chat_id=chat_id, 
                        text=content, 
                        reply_to_message_id=reply_to_message_id, 
                        disable_web_page_preview=disable_preview,
                        parse_mode=None  # No parse mode to avoid formatting conflicts
                    )
            except Exception as fallback_error:
                logger.error(f"All fallback attempts failed: {fallback_error}")
                return None
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return None

    def _validate_and_convert_entities(self, text: str, entities: List) -> List:
        """Validate entities against text and convert them with comprehensive bounds checking"""
        if not text or not entities:
            return []
        
        try:
            # Calculate text length in UTF-16 units (Telegram's standard)
            text_bytes = text.encode('utf-16-le')
            text_length = len(text_bytes) // 2
            
            # Convert entities first
            converted_entities = self._convert_entities_for_telegram(entities)
            
            # Validate and adjust entities with bounds checking
            valid_entities = []
            for entity in converted_entities:
                if not hasattr(entity, 'offset') or not hasattr(entity, 'length'):
                    continue
                    
                offset = entity.offset
                length = entity.length
                
                # Skip invalid entities
                if offset < 0 or length <= 0:
                    continue
                
                # Adjust entities that exceed text bounds
                if offset >= text_length:
                    continue  # Entity starts beyond text
                
                if offset + length > text_length:
                    # Truncate entity to fit within text bounds
                    adjusted_length = text_length - offset
                    if adjusted_length > 0:
                        # Create new entity with adjusted length
                        adjusted_entity = MessageEntity(
                            entity.type,
                            offset,
                            adjusted_length,
                            url=getattr(entity, 'url', None),
                            user=getattr(entity, 'user', None),
                            language=getattr(entity, 'language', None),
                            custom_emoji_id=getattr(entity, 'custom_emoji_id', None)
                        )
                        valid_entities.append(adjusted_entity)
                        logger.debug(f"Adjusted entity length from {length} to {adjusted_length}")
                else:
                    valid_entities.append(entity)
            
            # Sort entities by offset to maintain order
            valid_entities.sort(key=lambda e: e.offset)
            
            return valid_entities
            
        except Exception as e:
            logger.error(f"Error validating entities: {e}")
            return []
    
    async def _find_reply_target(self, event, pair: MessagePair) -> Optional[int]:
        """Find the destination message ID for reply"""
        try:
            if not event.reply_to_msg_id:
                return None
            
            # Look up the original message mapping
            mapping = await self.db_manager.get_message_mapping(event.reply_to_msg_id, pair.id)
            if mapping:
                return mapping.destination_message_id
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding reply target: {e}")
            return None
    
    def _get_message_type(self, event) -> str:
        """Determine message type"""
        if event.media:
            return self._get_media_type(event.media)
        return "text"
    
    def _get_media_type(self, media) -> str:
        """Determine media type with enhanced detection"""
        try:
            if isinstance(media, MessageMediaPhoto):
                return "photo"
            elif isinstance(media, MessageMediaDocument):
                document = getattr(media, 'document', None)
                if not document:
                    return "document"
                
                # Safely check document attributes first for specific type detection
                if getattr(document, 'attributes', None):
                    for attr in document.attributes:
                        attr_type = type(attr).__name__
                        
                        if attr_type == 'DocumentAttributeSticker':
                            return "sticker"
                        elif attr_type == 'DocumentAttributeAnimated':
                            return "animation"
                        elif attr_type == 'DocumentAttributeVideo':
                            if getattr(attr, 'round_message', False):
                                return "video_note"
                            return "video"
                        elif attr_type == 'DocumentAttributeAudio':
                            if getattr(attr, 'voice', False):
                                return "voice"
                            return "audio"
                
                # Check MIME type as fallback
                mime_type = getattr(document, 'mime_type', None)
                if mime_type:
                    mime_type = mime_type.lower()
                    
                    # Image types
                    if mime_type.startswith('image/'):
                        if 'gif' in mime_type:
                            return "animation"
                        return "photo"
                    
                    # Video types
                    elif mime_type.startswith('video/'):
                        return "video"
                    
                    # Audio types
                    elif mime_type.startswith('audio/'):
                        return "audio"
                
                return "document"
            
            # Handle web page media
            elif hasattr(media, '__class__') and 'MessageMediaWebPage' in str(media.__class__):
                return "webpage"
            
            return "unknown"
            
        except Exception as e:
            logger.error(f"Error determining media type: {e}")
            return "unknown"
    
    def _remove_mentions(self, text: str, placeholder: str) -> str:
        """Enhanced mention removal with comprehensive pattern matching and clean formatting"""
        if not text:
            return text
        try:
            original_text = text
            
            # Step 1: Handle mentions in parentheses - remove entire parentheses
            text = re.sub(r'\(\s*@[a-zA-Z0-9_]{1,32}\s*\)', '', text)
            
            # Step 2: Handle @mentions preceded by punctuation - clean up punctuation  
            text = re.sub(r'([,\.;:!?]\s*)@[a-zA-Z0-9_]{1,32}\b', r'\1', text)
            
            # Step 3: Handle standard @mentions (but not email addresses) - replace with placeholder or remove
            if placeholder:
                # Match @mentions at word boundaries, but not after alphanumeric chars (emails)
                text = re.sub(r'(?<!\w)@[a-zA-Z0-9_]{1,32}\b', placeholder, text)
            else:
                text = re.sub(r'(?<!\w)@[a-zA-Z0-9_]{1,32}\b', '', text)
            
            # Step 4: Handle user ID links
            text = re.sub(r'tg://user\?id=\d+', placeholder if placeholder else '', text)
            
            # Clean up formatting issues
            if placeholder:
                # Remove duplicate placeholders
                text = re.sub(f'{re.escape(placeholder)}(\\s*{re.escape(placeholder)})+', placeholder, text)
                # Clean up extra spaces around placeholders
                text = re.sub(f'\\s+{re.escape(placeholder)}\\s+', f' {placeholder} ', text)
                text = re.sub(f'^\\s*{re.escape(placeholder)}\\s*', f'{placeholder} ', text)
                text = re.sub(f'\\s*{re.escape(placeholder)}\\s*$', f' {placeholder}', text)
            
            # Clean up excessive spaces and trailing punctuation left behind
            text = re.sub(r'\s*,\s*,\s*', ', ', text)  # Fix double commas
            text = re.sub(r'\s*,\s*$', '', text)  # Remove trailing comma
            text = re.sub(r'^\s*,\s*', '', text)  # Remove leading comma
            text = re.sub(r'\s+', ' ', text)  # Multiple spaces to single space
            text = text.strip()
            
            # If result is empty or only placeholder, return original
            if not text or (placeholder and text == placeholder):
                return original_text
            
            return text
            
        except Exception as e:
            logger.error(f"Error removing mentions: {e}")
            return text
    
    def _remove_headers(self, text: str, patterns: List[str]) -> str:
        """Enhanced header removal with exact phrase matching at beginning of message"""
        if not text:
            return text
            
        original_text = text
        
        # Conservative exact-match patterns if none provided
        if not patterns:
            exact_header_patterns = [
                r'^🔥\s*VIP\s*ENTRY\b.*?$',      # Exact: "🔥 VIP ENTRY"
                r'^📢\s*SIGNAL\s*ALERT\b.*?$',   # Exact: "📢 SIGNAL ALERT"
                r'^VIP\s*Channel\b.*?$',         # Exact: "VIP Channel"
                r'^📊\s*Analysis\b.*?$',         # Exact: "📊 Analysis"
                r'^🚨\s*Alert\b.*?$',            # Exact: "🚨 Alert"
                r'^🔚\s*END\b.*?$',              # Exact: "🔚 END"
            ]
            patterns = exact_header_patterns
        
        # Process headers at beginning of message only
        lines = text.split('\n')
        filtered_lines = []
        header_section = True  # Track if we're still in header section
        
        for line in lines:
            line_removed = False
            
            # Only check for headers at the beginning of the message
            if header_section and line.strip():
                # Check each pattern against the current line
                for pattern in patterns:
                    try:
                        # Create exact match pattern for headers
                        # Match the exact phrase at start of line
                        if re.match(pattern, line.strip(), re.IGNORECASE):
                            line_removed = True
                            logger.debug(f"Header removed: '{line}' matched pattern: {pattern}")
                            break
                    except re.error as e:
                        logger.warning(f"Invalid header pattern '{pattern}': {e}")
                        continue
                
                # Once we encounter a non-header line, stop looking for headers
                if not line_removed:
                    header_section = False
            
            # Keep lines that don't match header patterns
            if not line_removed:
                filtered_lines.append(line)
        
        # Rejoin lines preserving original formatting
        result_text = '\n'.join(filtered_lines)
        
        # Clean up leading whitespace but preserve formatting
        result_text = result_text.lstrip()
        
        # Return original if result is empty
        if not result_text or result_text.isspace():
            return original_text
        
        return result_text
    
    def _remove_footers(self, text: str, patterns: List[str]) -> str:
        """Enhanced footer removal with exact phrase matching at end of message"""
        if not text:
            return text
            
        original_text = text
        
        # Conservative exact-match patterns if none provided
        if not patterns:
            exact_footer_patterns = [
                r'^🔚\s*END\b.*?$',              # Exact: "🔚 END"
                r'^👉\s*Join\b.*?$',             # Exact: "👉 Join our VIP channel"
                r'^Contact\s*@admin\b.*?$',      # Exact: "Contact @admin for more info"
                r'^📱\s*Contact\b.*?$',          # Exact: "📱 Contact us"
                r'^💌\s*Subscribe\b.*?$',        # Exact: "💌 Subscribe to"
            ]
            patterns = exact_footer_patterns
        
        # Process footers at end of message only
        lines = text.split('\n')
        filtered_lines = list(lines)
        
        # Process lines from bottom to top to remove footers cleanly
        footer_section = True  # Track if we're still in footer section
        
        for i in range(len(lines) - 1, -1, -1):
            line = lines[i]
            line_removed = False
            
            # Only check for footers at the end of the message
            if footer_section and line.strip():
                # Check each pattern against the current line
                for pattern in patterns:
                    try:
                        # Create exact match pattern for footers
                        # Match the exact phrase at start of line (footers are typically single lines)
                        if re.match(pattern, line.strip(), re.IGNORECASE):
                            # Remove this line from the filtered list
                            if i < len(filtered_lines):
                                filtered_lines.pop(i)
                                line_removed = True
                                logger.debug(f"Footer removed: '{line}' matched pattern: {pattern}")
                                break
                    except re.error as e:
                        logger.warning(f"Invalid footer pattern '{pattern}': {e}")
                        continue
                
                # Once we encounter a non-footer line, stop looking for footers
                if not line_removed:
                    footer_section = False
            # Skip empty lines while in footer section
            elif not line.strip():
                continue
            else:
                footer_section = False
        
        # Rejoin lines preserving original formatting
        result_text = '\n'.join(filtered_lines)
        
        # Clean up trailing whitespace but preserve formatting
        result_text = result_text.rstrip()
        
        # Return original if result is empty
        if not result_text or result_text.isspace():
            return original_text
        
        return result_text
    
    def _convert_entities_for_telegram(self, entities: List) -> List:
        """Convert Telethon entities to python-telegram-bot format with comprehensive validation"""
        try:
            from telegram import MessageEntity
            
            if not entities:
                return []
            
            converted_entities = []
            
            for entity in entities:
                entity_type = None
                try:
                    # Get entity type safely
                    if hasattr(entity, '__class__'):
                        entity_type = entity.__class__.__name__
                    else:
                        entity_type = str(type(entity)).split('.')[-1].replace("'", "").replace(">", "")
                    
                    offset = getattr(entity, 'offset', 0)
                    length = getattr(entity, 'length', 0)
                    
                    # Validate entity bounds
                    if offset < 0 or length <= 0:
                        continue
                    
                    # Map Telethon entity types to Telegram entity types with comprehensive coverage
                    if 'MessageEntityBold' in entity_type or 'Bold' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.BOLD, offset, length))
                    elif 'MessageEntityItalic' in entity_type or 'Italic' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.ITALIC, offset, length))
                    elif 'MessageEntityCode' in entity_type or 'Code' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.CODE, offset, length))
                    elif 'MessageEntityPre' in entity_type or 'Pre' in entity_type:
                        language = getattr(entity, 'language', None)
                        converted_entities.append(MessageEntity(MessageEntity.PRE, offset, length, language=language))
                    elif 'MessageEntityStrike' in entity_type or 'Strike' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.STRIKETHROUGH, offset, length))
                    elif 'MessageEntityUnderline' in entity_type or 'Underline' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.UNDERLINE, offset, length))
                    elif 'MessageEntitySpoiler' in entity_type or 'Spoiler' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.SPOILER, offset, length))
                    elif 'MessageEntityUrl' in entity_type or 'Url' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.URL, offset, length))
                    elif 'MessageEntityTextUrl' in entity_type or 'TextUrl' in entity_type:
                        url = getattr(entity, 'url', '')
                        if url:
                            converted_entities.append(MessageEntity(MessageEntity.TEXT_LINK, offset, length, url=url))
                    elif 'MessageEntityMention' in entity_type or 'Mention' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.MENTION, offset, length))
                    elif 'MessageEntityMentionName' in entity_type or 'MentionName' in entity_type:
                        user_id = getattr(entity, 'user_id', None)
                        if user_id:
                            converted_entities.append(MessageEntity(MessageEntity.TEXT_MENTION, offset, length, user=user_id))
                    elif 'MessageEntityCustomEmoji' in entity_type or 'CustomEmoji' in entity_type:
                        custom_emoji_id = getattr(entity, 'document_id', '')
                        if custom_emoji_id:
                            converted_entities.append(MessageEntity(MessageEntity.CUSTOM_EMOJI, offset, length, custom_emoji_id=str(custom_emoji_id)))
                    elif 'MessageEntityHashtag' in entity_type or 'Hashtag' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.HASHTAG, offset, length))
                    elif 'MessageEntityCashtag' in entity_type or 'Cashtag' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.CASHTAG, offset, length))
                    elif 'MessageEntityBotCommand' in entity_type or 'BotCommand' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.BOT_COMMAND, offset, length))
                    elif 'MessageEntityEmail' in entity_type or 'Email' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.EMAIL, offset, length))
                    elif 'MessageEntityPhone' in entity_type or 'Phone' in entity_type:
                        converted_entities.append(MessageEntity(MessageEntity.PHONE_NUMBER, offset, length))
                    else:
                        # Log unknown entity types for future enhancement
                        logger.debug(f"Unknown entity type: {entity_type}")
                        
                except Exception as entity_error:
                    logger.warning(f"Failed to convert entity {entity_type or 'unknown'}: {entity_error}")
                    continue
            
            return converted_entities
            
        except Exception as e:
            logger.error(f"Error converting entities: {e}")
            return []
    
    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics"""
        return self.stats.copy()
    
    def is_blocked_word(self, text: str, pair: Optional[MessagePair] = None) -> bool:
        """
        Check if text contains blocked words (global or pair-specific)
        Returns True if the message should be blocked
        """
        if not text:
            return False
        
        text_lower = text.lower().strip()
        
        # Check global blocked words from config first
        BLOCKED_WORDS = getattr(self.config, 'GLOBAL_BLOCKED_WORDS', None)
        if BLOCKED_WORDS is None:
            # Fallback to environment variable or default list
            import os
            env_words = os.getenv('GLOBAL_BLOCKED_WORDS', '')
            if env_words:
                BLOCKED_WORDS = [word.strip() for word in env_words.split(',') if word.strip()]
            else:
                BLOCKED_WORDS = [
                    "join", "promo", "subscribe", "contact", "spam", "advertisement", 
                    "click here", "free", "limited time", "act now", "don't miss"
                ]
        
        # Check global blocked words
        if BLOCKED_WORDS:
            for word in BLOCKED_WORDS:
                if isinstance(word, str) and word.strip():
                    word_lower = word.strip().lower()
                    if word_lower in text_lower:
                        logger.info(f"Text blocked for global word: '{word}' found in: {text[:100]}...")
                        return True
        
        # Check pair-specific blocked words
        if pair:
            pair_blocked_words = pair.filters.get("blocked_words", [])
            if pair_blocked_words:
                for word in pair_blocked_words:
                    if isinstance(word, str) and word.strip():
                        word_lower = word.strip().lower()
                        if word_lower in text_lower:
                            logger.info(f"Text blocked for pair {pair.id} word: '{word}' found in: {text[:100]}...")
                            return True
        
        return False

    def _remove_mentions_from_text(self, text: str, placeholder: str = "[User]") -> str:
        """Remove mentions from text and replace with placeholder"""
        if not text:
            return text
        
        # Pattern to match @username mentions  
        mention_pattern = r'@\w+'
        return re.sub(mention_pattern, placeholder, text)
    
    def _remove_header_footer(self, text: str, header_pattern: Optional[str] = None, footer_pattern: Optional[str] = None) -> str:
        """Remove header and footer from text using regex patterns"""
        if not text:
            return text
        
        filtered_text = text
        
        # Remove header
        if header_pattern:
            try:
                compiled_header = re.compile(header_pattern, re.IGNORECASE | re.MULTILINE)
                match = compiled_header.search(filtered_text)
                if match:
                    # Remove header
                    header_end = match.end()
                    filtered_text = filtered_text[header_end:].strip()
            except re.error as e:
                logger.warning(f"Invalid header regex pattern {header_pattern}: {e}")
        
        # Remove footer
        if footer_pattern:
            try:
                compiled_footer = re.compile(footer_pattern, re.IGNORECASE | re.MULTILINE)
                match = compiled_footer.search(filtered_text)
                if match:
                    # Remove footer
                    footer_start = match.start()
                    filtered_text = filtered_text[:footer_start].strip()
            except re.error as e:
                logger.warning(f"Invalid footer regex pattern {footer_pattern}: {e}")
        
        return filtered_text
    
    def _contains_urls(self, text: str) -> bool:
        """Check if text contains URLs that should have webpage previews"""
        if not text:
            return False
        
        import re
        # Enhanced URL patterns that typically generate webpage previews
        url_patterns = [
            r'https?://[^\s<>")\]]+',                       # HTTP/HTTPS URLs (exclude ) and ])
            r'www\.[^\s<>")\]]+\.[a-zA-Z]{2,}[^\s<>")\]]*', # www URLs with domain and optional path
            r't\.me/[^\s<>")\]]+',                          # Telegram links
            r'(?<![a-zA-Z0-9@])[a-zA-Z0-9.-]+\.(com|org|net|edu|gov|co|io|tv|me|ly|to|cc|repl|dev|app)[^\s<>")\]]*', # Common TLD URLs (exclude emails)
            r'ftp://[^\s<>")\]]+',                          # FTP URLs
            r'[a-zA-Z0-9.-]+\.replit\.com[^\s<>")\]]*',     # Replit URLs
            r'[a-zA-Z0-9.-]+\.replit\.app[^\s<>")\]]*',     # Replit app URLs
        ]
        
        # Check for markdown-style links: [text](url)
        markdown_link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
        markdown_matches = re.findall(markdown_link_pattern, text)
        if markdown_matches:
            for link_text, link_url in markdown_matches:
                # Check if the URL part contains a valid URL
                for pattern in url_patterns:
                    if re.search(pattern, link_url, re.IGNORECASE):
                        logger.info(f"Found markdown URL in text: [{link_text}]({link_url})")
                        return True
        
        # Check for regular URL patterns
        for pattern in url_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                logger.info(f"Found URL pattern '{pattern}' in text: {text[:200]}... (matched: {match.group()})")
                return True
        
        logger.debug(f"No URL patterns found in text: {text[:200]}...")
        return False
    
    def _contains_simple_urls(self, text: str) -> bool:
        """Fallback check for simple URL patterns"""
        if not text:
            return False
        
        import re
        # Simple patterns for URLs that might be missed
        simple_patterns = [
            r'[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/[^\s]*)?',     # Basic domain.tld pattern
            r'(?:^|\s)([\w.-]+\.[\w]{2,})(?:\s|$)',        # Domain at word boundaries
        ]
        
        # Also check inside markdown links
        markdown_link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
        markdown_matches = re.findall(markdown_link_pattern, text)
        if markdown_matches:
            for link_text, link_url in markdown_matches:
                for pattern in simple_patterns:
                    if re.search(pattern, link_url, re.IGNORECASE):
                        logger.info(f"Found simple URL in markdown: [{link_text}]({link_url})")
                        return True
        
        for pattern in simple_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        return False
    
    async def is_blocked_image(self, event, pair: MessagePair) -> bool:
        """
        Global function to check if image should be blocked using pHash
        Returns True if the image should be blocked
        """
        if not event.media:
            return False
        
        try:
            # Use the existing image handler logic
            return await self.image_handler.is_image_blocked(event, pair)
        except Exception as e:
            logger.error(f"Error checking image block: {e}")
            return False
