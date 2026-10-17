"""
Image duplicate detection and blocking system with watermarking support
"""

import logging
import hashlib
import os
import tempfile
from typing import Optional, Dict, List, Any
from io import BytesIO
from datetime import datetime

from database import DatabaseManager, MessagePair
from config import Config

logger = logging.getLogger(__name__)

# Keep watermarking available when Pillow exists even if optional perceptual hashing does not.
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    Image = None
    ImageDraw = None
    ImageFont = None
    logger.warning("Pillow is unavailable. Image watermarking and image blocking are disabled.")

try:
    import imagehash
    IMAGEHASH_AVAILABLE = True
except ImportError:
    IMAGEHASH_AVAILABLE = False
    imagehash = None
    logger.warning("imagehash is unavailable. Perceptual duplicate detection is disabled; watermarking remains available.")

IMAGE_PROCESSING_AVAILABLE = PIL_AVAILABLE and IMAGEHASH_AVAILABLE

class ImageHandler:
    """Image duplicate detection and blocking system with watermarking support"""
    
    def __init__(self, db_manager: DatabaseManager, config: Config):
        self.db_manager = db_manager
        self.config = config
        self.enabled = PIL_AVAILABLE
        self.hash_enabled = PIL_AVAILABLE and IMAGEHASH_AVAILABLE
        
        # Hash cache to avoid recomputing
        self._hash_cache: Dict[int, str] = {}
        
    async def is_image_blocked(self, event, pair: MessagePair) -> bool:
        """Check if image should be blocked as duplicate"""
        if not self.hash_enabled:
            return False
        
        try:
            # Process photo messages and documents that are images
            if not hasattr(event, 'media') or not event.media:
                return False
            
            from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
            
            # Check if it's a photo or image document
            is_image = False
            if isinstance(event.media, MessageMediaPhoto):
                is_image = True
            elif isinstance(event.media, MessageMediaDocument):
                document = getattr(event.media, 'document', None)
                if document and hasattr(document, 'mime_type'):
                    mime_type = getattr(document, 'mime_type', '').lower()
                    is_image = mime_type.startswith('image/')
            
            if not is_image:
                return False
            
            # Download and hash the image
            image_hash = await self._get_image_hash(event)
            if not image_hash:
                return False
            
            # Check against blocked images
            is_blocked = await self._check_blocked_images(image_hash, pair)
            
            if is_blocked:
                logger.debug(f"Image blocked as duplicate: {image_hash}")
                # Update usage count
                await self._update_image_usage(image_hash, pair)
                return True
            
            return False
            
        except Exception as e:
            try:
                from telethon.errors import FloodWaitError
                if isinstance(e, FloodWaitError):
                    raise
            except ImportError:
                pass
            logger.error("Error checking image block: %s", e, exc_info=True)
            return False
    
    async def add_image_block(self, event, pair: Optional[MessagePair] = None, 
                             description: str = "", 
                             blocked_by: str = "",
                             block_scope: str = "pair",
                             similarity_threshold: Optional[int] = None) -> bool:
        """Add image to block list"""
        if not self.hash_enabled:
            return False
        
        try:
            image_hash = await self._get_image_hash(event)
            if not image_hash:
                return False
            
            # Use config default if threshold not specified
            if similarity_threshold is None:
                similarity_threshold = self.config.SIMILARITY_THRESHOLD
            
            # Save to database
            async with self.db_manager.get_connection() as conn:
                await conn.execute('''
                    INSERT OR REPLACE INTO blocked_images 
                    (phash, pair_id, description, blocked_by, block_scope, similarity_threshold)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    image_hash,
                    pair.id if (pair and block_scope == "pair") else None,
                    description,
                    blocked_by,
                    block_scope,
                    similarity_threshold
                ))
                await conn.commit()
            
            logger.info(f"Added image block: {image_hash} (scope: {block_scope})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add image block: {e}")
            return False
    
    async def remove_image_block(self, image_hash: str, pair_id: Optional[int] = None) -> bool:
        """Remove image from block list"""
        try:
            async with self.db_manager.get_connection() as conn:
                if pair_id:
                    await conn.execute(
                        'DELETE FROM blocked_images WHERE phash = ? AND pair_id = ?',
                        (image_hash, pair_id)
                    )
                else:
                    await conn.execute(
                        'DELETE FROM blocked_images WHERE phash = ?',
                        (image_hash,)
                    )
                await conn.commit()
            
            logger.info(f"Removed image block: {image_hash}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove image block: {e}")
            return False
    
    async def _get_image_hash(self, event) -> Optional[str]:
        """Get perceptual hash of image"""
        if not self.hash_enabled:
            return None
        
        try:
            # Check cache first
            message_id = event.id
            if message_id in self._hash_cache:
                return self._hash_cache[message_id]
            
            # Download image
            buffer = BytesIO()
            # Check if client is available
            if not hasattr(event, 'client') or event.client is None:
                logger.error("Event client is None, cannot download media")
                return None
            
            await event.client.download_media(event.media, file=buffer)
            buffer.seek(0)
            
            # Open with PIL and compute hash
            if not self.hash_enabled:
                logger.warning("Perceptual hashing library unavailable")
                return None
                
            with Image.open(buffer) as img:
                # Convert to RGB if necessary
                if img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Compute perceptual hash
                phash = imagehash.phash(img)
                hash_str = str(phash)
                
                # Cache the result
                self._hash_cache[message_id] = hash_str
                
                return hash_str
                
        except Exception as e:
            logger.error(f"Failed to compute image hash: {e}")
            return None
    
    async def _check_blocked_images(self, image_hash: str, pair: MessagePair) -> bool:
        """Check if image hash matches any blocked images"""
        try:
            async with self.db_manager.get_connection() as conn:
                # Check pair-specific blocks first
                cursor = await conn.execute('''
                    SELECT phash, similarity_threshold FROM blocked_images 
                    WHERE (pair_id = ? OR block_scope = 'global') 
                    AND (block_scope = 'pair' OR block_scope = 'global')
                ''', (pair.id,))
                
                blocked_images = await cursor.fetchall()
                
                if not blocked_images:
                    return False
                
                # Compare hashes
                for blocked_hash, threshold in blocked_images:
                    similarity = self._calculate_hash_similarity(image_hash, blocked_hash)
                    if similarity <= threshold:
                        return True
                
                return False
                
        except Exception as e:
            logger.error(f"Error checking blocked images: {e}")
            return False
    
    def _calculate_hash_similarity(self, hash1: str, hash2: str) -> int:
        """Calculate Hamming distance between two hashes"""
        if not self.hash_enabled:
            return 100
        
        try:
            # Convert hex strings to imagehash objects
            ihash1 = imagehash.hex_to_hash(hash1)
            ihash2 = imagehash.hex_to_hash(hash2)
            
            # Calculate Hamming distance
            return ihash1 - ihash2
            
        except Exception as e:
            logger.error(f"Error calculating hash similarity: {e}")
            return 100  # Return high value on error
    
    async def _update_image_usage(self, image_hash: str, pair: MessagePair):
        """Update usage count for blocked image"""
        try:
            async with self.db_manager.get_connection() as conn:
                await conn.execute('''
                    UPDATE blocked_images 
                    SET usage_count = usage_count + 1 
                    WHERE phash = ? AND (pair_id = ? OR block_scope = 'global')
                ''', (image_hash, pair.id))
                await conn.commit()
                
        except Exception as e:
            logger.error(f"Failed to update image usage: {e}")
    
    async def get_blocked_images(self, pair_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get list of blocked images"""
        try:
            async with self.db_manager.get_connection() as conn:
                if pair_id:
                    cursor = await conn.execute('''
                        SELECT * FROM blocked_images 
                        WHERE pair_id = ? OR block_scope = 'global'
                        ORDER BY created_at DESC
                    ''', (pair_id,))
                else:
                    cursor = await conn.execute('''
                        SELECT * FROM blocked_images 
                        ORDER BY created_at DESC
                    ''')
                
                rows = await cursor.fetchall()
                
                blocked_images = []
                for row in rows:
                    blocked_images.append({
                        'id': row[0],
                        'phash': row[1],
                        'pair_id': row[2],
                        'description': row[3],
                        'blocked_by': row[4],
                        'usage_count': row[5],
                        'block_scope': row[6],
                        'similarity_threshold': row[7],
                        'created_at': row[8]
                    })
                
                return blocked_images
                
        except Exception as e:
            logger.error(f"Failed to get blocked images: {e}")
            return []
    
    def add_text_watermark(self, input_path: str, output_path: str, text: str) -> bool:
        """Apply a large, centered, light-gray translucent text watermark."""
        if not PIL_AVAILABLE:
            logger.warning("Pillow is unavailable; watermark was not applied")
            return False
        if not input_path or not os.path.isfile(input_path) or os.path.getsize(input_path) <= 0:
            logger.error("Watermark input is missing or empty: %s", input_path)
            return False
        text = (text or "@InsiderX").strip() or "@InsiderX"
        try:
            with Image.open(input_path) as opened:
                source_format = (opened.format or "JPEG").upper()
                # Flattening an animated source would silently destroy its animation.
                if getattr(opened, "n_frames", 1) > 1:
                    logger.warning("Skipping watermark for animated image %s to preserve animation", input_path)
                    return False
                base = opened.convert("RGBA")
                source_size = opened.size
                source_info = dict(opened.info)

            width, height = base.size
            if width < 1 or height < 1:
                logger.error("Invalid image dimensions for watermark: %s", base.size)
                return False

            # Use serif fonts first, then fall back to common sans-serif fonts.
            font_paths = [
                "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf",
                "/usr/share/fonts/truetype/liberation2/LiberationSerif-Regular.ttf",
                "/usr/share/fonts/truetype/noto/NotoSerif-Regular.ttf",
                "/System/Library/Fonts/Times.ttc",
                "/System/Library/Fonts/Supplemental/Times New Roman.ttf",
                "C:/Windows/Fonts/times.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            ]
            font_path = next((candidate for candidate in font_paths if os.path.isfile(candidate)), None)
            draw = ImageDraw.Draw(Image.new("RGBA", (1, 1)))
            target_width = max(1, int(width * 0.86))
            min_size = max(10, min(18, width // 25 or 10))
            max_size = max(min_size, min(int(width * 0.28), int(height * 0.38)))

            def load_font(size: int):
                if font_path:
                    try:
                        return ImageFont.truetype(font_path, size=size)
                    except (OSError, ValueError):
                        pass
                try:
                    return ImageFont.load_default(size=size)
                except TypeError:  # Compatibility with older Pillow versions.
                    return ImageFont.load_default()

            # Binary-search the largest font size that spans about 86% of the image width.
            low, high = min_size, max_size
            chosen_font = load_font(low)
            chosen_bbox = draw.textbbox((0, 0), text, font=chosen_font)
            while low <= high:
                mid = (low + high) // 2
                candidate_font = load_font(mid)
                bbox = draw.textbbox((0, 0), text, font=candidate_font)
                candidate_width = bbox[2] - bbox[0]
                if candidate_width <= target_width:
                    chosen_font, chosen_bbox = candidate_font, bbox
                    low = mid + 1
                else:
                    high = mid - 1
            text_width = chosen_bbox[2] - chosen_bbox[0]
            text_height = chosen_bbox[3] - chosen_bbox[1]
            # Scale down further on very short images to keep the entire word visible.
            max_height = max(1, int(height * 0.32))
            while text_height > max_height and high > min_size:
                high -= 1
                chosen_font = load_font(high)
                chosen_bbox = draw.textbbox((0, 0), text, font=chosen_font)
                text_width = chosen_bbox[2] - chosen_bbox[0]
                text_height = chosen_bbox[3] - chosen_bbox[1]

            x = (width - text_width) // 2 - chosen_bbox[0]
            y = (height - text_height) // 2 - chosen_bbox[1]
            overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
            overlay_draw = ImageDraw.Draw(overlay)
            stroke_width = max(1, int(min(width, height) * 0.0012))
            # Main watermark opacity is 16.5%, within the requested 10-18% starting range.
            overlay_draw.text(
                (x, y), text, font=chosen_font, fill=(218, 218, 218, 42),
                stroke_width=stroke_width, stroke_fill=(0, 0, 0, 24),
            )
            result = Image.alpha_composite(base, overlay)

            output_format = {
                "JPG": "JPEG", "JPEG": "JPEG", "PNG": "PNG", "WEBP": "WEBP",
                "BMP": "BMP", "TIFF": "TIFF", "TIF": "TIFF",
            }.get(source_format, "JPEG")
            save_options = {}
            if output_format == "JPEG":
                result = result.convert("RGB")
                save_options.update(quality=95, optimize=True, subsampling=0)
                exif = source_info.get("exif")
                if exif:
                    save_options["exif"] = exif
            elif output_format == "PNG":
                save_options.update(optimize=True, compress_level=6)
            elif output_format == "WEBP":
                save_options.update(quality=95, method=4)
            elif output_format == "TIFF":
                save_options.update(compression="tiff_lzw")
            result.save(output_path, format=output_format, **save_options)
            if not os.path.isfile(output_path) or os.path.getsize(output_path) <= 0:
                logger.error("Watermark output missing/empty: %s", output_path)
                return False
            with Image.open(output_path) as verification:
                if verification.size != source_size:
                    logger.error("Watermark changed image dimensions: %s -> %s", source_size, verification.size)
                    return False
            logger.info(
                "Watermark applied: %s (%sx%s, font=%s, text width=%.1f%%, centered, alpha=16.5%%)",
                output_path, width, height, getattr(chosen_font, "size", "fallback"),
                (text_width / width * 100) if width else 0,
            )
            return True
        except Exception as e:
            logger.error("Watermark failed for %s: %s", input_path, e, exc_info=True)
            return False

    async def cleanup_unused_blocks(self, days: int = 30):
        """Clean up unused image blocks"""
        try:
            from datetime import datetime, timedelta
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            async with self.db_manager.get_connection() as conn:
                # Remove blocks that haven't been used recently
                cursor = await conn.execute('''
                    DELETE FROM blocked_images 
                    WHERE usage_count = 0 AND created_at < ?
                ''', (cutoff_date,))
                deleted_count = cursor.rowcount
                await conn.commit()
                
            logger.info(f"Cleaned up {deleted_count} unused image blocks older than {days} days")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup image blocks: {e}")
            return 0

    async def cleanup_orphaned_hashes(self) -> int:
        """Clean up orphaned image hashes that are no longer referenced"""
        try:
            async with self.db_manager.get_connection() as conn:
                # Find hashes that are not referenced by any active pairs
                cursor = await conn.execute('''
                    DELETE FROM blocked_images 
                    WHERE pair_id IS NOT NULL 
                    AND pair_id NOT IN (SELECT id FROM pairs WHERE status = 'active')
                ''')
                orphaned_count = cursor.rowcount
                await conn.commit()
                
                logger.info(f"Cleaned up {orphaned_count} orphaned image hashes")
                return orphaned_count
                
        except Exception as e:
            logger.error(f"Failed to cleanup orphaned hashes: {e}")
            return 0
    
    async def get_image_stats(self) -> Dict[str, int]:
        """Get image blocking statistics"""
        try:
            async with self.db_manager.get_connection() as conn:
                stats = {}
                
                # Total blocked images
                cursor = await conn.execute('SELECT COUNT(*) FROM blocked_images')
                result = await cursor.fetchone()
                stats['total_blocked'] = result[0] if result and result[0] else 0
                
                # Global blocks
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM blocked_images WHERE block_scope = "global"'
                )
                result = await cursor.fetchone()
                stats['global_blocks'] = result[0] if result and result[0] else 0
                
                # Pair-specific blocks
                cursor = await conn.execute(
                    'SELECT COUNT(*) FROM blocked_images WHERE block_scope = "pair"'
                )
                result = await cursor.fetchone()
                stats['pair_blocks'] = result[0] if result and result[0] else 0
                
                # Total usage
                cursor = await conn.execute('SELECT SUM(usage_count) FROM blocked_images')
                result = await cursor.fetchone()
                stats['total_blocks_triggered'] = result[0] if result and result[0] else 0
                
                return stats
                
        except Exception as e:
            logger.error(f"Failed to get image stats: {e}")
            return {}
    
    async def remove_image_block_by_hash(self, image_hash: str, pair_id: Optional[int] = None) -> bool:
        """Remove image block by hash"""
        try:
            async with self.db_manager.get_connection() as conn:
                if pair_id:
                    # Remove pair-specific block
                    await conn.execute('''
                        DELETE FROM blocked_images 
                        WHERE phash = ? AND pair_id = ?
                    ''', (image_hash, pair_id))
                else:
                    # Remove global block
                    await conn.execute('''
                        DELETE FROM blocked_images 
                        WHERE phash = ? AND block_scope = 'global'
                    ''', (image_hash,))
                
                await conn.commit()
                logger.info(f"Removed image block: {image_hash[:16]}...")
                return True
                
        except Exception as e:
            logger.error(f"Error removing image block: {e}")
            return False
    
    def clear_cache(self):
        """Clear hash cache"""
        self._hash_cache.clear()
        logger.info("Image hash cache cleared")
