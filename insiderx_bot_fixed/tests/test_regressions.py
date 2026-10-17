"""Offline regression tests for the watermark and edit/delete sync code paths.

These tests use lightweight dependency shims because the upload environment does not
include the Telegram runtime packages. Live Bot API behavior still requires integration tests.
"""
from __future__ import annotations

import asyncio
import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from PIL import Image, ImageChops

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def install_dependency_shims() -> None:
    """Provide only import-time names required by modules under unit test."""
    database = types.ModuleType("database")
    database.DatabaseManager = type("DatabaseManager", (), {})
    database.MessagePair = type("MessagePair", (), {})
    database.MessageMapping = type("MessageMapping", (), {})
    sys.modules["database"] = database

    config = types.ModuleType("config")
    config.Config = type("Config", (), {})
    sys.modules["config"] = config

    telegram = types.ModuleType("telegram")
    telegram.Bot = type("Bot", (), {})
    telegram.MessageEntity = type("MessageEntity", (), {})
    telegram.InputFile = type("InputFile", (), {})
    for class_name in (
        "InputMediaPhoto", "InputMediaVideo", "InputMediaDocument",
        "InputMediaAudio", "InputMediaAnimation",
    ):
        setattr(telegram, class_name, type(class_name, (), {"__init__": lambda self, **kw: self.__dict__.update(kw)}))
    sys.modules["telegram"] = telegram

    telegram_error = types.ModuleType("telegram.error")
    class TelegramError(Exception):
        pass
    class BadRequest(TelegramError):
        pass
    class Forbidden(TelegramError):
        pass
    telegram_error.TelegramError = TelegramError
    telegram_error.BadRequest = BadRequest
    telegram_error.Forbidden = Forbidden
    class RetryAfter(TelegramError):
        def __init__(self, retry_after):
            super().__init__(f"retry after {retry_after}")
            self.retry_after = retry_after
    telegram_error.RetryAfter = RetryAfter
    sys.modules["telegram.error"] = telegram_error

    telethon = types.ModuleType("telethon")
    telethon_tl = types.ModuleType("telethon.tl")
    telethon_types = types.ModuleType("telethon.tl.types")
    telethon_types.MessageMediaPhoto = type("MessageMediaPhoto", (), {})
    telethon_types.MessageMediaDocument = type("MessageMediaDocument", (), {})
    sys.modules["telethon"] = telethon
    sys.modules["telethon.tl"] = telethon_tl
    sys.modules["telethon.tl.types"] = telethon_types
    telethon_errors = types.ModuleType("telethon.errors")
    telethon_errors.FloodWaitError = type("FloodWaitError", (Exception,), {})
    sys.modules["telethon.errors"] = telethon_errors


install_dependency_shims()
image_handler_module = importlib.import_module("image_handler")
message_processor_module = importlib.import_module("message_processor")


class WatermarkRegressionTests(unittest.TestCase):
    def setUp(self):
        self.handler = image_handler_module.ImageHandler(None, None)
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

    def _test_watermark_dimensions(self, size):
        source = Path(self.temp_dir.name) / f"source_{size[0]}x{size[1]}.png"
        output = Path(self.temp_dir.name) / f"output_{size[0]}x{size[1]}.png"
        Image.new("RGB", size, (95, 115, 135)).save(source, "PNG")
        self.assertTrue(self.handler.add_text_watermark(str(source), str(output), "@InsiderX"))
        with Image.open(source) as original, Image.open(output) as watermarked:
            self.assertEqual(watermarked.size, original.size)
            diff = ImageChops.difference(original.convert("RGB"), watermarked.convert("RGB"))
            bbox = diff.getbbox()
            self.assertIsNotNone(bbox, "watermark must visibly change image pixels")
            x0, y0, x1, y1 = bbox
            self.assertGreaterEqual((x1 - x0) / size[0], 0.70, "watermark should span most of the width")
            self.assertLess(abs(((y0 + y1) / 2) - size[1] / 2), size[1] * 0.12, "watermark should be vertically centered")

    def test_landscape_watermark_size_and_center(self):
        self._test_watermark_dimensions((1200, 700))

    def test_portrait_watermark_size_and_center(self):
        self._test_watermark_dimensions((480, 1000))

    def test_animated_images_are_not_silently_flattened(self):
        source = Path(self.temp_dir.name) / "animated.gif"
        output = Path(self.temp_dir.name) / "animated_watermarked.gif"
        frames = [Image.new("RGB", (160, 90), color) for color in ((255, 0, 0), (0, 0, 255))]
        frames[0].save(source, save_all=True, append_images=frames[1:], duration=100, loop=0)
        self.assertFalse(self.handler.add_text_watermark(str(source), str(output), "@InsiderX"))
        with Image.open(source) as original:
            self.assertEqual(original.n_frames, 2)
        self.assertFalse(output.exists() and output.stat().st_size > 0)

    def test_watermark_defaults_when_text_is_empty(self):
        source = Path(self.temp_dir.name) / "plain.png"
        output = Path(self.temp_dir.name) / "watermarked.png"
        Image.new("RGB", (640, 420), (245, 245, 245)).save(source, "PNG")
        self.assertTrue(self.handler.add_text_watermark(str(source), str(output), ""))
        self.assertTrue(output.exists())


class MessageSyncRegressionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.processor = message_processor_module.MessageProcessor.__new__(message_processor_module.MessageProcessor)
        self.processor.db_manager = SimpleNamespace(
            get_message_mapping=AsyncMock(),
            delete_message_mapping=AsyncMock(),
            update_pair=AsyncMock(),
        )
        self.processor._process_message_content = AsyncMock(return_value=("updated caption", []))
        self.processor._validate_and_convert_entities = lambda text, entities: []
        self.processor._contains_urls = lambda text: False
        self.processor._get_message_type = lambda event: "photo"
        self.pair = SimpleNamespace(id=7, destination_chat_id=-200, stats={})
        self.mapping = SimpleNamespace(
            source_message_id=123,
            destination_message_id=456,
            pair_id=7,
            destination_chat_id=-200,
            has_media=True,
            message_type="photo",
        )
        self.processor.db_manager.get_message_mapping.return_value = self.mapping

    async def test_image_caption_edit_uses_edit_media_and_not_new_send(self):
        bot = SimpleNamespace(
            edit_message_media=AsyncMock(),
            edit_message_caption=AsyncMock(),
            edit_message_text=AsyncMock(),
            send_photo=AsyncMock(),
        )
        handle = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        handle.write(b"fake-media")
        handle.close()
        path = Path(handle.name)
        self.addCleanup(lambda: path.exists() and path.unlink())
        self.processor._download_and_prepare_media = AsyncMock(return_value={"file_path": str(path), "type": "photo"})
        event = SimpleNamespace(id=123, media=object())

        result = await self.processor.process_message_edit(event, self.pair, bot, 0)

        self.assertTrue(result)
        bot.edit_message_media.assert_awaited_once()
        bot.edit_message_caption.assert_not_awaited()
        bot.edit_message_text.assert_not_awaited()
        bot.send_photo.assert_not_awaited()
        self.assertFalse(path.exists(), "temporary replacement-media file must be cleaned up")

    async def test_text_edit_uses_text_endpoint(self):
        self.mapping.has_media = False
        self.mapping.message_type = "text"
        self.processor._process_message_content = AsyncMock(return_value=("edited text", []))
        bot = SimpleNamespace(
            edit_message_media=AsyncMock(),
            edit_message_caption=AsyncMock(),
            edit_message_text=AsyncMock(),
            send_message=AsyncMock(),
        )
        event = SimpleNamespace(id=123, media=None)

        result = await self.processor.process_message_edit(event, self.pair, bot, 0)

        self.assertTrue(result)
        bot.edit_message_text.assert_awaited_once()
        bot.edit_message_media.assert_not_awaited()
        bot.edit_message_caption.assert_not_awaited()
        bot.send_message.assert_not_awaited()

    async def test_retry_after_propagates_to_queue_worker(self):
        retry_after = message_processor_module.RetryAfter(3)
        bot = SimpleNamespace(edit_message_text=AsyncMock(side_effect=retry_after))
        self.mapping.has_media = False
        self.mapping.message_type = "text"
        self.processor._process_message_content = AsyncMock(return_value=("edited text", []))
        event = SimpleNamespace(id=123, media=None)

        with self.assertRaises(message_processor_module.RetryAfter):
            await self.processor.process_message_edit(event, self.pair, bot, 0)

    async def test_delete_sync_removes_mapping_after_destination_deletion(self):
        bot = SimpleNamespace(delete_message=AsyncMock())
        event = SimpleNamespace(deleted_ids=[123, 123, 999])
        async def lookup(source_id, pair_id):
            return self.mapping if source_id == 123 else None
        self.processor.db_manager.get_message_mapping.side_effect = lookup

        result = await self.processor.process_message_delete(event, self.pair, bot, 0)

        self.assertTrue(result)
        bot.delete_message.assert_awaited_once_with(chat_id=-200, message_id=456)
        self.processor.db_manager.delete_message_mapping.assert_awaited_once_with(123, 7)
        self.processor.db_manager.update_pair.assert_awaited_once_with(self.pair)


if __name__ == "__main__":
    unittest.main()
