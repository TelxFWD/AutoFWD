# InsiderX Telegram Forwarding Bot — patched source bundle

This bundle contains the eight Python modules supplied for the existing bot, targeted fixes, and offline regression tests. The existing Python/PTB/Telethon architecture and SQLite schema are retained.

## Setup

Use Python 3.10+ in the directory containing `main.py`.

```bash
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Create a `.env` file with the required values:

```dotenv
TELEGRAM_API_ID=your_telegram_api_id
TELEGRAM_API_HASH=your_telegram_api_hash
TELEGRAM_PHONE=+countrycode_number
TELEGRAM_BOT_TOKEN_1=your_message_sender_bot_token
ADMIN_BOT_TOKEN=your_admin_bot_token
ADMIN_USER_IDS=your_numeric_telegram_user_id
DATABASE_PATH=bot.db
```

Optional environment settings remain supported by `config.py`, including `TELEGRAM_BOT_TOKEN_2` through `_10`, `MAX_WORKERS`, `MAX_RETRIES`, `MESSAGE_QUEUE_SIZE`, timeouts/rate limits, and health-monitor thresholds. Keep secrets only in `.env` or your hosting secret store; do not include `.env` in the ZIP or commit it to source control.

Start locally:

```bash
python main.py
```

Example PM2 deployment:

```bash
pm2 start main.py --name insiderx-forwarder --interpreter python3 --cwd /path/to/insiderx_bot_fixed
pm2 save
pm2 logs insiderx-forwarder
```

## Watermark command

In the configured admin bot, for a valid forwarding pair:

```text
/watermark 1 enable @InsiderX
/watermark 1 disable
```

`/watermark 1 enable` also works and defaults to `@InsiderX`. A custom text can still be passed. The watermark is large, centered, light gray, serif-first, and translucent; dimensions and placement adapt to the image. Static supported image formats are watermarked. Animated images are deliberately left unchanged rather than silently flattened into a still image.

## Tests

```bash
python -m compileall -q .
python -m unittest discover -s tests -v
```

The regression tests use mocked Telegram dependencies for synchronization paths and the real Pillow library for image output checks. They do not require Telegram credentials.

## Operational notes

- Edit synchronization updates the existing destination message; it does not send a replacement message. Photos and the supported Bot API media types are edited via the media-edit endpoint, with caption edits handled separately.
- Delete synchronization is now enabled for loaded pairs. For Telegram delete updates that have no chat ID, the handler derives candidate pairs from saved mappings. If identical numeric message IDs are mapped from multiple source chats and the update cannot be disambiguated, the handler logs and skips the deletion rather than risk deleting the wrong destination message.
- Queue backpressure replaces the prior behavior of dropping an arbitrary queued item. Per-pair processing order is serialized, retries use bounded exponential backoff and jitter, and Telegram `RetryAfter` values are respected.
- Image duplicate detection still requires the optional `ImageHash` dependency. Watermarking only requires Pillow; a missing `ImageHash` package no longer disables watermark processing.
- No database table migration is needed for these changes. The configured `DATABASE_PATH` is now used by startup.
