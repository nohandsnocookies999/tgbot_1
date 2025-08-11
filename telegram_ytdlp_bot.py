"""
Telegram bot that downloads YouTube videos (or audio) with yt-dlp
and sends them back in chat.

⚠️ Use only for content you own or have permission to download.
   Respect YouTube's Terms of Service and your local laws.

Quick start
-----------
1) Python 3.10+
2) Install system ffmpeg (required by yt-dlp for muxing/transcoding)
3) pip install -r requirements OR minimal:
   pip install aiogram==3.* yt-dlp==2025.* python-dotenv==1.*
4) Create .env next to this file:
   BOT_TOKEN=123456:ABC...
5) Run:  python telegram_ytdlp_bot.py

Commands
--------
/start                              — brief help
/get <url> [video|audio] [360|480|720]  — download & send
/getall <channel_or_playlist_url> [video|audio] [360|480|720] [limit=ALL|N]
Examples:
  /get https://youtu.be/dQw4w9WgXcQ
  /get https://youtu.be/dQw4w9WgXcQ audio
  /get https://youtu.be/dQw4w9WgXcQ video 480
  /getall https://www.youtube.com/@YourChannel/videos limit=ALL

Notes on file sizes
-------------------
• Using the public Bot API endpoint, Telegram accepts uploads up to ~50 MB for files you upload via multipart/form-data.
• If you self‑host Telegram's Local Bot API Server, uploads up to 2 GB are allowed.
• This bot tries to keep outputs ≤ 49 MB for reliability. If a video is larger, it will attempt a fast transcode to shrink it.

Author: you + GPT
"""
from __future__ import annotations
import asyncio
import os
import shlex
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile
from dotenv import load_dotenv
import yt_dlp

# ------------ config -------------
TARGET_MB = 49  # aim to stay below typical Bot API upload limit
DEFAULT_HEIGHT = 480
ALLOWED_NETLOC = {"youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com"}
# ---------------------------------

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Please set BOT_TOKEN environment variable (e.g., via .env)")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

GUIDE_TEXT = (
    """
<b>YT-DLP Telegram Bot — Guide</b>

<b>Что умеет</b>
• /get &lt;url&gt; [video|audio] [360|480|720] — скачать одно видео/аудио
• /getall &lt;channel_or_playlist_url&gt; [video|audio] [360|480|720] [limit=ALL|N] — скачать пачку

<b>Подсказки</b>
• Для канала указывай URL со вкладкой /videos (пример: https://www.youtube.com/@YourChannel/videos)
• Если файл большой, бот попробует ужать до ~49 MB. Для надёжности укажи “video 360”.
• Загружай только контент, на который у тебя есть права.

<b>Лимиты Telegram</b>
• Через публичный Bot API: отправка до ~50 MB на файл.
• Через Local Bot API Server: до 2 GB.

<b>Развёртывание в облаке</b>
1) Dockerfile + requirements.txt в репозитории.
2) Переменная окружения BOT_TOKEN.
3) Запуск как Background Worker (Render/Railway/Fly.io) или как systemd‑сервис на VPS.

<b>Команды</b>
/start — краткая помощь
/help — эта справка
/guide — прислать справку файлом
"""
)

YTDLP_COMMON = {
    "outtmpl": "%(title).80s.%(ext)s",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    # speed up networking a bit
    "concurrent_fragment_downloads": 5,
}

@dataclass
class DLResult:
    path: Path
    title: str
    ext: str


def is_youtube_url(url: str) -> bool:
    try:
        netloc = urlparse(url).netloc.lower()
        return any(netloc.endswith(d) for d in ALLOWED_NETLOC)
    except Exception:
        return False


async def run_cmd(cmd: str) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out_b, err_b = await proc.communicate()
    return proc.returncode, out_b.decode("utf-8", "ignore"), err_b.decode("utf-8", "ignore")


def sizeof_mb(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024)


async def ffprobe_duration(path: Path) -> Optional[float]:
    cmd = f"ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 {shlex.quote(str(path))}"
    code, out, _ = await run_cmd(cmd)
    if code == 0:
        try:
            return float(out.strip())
        except Exception:
            return None
    return None


async def shrink_video(in_path: Path, out_path: Path, target_mb: int = TARGET_MB, height: int = 360) -> bool:
    """Transcode H.264 + AAC roughly targeting <= target_mb using simple bitrate math.
    Returns True if out_path created.
    """
    duration = await ffprobe_duration(in_path) or 0
    if duration <= 0:
        # fallback single-pass
        cmd = (
            f"ffmpeg -y -i {shlex.quote(str(in_path))} -vf scale=-2:{height} "
            f"-c:v libx264 -preset veryfast -crf 28 -c:a aac -b:a 96k -movflags +faststart {shlex.quote(str(out_path))}"
        )
        code, _, _ = await run_cmd(cmd)
        return code == 0 and out_path.exists()

    audio_kbps = 96
    # bits budget
    target_bits = target_mb * 8 * 1024 * 1024
    vbps = max(300_000, int(target_bits / duration) - audio_kbps * 1000)

    cmd = (
        f"ffmpeg -y -i {shlex.quote(str(in_path))} -vf scale=-2:{height} "
        f"-c:v libx264 -preset veryfast -b:v {vbps} -maxrate {int(vbps*1.2)} -bufsize {int(vbps*2)} "
        f"-c:a aac -b:a {audio_kbps}k -movflags +faststart {shlex.quote(str(out_path))}"
    )
    code, _, _ = await run_cmd(cmd)
    return code == 0 and out_path.exists()


async def ytdlp_download(url: str, mode: str, height: int, workdir: Path) -> DLResult:
    """Download using yt-dlp. mode: 'video' or 'audio'"""
    opts = dict(YTDLP_COMMON)
    opts["paths"] = {"home": str(workdir)}

    if mode == "audio":
        opts.update(
            {
                "format": "bestaudio/best",
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "192",
                    }
                ],
                "merge_output_format": None,
            }
        )
    else:  # video
        fmt = f"bestvideo[height<={height}]+bestaudio/best[height<={height}]"
        opts.update(
            {
                "format": fmt,
                "merge_output_format": "mp4",
            }
        )

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        # Final path inference
        if "requested_downloads" in info and info["requested_downloads"]:
            fn = Path(info["requested_downloads"][0]["filepath"])  # newer yt-dlp
        else:
            fn = Path(ydl.prepare_filename(info)).with_suffix(".mp3" if mode == "audio" else ".mp4")
        title = info.get("title", fn.stem)
        return DLResult(path=fn, title=title, ext=fn.suffix.lstrip("."))


@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.reply(
        "Привет! Пришли команду в формате:
"
        "/get <YouTube URL> [video|audio] [360|480|720]

"
        "Например:
"
        "/get https://youtu.be/dQw4w9WgXcQ
"
        "/get https://youtu.be/dQw4w9WgXcQ audio
"
        "/get https://youtu.be/dQw4w9WgXcQ video 480

"
        "⚠️ Загружай только то, на что у тебя есть права.

"
        "Доп. команды: /help /guide /getall"
    )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(GUIDE_TEXT, parse_mode=ParseMode.HTML)


@dp.message(Command("guide"))
async def cmd_guide(message: Message):
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "yt_dlp_bot_guide.html"
        p.write_text(GUIDE_TEXT, encoding="utf-8")
        await message.answer_document(
            FSInputFile(str(p), filename="yt_dlp_bot_guide.html"),
            caption="Справка к боту"
        )


@dp.message(Command("get"))
async def cmd_get(message: Message):
    args = (message.text or "").split()
    if len(args) < 2:
        await message.reply("Дай ссылку на YouTube после /get")
        return
    url = args[1].strip()

    if not is_youtube_url(url):
        await message.reply("Похоже, это не ссылка на YouTube.")
        return

    mode = "video"
    height = DEFAULT_HEIGHT

    if len(args) >= 3:
        m = args[2].lower()
        if m in {"video", "audio"}:
            mode = m
        elif m.isdigit():
            height = int(m)
    if len(args) >= 4 and args[3].isdigit():
        height = int(args[3])

    # acknowledge
    await message.reply(f"Ок, качаю {mode}… это может занять минутку")

    with tempfile.TemporaryDirectory() as td:
        workdir = Path(td)
        try:
            dl = await asyncio.to_thread(ytdlp_download, url, mode, height, workdir)
        except Exception as e:
            await message.reply(f"Не получилось скачать: {e}")
            return

        out_path = dl.path
        # If video and size is too big, try shrink
        try:
            if mode == "video" and sizeof_mb(out_path) > TARGET_MB:
                smaller = workdir / f"{out_path.stem}.small.mp4"
                ok = await shrink_video(out_path, smaller, target_mb=TARGET_MB, height=min(height, 360))
                if ok and sizeof_mb(smaller) < sizeof_mb(out_path):
                    out_path = smaller
        except Exception:
            pass

        # Choose sending method
        try:
            caption = f"{dl.title}
(через yt-dlp)"
            file = FSInputFile(str(out_path))
            await message.answer_document(file, caption=caption)
        except Exception as e:
            size = sizeof_mb(out_path)
            msg = (
                "Не удалось отправить файл. "
                f"Размер: {size:.1f} MB. Попробуй /get <url> video 360 или аудио.
"
                f"Тех. причина: {e}"
            )
            await message.reply(msg)


# ---------- bulk channel/playlist download ----------

def _normalize_watch_url(entry: dict) -> Optional[str]:
    url = entry.get("webpage_url") or entry.get("url") or ""
    if not url:
        return None
    if not url.startswith("http"):
        # often yt-dlp returns bare IDs when extract_flat is used
        return f"https://www.youtube.com/watch?v={url}"
    return url


def list_playlist_urls(url: str, limit: Optional[int] = None) -> list[str]:
    """Return a list of watch URLs from a channel/playlist URL.
    Uses yt-dlp in extract-only mode (no downloads)."""
    opts = dict(YTDLP_COMMON)
    opts.update({
        "noplaylist": False,            # allow playlists/channels
        "extract_flat": "in_playlist", # faster listing
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
    })
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        entries = info.get("entries") or []
        urls: list[str] = []
        seen = set()
        for e in entries:
            if not isinstance(e, dict):
                continue
            w = _normalize_watch_url(e)
            if not w or w in seen:
                continue
            seen.add(w)
            urls.append(w)
        if limit and limit > 0:
            urls = urls[:limit]
        return urls


@dp.message(Command("getall"))
async def cmd_getall(message: Message):
    """Download and send multiple videos from a channel/playlist.

    Usage:
      /getall <channel_or_playlist_url> [video|audio] [360|480|720] [limit=ALL|N]

    Notes:
      • По умолчанию скачиваем первые 10, чтобы не заспамить чат. Укажи limit=ALL, чтобы взять все.
      • Большие файлы бот попробует ужать до ~49 MB.
    """
    args = (message.text or "").split()
    if len(args) < 2:
        await message.reply(
            "Использование: /getall <ссылка на канал/плейлист> [video|audio] [360|480|720] [limit=ALL|N]"
        )
        return

    url = args[1].strip()
    if not is_youtube_url(url):
        await message.reply("Похоже, это не ссылка на YouTube.")
        return

    mode = "video"
    height = DEFAULT_HEIGHT
    limit: Optional[int] = 10  # по умолчанию берём первые 10

    for token in args[2:]:
        t = token.lower()
        if t in {"video", "audio"}:
            mode = t
        elif t.isdigit():
            height = int(t)
        elif t.startswith("limit="):
            val = t.split("=", 1)[1]
            if val == "all":
                limit = None
            elif val.isdigit():
                limit = int(val)

    await message.reply(f"Собираю список… limit={limit or 'ALL'}")

    try:
        urls = await asyncio.to_thread(list_playlist_urls, url, limit)
    except Exception as e:
        await message.reply(f"Не удалось получить список видео: {e}")
        return

    if not urls:
        await message.reply("Видео не нашлись. Возможно, нужен другой URL (например, вкладка /videos канала).")
        return

    sent = 0
    total = len(urls)
    await message.reply(f"Нашёл {total} видео. Начинаю отправку…")

    for idx, watch_url in enumerate(urls, 1):
        try:
            note = await message.answer(f"{idx}/{total} — скачиваю…")
            with tempfile.TemporaryDirectory() as td:
                workdir = Path(td)
                try:
                    dl = await asyncio.to_thread(ytdlp_download, watch_url, mode, height, workdir)
                except Exception as e:
                    await note.edit_text(f"{idx}/{total} — ошибка скачивания: {e}")
                    continue

                out_path = dl.path
                try:
                    if mode == "video" and sizeof_mb(out_path) > TARGET_MB:
                        smaller = workdir / f"{out_path.stem}.small.mp4"
                        ok = await shrink_video(out_path, smaller, target_mb=TARGET_MB, height=min(height, 360))
                        if ok and sizeof_mb(smaller) < sizeof_mb(out_path):
                            out_path = smaller
                except Exception:
                    pass

                try:
                    caption = f"{dl.title}
(через yt-dlp)"
                    file = FSInputFile(str(out_path))
                    await message.answer_document(file, caption=caption)
                    sent += 1
                    await note.edit_text(f"{idx}/{total} — готово ✅")
                except Exception as e:
                    await note.edit_text(f"{idx}/{total} — не удалось отправить файл: {e}")
            await asyncio.sleep(1.2)  # лёгкая пауза, чтобы не ловить лимиты
        except Exception:
            # продолжаем следующий ролик даже если этот упал
            continue

    await message.reply(f"Готово. Отправлено: {sent} из {total}.")


async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
