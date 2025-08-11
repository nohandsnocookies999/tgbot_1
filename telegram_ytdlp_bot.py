# Telegram YT-DLP Bot (MAX quality + archive every 10)
# Use only for content you are allowed to download.

from __future__ import annotations

import asyncio
import os
import re
import shlex
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List, Dict
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, FSInputFile
from dotenv import load_dotenv
import yt_dlp
from yt_dlp.utils import DownloadError

# ---------------- configuration ----------------
BATCH_SIZE = 10         # archive each N videos in /getall
DEFAULT_HEIGHT = 0      # 0 => MAX available quality by default
ALLOWED_NETLOC = {"youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com"}
# ------------------------------------------------

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Please set BOT_TOKEN environment variable (e.g., in .env)")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# Guide text (avoid multiline literals)
GUIDE_TEXT = "
".join([
    "<b>YT‑DLP Telegram Bot — Guide</b>",
    "",
    "<b>Commands</b>",
    "/get <url> [video|audio] [360|480|720|1080|max]",
    "/getall <channel_or_playlist_url> [video|audio] [360|480|720|1080|max]",
    "",
    "По умолчанию качество: MAX. В /getall бот архивирует каждые 10 видео (без ограничений по размеру).",
    "Используйте только контент, на который у вас есть права.",
])

YTDLP_COMMON: Dict[str, object] = {
    "outtmpl": "%(title).80s.%(ext)s",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "concurrent_fragment_downloads": 5,
    # headers + extractor args help avoid some 403 / format issues
    "http_headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.youtube.com/",
    },
    "extractor_args": {
        "youtube": {
            "player_client": ["android", "web_safari", "web"],
        }
    },
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


def _build_format_string(mode: str, height: int) -> str:
    if mode == "audio":
        return "ba/bestaudio/best"
    # video
    if height and height > 0:
        # prefer MP4-compatible streams, then wider fallbacks
        return (
            "bv*[height<=" + str(height) + "][ext=mp4]+ba[ext=m4a]/"
            "bv*[height<=" + str(height) + "]+ba/"
            "b[height<=" + str(height) + "]/"
            "bv*+ba/b"
        )
    # MAX quality
    return "bv*+ba/b"


def ytdlp_download(url: str, mode: str, height: int, workdir: Path) -> DLResult:
    """Synchronous helper used in a thread via asyncio.to_thread with robust fallbacks."""
    opts = dict(YTDLP_COMMON)
    opts["paths"] = {"home": str(workdir)}
    opts["merge_output_format"] = "mp4"

    fmt = _build_format_string(mode, height)
    opts["format"] = fmt

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
    except DownloadError as e:
        msg = str(e)
        # Retry 1: if specific format missing, simplify
        if "Requested format is not available" in msg:
            opts_simple = dict(opts)
            opts_simple["format"] = "b/best" if mode != "audio" else "ba/bestaudio/best"
            with yt_dlp.YoutubeDL(opts_simple) as y2:
                info = y2.extract_info(url, download=True)
        # Retry 2: 403 workaround
        elif "HTTP Error 403" in msg or "Forbidden" in msg:
            opts_android = dict(opts)
            opts_android["extractor_args"] = {"youtube": {"player_client": ["android"]}}
            with yt_dlp.YoutubeDL(opts_android) as y3:
                info = y3.extract_info(url, download=True)
        else:
            raise

    if "requested_downloads" in info and info["requested_downloads"]:
        fn = Path(info["requested_downloads"][0]["filepath"])
    else:
        suffix = ".mp3" if mode == "audio" else ".mp4"
        with yt_dlp.YoutubeDL(opts) as ydl_tmp:
            fn = Path(ydl_tmp.prepare_filename(info)).with_suffix(suffix)
    title = info.get("title", fn.stem)
    return DLResult(path=fn, title=title, ext=fn.suffix.lstrip("."))


# -------------------- playlist helpers --------------------

def _safe_stem(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._ -]+", "_", (name or "file")).strip()
    return s or "file"


def _normalize_watch_url(entry: dict) -> Optional[str]:
    url = entry.get("webpage_url") or entry.get("url") or ""
    if not url:
        return None
    if not url.startswith("http"):
        return "https://www.youtube.com/watch?v=" + url
    return url


def list_playlist_urls(url: str) -> List[str]:
    opts = dict(YTDLP_COMMON)
    opts.update({
        "noplaylist": False,
        "extract_flat": "in_playlist",
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
    })
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        entries = info.get("entries") or []
        urls: List[str] = []
        seen = set()
        for e in entries:
            if not isinstance(e, dict):
                continue
            w = _normalize_watch_url(e)
            if not w or w in seen:
                continue
            seen.add(w)
            urls.append(w)
        return urls


def make_zip(files: List[Tuple[Path, str]], outdir: Path, batch_idx: int) -> Path:
    zpath = outdir / ("batch_" + str(batch_idx).zfill(3) + ".zip")
    zf = zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True)
    for p, title in files:
        if not p.exists():
            continue
        arcname = _safe_stem(title) + p.suffix.lower()
        zf.write(p, arcname=arcname)
    zf.close()
    return zpath


# -------------------- Telegram handlers --------------------

@dp.message(Command("start"))
async def cmd_start(message: Message):
    lines = [
        "Привет! Пришли команду в формате:",
        "/get <YouTube URL> [video|audio] [360|480|720|1080|max]",
        "/getall <channel_or_playlist_url> [video|audio] [360|480|720|1080|max]",
        "",
        "По умолчанию качество: MAX. В /getall архивируем каждые 10 видео (без ограничений по размеру).",
    ]
    await message.reply("
".join(lines))


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
            caption="Справка к боту",
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
    height = DEFAULT_HEIGHT  # 0 => MAX

    if len(args) >= 3:
        m = args[2].lower()
        if m in {"video", "audio"}:
            mode = m
        elif m == "max":
            height = 0
        elif m.isdigit():
            height = int(m)
    if len(args) >= 4:
        m2 = args[3].lower()
        if m2 == "max":
            height = 0
        elif m2.isdigit():
            height = int(m2)

    await message.reply("Ок, качаю " + ("аудио" if mode == "audio" else "видео") + ": качество = " + ("MAX" if height == 0 else str(height)))

    with tempfile.TemporaryDirectory() as td:
        workdir = Path(td)
        try:
            dl = await asyncio.to_thread(ytdlp_download, url, mode, height, workdir)
        except Exception as e:
            await message.reply("Не получилось скачать: " + str(e))
            return

        out_path = dl.path

        try:
            caption = str(dl.title) + " (через yt-dlp)"
            file = FSInputFile(str(out_path))
            await message.answer_document(file, caption=caption)
        except Exception as e:
            await message.reply("Не удалось отправить файл: " + str(e))


@dp.message(Command("getall"))
async def cmd_getall(message: Message):
    args = (message.text or "").split()
    if len(args) < 2:
        await message.reply(
            "Использование: /getall <ссылка на канал/плейлист> [video|audio] [360|480|720|1080|max]"
        )
        return

    url = args[1].strip()
    if not is_youtube_url(url):
        await message.reply("Похоже, это не ссылка на YouTube.")
        return

    mode = "video"
    height = DEFAULT_HEIGHT  # 0 => MAX

    for token in args[2:]:
        t = token.lower()
        if t in {"video", "audio"}:
            mode = t
        elif t == "max":
            height = 0
        elif t.isdigit():
            height = int(t)

    await message.reply("Собираю список видео…")

    try:
        urls = await asyncio.to_thread(list_playlist_urls, url)
    except Exception as e:
        await message.reply("Не удалось получить список видео: " + str(e))
        return

    if not urls:
        await message.reply("Видео не нашлись. Возможно, нужен другой URL (например, вкладка /videos канала).")
        return

    total = len(urls)
    await message.reply("Найдено " + str(total) + " видео. Скачиваю и архивирую каждые " + str(BATCH_SIZE) + ".")

    # One persistent temp dir so files live until zipped
    with tempfile.TemporaryDirectory() as session_td:
        session_dir = Path(session_td)
        workdir = session_dir / "items"
        workdir.mkdir(parents=True, exist_ok=True)

        batch_files: List[Tuple[Path, str]] = []
        batch_index = 1
        processed = 0

        for idx, watch_url in enumerate(urls, 1):
            try:
                note = await message.answer(str(idx) + "/" + str(total) + " — скачиваю…")
                try:
                    dl = await asyncio.to_thread(ytdlp_download, watch_url, mode, height, workdir)
                except Exception as e:
                    await note.edit_text(str(idx) + "/" + str(total) + " — ошибка: " + str(e))
                    continue

                batch_files.append((dl.path, dl.title))
                processed += 1
                await note.edit_text(str(idx) + "/" + str(total) + " — готово, добавлено в пакет")

                # every BATCH_SIZE videos → make a ZIP and send
                if len(batch_files) >= BATCH_SIZE:
                    z = make_zip(batch_files, session_dir, batch_index)
                    try:
                        cap = "Пакет " + str(batch_index) + " (" + str(len(batch_files)) + " видео) — " + z.name
                        await message.answer_document(FSInputFile(str(z)), caption=cap)
                    except Exception as e:
                        await message.answer("Не удалось отправить архив " + z.name + ": " + str(e))
                    batch_files = []
                    batch_index += 1

                await asyncio.sleep(0.8)
            except Exception:
                continue

        # remaining files → last ZIP
        if batch_files:
            z = make_zip(batch_files, session_dir, batch_index)
            try:
                cap = "Пакет " + str(batch_index) + " (" + str(len(batch_files)) + " видео) — " + z.name
                await message.answer_document(FSInputFile(str(z)), caption=cap)
            except Exception as e:
                await message.answer("Не удалось отправить архив " + z.name + ": " + str(e))

    await message.reply("Готово. Обработано: " + str(processed) + " из " + str(total) + ".")


# -------------------- entrypoint --------------------

async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
