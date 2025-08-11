# Telegram YT-DLP Bot — MAX quality + archive every 10 videos (upload to PixelDrain)
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
import requests

# ---------------- configuration ----------------
BATCH_SIZE = 10         # /getall: make one ZIP after every N downloaded videos
DEFAULT_HEIGHT = 0      # 0 => MAX quality by default
ALLOWED_NETLOC = {"youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com"}
PIXEL_API = "https://pixeldrain.com/api/file"
PIXEL_VIEW = "https://pixeldrain.com/u/"
PIXEL_DL   = "https://pixeldrain.com/api/file/"
# Optional: if you have a PixelDrain API key (to attach uploads to your account)
PIXEL_API_KEY = os.getenv("PIXELDRAIN_API_KEY") or os.getenv("PIXEL_API_KEY")
# ------------------------------------------------

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Please set BOT_TOKEN environment variable (e.g., in .env)")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# Guide text (no multiline literals inside handlers)
GUIDE_TEXT = "\n".join([
    "<b>YT-DLP Telegram Bot — Guide</b>",
    "",
    "<b>Commands</b>",
    "/get <url> [video|audio] [360|480|720|1080|max]",
    "/getall <channel_or_playlist_url> [video|audio] [360|480|720|1080|max]",
    "",
    "Default quality: MAX.",
    "In /getall the bot archives every 10 videos and uploads ZIP to PixelDrain, replying with a link.",
])

YTDLP_COMMON: Dict[str, object] = {
    "outtmpl": "%(title).80s.%(ext)s",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "concurrent_fragment_downloads": 5,
    # Headers and extractor args reduce 403 and format issues
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
        # Prefer MP4-compatible, then broader fallbacks; finally progressive best
        return (
            "bv*[height<=" + str(height) + "][ext=mp4]+ba[ext=m4a]/"
            "bv*[height<=" + str(height) + "]+ba/"
            "b[height<=" + str(height) + "]/"
            "bv*+ba/b"
        )
    # MAX quality
    return "bv*+ba/b"


def ytdlp_download(url: str, mode: str, height: int, workdir: Path) -> DLResult:
    """Blocking download with robust format fallbacks. Called via asyncio.to_thread."""
    opts = dict(YTDLP_COMMON)
    opts["paths"] = {"home": str(workdir)}
    opts["merge_output_format"] = "mp4"
    opts["format"] = _build_format_string(mode, height)

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
    except DownloadError as e:
        msg = str(e)
        if "Requested format is not available" in msg:
            opts2 = dict(opts)
            opts2["format"] = "b/best" if mode != "audio" else "ba/bestaudio/best"
            with yt_dlp.YoutubeDL(opts2) as y2:
                info = y2.extract_info(url, download=True)
        elif "HTTP Error 403" in msg or "Forbidden" in msg:
            opts3 = dict(opts)
            opts3["extractor_args"] = {"youtube": {"player_client": ["android"]}}
            with yt_dlp.YoutubeDL(opts3) as y3:
                info = y3.extract_info(url, download=True)
        else:
            raise

    if "requested_downloads" in info and info["requested_downloads"]:
        fn = Path(info["requested_downloads"][0]["filepath"])
    else:
        suffix = ".mp3" if mode == "audio" else ".mp4"
        with yt_dlp.YoutubeDL(opts) as ytmp:
            fn = Path(ytmp.prepare_filename(info)).with_suffix(suffix)
    title = info.get("title", fn.stem)
    return DLResult(path=fn, title=title, ext=fn.suffix.lstrip("."))


# ---------------- PixelDrain upload helpers ----------------

def upload_pixeldrain(path: Path) -> Tuple[str, str]:
    auth = ("user", PIXEL_API_KEY) if PIXEL_API_KEY else None
    with open(path, "rb") as f:
        r = requests.post(PIXEL_API, files={"file": (path.name, f)}, auth=auth, timeout=120)
    r.raise_for_status()
    data = r.json()
    fid = str(data.get("id") or "")
    if not fid:
        raise RuntimeError("PixelDrain: no id in response")
    view = PIXEL_VIEW + fid
    direct = PIXEL_DL + fid
    return view, direct


# --------------- playlist helpers & archiving ---------------

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


def make_zip_single(files: List[Tuple[Path, str]], outdir: Path, batch_idx: int) -> Path:
    zpath = outdir / ("batch_" + str(batch_idx).zfill(3) + ".zip")
    zf = zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True)
    for p, title in files:
        if not p.exists():
            continue
        arcname = _safe_stem(title) + p.suffix.lower()
        zf.write(p, arcname=arcname)
    zf.close()
    return zpath


# ------------------------ handlers -------------------------

@dp.message(Command("start"))
async def cmd_start(message: Message):
    lines = [
        "Привет! Команды:",
        "/get <YouTube URL> [video|audio] [360|480|720|1080|max]",
        "/getall <канал/плейлист URL> [video|audio] [360|480|720|1080|max]",
        "По умолчанию качество: MAX. В /getall архивируем каждые 10 видео и заливаем ZIP на PixelDrain.",
    ]
    await message.reply("\n".join(lines))


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
            caption="Guide",
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

    msg = "Ок, качаю {}: качество = {}".format("аудио" if mode == "audio" else "видео", "MAX" if height == 0 else str(height))
    await message.reply(msg)

    with tempfile.TemporaryDirectory() as td:
        workdir = Path(td)
        try:
            dl = await asyncio.to_thread(ytdlp_download, url, mode, height, workdir)
        except Exception as e:
            await message.reply("Не получилось скачать: " + str(e))
            return

        try:
            caption = str(dl.title) + " (yt-dlp)"
            await message.answer_document(FSInputFile(str(dl.path)), caption=caption)
        except Exception as e:
            err = str(e)
            if ("Too Large" in err) or ("too big" in err) or ("413" in err):
                try:
                    view, direct = await asyncio.to_thread(upload_pixeldrain, dl.path)
                    text = f"Файл большой, залил на PixelDrain:\n{view}\nПрямая ссылка: {direct}"
                    await message.answer(text)
                except Exception as e2:
                    await message.reply("Не удалось отправить файл и загрузить на PixelDrain: " + str(e2))
            else:
                await message.reply("Не удалось отправить файл: " + err)


@dp.message(Command("getall"))
async def cmd_getall(message: Message):
    args = (message.text or "").split()
    if len(args) < 2:
        await message.reply("Использование: /getall <ссылка на канал/плейлист> [video|audio] [360|480|720|1080|max]")
        return

    url = args[1].strip()
    if not is_youtube_url(url):
        await message.reply("Похоже, это не ссылка на YouTube.")
        return

    mode = "video"
    height = DEFAULT_HEIGHT

    for token in args[2:]:
        t = token.lower()
        if t in {"video", "audio"}:
            mode = t
        elif t == "max":
            height = 0
        elif t.isdigit():
            height = int(t)

    await message.reply("Собираю список…")
    try:
        urls = await asyncio.to_thread(list_playlist_urls, url)
    except Exception as e:
        await message.reply("Не удалось получить список видео: " + str(e))
        return

    if not urls:
        await message.reply("Видео не нашлись. Возможно, нужен /videos URL у канала.")
        return

    total = len(urls)
    await message.reply("Найдено " + str(total) + " видео. Пакеты по " + str(BATCH_SIZE) + " шт.; для каждого будет ссылка PixelDrain.")

    # Persistent temp dir for the whole batch so files live until zipped
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

                if len(batch_files) >= BATCH_SIZE:
                    z = make_zip_single(batch_files, session_dir, batch_index)
                    try:
                        view, direct = await asyncio.to_thread(upload_pixeldrain, z)
                        text = f"Пакет {batch_index} ({len(batch_files)} видео):\n{view}\nПрямая ссылка: {direct}"
                        await message.answer(text)
                    except Exception as e:
                        await message.answer("Не удалось загрузить архив на PixelDrain: " + str(e))
                    batch_files = []
                    batch_index += 1

                await asyncio.sleep(0.8)
            except Exception:
                continue

        # Remaining files
        if batch_files:
            z = make_zip_single(batch_files, session_dir, batch_index)
            try:
                view, direct = await asyncio.to_thread(upload_pixeldrain, z)
                text = f"Пакет {batch_index} ({len(batch_files)} видео):\n{view}\nПрямая ссылка: {direct}"
                await message.answer(text)
            except Exception as e:
                await message.answer("Не удалось загрузить архив на PixelDrain: " + str(e))

    await message.reply("Готово. Обработано: " + str(processed) + " из " + str(total) + ".")


# -------------------- lightweight self-tests --------------------

def _selftest() -> None:
    # format string tests
    assert _build_format_string("audio", 0).startswith("ba"), "audio best format"
    assert "height<=720" in _build_format_string("video", 720), "height filter in format"

    # safe stem
    assert _safe_stem("a*b?c").startswith("a_b_c"), "safe stem replaces illegal chars"

    # message formatting (the bug source): ensure f-strings with \n are correct
    view = "https://pixeldrain.com/u/XYZ"
    direct = "https://pixeldrain.com/api/file/XYZ"
    txt = f"Файл большой, залил на PixelDrain:\n{view}\nПрямая ссылка: {direct}"
    assert "\n" in txt and view in txt and direct in txt


# -------------------- entrypoint --------------------

async def main():
    if os.getenv("SELFTEST") == "1":
        _selftest()
        print("SELFTEST passed")
        return
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
