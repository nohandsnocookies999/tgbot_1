"""
Telegram bot that downloads YouTube videos (or audio) with yt-dlp
and sends them back in chat.

⚠️ Use only for content you own or have permission to download.
"""

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

# ---------------- configuration ----------------
TARGET_MB = 49          # aim to stay below common Bot API upload limit
ARCHIVE_PART_MB = 47    # ZIP part size when archiving (leave headroom)
DEFAULT_HEIGHT = 480    # default max height for video
ALLOWED_NETLOC = {"youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com"}
# ------------------------------------------------

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Please set BOT_TOKEN environment variable (e.g., in .env)")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# Guide text as safe joined lines (no triple quotes)
GUIDE_LINES: List[str] = [
    "<b>YT-DLP Telegram Bot — Guide</b>",
    "",
    "<b>What it does</b>",
    "• /get &lt;url&gt; [video|audio] [360|480|720|1080] — download single video or audio",
    "• /getall &lt;channel_or_playlist_url&gt; [video|audio] [360|480|720|1080] [limit=ALL|N] [archive] — bulk",
    "",
    "<b>Tips</b>",
    "• For channels use the /videos URL (e.g. https://www.youtube.com/@YourChannel/videos)",
    "• If a file is large, the bot will try to shrink to ~49 MB. For reliability use “video 360”.",
    "• When you set <code>limit=ALL</code>, archiving turns ON automatically. You can also force it with the word <code>archive</code>.",
    "• Download only content you have rights to.",
    "",
    "<b>Telegram limits</b>",
    "• Public Bot API uploads: ~50 MB per file.",
    "• Local Bot API Server: up to 2 GB.",
]
GUIDE_TEXT = "\n".join(GUIDE_LINES)

YTDLP_COMMON: Dict[str, object] = {
    "outtmpl": "%(title).80s.%(ext)s",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
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
    cmd = (
        "ffprobe -v error -show_entries format=duration "
        + "-of default=noprint_wrappers=1:nokey=1 "
        + shlex.quote(str(path))
    )
    code, out, _ = await run_cmd(cmd)
    if code == 0:
        try:
            return float(out.strip())
        except Exception:
            return None
    return None


async def shrink_video(in_path: Path, out_path: Path, target_mb: int = TARGET_MB, height: int = 360) -> bool:
    """Transcode H.264+AAC roughly targeting <= target_mb. Returns True if out_path created."""
    duration = await ffprobe_duration(in_path) or 0.0
    if duration <= 0:
        cmd = (
            "ffmpeg -y -i " + shlex.quote(str(in_path))
            + " -vf scale=-2:" + str(height)
            + " -c:v libx264 -preset veryfast -crf 28 -c:a aac -b:a 96k -movflags +faststart "
            + shlex.quote(str(out_path))
        )
        code, _, _ = await run_cmd(cmd)
        return code == 0 and out_path.exists()

    audio_kbps = 96
    target_bits = target_mb * 8 * 1024 * 1024
    vbps = max(300_000, int(target_bits / duration) - audio_kbps * 1000)

    cmd = (
        "ffmpeg -y -i " + shlex.quote(str(in_path))
        + " -vf scale=-2:" + str(height)
        + " -c:v libx264 -preset veryfast -b:v " + str(vbps)
        + " -maxrate " + str(int(vbps * 1.2))
        + " -bufsize " + str(int(vbps * 2))
        + " -c:a aac -b:a " + str(audio_kbps) + "k -movflags +faststart "
        + shlex.quote(str(out_path))
    )
    code, _, _ = await run_cmd(cmd)
    return code == 0 and out_path.exists()


def ytdlp_download(url: str, mode: str, height: int, workdir: Path) -> DLResult:
    """Synchronous helper used in a thread via asyncio.to_thread."""
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
    else:
        fmt = "bestvideo[height<=" + str(height) + "]+bestaudio/best[height<=" + str(height) + "]"
        opts.update({"format": fmt, "merge_output_format": "mp4"})

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        if "requested_downloads" in info and info["requested_downloads"]:
            fn = Path(info["requested_downloads"][0]["filepath"])
        else:
            suffix = ".mp3" if mode == "audio" else ".mp4"
            fn = Path(ydl.prepare_filename(info)).with_suffix(suffix)
        title = info.get("title", fn.stem)
        return DLResult(path=fn, title=title, ext=fn.suffix.lstrip("."))


# -------------------- archiving helpers --------------------

def _safe_stem(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._ -]+", "_", (name or "file")).strip()
    return s or "file"


def make_zip_parts(files: List[Tuple[Path, str]], outdir: Path, part_mb: int = ARCHIVE_PART_MB) -> List[Path]:
    """Create multi-part ZIPs (stored) about <= part_mb each."""
    parts: List[Path] = []
    if not files:
        return parts

    part_bytes = part_mb * 1024 * 1024
    part_idx = 1
    current_size = 0
    zpath = outdir / ("bundle_part" + str(part_idx).zfill(2) + ".zip")
    zf = zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_STORED)

    for p, title in files:
        sz = p.stat().st_size
        if current_size > 0 and (current_size + sz) > part_bytes:
            zf.close()
            parts.append(zpath)
            part_idx += 1
            zpath = outdir / ("bundle_part" + str(part_idx).zfill(2) + ".zip")
            zf = zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_STORED)
            current_size = 0
        arcname = _safe_stem(title) + p.suffix.lower()
        zf.write(p, arcname=arcname)
        current_size += sz

    zf.close()
    parts.append(zpath)
    return parts


# -------------------- Telegram handlers --------------------

@dp.message(Command("start"))
async def cmd_start(message: Message):
    lines = [
        "Привет! Пришли команду в формате:",
        "/get <YouTube URL> [video|audio] [360|480|720|1080]",
        "",
        "Например:",
        "/get https://youtu.be/dQw4w9WgXcQ",
        "/get https://youtu.be/dQw4w9WgXcQ audio",
        "/get https://youtu.be/dQw4w9WgXcQ video 480",
        "",
        "⚠️ Загружай только то, на что у тебя есть права.",
        "",
        "Доп. команды: /help /guide /getall",
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
    height = DEFAULT_HEIGHT

    if len(args) >= 3:
        m = args[2].lower()
        if m in {"video", "audio"}:
            mode = m
        elif m.isdigit():
            height = int(m)
    if len(args) >= 4 and args[3].isdigit():
        height = int(args[3])

    await message.reply("Ок, качаю " + mode + "… это может занять минутку")

    with tempfile.TemporaryDirectory() as td:
        workdir = Path(td)
        try:
            dl = await asyncio.to_thread(ytdlp_download, url, mode, height, workdir)
        except Exception as e:
            await message.reply("Не получилось скачать: " + str(e))
            return

        out_path = dl.path

        # shrink if needed
        try:
            if mode == "video" and sizeof_mb(out_path) > TARGET_MB:
                smaller = workdir / (out_path.stem + ".small.mp4")
                ok = await shrink_video(out_path, smaller, target_mb=TARGET_MB, height=min(height, 360))
                if ok and sizeof_mb(smaller) < sizeof_mb(out_path):
                    out_path = smaller
        except Exception:
            pass

        # send back (NO multiline strings)
        try:
            caption = str(dl.title) + " (через yt-dlp)"
            file = FSInputFile(str(out_path))
            await message.answer_document(file, caption=caption)
        except Exception as e:
            size = sizeof_mb(out_path)
            msg = "Не удалось отправить файл. Размер: " + "{:.1f}".format(size) + \
                  " MB. Попробуй /get <url> video 360 или аудио.\nТех. причина: " + str(e)
            await message.reply(msg)


# ------------- bulk channel/playlist support -------------

def _normalize_watch_url(entry: dict) -> Optional[str]:
    url = entry.get("webpage_url") or entry.get("url") or ""
    if not url:
        return None
    if not url.startswith("http"):
        return "https://www.youtube.com/watch?v=" + url
    return url


def list_playlist_urls(url: str, limit: Optional[int] = None) -> List[str]:
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
        if limit and limit > 0:
            urls = urls[:limit]
        return urls


@dp.message(Command("getall"))
async def cmd_getall(message: Message):
    args = (message.text or "").split()
    if len(args) < 2:
        await message.reply(
            "Использование: /getall <ссылка на канал/плейлист> [video|audio] [360|480|720|1080] [limit=ALL|N] [archive]"
        )
        return

    url = args[1].strip()
    if not is_youtube_url(url):
        await message.reply("Похоже, это не ссылка на YouTube.")
        return

    mode = "video"
    height = DEFAULT_HEIGHT
    limit: Optional[int] = 10
    do_archive = False

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
                do_archive = True
            elif val.isdigit():
                limit = int(val)
        elif t == "archive":
            do_archive = True
        elif t == "noarchive":
            do_archive = False

    await message.reply(
        "Собираю список… limit=" + ("ALL" if (limit is None) else str(limit))
        + " | archive=" + ("ON" if do_archive else "OFF")
    )

    try:
        urls = await asyncio.to_thread(list_playlist_urls, url, limit)
    except Exception as e:
        await message.reply("Не удалось получить список видео: " + str(e))
        return

    if not urls:
        await message.reply("Видео не нашлись. Возможно, нужен другой URL (например, вкладка /videos канала).")
        return

    sent = 0
    total = len(urls)
    await message.reply("Нашёл " + str(total) + " видео. Начинаю скачивание…")

    files_for_zip: List[Tuple[Path, str]] = []

    for idx, watch_url in enumerate(urls, 1):
        try:
            note = await message.answer(str(idx) + "/" + str(total) + " — скачиваю…")
            with tempfile.TemporaryDirectory() as td:
                workdir = Path(td)
                try:
                    dl = await asyncio.to_thread(ytdlp_download, watch_url, mode, height, workdir)
                except Exception as e:
                    await note.edit_text(str(idx) + "/" + str(total) + " — ошибка скачивания: " + str(e))
                    continue

                out_path = dl.path
                try:
                    if mode == "video" and sizeof_mb(out_path) > TARGET_MB:
                        smaller = workdir / (out_path.stem + ".small.mp4")
                        ok = await shrink_video(out_path, smaller, target_mb=TARGET_MB, height=min(height, 360))
                        if ok and sizeof_mb(smaller) < sizeof_mb(out_path):
                            out_path = smaller
                except Exception:
                    pass

                if do_archive:
                    files_for_zip.append((out_path, dl.title))
                    await note.edit_text(str(idx) + "/" + str(total) + " — готово (в архив)")
                else:
                    try:
                        caption = str(dl.title) + " (через yt-dlp)"
                        file = FSInputFile(str(out_path))
                        await message.answer_document(file, caption=caption)
                        sent += 1
                        await note.edit_text(str(idx) + "/" + str(total) + " — готово ✅")
                    except Exception as e:
                        await note.edit_text(str(idx) + "/" + str(total) + " — не удалось отправить файл: " + str(e))

            await asyncio.sleep(1.2)
        except Exception:
            continue

    if do_archive:
        await message.reply("Формирую ZIP-архивы…")
        with tempfile.TemporaryDirectory() as zd:
            zdir = Path(zd)
            parts = make_zip_parts(files_for_zip, zdir, part_mb=ARCHIVE_PART_MB)
            if not parts:
                await message.reply("Нечего архивировать (список пуст).")
                return
            for i, z in enumerate(parts, 1):
                try:
                    cap = "Архив " + str(i) + "/" + str(len(parts)) + " — " + z.name
                    await message.answer_document(FSInputFile(str(z)), caption=cap)
                except Exception as e:
                    await message.answer("Не удалось отправить архив " + z.name + ": " + str(e))
        sent = len(files_for_zip)

    await message.reply("Готово. Обработано: " + str(sent if not do_archive else len(files_for_zip)) + " из " + str(total) + ".")


# -------------------- entrypoint --------------------

async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
