# Telegram YT-DLP Bot — MAX quality + archive every 10 (PixelDrain) + Inline menu
# Use only for content you are allowed to download.

from __future__ import annotations

import asyncio
import os
import re
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List, Dict
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import Command, Text
from aiogram.types import (
    Message, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from dotenv import load_dotenv
import yt_dlp
from yt_dlp.utils import DownloadError
import requests

# ---------------- configuration ----------------
BATCH_SIZE = 10         # /getall: make one ZIP after every N downloaded videos
DEFAULT_HEIGHT = 0      # 0 => MAX quality by default (always MAX as per requirements)
ALLOWED_NETLOC = {"youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com"}
PIXEL_API = "https://pixeldrain.com/api/file"
PIXEL_VIEW = "https://pixeldrain.com/u/"
PIXEL_DL   = "https://pixeldrain.com/api/file/"
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
    "Бот завжди качає у максимально можливій якості (MAX).",
    "",
    "Меню: кнопками можна обрати режим — одне відео, всі/останні N, топ-20 за переглядами, увесь плейліст.",
    "При пакетах бот архівує кожні 10 відео і вантажить ZIP на PixelDrain та дає лінки.",
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


# ---------------- utilities ----------------

def is_youtube_url(url: str) -> bool:
    try:
        netloc = urlparse(url).netloc.lower()
        return any(netloc.endswith(d) for d in ALLOWED_NETLOC)
    except Exception:
        return False


def _normalize_watch_url(entry: dict) -> Optional[str]:
    url = entry.get("webpage_url") or entry.get("url") or ""
    if not url:
        return None
    if not url.startswith("http"):
        return "https://www.youtube.com/watch?v=" + url
    return url


def _safe_stem(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._ -]+", "_", (name or "file")).strip()
    return s or "file"


async def run_cmd(cmd: str) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out_b, err_b = await proc.communicate()
    return proc.returncode, out_b.decode("utf-8", "ignore"), err_b.decode("utf-8", "ignore")


# ---------------- yt-dlp helpers ----------------

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


# ---------------- list & selection helpers ----------------

def list_entries_with_meta(url: str) -> List[Dict[str, object]]:
    """Return list of entries with url, timestamp, view_count when available (no download)."""
    opts = dict(YTDLP_COMMON)
    opts.update({
        "noplaylist": False,
        "extract_flat": "in_playlist",
        "skip_download": True,
        "quiet": True,
        "no_warnings": True,
    })
    entries: List[Dict[str, object]] = []
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        raw = info.get("entries") or []
        for e in raw:
            if not isinstance(e, dict):
                continue
            w = _normalize_watch_url(e)
            if not w:
                continue
            # try to collect view_count and timestamp
            vc = e.get("view_count")
            ts = e.get("timestamp")
            if not ts and e.get("upload_date"):
                # upload_date like YYYYMMDD -> keep as int for ordering
                try:
                    ud = str(e.get("upload_date"))
                    ts = int(ud)
                except Exception:
                    ts = None
            entries.append({
                "url": w,
                "view_count": vc if isinstance(vc, int) else None,
                "timestamp": ts if isinstance(ts, int) else None,
            })
    return entries


def enrich_view_counts(urls: List[str], limit: int = 200) -> Dict[str, int]:
    """Fetch view_count for up to 'limit' URLs (skip_download). Returns dict url->views."""
    opts = dict(YTDLP_COMMON)
    opts.update({"skip_download": True, "quiet": True, "no_warnings": True, "noplaylist": True})
    views: Dict[str, int] = {}
    with yt_dlp.YoutubeDL(opts) as ydl:
        for u in urls[:limit]:
            try:
                info = ydl.extract_info(u, download=False)
                vc = info.get("view_count")
                if isinstance(vc, int):
                    views[u] = vc
            except Exception:
                continue
    return views


def select_urls(mode: str, src_url: str) -> List[str]:
    """mode: 'all', 'latest:10/20/30', 'top20', 'playlist_all'"""
    items = list_entries_with_meta(src_url)
    if not items:
        return []
    if mode == "all" or mode == "playlist_all":
        return [it["url"] for it in items]
    if mode.startswith("latest:"):
        n = int(mode.split(":", 1)[1])
        # sort by timestamp desc, fallback to original order
        items2 = [it for it in items if it.get("timestamp") is not None]
        items2.sort(key=lambda x: int(x.get("timestamp") or 0), reverse=True)
        if len(items2) < n:
            # pad with remaining in original order
            seen = set(u["url"] for u in items2)
            for it in items:
                if it["url"] not in seen:
                    items2.append(it)
                    if len(items2) >= n:
                        break
        return [it["url"] for it in items2[:n]]
    if mode == "top20":
        # try sort by available view_count, enrich if needed
        with_v = [it for it in items if isinstance(it.get("view_count"), int)]
        without_v = [it for it in items if not isinstance(it.get("view_count"), int)]
        if len(with_v) < 20 and without_v:
            pool = [it["url"] for it in items]
            extra = enrich_view_counts(pool, limit=200)
            for it in items:
                if it["url"] in extra:
                    it["view_count"] = extra[it["url"]]
            with_v = [it for it in items if isinstance(it.get("view_count"), int)]
        with_v.sort(key=lambda x: int(x.get("view_count") or 0), reverse=True)
        return [it["url"] for it in with_v[:20]]
    return []


# ---------------- archiving ----------------

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


# ---------------- keyboards ----------------

def menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📹 Скачати одне відео", callback_data="mode:single")],
        [InlineKeyboardButton(text="📺 Всі відео з каналу", callback_data="mode:all")],
        [InlineKeyboardButton(text="🆕 Останні 10", callback_data="mode:latest:10"),
         InlineKeyboardButton(text="🆕 Останні 20", callback_data="mode:latest:20"),
         InlineKeyboardButton(text="🆕 Останні 30", callback_data="mode:latest:30")],
        [InlineKeyboardButton(text="🔥 Топ-20 за переглядами", callback_data="mode:top20")],
        [InlineKeyboardButton(text="🎞 Увесь плейліст", callback_data="mode:playlist_all")],
    ])


class AwaitLink(StatesGroup):
    waiting_for_link = State()


# ---------------- handlers ----------------

@dp.message(Command("start", "menu"))
async def cmd_start(message: Message):
    text = "\n".join([
        "Можемо скачати:",
        "",
        "• 📹 одне відео — попрошу лінк на відео і відправлю файл у Телеграм (якщо надто великий — лінк на PixelDrain)",
        "• 📺 всі відео з каналу — лінк на канал, архівація по 10 і завантаження на PixelDrain",
        "• 🆕 останні 10 / 20 / 30 — лінк на канал, архівація по 10 і завантаження на PixelDrain",
        "• 🔥 топ-20 за переглядами — лінк на канал, архівація по 10 і завантаження на PixelDrain",
        "• 🎞 увесь плейліст — лінк на плейліст, архівація по 10 і завантаження на PixelDrain",
        "",
        "Якість: завжди максимально доступна.",
    ])
    await message.answer(text, reply_markup=menu_kb())


@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(GUIDE_TEXT, parse_mode=ParseMode.HTML, reply_markup=menu_kb())


@dp.callback_query(Text(startswith="mode:"))
async def cb_mode(call: CallbackQuery, state: FSMContext):
    data = call.data  # e.g. mode:latest:20
    await state.update_data(sel=data)
    if data == "mode:single":
        await call.message.answer("Кинь лінк на відео YouTube")
    elif data == "mode:all":
        await call.message.answer("Кинь лінк на канал YouTube (сторінка /videos)")
    elif data.startswith("mode:latest:"):
        await call.message.answer("Кинь лінк на канал YouTube (сторінка /videos)")
    elif data == "mode:top20":
        await call.message.answer("Кинь лінк на канал YouTube (сторінка /videos)")
    elif data == "mode:playlist_all":
        await call.message.answer("Кинь лінк на плейліст YouTube")
    await state.set_state(AwaitLink.waiting_for_link)
    await call.answer()


@dp.message(AwaitLink.waiting_for_link)
async def on_link(message: Message, state: FSMContext):
    url = (message.text or "").strip()
    if not is_youtube_url(url):
        await message.reply("Схоже, це не лінк на YouTube. Спробуй ще раз або /menu.")
        return

    data = await state.get_data()
    sel = data.get("sel", "mode:single")

    if sel == "mode:single":
        await do_single(message, url)
        await state.clear()
        return

    # Bulk selections
    if sel == "mode:all":
        urls = select_urls("all", url)
    elif sel == "mode:playlist_all":
        urls = select_urls("playlist_all", url)
    elif sel.startswith("mode:latest:"):
        n = sel.split(":")[-1]
        urls = select_urls("latest:" + n, url)
    elif sel == "mode:top20":
        urls = select_urls("top20", url)
    else:
        urls = []

    if not urls:
        await message.reply("Не вдалося зібрати список відео. Перевір лінк або спробуй інший режим.")
        return

    await do_bulk(message, urls)
    await state.clear()


# ---------------- single & bulk flows ----------------

async def do_single(message: Message, url: str) -> None:
    await message.reply("Ок, качаю одне відео у максимальній якості…")
    with tempfile.TemporaryDirectory() as td:
        workdir = Path(td)
        try:
            dl = await asyncio.to_thread(ytdlp_download, url, "video", DEFAULT_HEIGHT, workdir)
        except Exception as e:
            await message.reply("Не вийшло скачати: " + str(e))
            return
        try:
            caption = str(dl.title) + " (yt-dlp)"
            await message.answer_document(FSInputFile(str(dl.path)), caption=caption)
        except Exception as e:
            err = str(e)
            if ("Too Large" in err) or ("too big" in err) or ("413" in err):
                try:
                    view, direct = await asyncio.to_thread(upload_pixeldrain, dl.path)
                    text = "Файл великий, залив на PixelDrain:\n" + view + "\nПряма лінка: " + direct
                    await message.answer(text)
                except Exception as e2:
                    await message.reply("Не вдалося надіслати файл і завантажити на PixelDrain: " + str(e2))
            else:
                await message.reply("Не вдалося надіслати файл: " + err)


async def do_bulk(message: Message, urls: List[str]) -> None:
    total = len(urls)
    await message.reply("Знайшов " + str(total) + " відео. Пакетую по " + str(BATCH_SIZE) + " у ZIP та вантажу на PixelDrain…")

    with tempfile.TemporaryDirectory() as session_td:
        session_dir = Path(session_td)
        workdir = session_dir / "items"
        workdir.mkdir(parents=True, exist_ok=True)

        batch_files: List[Tuple[Path, str]] = []
        batch_index = 1
        processed = 0

        for idx, watch_url in enumerate(urls, 1):
            try:
                note = await message.answer(str(idx) + "/" + str(total) + " — качаю…")
                try:
                    dl = await asyncio.to_thread(ytdlp_download, watch_url, "video", DEFAULT_HEIGHT, workdir)
                except Exception as e:
                    await note.edit_text(str(idx) + "/" + str(total) + " — помилка: " + str(e))
                    continue

                batch_files.append((dl.path, dl.title))
                processed += 1
                await note.edit_text(str(idx) + "/" + str(total) + " — готово, додано у пакет")

                if len(batch_files) >= BATCH_SIZE:
                    z = make_zip_single(batch_files, session_dir, batch_index)
                    try:
                        view, direct = await asyncio.to_thread(upload_pixeldrain, z)
                        text = "Пакет " + str(batch_index) + " (" + str(len(batch_files)) + " відео):\n" + view + "\nПряма лінка: " + direct
                        await message.answer(text)
                    except Exception as e:
                        await message.answer("Не вдалося завантажити архів на PixelDrain: " + str(e))
                    batch_files = []
                    batch_index += 1

                await asyncio.sleep(0.8)
            except Exception:
                continue

        # remaining
        if batch_files:
            z = make_zip_single(batch_files, session_dir, batch_index)
            try:
                view, direct = await asyncio.to_thread(upload_pixeldrain, z)
                text = "Пакет " + str(batch_index) + " (" + str(len(batch_files)) + " відео):\n" + view + "\nПряма лінка: " + direct
                await message.answer(text)
            except Exception as e:
                await message.answer("Не вдалося завантажити архів на PixelDrain: " + str(e))

    await message.reply("Готово. Оброблено: " + str(processed) + " з " + str(total) + ".")


# ---------------- lightweight self-tests --------------------

def _selftest() -> None:
    # format string tests
    assert _build_format_string("audio", 0).startswith("ba"), "audio best format"
    assert "height<=720" in _build_format_string("video", 720), "height filter in format"
    assert _build_format_string("video", 0) == "bv*+ba/b", "MAX quality default"

    # safe stem
    assert _safe_stem("a*b?c").startswith("a_b_c"), "safe stem replaces illegal chars"
    assert _safe_stem("") == "file", "empty stem fallback"

    # message formatting with \n
    view = "https://pixeldrain.com/u/XYZ"
    direct = "https://pixeldrain.com/api/file/XYZ"
    txt = "Файл великий, залив на PixelDrain:\n" + view + "\nПряма лінка: " + direct
    assert "\n" in txt and view in txt and direct in txt

    # keyboards
    kb = menu_kb()
    cds = [btn.callback_data for row in kb.inline_keyboard for btn in row]
    for must in ["mode:single", "mode:all", "mode:latest:10", "mode:latest:20", "mode:latest:30", "mode:top20", "mode:playlist_all"]:
        assert must in cds, "missing menu item: " + must

    # selection helpers: synthetic ordering
    items = [
        {"url": "u1", "timestamp": 20240101, "view_count": 10},
        {"url": "u2", "timestamp": 20240103, "view_count": 50},
        {"url": "u3", "timestamp": 20240102, "view_count": 20},
    ]
    latest_sorted = sorted(items, key=lambda x: int(x.get("timestamp") or 0), reverse=True)
    assert [i["url"] for i in latest_sorted][:2] == ["u2", "u3"], "latest ordering"
    top_sorted = sorted(items, key=lambda x: int(x.get("view_count") or 0), reverse=True)
    assert [i["url"] for i in top_sorted][:2] == ["u2", "u3"], "top ordering"


# ---------------- entrypoint ----------------

async def main():
    if os.getenv("SELFTEST") == "1":
        _selftest()
        print("SELFTEST passed")
        return
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
