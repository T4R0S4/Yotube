import os
import sys
import logging
import sqlite3
import subprocess
import tempfile
import shutil
from datetime import datetime
import pytz
import asyncio
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from pytubefix import YouTube, exceptions  # Menggunakan pytubefix sebagai pengganti pytube
import re
import httpx
from urllib.error import HTTPError
from telegram.helpers import escape_markdown  # Untuk menghindari error parse entities
from pydub import AudioSegment

# Konfigurasi logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Konfigurasi database
DATABASE_FILE = "youtube_downloader_bot.db"
WIB = pytz.timezone('Asia/Jakarta')

def init_database():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        join_date TEXT
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS usage_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        action TEXT,
        video_url TEXT,
        format TEXT,
        quality TEXT,
        timestamp TEXT,
        status TEXT,
        error_message TEXT
    )
    ''')
    conn.commit()
    cursor.execute("PRAGMA table_info(usage_logs)")
    columns = [info[1] for info in cursor.fetchall()]
    if "error_message" not in columns:
        try:
            cursor.execute("ALTER TABLE usage_logs ADD COLUMN error_message TEXT")
            logger.info("Migrasi: Kolom error_message berhasil ditambahkan ke usage_logs")
            conn.commit()
        except Exception as e:
            logger.error(f"Migrasi gagal: {str(e)}")
    conn.close()

def save_user(user_id, username, first_name, last_name):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    if cursor.fetchone() is None:
        join_date = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            "INSERT INTO users (user_id, username, first_name, last_name, join_date) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, first_name, last_name, join_date)
        )
        conn.commit()
    conn.close()

def log_usage(user_id, action, video_url="", format="", quality="", status="started", error_message=""):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    timestamp = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(
        "INSERT INTO usage_logs (user_id, action, video_url, format, quality, timestamp, status, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (user_id, action, video_url, format, quality, timestamp, status, error_message)
    )
    conn.commit()
    last_id = cursor.lastrowid
    conn.close()
    return last_id

def update_log_status(log_id, status, error_message=""):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE usage_logs SET status = ?, error_message = ? WHERE id = ?",
        (status, error_message, log_id)
    )
    conn.commit()
    conn.close()

def get_user_stats():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT user_id) FROM users")
    total_users = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM usage_logs WHERE action = 'download'")
    total_downloads = cursor.fetchone()[0]
    cursor.execute("""
    SELECT users.username, COUNT(*) as download_count 
    FROM usage_logs 
    JOIN users ON usage_logs.user_id = users.user_id 
    WHERE action = 'download' AND status = 'completed'
    GROUP BY users.user_id 
    ORDER BY download_count DESC 
    LIMIT 5
    """)
    top_users = cursor.fetchall()
    cursor.execute("""
    SELECT action, video_url, error_message, timestamp 
    FROM usage_logs 
    WHERE error_message != '' 
    ORDER BY timestamp DESC 
    LIMIT 5
    """)
    recent_errors = cursor.fetchall()
    conn.close()
    return {
        "total_users": total_users,
        "total_downloads": total_downloads,
        "top_users": top_users,
        "recent_errors": recent_errors
    }

def is_valid_youtube_url(url):
    youtube_regex = r'(https?://)?(www\.)?(youtube\.com/watch\?v=|youtu\.be/)[a-zA-Z0-9_-]+'
    return bool(re.match(youtube_regex, url))

def extract_video_id(url):
    pattern1 = r'(?:https?:\/\/)?(?:www\.)?youtube\.com\/watch\?v=([a-zA-Z0-9_-]+)'
    pattern2 = r'(?:https?:\/\/)?(?:www\.)?youtu\.be\/([a-zA-Z0-9_-]+)'
    match = re.search(pattern1, url)
    if match:
        return match.group(1)
    match = re.search(pattern2, url)
    if match:
        return match.group(1)
    return None

ALLOWED_VIDEO_RESOLUTIONS = {"144p", "240p", "360p", "480p", "720p", "1080p"}

def merge_video_audio(video_file, audio_file, output_file):
    cmd = ['ffmpeg', '-y', '-i', video_file, '-i', audio_file, '-c:v', 'copy', '-c:a', 'aac', output_file]
    subprocess.run(cmd, capture_output=True, check=True)

# Fungsi konversi ke MP3 menggunakan pydub
def convert_to_mp3(file_path):
    base, ext = os.path.splitext(file_path)
    if ext.lower() != ".mp3":
        try:
            audio = AudioSegment.from_file(file_path)
            mp3_path = base + ".mp3"
            audio.export(mp3_path, format="mp3")
            os.remove(file_path)
            return mp3_path
        except Exception as e:
            logger.error(f"Error converting to mp3: {str(e)}")
            return file_path
    return file_path

# Fungsi retry untuk get_video_info
async def get_video_info_with_retry(url, retries=3):
    for attempt in range(retries):
        info = await get_video_info(url)
        if info and "error" not in info:
            return info
        logger.warning(f"Attempt {attempt+1} gagal mendapatkan video info. Retrying...")
        await asyncio.sleep(1)
    return info

# Helper: Ambil video info berdasarkan video ID
async def get_video_info_by_id(video_id):
    url = f"https://youtu.be/{video_id}"
    return await get_video_info_with_retry(url)

async def get_video_info(url):
    try:
        video_id = extract_video_id(url)
        logger.info(f"Attempting to get info for video ID: {video_id}")
        # Inisialisasi YouTube tanpa parameter po_token
        yt = YouTube(url, use_po_token=True, client="WEB")
        audio_streams = yt.streams.filter(only_audio=True).order_by('abr').desc()
        audio_options = []
        for stream in audio_streams:
            if stream.abr:
                audio_options.append({
                    'itag': stream.itag,
                    'format': 'audio',
                    'quality': stream.abr,
                    'extension': stream.subtype
                })
        video_streams_prog = yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc()
        video_options = []
        for stream in video_streams_prog:
            if stream.resolution in ALLOWED_VIDEO_RESOLUTIONS:
                video_options.append({
                    'itag': stream.itag,
                    'format': 'video',
                    'quality': stream.resolution,
                    'extension': stream.subtype,
                    'ptype': 'prog'
                })
        video_streams_adapt = yt.streams.filter(only_video=True, file_extension='mp4').order_by('resolution').desc()
        for stream in video_streams_adapt:
            if stream.resolution in ALLOWED_VIDEO_RESOLUTIONS:
                if any(opt['quality'] == stream.resolution for opt in video_options):
                    continue
                video_options.append({
                    'itag': stream.itag,
                    'format': 'video',
                    'quality': stream.resolution,
                    'extension': stream.subtype,
                    'ptype': 'adapt'
                })
        if not audio_options and not video_options:
            logger.warning(f"No streams available for {url}")
            return None
        return {
            'title': yt.title,
            'thumbnail': yt.thumbnail_url,
            'duration': yt.length,
            'author': yt.author,
            'audio_options': audio_options[:3],
            'video_options': video_options[:10]
        }
    except (exceptions.RegexMatchError, exceptions.VideoUnavailable) as e:
        error_msg = f"Video tidak tersedia atau dibatasi: {str(e)}"
        logger.error(f"YouTube error: {error_msg}")
        return {"error": error_msg}
    except HTTPError as e:
        error_msg = f"HTTP Error {e.code}: {e.reason}"
        logger.error(f"HTTP error: {error_msg}")
        return {"error": error_msg}
    except Exception as e:
        error_msg = f"Error tidak terduga: {str(e)}"
        logger.error(f"Error getting video info: {error_msg}")
        return {"error": error_msg}

async def download_youtube(url, itag, format_type, user_id, log_id, ptype="prog"):
    try:
        video_id = extract_video_id(url)
        logger.info(f"Downloading video ID: {video_id} with itag: {itag}")
        yt = YouTube(url, use_po_token=True, client="WEB")
        # Buat direktori temporary
        temp_dir = tempfile.mkdtemp()
        if format_type == 'video' and ptype == 'adapt':
            video_stream = yt.streams.get_by_itag(itag)
            if not video_stream:
                error_msg = f"Stream dengan itag {itag} tidak tersedia"
                logger.error(error_msg)
                update_log_status(log_id, "failed", error_msg)
                shutil.rmtree(temp_dir)
                return None, None
            video_path = video_stream.download(output_path=temp_dir, filename_prefix="video_")
            audio_stream = yt.streams.filter(only_audio=True).order_by('abr').desc().first()
            if not audio_stream:
                error_msg = "Audio stream tidak tersedia untuk penggabungan adaptive video"
                logger.error(error_msg)
                update_log_status(log_id, "failed", error_msg)
                shutil.rmtree(temp_dir)
                return None, None
            audio_path = audio_stream.download(output_path=temp_dir, filename_prefix="audio_")
            output_path = os.path.join(temp_dir, f"{yt.title}_{video_id}.mp4")
            merge_video_audio(video_path, audio_path, output_path)
            os.remove(video_path)
            os.remove(audio_path)
            update_log_status(log_id, "completed")
            return output_path, temp_dir
        else:
            stream = yt.streams.get_by_itag(itag)
            if not stream:
                error_msg = f"Stream dengan itag {itag} tidak tersedia"
                logger.error(error_msg)
                update_log_status(log_id, "failed", error_msg)
                shutil.rmtree(temp_dir)
                return None, None
            file_path = stream.download(output_path=temp_dir)
            if format_type == "audio":
                file_path = convert_to_mp3(file_path)
            update_log_status(log_id, "completed")
            return file_path, temp_dir
    except exceptions.VideoUnavailable as e:
        error_msg = f"Video tidak tersedia: {str(e)}"
        logger.error(error_msg)
        update_log_status(log_id, "failed", error_msg)
        return None, None
    except Exception as e:
        error_msg = f"Error mengunduh video: {str(e)}"
        logger.error(error_msg)
        update_log_status(log_id, "failed", error_msg)
        return None, None

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        save_user(user.id, user.username, user.first_name, user.last_name)
        log_usage(user.id, "start")
        await update.message.reply_text(
            f"Halo {user.first_name}! Selamat datang di YouTube Downloader Bot.\n\n"
            "Silakan kirim URL video YouTube untuk mengunduh video atau audio."
        )
    except Exception as e:
        logger.error(f"Error in start command: {str(e)}")
        await update.message.reply_text("Terjadi kesalahan saat memulai bot. Silakan coba lagi.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        log_usage(user.id, "help")
        help_text = (
            "🔰 *Bantuan YouTube Downloader Bot* 🔰\n\n"
            "*Perintah Tersedia:*\n"
            "/start - Memulai bot\n"
            "/help - Menampilkan pesan bantuan ini\n"
            "/stats - Melihat statistik bot (hanya admin)\n\n"
            "*Cara Penggunaan:*\n"
            "1. Kirim URL video YouTube (contoh: https://youtube.com/watch?v=xxxx atau https://youtu.be/xxxx)\n"
            "2. Pilih format yang diinginkan:\n"
            "   • Audio: Dropdown pilihan kualitas audio (akan dikonversi ke MP3 jika diperlukan)\n"
            "   • Video: Dropdown pilihan resolusi (misal: 144p, 480p, 720p, 1080p, dll.)\n"
            "3. Pilih opsi dari dropdown\n"
            "4. Tunggu hingga proses unduhan selesai\n"
            "5. File akan dikirim ke chat Anda\n\n"
            "⚠️ *Catatan:* Jika terjadi error, silakan coba URL lain atau hubungi admin."
        )
        safe_text = escape_markdown(help_text, version=2)
        await update.message.reply_text(safe_text, parse_mode='MarkdownV2')
    except Exception as e:
        logger.error(f"Error in help command: {str(e)}")
        await update.message.reply_text("Terjadi kesalahan saat menampilkan bantuan. Silakan coba lagi.")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        ADMIN_IDS = [1390557485]
        if user.id not in ADMIN_IDS:
            await update.message.reply_text("Maaf, Anda tidak memiliki izin untuk mengakses statistik.")
            return
        log_usage(user.id, "stats")
        stats = get_user_stats()
        message = f"📊 *Statistik Bot*\n\n"
        message += f"👥 Total Pengguna: {stats['total_users']}\n"
        message += f"⬇️ Total Unduhan: {stats['total_downloads']}\n\n"
        message += "🏆 *Pengguna Teratas*:\n"
        for i, (username, count) in enumerate(stats['top_users'], 1):
            username = username if username else "Tanpa Username"
            message += f"{i}. {username}: {count} unduhan\n"
        message += "\n❌ *Error Terbaru*:\n"
        for action, url, error, timestamp in stats['recent_errors']:
            vid = extract_video_id(url) if url else "N/A"
            message += f"• [{timestamp}] {action}: {vid} - {error[:50]}...\n"
        safe_message = escape_markdown(message, version=2)
        await update.message.reply_text(safe_message, parse_mode='MarkdownV2')
    except Exception as e:
        logger.error(f"Error in stats command: {str(e)}")
        await update.message.reply_text("Terjadi kesalahan saat mengambil statistik. Silakan coba lagi.")

async def url_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text
    user = update.effective_user
    log_id = log_usage(user.id, "url_check", url)
    try:
        if not is_valid_youtube_url(url):
            update_log_status(log_id, "invalid_url", "URL tidak valid")
            await update.message.reply_text(
                "❌ URL tidak valid. Harap kirim URL YouTube yang valid.\nContoh: https://youtube.com/watch?v=xxxx atau https://youtu.be/xxxx"
            )
            return
        await update.message.reply_text("🔍 Mengambil informasi video... Mohon tunggu sebentar.")
        video_info = await get_video_info_with_retry(url, retries=3)
        if not video_info:
            update_log_status(log_id, "failed_to_get_info", "Tidak dapat mengambil informasi video")
            await update.message.reply_text("❌ Gagal mendapatkan informasi video. Silakan coba lagi nanti atau gunakan URL lain.")
            return
        if "error" in video_info:
            update_log_status(log_id, "error_getting_info", video_info["error"])
            await update.message.reply_text(f"❌ Error: {video_info['error']}\n\nSilakan coba URL video lain.")
            return
        update_log_status(log_id, "info_retrieved")
        vid_id = extract_video_id(url)
        # Tampilkan pilihan format: Audio / Video
        keyboard = [
            [InlineKeyboardButton("🎵 Audio", callback_data=f"option|{vid_id}|audio")],
            [InlineKeyboardButton("🎬 Video", callback_data=f"option|{vid_id}|video")],
            [InlineKeyboardButton("❌ Batal", callback_data="cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"📹 *{video_info['title']}*\n\n"
            f"👤 *Channel*: {video_info['author']}\n"
            f"⏱️ *Durasi*: {video_info['duration']} detik\n\n"
            "Pilih format yang diinginkan:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    except Exception as e:
        error_msg = f"Error tidak terduga: {str(e)}"
        logger.error(error_msg)
        update_log_status(log_id, "unexpected_error", error_msg)
        await update.message.reply_text(
            "❌ Terjadi kesalahan yang tidak terduga. Silakan coba lagi nanti.\nJika masalah berlanjut, hubungi admin bot."
        )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    try:
        await query.answer()
        data = query.data.split('|')
        if data[0] == "cancel":
            await query.edit_message_text("❌ Operasi dibatalkan. Kirim URL YouTube lain untuk mencoba lagi.")
            return
        # Jika opsi format dipilih
        if data[0] == "option":
            vid_id = data[1]
            choice = data[2]  # 'audio' atau 'video'
            video_info = await get_video_info_by_id(vid_id)
            if not video_info or "error" in video_info:
                await query.edit_message_text("❌ Gagal mengambil opsi. Silakan coba URL lain.")
                return
            keyboard = []
            if choice == "audio":
                for audio in video_info['audio_options']:
                    keyboard.append([
                        InlineKeyboardButton(
                            f"🎵 Audio {audio['quality']} ({audio['extension']})",
                            callback_data=f"download|{vid_id}|{audio['itag']}|audio|{audio['quality']}"
                        )
                    ])
            elif choice == "video":
                for video in video_info['video_options']:
                    keyboard.append([
                        InlineKeyboardButton(
                            f"🎬 {video['quality']} ({video.get('ptype','prog')})",
                            callback_data=f"download|{vid_id}|{video['itag']}|video|{video['quality']}|{video.get('ptype','prog')}"
                        )
                    ])
            keyboard.append([InlineKeyboardButton("❌ Batal", callback_data="cancel")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            text = f"📹 *{video_info['title']}*\n\nPilih opsi {choice.capitalize()}:"
            await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
            return
        # Jika opsi download dipilih
        if data[0] == "download":
            vid_id = data[1]
            itag = int(data[2])
            format_type = data[3]
            quality = data[4]
            ptype = data[5] if len(data) > 5 else "prog"
            url = f"https://youtu.be/{vid_id}"
            log_id = log_usage(user.id, "download", url, format_type, quality)
            await query.edit_message_text(
                f"⬇️ Mengunduh {'audio' if format_type=='audio' else 'video'} dengan kualitas {quality}...\nProses ini mungkin memerlukan waktu beberapa saat."
            )
            file_path, temp_dir = await download_youtube(url, itag, format_type, user.id, log_id, ptype)
            if file_path:
                await query.edit_message_text("✅ Pengunduhan selesai! Mengirim file...")
                try:
                    if format_type == 'audio':
                        await asyncio.wait_for(
                            context.bot.send_audio(
                                chat_id=user.id,
                                audio=open(file_path, 'rb'),
                                caption=f"🎵 Audio {quality}"
                            ),
                            timeout=3600  # Timeout 1 jam
                        )
                    else:
                        await asyncio.wait_for(
                            context.bot.send_video(
                                chat_id=user.id,
                                video=open(file_path, 'rb'),
                                caption=f"🎬 Video {quality}"
                            ),
                            timeout=3600  # Timeout 1 jam
                        )
                    os.remove(file_path)
                    if temp_dir and os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir)
                    await query.edit_message_text("✅ File berhasil dikirim! Kirim URL lain untuk mengunduh video/audio lainnya.")
                except Exception as e:
                    error_msg = f"Error mengirim file: {str(e)}"
                    logger.error(error_msg)
                    update_log_status(log_id, "failed_to_send", error_msg)
                    if "Request Entity Too Large" in str(e):
                        await query.edit_message_text("❌ File terlalu besar untuk dikirim melalui Telegram (batas 50MB).\nCoba pilih kualitas yang lebih rendah.")
                    else:
                        await query.edit_message_text("❌ Pengunduhan berhasil, tetapi gagal mengirim file.\nSilakan coba lagi nanti atau pilih format lain.")
            else:
                await query.edit_message_text("❌ Gagal mengunduh file. Silakan coba format lain atau URL video yang berbeda.")
    except Exception as e:
        error_msg = f"Error in button handler: {str(e)}"
        logger.error(error_msg)
        try:
            await query.edit_message_text("❌ Terjadi kesalahan yang tidak terduga.\nSilakan kirim URL video lain atau hubungi admin bot.")
        except:
            pass

async def error_handler(update, context):
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Terjadi kesalahan saat memproses permintaan Anda. Silakan coba lagi nanti."
            )
        except:
            pass

def main():
    try:
        init_database()
        application = ApplicationBuilder().token('8012132104:AAFAUyz7ifY93IpbQGeRpwZ5CZG6w_BHNDo').build()
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("stats", stats_command))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, url_handler))
        application.add_handler(CallbackQueryHandler(button_handler))
        application.add_error_handler(error_handler)
        logger.info("Bot telah dimulai")
        application.run_polling()
    except Exception as e:
        logger.critical(f"Error fatal saat menjalankan bot: {str(e)}")

if __name__ == '__main__':
    main()
