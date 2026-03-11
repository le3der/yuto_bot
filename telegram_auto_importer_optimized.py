import os
import asyncio
import psycopg2
import psycopg2.pool
import zipfile
import rarfile
import py7zr
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from telethon import TelegramClient, events

# ========= TELEGRAM CONFIG =========

API_ID = 24442137
API_HASH = "726c527ff4dce0d954ef3918d218d505"
CHANNEL_ID = -1003753676375

# ========= PATHS =========

DOWNLOAD_DIR = "/root/yuto_bot/data_files"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

client = TelegramClient("import_session", API_ID, API_HASH)

# ========= DATABASE POOL =========

DB_CONFIG = {
    "dbname":   os.environ.get("DB_NAME",     "scanner"),
    "user":     os.environ.get("DB_USER",     "postgres"),
    "password": os.environ.get("DB_PASSWORD", "123456"),
    "host":     os.environ.get("DB_HOST",     "127.0.0.1"),
    "port":     os.environ.get("DB_PORT",     "5432"),
}

_db_pool = psycopg2.pool.ThreadedConnectionPool(minconn=2, maxconn=10, **DB_CONFIG)
_executor = ThreadPoolExecutor(max_workers=4)

# ========= DATABASE =========

def insert_batch(lines):
    conn = _db_pool.getconn()
    try:
        cursor = conn.cursor()
        data = [(l.strip(),) for l in lines if l.strip()]
        cursor.executemany(
            "INSERT INTO data (content) VALUES (%s) ON CONFLICT DO NOTHING",
            data
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        conn.rollback()
        print("DB ERROR:", e)
    finally:
        _db_pool.putconn(conn)


# ========= READ TXT =========

def read_txt(path):

    batch = []

    with open(path, "r", errors="ignore") as f:

        for line in f:

            batch.append(line)

            if len(batch) >= 5000:
                insert_batch(batch)
                batch = []

    if batch:
        insert_batch(batch)


# ========= READ CSV =========

def read_csv(path):

    df = pd.read_csv(path, dtype=str, on_bad_lines="skip")

    rows = []

    for row in df.astype(str).values:
        rows.append(",".join(row))

        if len(rows) >= 5000:
            insert_batch(rows)
            rows = []

    if rows:
        insert_batch(rows)


# ========= EXTRACT =========

def extract_archive(path):

    if path.endswith(".zip"):

        with zipfile.ZipFile(path) as z:
            z.extractall(DOWNLOAD_DIR)

    elif path.endswith(".rar"):

        with rarfile.RarFile(path) as r:
            r.extractall(DOWNLOAD_DIR)

    elif path.endswith(".7z"):

        with py7zr.SevenZipFile(path) as z:
            z.extractall(DOWNLOAD_DIR)


# ========= PROCESS =========

def process_files():

    for file in os.listdir(DOWNLOAD_DIR):

        path = os.path.join(DOWNLOAD_DIR, file)

        try:

            if file.endswith(".txt"):
                read_txt(path)
                os.remove(path)

            elif file.endswith(".csv"):
                read_csv(path)
                os.remove(path)

            elif file.endswith((".zip", ".rar", ".7z")):

                extract_archive(path)
                os.remove(path)

        except Exception as e:
            print("ERROR:", e)


# ========= TELEGRAM WATCH =========

@client.on(events.NewMessage(chats=CHANNEL_ID))
async def handler(event):

    if not event.file:
        return

    name = event.file.name or ""

    if not name.lower().endswith(
        (".txt", ".csv", ".zip", ".rar", ".7z")
    ):
        return

    print("Downloading:", name)

    path = await event.download_media(DOWNLOAD_DIR)

    print("Downloaded:", path)

    # Run in background thread — never blocks the event loop
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(_executor, process_files)

    print("Processing done")


# ========= START =========

async def main():

    await client.start()

    print("Telegram Importer Started")

    await client.run_until_disconnected()


asyncio.run(main())
