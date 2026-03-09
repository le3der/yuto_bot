import os
import asyncio
import sqlite3
import zipfile
import rarfile
import py7zr
import pandas as pd

from telethon import TelegramClient, events

# ========= TELEGRAM CONFIG =========

API_ID = 24442137
API_HASH = "726c527ff4dce0d954ef3918d218d505"
CHANNEL_ID = -1003753676375

# ========= PATHS =========

DOWNLOAD_DIR = "/root/yuto_bot/data_files"
DB_PATH = "/root/yuto_bot/scanner.db"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

client = TelegramClient("import_session", API_ID, API_HASH)

# ========= DATABASE =========

def insert_batch(lines):

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    data = [(l.strip(),) for l in lines if l.strip()]

    cursor.executemany(
        "INSERT INTO data (content) VALUES (?)",
        data
    )

    conn.commit()
    conn.close()


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

    process_files()

    print("Processing done")


# ========= START =========

async def main():

    await client.start()

    print("Telegram Importer Started")

    await client.run_until_disconnected()


asyncio.run(main())
