#!/usr/bin/env python3

import json
import os
import random
import time
from asyncio import gather, sleep

import aiosqlite
import nodriver as uc
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telethon import TelegramClient, events

CLIENT = None
# timestamp of the last performed GET request
LAST_EXECUTION_TIME = 0


async def parse_message(message: str):
    """
    Function to parse a message and return a JSON object.
    Message format:
    {
        "username": "Amelie Lens",
        "title": "Amelie Lens Presents EXHALE Radio 099 w/ Clara Cuvé",
        "trackurl": "https://soundcloud.com/amelielens/amelie-lens-presents-exhale-radio-099-w-clara-cuve"
    }

    Note: The title field may contain double quotes, which need to be escaped.
    """
    text = message.split("\n")
    prefix = '"title": "'
    prefix_end_index = text[2].find(prefix) + len(prefix)
    title = text[2][prefix_end_index:-2]
    if '"' in title:
        title_escaped = title.replace('"', "'")
        text[2] = text[2].replace(title, title_escaped)
    text = "\n".join(text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        print(f"{message} is not a valid JSON")


async def get_duration(browser: uc.Browser, url: str):
    global LAST_EXECUTION_TIME
    css_class = "playbackTimeline__duration"

    attempts = 0
    retry = True

    duration = "0:00"
    while retry:
        # avoid getting rate limited by SoundCloud
        base_delay = 15
        random_variation = random.uniform(-5, 5)
        wait_time = max(
            0, base_delay + random_variation - (time.time() - LAST_EXECUTION_TIME)
        )
        await sleep(wait_time)

        try:
            page = await browser.get(url, new_tab=True)
            await page.wait_for(f".{css_class}")
            LAST_EXECUTION_TIME = time.time()
            soup = BeautifulSoup(await page.get_content(), "html.parser")
            await page.close()
            if soup.find("div", class_="blockedTrack"):
                print(f"Looks like the track does not exist anymore: {url}")
                break
            duration = soup.find("div", class_=css_class).find_all("span")[1].text
            retry = False
        except Exception as e:
            print(f"Error: {e}, {url}")
            await sleep(60)
            attempts += 1
            # give up at some point
            retry = attempts < 5

    return duration


async def insert_message(
    conn: aiosqlite.Connection, cursor: aiosqlite.Cursor, message
):
    payload = await parse_message(message.text)
    await cursor.execute(
        "INSERT OR IGNORE INTO queue (id, artist, title, url) VALUES (?, ?, ?, ?)",
        (message.id, payload["username"], payload["title"], payload["trackurl"]),
    )
    await conn.commit()
    # await CLIENT.delete_messages("IFTTT", message_ids=message.id)


async def parse_previous_messages():
    conn = await aiosqlite.connect("scs.db")
    cursor = await conn.cursor()

    print("Iterating over previous messages")
    async for message in CLIENT.iter_messages("IFTTT", reverse=True):
        await insert_message(conn, cursor, message)
    print("Finished iterating over previous messages")

    await cursor.close()
    await conn.close()


def handle_new_messages():
    print("Listening for new messages")

    @CLIENT.on(events.NewMessage(from_users="IFTTT"))
    async def handler(message):
        conn = await aiosqlite.connect("scs.db")
        cursor = await conn.cursor()
        await insert_message(conn, cursor, message)
        await conn.commit()
        await cursor.close()
        await conn.close()


async def check_durations():
    conn = await aiosqlite.connect("scs.db")
    cursor = await conn.cursor()

    print("Starting browser")
    browser = await uc.start(headless=True)

    print("Consuming messages")
    while True:
        await cursor.execute(
            "SELECT id, artist, title, url FROM queue WHERE processed = 0 ORDER BY id LIMIT 1"
        )
        row = await cursor.fetchone()
        if row:
            id, artist, title, url = row
            print(f"Processing ID: {id}")
            # Process the URL here
            duration = await get_duration(browser, url)
            tmp = duration.split(":")
            if len(tmp) == 3 or (len(tmp) == 2 and int(tmp[0]) > 30):
                print(
                    f'likely a set (artist {artist}, title "{title}", duration {duration})'
                )
            await cursor.execute("UPDATE queue SET processed = 1 WHERE id = ?", (id,))
            await conn.commit()
        else:
            await sleep(1)

    browser.stop()
    await cursor.close()
    await conn.close()


async def initialize_database():
    conn = await aiosqlite.connect("scs.db")
    cursor = await conn.cursor()

    # Create table if it doesn't exist
    await cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY,
        artist TEXT NOT NULL,
        title TEXT NOT NULL,
        url TEXT NOT NULL,
        processed INTEGER DEFAULT 0
    )
    """
    )
    await conn.commit()
    await cursor.close()
    await conn.close()


async def main():
    print("Initializing database")
    await initialize_database()

    print("Starting producer and consumer")
    handle_new_messages()
    await gather(parse_previous_messages(), check_durations())


if __name__ == "__main__":
    load_dotenv(".env")
    try:
        api_id = int(os.getenv("API_ID"))
        api_hash = os.getenv("API_HASH")
    except KeyError:
        print(
            "Please provide API_ID and API_HASH in .env file or as environment variables"
        )
        exit(1)
    except ValueError:
        print("API_ID must be an integer")
        exit(1)

    CLIENT = TelegramClient("anon", api_id, api_hash)
    with CLIENT:
        CLIENT.loop.run_until_complete(main())
