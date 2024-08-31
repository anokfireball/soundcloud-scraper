#!/usr/bin/env python3

import json
import os
import sqlite3
from asyncio import sleep

import nodriver as uc
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telethon import TelegramClient

CLIENT = None
CONN = None
CURSOR = None
BROWSER = None

prefix = '"title": "'
css_class = "playbackTimeline__duration"


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


async def get_duration(url: str):
    page = await BROWSER.get(url)
    await page.wait_for(f".{css_class}")
    soup = BeautifulSoup(await page.get_content(), "html.parser")
    duration = soup.find("div", class_=css_class).find_all("span")[1].text
    return duration


async def main():
    BROWSER = await uc.start(headless=True)

    async for message in CLIENT.iter_messages("IFTTT"):
        print(message.id)
        payload = await parse_message(message.text)
        CURSOR.execute(
            "INSERT OR IGNORE INTO queue (id, artist, title, url) VALUES (?, ?, ?, ?)",
            (message.id, payload["username"], payload["title"], payload["trackurl"]),
        )
        CONN.commit()
        # duration = await get_duration(payload["trackurl"])
        # tmp = duration.split(":")
        # if len(tmp) == 3 or (len(tmp) == 2 and int(tmp[0]) > 30):
        #     print(f'likely a set (title "{payload["title"]}", duration {duration})')
        # else:
        #     print(
        #         f'probably not a set (title "{payload["title"]}", duration {duration})'
        #     )

        # await sleep(5)
        # CLIENT.delete_messages("IFTTT", message_ids=message.id)


if __name__ == "__main__":
    CONN = sqlite3.connect("scs.db")  # implicitly creates
    CURSOR = CONN.cursor()

    # Create table if it doesn't exist
    CURSOR.execute(
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
    CONN.commit()

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
