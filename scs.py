#!/usr/bin/env python3

import json
from asyncio import sleep

import nodriver as uc
from bs4 import BeautifulSoup
from dotenv import dotenv_values
from telethon import TelegramClient

CLIENT = None

prefix = '"title": "'
css_class = "playbackTimeline__duration"


async def parse_message(message: str):
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
        print(f"{message.text} is not a valid JSON")


async def main():
    browser = await uc.start(headless=True)

    async for message in CLIENT.iter_messages("IFTTT"):
        payload = await parse_message(message.text)
        page = await browser.get(payload["trackurl"])
        await page.wait_for(f".{css_class}")
        soup = BeautifulSoup(await page.get_content(), "html.parser")
        duration = soup.find("div", class_=css_class).find_all("span")[1].text
        tmp = duration.split(":")
        if len(tmp) == 3 or (len(tmp) == 2 and int(tmp[0]) > 30):
            print(f'likely a set (title "{payload["title"]}", duration {duration})')
        else:
            print(
                f'probably not a set (title "{payload["title"]}", duration {duration})'
            )

        await sleep(5)
        # CLIENT.delete_messages(None, message_ids=message.id)


if __name__ == "__main__":
    config = dotenv_values(".env")
    try:
        api_id = int(config["API_ID"])
        api_hash = config["API_HASH"]
    except KeyError:
        print("Please provide API_ID and API_HASH in .env file")
        exit(1)
    except ValueError:
        print("API_ID must be an integer")
        exit(1)
    CLIENT = TelegramClient("anon", api_id, api_hash)
    with CLIENT:
        CLIENT.loop.run_until_complete(main())
