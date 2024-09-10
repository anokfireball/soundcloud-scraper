import asyncio
import base64
import datetime
import hashlib
import json
import os
import random
import re
import string
import time
import urllib.parse
from typing import Dict, List, Tuple

import aiohttp
import aiosqlite
from asynciolimiter import StrictLimiter
from dotenv import load_dotenv
from tqdm import tqdm

BASE = "https://api.soundcloud.com"


def custom_encoder(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def custom_decoder(dct):
    for key, value in dct.items():
        try:
            dct[key] = datetime.datetime.fromisoformat(value)
        except (ValueError, TypeError):
            pass
    return dct


class ScCrawler:
    def __init__(self, authMethod="client"):
        self.authMethod = authMethod

        # https://github.com/soundcloud/api/issues/182#issuecomment-1036138170
        self.searchLimiter = StrictLimiter(30000 / 3600)
        # https://developers.soundcloud.com/docs/api/guide#authentication
        self.clientCredentialLimiter = StrictLimiter(50 / (12 * 3600))

        self.codeChallenge = None
        self.codeVerifier = None
        self.state = None
        self.code = None
        self.accessToken = None
        self.refreshToken = None
        self.tokenExpires = None

        # TODO when and how to clean these up?
        self.conn = None
        self.cursor = None

    async def init(self):
        try:
            await self.readState()
        except FileNotFoundError:
            # PKCE for SoundCloud
            self.state = "".join(
                random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                for _ in range(64)
            )

            code_verifier = base64.urlsafe_b64encode(os.urandom(40)).decode("utf-8")
            self.codeVerifier = re.sub("[^a-zA-Z0-9]+", "", code_verifier)

            code_challenge = hashlib.sha256(code_verifier.encode("utf-8")).digest()
            code_challenge = base64.urlsafe_b64encode(code_challenge).decode("utf-8")
            self.codeChallenge = code_challenge.replace("=", "")

            # await self.login("account")

        # database
        self.conn = await aiosqlite.connect("scrawler.db")
        self.cursor = await self.conn.cursor()
        await self.cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS tracks (
            trackid INTEGER PRIMARY KEY,
            userid INTEGER NOT NULL,
            timestamp INTEGER NOT NULL
        )
        """
        )
        await self.conn.commit()

    def setTokens(self, response):
        self.accessToken = response["access_token"]
        self.refreshToken = response["refresh_token"]
        self.tokenExpires = datetime.datetime.now() + datetime.timedelta(
            seconds=response["expires_in"]
        )

    async def writeState(self):
        state = {
            "codeChallenge": self.codeChallenge,
            "codeVerifier": self.codeVerifier,
            "state": self.state,
            "code": self.code,
            "accessToken": self.accessToken,
            "refreshToken": self.refreshToken,
            "tokenExpires": self.tokenExpires,
        }

        with open(".state", "w") as f:
            json.dump(state, f, default=custom_encoder)

    async def readState(self):
        with open(".state", "r") as f:
            state = json.load(f, object_hook=custom_decoder)

        self.codeChallenge = state["codeChallenge"]
        self.codeVerifier = state["codeVerifier"]
        self.state = state["state"]

        self.accessToken = state["accessToken"]
        self.refreshToken = state["refreshToken"]
        self.tokenExpires = state["tokenExpires"]

    async def get(
        self, url: str, headers: dict = {}, params: dict = None, withAuth=True
    ) -> Dict:
        while True:
            headers["accept"] = "application/json; charset=utf-8"
            if withAuth:
                if self.accessToken is None:
                    await self.login(self.authMethod)
                if datetime.datetime.now() > (
                    self.tokenExpires - datetime.timedelta(seconds=30)
                ):
                    await self.reauth()
                headers["Authorization"] = f"Bearer {self.accessToken}"

            await self.searchLimiter.wait()

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        headers=headers,
                        params=params,
                    ) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        elif resp.status == 429:
                            print("Rate limited, this shoudn't happen")
                        else:
                            raise ValueError(f"Error: {resp.status}")
            except aiohttp.client_exceptions.ClientConnectorError as e:
                if "Temporary failure in name resolution" in str(e):
                    print("Temporary failure in name resolution, retrying...")
                    await asyncio.sleep(10)
                else:
                    raise e

    async def post(
        self, url: str, headers: dict = {}, data: dict = None, withAuth=True
    ) -> Dict:
        while True:
            headers["accept"] = "application/json; charset=utf-8"
            if withAuth:
                if self.accessToken is None:
                    await self.login()
                if datetime.datetime.now() > (
                    self.tokenExpires + datetime.timedelta(seconds=30)
                ):
                    await self.reauth()
                headers["Authorization"] = f"Bearer {self.accessToken}"

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url,
                        headers=headers,
                        data=data,
                    ) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        elif resp.status == 429:
                            print("Rate limited, waiting 12 hours")
                            await asyncio.sleep(60 * 60 * 12)
                        else:
                            raise ValueError(f"Error: {resp.status}")
            except aiohttp.client_exceptions.ClientConnectorError as e:
                if "Temporary failure in name resolution" in str(e):
                    print("Temporary failure in name resolution, retrying...")
                    await asyncio.sleep(10)
                else:
                    raise e

    async def login(self, authMethod):
        if authMethod == "client":
            basicAuth = base64.b64encode(
                f"{CLIENT_ID}:{CLIENT_SECRET}".encode("utf-8")
            ).decode("utf-8")

            await self.clientCredentialLimiter.wait()

            res = await self.post(
                url="https://secure.soundcloud.com/oauth/token",
                headers={
                    "Authorization": f"Basic {basicAuth}",
                },
                data={"grant_type": "client_credentials"},
                withAuth=False,
            )
        elif authMethod == "account":
            url = (
                "https://secure.soundcloud.com/authorize?"
                + f"client_id={urllib.parse.quote_plus(CLIENT_ID.encode('utf-8'))}&"
                + f"redirect_uri={urllib.parse.quote_plus(REDIRECT_URI.encode('utf-8'))}&"
                + "response_type=code&"
                + f"code_challenge={urllib.parse.quote_plus(self.codeChallenge.encode('utf-8'))}&"
                + "code_challenge_method=S256&"
                + f"state={urllib.parse.quote_plus(self.state.encode('utf-8'))}"
            )

            print(f"Please visit and authenticate: {url}")
            self.code = input("Please enter the received CODE: ")

            await asyncio.sleep(5)

            res = await self.post(
                url="https://secure.soundcloud.com/oauth/token",
                data={
                    "grant_type": "authorization_code",
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "redirect_uri": REDIRECT_URI,
                    "code_verifier": self.codeVerifier,
                    "code": self.code,
                },
                withAuth=False,
            )
        else:
            raise ValueError(f"Invalid authMethod: {self.authMethod}")
        self.setTokens(res)
        await self.writeState()

    async def reauth(self):
        print("Refreshing token")
        if self.authMethod == "client":
            await self.clientCredentialLimiter.wait()

        res = await self.post(
            url="https://secure.soundcloud.com/oauth/token",
            data={
                "grant_type": "refresh_token",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": self.refreshToken,
            },
            withAuth=False,
        )
        self.setTokens(res)
        await self.writeState()

    async def getUserId(self, username: str) -> int:
        res = await self.get(
            url=f"{BASE}/resolve",
            params={"url": f"https://soundcloud.com/{username}"},
        )
        return res["id"]

    async def getFollowedUsers(self, userId: int) -> List:
        followed = []
        url = f"{BASE}/users/{userId}/followings"
        while True:
            res = await self.get(url=url, params={"limit": 200})
            for user in res["collection"]:
                followed.append(user)

            # paginate
            if "next_href" not in res or not res["next_href"]:
                break
            url = res["next_href"]
        return followed

    async def getTracks(
        self, userId: int, until: int = 0, limit: int = 199
    ) -> Tuple[int, List]:
        tracks = []
        url = f"{BASE}/users/{userId}/tracks"
        while True:
            res = await self.get(
                url=url,
                params={
                    "limit": limit,
                    "access": "playable",
                    "linked_partitioning": "true",
                },
            )
            for track in res["collection"]:
                track["created_at"] = int(
                    datetime.datetime.strptime(
                        track["created_at"], "%Y/%m/%d %H:%M:%S %z"
                    ).timestamp()
                )
                tracks.append(track)

            # tracks are fetched in descending order
            # no need to go back further in time
            # we only want the most recent tracks
            if (
                not any(track["created_at"] > until for track in res["collection"])
                or len(tracks) >= 1
            ):
                break

            # paginate
            if "next_href" not in res or not res["next_href"]:
                break
            url = res["next_href"]

        tracks = list(filter(lambda x: x["created_at"] > until, tracks))
        return userId, tracks

    def filterSets(self, tracks: List) -> List:
        return list(filter(lambda x: x["duration"] > 30 * 60 * 1000, tracks))

    def mostRecentTrack(self, tracks: List):
        return sorted(tracks, key=lambda x: x["created_at"], reverse=True)[0]

    async def getLatestTimestamp(self, userId: int) -> int:
        await self.cursor.execute(
            """
                SELECT MAX(timestamp)
                FROM tracks
                WHERE userid = ?
                """,
            (userId,),
        )
        result = await self.cursor.fetchone()
        return int(result[0]) if result and result[0] else 0

    async def insertTracks(self, userId: int, tracks: List):
        for track in tracks:
            await self.cursor.execute(
                """
                INSERT INTO tracks (userid, trackid, timestamp)
                VALUES (?, ?, ?)
                """,
                (userId, track["id"], track["created_at"]),
            )
        await self.conn.commit()

    async def run(self):
        userId = await self.getUserId("psykko0")
        follows = await self.getFollowedUsers(userId)
        usernames = {user["id"]: user["username"] for user in follows}


        tasks = [
            self.getTracks(user["id"], until=await self.getLatestTimestamp(user["id"]))
            for user in follows
        ]
        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks))
        for task in pbar:
            userId, tracks = await task
            pbar.set_description(f"{usernames[userId][:30].ljust(30)}")
            sets = self.filterSets(tracks)
            for set in sets:
                print(f"Found set: {set['title'], set['permalink_url']}")
            await self.insertTracks(userId, tracks)


async def main():
    crawler = ScCrawler(authMethod="client")
    await crawler.init()
    while True:
        start_time = time.time()
        await crawler.run()
        elapsed_time = time.time() - start_time
        # sleep_time = max(0, (60 * 15) - elapsed_time)
        # await asyncio.sleep(sleep_time)


if __name__ == "__main__":
    load_dotenv(".env")
    try:
        CLIENT_ID = os.getenv("CLIENT_ID")
        CLIENT_SECRET = os.getenv("CLIENT_SECRET")
        REDIRECT_URI = os.getenv("REDIRECT_URI")
    except KeyError:
        print(
            "Please provide CLIENT_ID, CLIENT_SECRET, and REDIRECT_URI in .env file or as environment variables"
        )
        exit(1)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
