import asyncio
import base64
import datetime
import email.utils
import hashlib
import json
import os
import random
import re
import string
import sys
import time
import urllib.parse
from enum import Enum
from typing import Dict, List, Tuple

import aiohttp
import aiosqlite
from asynciolimiter import LeakyBucketLimiter, StrictLimiter
from loguru import logger

BASE = "https://api.soundcloud.com"
AUTH_BASE = "https://secure.soundcloud.com"
TOKEN_REFRESH_THRESHOLD = 3 * 60
LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class AuthMethod(Enum):
    CLIENT = "client"
    USER = "user"


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


class Crawler:
    def __init__(self):
        self.authMethod = AuthMethod(API_AUTH_METHOD)

        # https://github.com/soundcloud/api/issues/182#issuecomment-1036138170
        self.searchLimiter = StrictLimiter(27500 / 3600)
        # https://developers.soundcloud.com/docs/api/guide#authentication
        self.clientCredentialLimiter = LeakyBucketLimiter(45 / (12 * 3600), capacity=5)

        self.initState()

        self.conn = None
        self.cursor = None
        self.session = None

        hash = hashlib.sha256()
        hash.update(SOUNDCLOUD_USERNAME.encode("utf-8"))
        self.sessionSuffix = hash.hexdigest()[:16]

    def initState(self):
        self.accessToken = None
        self.refreshToken = None
        self.tokenExpires = None

        if self.authMethod == AuthMethod.USER:
            self.codeChallenge = None
            self.codeVerifier = None
            self.state = None
            self.code = None

    async def init(self):
        logger.info("Initializing Crawler")
        await self.readState()

        # database
        dbPath = os.path.join(DATA_DIR, f"{self.sessionSuffix}.db")
        self.conn = await aiosqlite.connect(os.path.join(dbPath))
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
        logger.debug(f"Connected to database {dbPath}.db')")

        # Create a single aiohttp session
        self.session = aiohttp.ClientSession()

    async def close(self):
        await self.session.close()
        await self.conn.close()

    def setTokens(self, response):
        self.accessToken = response["access_token"]
        self.refreshToken = response["refresh_token"]
        self.tokenExpires = datetime.datetime.now() + datetime.timedelta(
            seconds=response["expires_in"]
        )

    async def writeState(self):
        state = {
            "accessToken": self.accessToken,
            "refreshToken": self.refreshToken,
            "tokenExpires": self.tokenExpires,
        }
        if self.authMethod == AuthMethod.USER:
            state["codeChallenge"] = self.codeChallenge
            state["codeVerifier"] = self.codeVerifier
            state["state"] = self.state
            state["code"] = self.code

        with open(os.path.join(DATA_DIR, f"{self.sessionSuffix}.state"), "w") as f:
            json.dump(state, f, default=custom_encoder)

    async def readState(self):
        path = os.path.join(DATA_DIR, f"{self.sessionSuffix}.state")
        if os.path.exists(path):
            logger.info(f"Reading existing state from {path}")
            with open(path, "r") as f:
                state = json.load(f, object_hook=custom_decoder)

            self.accessToken = state["accessToken"]
            self.refreshToken = state["refreshToken"]
            self.tokenExpires = state["tokenExpires"]

            if self.authMethod == AuthMethod.USER:
                self.codeChallenge = state["codeChallenge"]
                self.codeVerifier = state["codeVerifier"]
                self.state = state["state"]
        elif self.authMethod == AuthMethod.USER:
            logger.info("No existing state found, authencating from scratch")
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

            await self.login(AuthMethod.USER)

    async def get(
        self, url: str, headers: dict = {}, params: dict = None, withAuth=True
    ) -> Dict:
        while True:
            headers["accept"] = "application/json; charset=utf-8"
            if withAuth:
                await self.reauth()
                headers["Authorization"] = f"Bearer {self.accessToken}"

            await self.searchLimiter.wait()

            try:
                async with self.session.get(
                    url,
                    headers=headers,
                    params=params,
                ) as resp:
                    log_headers = dict(headers)
                    if "Authorization" in log_headers:
                        del log_headers["Authorization"]
                    logger.debug(f"GET {url} {log_headers} {params}")
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        logger.warning("Rate limited, this shouldn't happen")
                    else:
                        logger.error(f"Got HTTP {resp.status}, retrying... ({url})")
            except aiohttp.client_exceptions.ClientConnectorError as e:
                if "Temporary failure in name resolution" in str(e):
                    logger.error("Temporary failure in name resolution, retrying...")
                    await asyncio.sleep(10)
                else:
                    raise e

    async def post(
        self, url: str, headers: dict = {}, data: dict = None, withAuth=True
    ) -> Dict:
        while True:
            headers["accept"] = "application/json; charset=utf-8"
            if withAuth:
                await self.reauth()
                headers["Authorization"] = f"Bearer {self.accessToken}"

            try:
                async with self.session.post(
                    url,
                    headers=headers,
                    data=data,
                ) as resp:
                    logger.debug(f"POST {url} {headers} {data}")
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        logger.warning("Rate limited, waiting 12 hours")
                        await asyncio.sleep(60 * 60 * 12)
                    else:
                        logger.error(
                            f"Got HTTP {resp.status}, retrying... ({url}, {data})"
                        )
            except aiohttp.client_exceptions.ClientConnectorError as e:
                if "Temporary failure in name resolution" in str(e):
                    logger.error("Temporary failure in name resolution, retrying...")
                    await asyncio.sleep(10)
                else:
                    raise e

    async def login(self, authMethod):
        if authMethod == AuthMethod.CLIENT:
            logger.info("Logging in with client credentials")
            basicAuth = base64.b64encode(
                f"{API_CLIENT_ID}:{API_CLIENT_SECRET}".encode("utf-8")
            ).decode("utf-8")

            await self.clientCredentialLimiter.wait()

            res = await self.post(
                url=f"{AUTH_BASE}/oauth/token",
                headers={
                    "Authorization": f"Basic {basicAuth}",
                },
                data={"grant_type": "client_credentials"},
                withAuth=False,
            )
        elif authMethod == AuthMethod.USER:
            logger.info("Logging in with user credentials")
            url = (
                f"{AUTH_BASE}/authorize?"
                + f"client_id={urllib.parse.quote_plus(API_CLIENT_ID.encode('utf-8'))}&"
                + f"redirect_uri={urllib.parse.quote_plus(API_REDIRECT_URI.encode('utf-8'))}&"
                + "response_type=code&"
                + f"code_challenge={urllib.parse.quote_plus(self.codeChallenge.encode('utf-8'))}&"
                + "code_challenge_method=S256&"
                + f"state={urllib.parse.quote_plus(self.state.encode('utf-8'))}"
            )

            # TODO have a listener on API_REDIRECT_URI, parse it automatically
            print(f"Please visit and authenticate: {url}")
            self.code = input("Please enter the received CODE: ")

            await asyncio.sleep(5)

            res = await self.post(
                url=f"{AUTH_BASE}/oauth/token",
                data={
                    "grant_type": "authorization_code",
                    "client_id": API_CLIENT_ID,
                    "client_secret": API_CLIENT_SECRET,
                    "redirect_uri": API_REDIRECT_URI,
                    "code_verifier": self.codeVerifier,
                    "code": self.code,
                },
                withAuth=False,
            )
        self.setTokens(res)
        logger.info(f"Login successful, token valid until {self.tokenExpires}")
        await self.writeState()

    async def reauth(self):
        remaining = self.tokenExpires - datetime.datetime.now() or datetime.timedelta(
            seconds=0
        )
        if self.accessToken is None or remaining < datetime.timedelta(seconds=5):
            logger.info("No valid token, performing full reauth")
            await self.login(self.authMethod)
            return

        if remaining > datetime.timedelta(seconds=TOKEN_REFRESH_THRESHOLD):
            return

        logger.info(
            f"Token close to expiry ({int(remaining.total_seconds())}s), refreshing"
        )
        await self.searchLimiter.wait()
        res = await self.post(
            url=f"{AUTH_BASE}/oauth/token",
            data={
                "grant_type": "refresh_token",
                "client_id": API_CLIENT_ID,
                "client_secret": API_CLIENT_SECRET,
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
        logger.debug(f"Resolved {username} to ID {res['id']}")
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
        logger.debug(f"Found {len(followed)} followed accounts")
        return followed

    async def getTracks(
        self, userId: int, userName: str, until: int = 0, limit: int = 200
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

            # tracks are fetched in (chronologically) descending order
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
        logger.debug(
            f"Found {len(tracks)} tracks for {userName} after "
            + f"{datetime.datetime.fromtimestamp(until).strftime('%Y/%m/%d %H:%M:%S')}"
        )
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
        await self.cursor.executemany(
            """
            INSERT INTO tracks (userid, trackid, timestamp)
            VALUES (?, ?, ?)
            """,
            [(userId, track["id"], track["created_at"]) for track in tracks],
        )
        await self.conn.commit()

    async def triggerWebhook(self, artist: dict, track: dict):
        created_at = datetime.datetime.fromtimestamp(
            track["created_at"], tz=datetime.timezone.utc
        )
        date = email.utils.format_datetime(created_at, usegmt=True)
        payload = {
            "trackid": track["id"],
            "artist": artist["username"],
            "title": track["title"],
            "url": track["permalink_url"],
            "timestamp": track["created_at"],
            "date": date,
        }
        logger.debug(f"Triggering webhook with payload: {payload}")
        await self.session.post(
            WEBHOOK,
            json=payload,
        )

    async def run(self):
        logger.info(f"Getting user ID for {SOUNDCLOUD_USERNAME}")
        userId = await self.getUserId(SOUNDCLOUD_USERNAME)
        logger.info(f"Getting followed users for {SOUNDCLOUD_USERNAME}")
        follows = await self.getFollowedUsers(userId)
        userMap = {user["id"]: user for user in follows}

        logger.info(f"Getting new tracks for {len(follows)} followed users")
        tasks = [
            self.getTracks(
                user["id"],
                user["username"],
                until=await self.getLatestTimestamp(user["id"]),
            )
            for user in follows
        ]
        for task in asyncio.as_completed(tasks):
            userId, tracks = await task
            sets = self.filterSets(tracks)
            # TODO because these are not done in lockstep, we might send tracks multiple times
            # if the webhook becomes unavailable midway through the loop
            for set in sets:
                await self.triggerWebhook(userMap[userId], set)
            await self.insertTracks(userId, tracks)


async def main():
    crawler = Crawler()
    await crawler.init()
    logger.debug(
        "Starting Crawler with config "
        + f"{LOG_DESTINATION=}, "
        + f"{LOG_LEVEL=}, "
        + f"{DATA_DIR=}, "
        + f"{SOUNDCLOUD_USERNAME=}, "
        + f"{API_AUTH_METHOD=}, "
        + f"{API_REDIRECT_URI=}, "
        + f"{WEBHOOK=}"
    )
    try:
        while True:
            start_time = time.time()
            await crawler.run()
            elapsed_time = time.time() - start_time
            logger.info(f"Crawl complete, took {elapsed_time} seconds")
            if ONESHOT:
                logger.info("Exiting since this is ONESHOT")
                break
            sleep_time = max(0, (60 * 15) - elapsed_time)
            logger.info(f"Sleeping until next iteration ({int(sleep_time/60)} minutes)")
            await asyncio.sleep(sleep_time)
    finally:
        await crawler.close()


if __name__ == "__main__":
    try:
        LOG_LEVEL = os.environ["LOG_LEVEL"].upper().strip()
        if LOG_LEVEL not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(f"LOG_LEVEL must be one of: {LOG_LEVELS}")
        LOG_DESTINATION = os.environ["LOG_DESTINATION"].strip()
        if LOG_DESTINATION.lower() == "stdout":
            LOG_DESTINATION = sys.stdout
        elif LOG_DESTINATION.lower() == "stderr":
            LOG_DESTINATION = sys.stderr
        else:
            LOG_DESTINATION = os.path.join(LOG_DESTINATION)
            if not os.path.exists(os.path.dirname(os.path.realpath(LOG_DESTINATION))):
                raise ValueError(
                    "LOG_DESTINATION must be one of: stdout, stderr, or valid path"
                )
        logger.remove()
        logger.add(
            LOG_DESTINATION, format="{time} | {level} | {message}", level=LOG_LEVEL
        )
        ONESHOT = os.environ["ONESHOT"].lower().strip() in [
            "true",
            "1",
            "t",
            "y",
            "yes",
        ]
        DATA_DIR = os.environ["DATA_DIR"]
        SOUNDCLOUD_USERNAME = os.environ["SOUNDCLOUD_USERNAME"]
        API_AUTH_METHOD = os.environ["API_AUTH_METHOD"]
        API_CLIENT_ID = os.environ["API_CLIENT_ID"]
        API_CLIENT_SECRET = os.environ["API_CLIENT_SECRET"]
        API_REDIRECT_URI = os.environ["API_REDIRECT_URI"]
        WEBHOOK = os.environ["WEBHOOK"]
    except KeyError as e:
        logger.error(f"Please provide {e} as environment variable")
        exit(1)

    if not os.path.exists(DATA_DIR):
        logger.info(f"Creating data directory {DATA_DIR}")
        os.makedirs(DATA_DIR)

    asyncio.run(main())
