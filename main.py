import asyncio
import time
import json
import httpx
import random
import io
import logging
import itertools
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager

# ✅ Rate Limiting
from slowapi import Limiter
from slowapi.util import get_remote_address

# ================= Config =================
CACHE = {}
CACHE_TTL = 240  # 4 minutes

TELEGRAM_BOT_TOKEN = "7652042264:AAGc6DQ-OkJ8PaBKJnc_NkcCseIwmfbHD-c"
TELEGRAM_CHAT_ID = "5029478739"

PROXY_LIST = [
    "http://tmhcfiqv:ufqev7kx5dwk@84.247.60.125:6095",
    "http://tmhcfiqv:ufqev7kx5dwk@45.38.107.97:6014",
    "http://tmhcfiqv:ufqev7kx5dwk@216.10.27.159:6837",
    "http://tmhcfiqv:ufqev7kx5dwk@198.23.239.134:6540",
    "http://tmhcfiqv:ufqev7kx5dwk@107.172.163.27:6543",
    "http://zudcfjwt:rmhlu4rptdpy@198.23.239.134:6540",
    "http://zudcfjwt:rmhlu4rptdpy@45.38.107.97:6014",
    "http://zudcfjwt:rmhlu4rptdpy@107.172.163.27:6543",
    "http://zudcfjwt:rmhlu4rptdpy@64.137.96.74:6641",
    "http://zudcfjwt:rmhlu4rptdpy@45.43.186.39:6257",
    "http://zudcfjwt:rmhlu4rptdpy@154.203.43.247:5536",
    "http://zudcfjwt:rmhlu4rptdpy@84.247.60.125:6095",
    "http://zudcfjwt:rmhlu4rptdpy@216.10.27.159:6837",
    "http://zudcfjwt:rmhlu4rptdpy@136.0.207.84:6661",
    "http://zudcfjwt:rmhlu4rptdpy@142.147.128.93:6593",

    # --- tbtjitxc ---
    "http://tbtjitxc:grxvqm7nmglv@198.23.239.134:6540",
    "http://tbtjitxc:grxvqm7nmglv@45.38.107.97:6014",
    "http://tbtjitxc:grxvqm7nmglv@107.172.163.27:6543",
    "http://tbtjitxc:grxvqm7nmglv@64.137.96.74:6641",
    "http://tbtjitxc:grxvqm7nmglv@45.43.186.39:6257",
    "http://tbtjitxc:grxvqm7nmglv@154.203.43.247:5536",
    "http://tbtjitxc:grxvqm7nmglv@84.247.60.125:6095",
    "http://tbtjitxc:grxvqm7nmglv@216.10.27.159:6837",
    "http://tbtjitxc:grxvqm7nmglv@136.0.207.84:6661",
    "http://tbtjitxc:grxvqm7nmglv@142.147.128.93:6593",

    # --- lpogdtoe ---
    "http://lpogdtoe:51pqyrokjz2i@198.23.239.134:6540",
    "http://lpogdtoe:51pqyrokjz2i@45.38.107.97:6014",
    "http://lpogdtoe:51pqyrokjz2i@107.172.163.27:6543",
    "http://lpogdtoe:51pqyrokjz2i@64.137.96.74:6641",
    "http://lpogdtoe:51pqyrokjz2i@45.43.186.39:6257",
    "http://lpogdtoe:51pqyrokjz2i@154.203.43.247:5536",
    "http://lpogdtoe:51pqyrokjz2i@84.247.60.125:6095",
    "http://lpogdtoe:51pqyrokjz2i@216.10.27.159:6837",
    "http://lpogdtoe:51pqyrokjz2i@136.0.207.84:6661",
    "http://lpogdtoe:51pqyrokjz2i@142.147.128.93:6593",

    # --- atjnhjkt ---
    "http://atjnhjkt:eu3ep55xhqmf@198.23.239.134:6540",
    "http://atjnhjkt:eu3ep55xhqmf@45.38.107.97:6014",
    "http://atjnhjkt:eu3ep55xhqmf@107.172.163.27:6543",
    "http://atjnhjkt:eu3ep55xhqmf@64.137.96.74:6641",
    "http://atjnhjkt:eu3ep55xhqmf@45.43.186.39:6257",
    "http://atjnhjkt:eu3ep55xhqmf@154.203.43.247:5536",
    "http://atjnhjkt:eu3ep55xhqmf@84.247.60.125:6095",
    "http://atjnhjkt:eu3ep55xhqmf@216.10.27.159:6837",
    "http://atjnhjkt:eu3ep55xhqmf@136.0.207.84:6661",
    "http://atjnhjkt:eu3ep55xhqmf@142.147.128.93:6593",

    # --- hwllwynb ---
    "http://hwllwynb:34cu8jntkdjp@198.23.239.134:6540",
    "http://hwllwynb:34cu8jntkdjp@45.38.107.97:6014",
    "http://hwllwynb:34cu8jntkdjp@107.172.163.27:6543",
    "http://hwllwynb:34cu8jntkdjp@64.137.96.74:6641",
    "http://hwllwynb:34cu8jntkdjp@45.43.186.39:6257",
    "http://hwllwynb:34cu8jntkdjp@154.203.43.247:5536",
    "http://hwllwynb:34cu8jntkdjp@84.247.60.125:6095",
    "http://hwllwynb:34cu8jntkdjp@216.10.27.159:6837",
    "http://hwllwynb:34cu8jntkdjp@136.0.207.84:6661",
    "http://hwllwynb:34cu8jntkdjp@142.147.128.93:6593",

    # --- hbawtbmj ---
    "http://hbawtbmj:b8t1vjdac2o4@198.23.239.134:6540",
    "http://hbawtbmj:b8t1vjdac2o4@45.38.107.97:6014",
    "http://hbawtbmj:b8t1vjdac2o4@107.172.163.27:6543",
    "http://hbawtbmj:b8t1vjdac2o4@64.137.96.74:6641",
    "http://hbawtbmj:b8t1vjdac2o4@45.43.186.39:6257",
    "http://hbawtbmj:b8t1vjdac2o4@154.203.43.247:5536",
    "http://hbawtbmj:b8t1vjdac2o4@84.247.60.125:6095",
    "http://hbawtbmj:b8t1vjdac2o4@216.10.27.159:6837",
    "http://hbawtbmj:b8t1vjdac2o4@136.0.207.84:6661",
    "http://hbawtbmj:b8t1vjdac2o4@142.147.128.93:6593",

    # --- sihyoiej ---
    "http://sihyoiej:xmmyge8qzbo0@198.23.239.134:6540",
    "http://sihyoiej:xmmyge8qzbo0@45.38.107.97:6014",
    "http://sihyoiej:xmmyge8qzbo0@107.172.163.27:6543",
    "http://sihyoiej:xmmyge8qzbo0@64.137.96.74:6641",
    "http://sihyoiej:xmmyge8qzbo0@45.43.186.39:6257",
    "http://sihyoiej:xmmyge8qzbo0@154.203.43.247:5536",
    "http://sihyoiej:xmmyge8qzbo0@84.247.60.125:6095",
    "http://sihyoiej:xmmyge8qzbo0@216.10.27.159:6837",
    "http://sihyoiej:xmmyge8qzbo0@136.0.207.84:6661",
    "http://sihyoiej:xmmyge8qzbo0@142.147.128.93:6593",

    # --- ecbgkzdh ---
    "http://ecbgkzdh:ktcg7pog3206@198.23.239.134:6540",
    "http://ecbgkzdh:ktcg7pog3206@45.38.107.97:6014",
    "http://ecbgkzdh:ktcg7pog3206@107.172.163.27:6543",
    "http://ecbgkzdh:ktcg7pog3206@64.137.96.74:6641",
    "http://ecbgkzdh:ktcg7pog3206@45.43.186.39:6257",
    "http://ecbgkzdh:ktcg7pog3206@154.203.43.247:5536",
    "http://ecbgkzdh:ktcg7pog3206@84.247.60.125:6095",
    "http://ecbgkzdh:ktcg7pog3206@216.10.27.159:6837",
    "http://ecbgkzdh:ktcg7pog3206@136.0.207.84:6661",
    "http://ecbgkzdh:ktcg7pog3206@142.147.128.93:6593"
]


# working proxies (updated by tester)
WORKING_PROXIES = PROXY_LIST.copy()
# status store: proxy -> {"ok": bool, "latency": float, "last_checked": ts}
PROXY_STATUS = {p: {"ok": False, "latency": None, "last_checked": None} for p in PROXY_LIST}

# round-robin index and lock for safe rotation
_proxy_index = 0
_proxy_lock = asyncio.Lock()

# ================= Test config (as requested) =================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "x-ig-app-id": "936619743392459",
}

TEST_URL = "https://i.instagram.com/api/v1/users/web_profile_info/?username=himanshu_yadav479"
TEST_TIMEOUT = 8.0
TEST_INTERVAL = 300  # 5 minutes in seconds

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("instagram-scraper")

# ✅ Header pool (unchanged)
HEADERS_POOL = [
    {
        "x-ig-app-id": "936619743392459",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/123.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
    },
    {
        "x-ig-app-id": "936619743392459",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                      "Version/17.4 Safari/605.1.15",
        "Accept-Language": "en-GB,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
    },
    {
        "x-ig-app-id": "936619743392459",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/121.0.6167.86 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
    },
    {
        "x-ig-app-id": "936619743392459",
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                      "Version/17.3 Mobile/15E148 Safari/604.1",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
    },
    {
        "x-ig-app-id": "936619743392459",
        "User-Agent": "Mozilla/5.0 (Linux; Android 14; Pixel 7 Pro) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.6261.105 Mobile Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
    },
    {
        "x-ig-app-id": "936619743392459",
        "User-Agent": "Mozilla/5.0 (Linux; Android 14; Pixel 6) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/123.0.0.0 Mobile Safari/537.36 "
                      "Instagram 320.0.0.23.111 Android",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
    },
]

def get_random_headers():
    return random.choice(HEADERS_POOL)

# ================= Utils =================
def format_error_message(username: str, proxy_url: str, attempt: int, error: str, status_code: int = None):
    """Format error message for logs and Telegram"""
    base = f"❌ ERROR | User: {username}\n🔁 Attempt: {attempt}\n🌐 Proxy: {proxy_url}"
    if status_code:
        return f"{base}\n📡 Status: {status_code} ({error})"
    else:
        return f"{base}\n⚠️ Exception: {error}"

async def cache_cleaner():
    """Background task to clean expired cache"""
    while True:
        now = time.time()
        expired_keys = [k for k, v in CACHE.items() if v["expiry"] < now]
        for k in expired_keys:
            CACHE.pop(k, None)
        await asyncio.sleep(60)

async def notify_telegram(message: str):
    """Send Telegram notification"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
        async with httpx.AsyncClient() as client:
            await client.post(url, data=payload)
    except Exception as e:
        logger.error(f"Failed to send Telegram notification: {e}")

async def handle_error(status_code: int, detail: str, notify_msg: str = None):
    """Raise HTTPException and optionally notify Telegram"""
    if notify_msg:
        await notify_telegram(notify_msg)
    raise HTTPException(status_code=status_code, detail=detail)

# ================= Proxy rotation helper (async) =================
async def get_next_proxy():
    """
    Return next proxy from WORKING_PROXIES in round-robin fashion.
    If WORKING_PROXIES is empty, fall back to PROXY_LIST.
    This function is async and uses a lock for concurrency safety.
    """
    global _proxy_index, WORKING_PROXIES
    async with _proxy_lock:
        pool = WORKING_PROXIES if WORKING_PROXIES else PROXY_LIST
        if not pool:
            raise RuntimeError("No proxies configured")
        proxy = pool[_proxy_index % len(pool)]
        _proxy_index = (_proxy_index + 1) % (len(pool) if len(pool) > 0 else 1)
        return proxy

# ================= Proxy tester =================
async def test_single_proxy(proxy_url: str):
    """
    Test a single proxy by fetching TEST_URL with HEADERS and timeout TEST_TIMEOUT.
    Returns dict: {"ok": bool, "latency": float or None, "status_code": int or None, "error": str or None}
    """
    start = time.perf_counter()
    try:
        timeout = httpx.Timeout(TEST_TIMEOUT)
        async with httpx.AsyncClient(proxies={"http://": proxy_url, "https://": proxy_url}, timeout=timeout) as client:
            resp = await client.get(TEST_URL, headers=HEADERS)
        latency = time.perf_counter() - start
        ok = (resp.status_code == 200)
        return {"ok": ok, "latency": latency if ok else None, "status_code": resp.status_code, "error": None if ok else f"Status {resp.status_code}"}
    except Exception as e:
        latency = None
        return {"ok": False, "latency": None, "status_code": None, "error": str(e)}

async def probe_all_proxies_once():
    """Probe all proxies in PROXY_LIST and update PROXY_STATUS and WORKING_PROXIES."""
    global PROXY_STATUS, WORKING_PROXIES
    results = {}
    logger.info("Starting proxy probe for %d proxies...", len(PROXY_LIST))
    # test proxies in parallel but with a semaphore to avoid too many concurrent connections
    sem = asyncio.Semaphore(100)
    async def _test(p):
        async with sem:
            res = await test_single_proxy(p)
            PROXY_STATUS[p]["ok"] = res["ok"]
            PROXY_STATUS[p]["latency"] = res["latency"]
            PROXY_STATUS[p]["last_checked"] = time.time()
            logger.info("Probe %s -> ok=%s latency=%s err=%s", p, res["ok"], res["latency"], res["error"])
            return p, res

    tasks = [asyncio.create_task(_test(p)) for p in PROXY_LIST]
    await asyncio.gather(*tasks)

    # Build new WORKING_PROXIES sorted by latency (fastest first). If latency is None, put at end.
    working = [p for p, s in PROXY_STATUS.items() if s.get("ok")]
    # sort by latency
    working.sort(key=lambda x: PROXY_STATUS[x]["latency"] if PROXY_STATUS[x]["latency"] is not None else float("inf"))
    # update the global WORKING_PROXIES in a thread-safe way
    async with _proxy_lock:
        WORKING_PROXIES = working.copy()
        # reset index to avoid out-of-range
        global _proxy_index
        _proxy_index = 0
    logger.info("Proxy probe complete. %d working proxies found.", len(WORKING_PROXIES))

async def probe_loop():
    """Run probe_all_proxies_once every TEST_INTERVAL seconds forever."""
    # run first probe immediately at startup
    try:
        await probe_all_proxies_once()
    except Exception as e:
        logger.exception("Initial proxy probe failed: %s", e)
    while True:
        await asyncio.sleep(TEST_INTERVAL)
        try:
            await probe_all_proxies_once()
        except Exception as e:
            logger.exception("Proxy probe failed: %s", e)
            # optionally notify once on failure
            try:
                await notify_telegram(f"Proxy probe loop error: {e}")
            except Exception:
                pass

# ================= Lifespan =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    # start cache cleaner
    asyncio.create_task(cache_cleaner())
    # start proxy probe loop
    asyncio.create_task(probe_loop())
    yield

# ================= App Init =================
app = FastAPI(lifespan=lifespan)

# ✅ Rate Limiter
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

# ✅ CORS config
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development only
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ================= API Logic =================
async def scrape_user(username: str, max_retries: int = 2):
    username = username.lower()
    cached = CACHE.get(username)
    if cached and cached["expiry"] > time.time():
        return cached["data"]

    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"

    for attempt in range(max_retries):
        headers = get_random_headers()
        proxy_url = await get_next_proxy()
        proxies = {"http://": proxy_url, "https://": proxy_url}

        try:
            async with httpx.AsyncClient(proxies=proxies, timeout=10.0) as client:
                result = await client.get(url, headers=headers)

            if result.status_code == 200:
                try:
                    data = result.json()
                except json.JSONDecodeError:
                    continue

                user = data.get("data", {}).get("user")
                if not user:
                    continue

                user_data = {
                    "username": user.get("username"),
                    "real_name": user.get("full_name"),
                    "profile_pic": user.get("profile_pic_url_hd"),
                    "followers": user.get("edge_followed_by", {}).get("count"),
                    "following": user.get("edge_follow", {}).get("count"),
                    "post_count": user.get("edge_owner_to_timeline_media", {}).get("count"),
                    "bio": user.get("biography"),
                }

                CACHE[username] = {"data": user_data, "expiry": time.time() + CACHE_TTL}
                return user_data
            else:
                msg = format_error_message(username, proxy_url, attempt+1, "Request Failed", result.status_code)
                logger.warning(msg)
                await notify_telegram(msg)

        except httpx.RequestError as e:
            msg = format_error_message(username, proxy_url, attempt+1, str(e))
            logger.warning(msg)
            await notify_telegram(msg)

    await handle_error(
        status_code=502,
        detail="All proxies failed",
        notify_msg=f"🚨 All proxies failed for {username}"
    )

# ================= Routes =================
@app.get("/scrape/{username}")
@limiter.limit("10/10minute")
async def get_user(username: str, request: Request):
    return await scrape_user(username)

@app.get("/proxy-image/")
@limiter.limit("10/10minute")
async def proxy_image(request: Request, url: str, max_retries: int = 2):
    for attempt in range(max_retries):
        proxy_url = await get_next_proxy()
        proxies = {"http://": proxy_url, "https://": proxy_url}

        try:
            async with httpx.AsyncClient(proxies=proxies, timeout=10.0) as client:
                resp = await client.get(url)

            if resp.status_code == 200:
                return StreamingResponse(
                    io.BytesIO(resp.content),
                    media_type=resp.headers.get("content-type", "image/jpeg")
                )
            else:
                msg = format_error_message("proxy-image", proxy_url, attempt+1, "Image fetch failed", resp.status_code)
                logger.warning(msg)
                await notify_telegram(msg)

        except httpx.RequestError as e:
            msg = format_error_message("proxy-image", proxy_url, attempt+1, str(e))
            logger.warning(msg)
            await notify_telegram(msg)

    raise HTTPException(status_code=502, detail="All proxies failed for image fetch")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "working_proxies_count": len(WORKING_PROXIES),
        "last_checked": {p: PROXY_STATUS[p]["last_checked"] for p in PROXY_LIST}
    }
