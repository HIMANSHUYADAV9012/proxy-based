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
from fastapi.responses import StreamingResponse, JSONResponse
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
 "http://tmhcfiqv:ufqev7kx5dwk@198.23.239.134:6540",
    "http://tmhcfiqv:ufqev7kx5dwk@107.172.163.27:6543",
    "http://zudcfjwt:rmhlu4rptdpy@198.23.239.134:6540",
    "http://zudcfjwt:rmhlu4rptdpy@107.172.163.27:6543",
    "http://tbtjitxc:grxvqm7nmglv@198.23.239.134:6540",
    "http://tbtjitxc:grxvqm7nmglv@107.172.163.27:6543",
    "http://lpogdtoe:51pqyrokjz2i@198.23.239.134:6540",
    "http://lpogdtoe:51pqyrokjz2i@107.172.163.27:6543",
    "http://atjnhjkt:eu3ep55xhqmf@198.23.239.134:6540",
    "http://atjnhjkt:eu3ep55xhqmf@107.172.163.27:6543",
    "http://hwllwynb:34cu8jntkdjp@198.23.239.134:6540",
    "http://hwllwynb:34cu8jntkdjp@107.172.163.27:6543",
    "http://hbawtbmj:b8t1vjdac2o4@107.172.163.27:6543",
    "http://sihyoiej:xmmyge8qzbo0@198.23.239.134:6540",
    "http://sihyoiej:xmmyge8qzbo0@107.172.163.27:6543",
    "http://ecbgkzdh:ktcg7pog3206@198.23.239.134:6540",
    "http://ecbgkzdh:ktcg7pog3206@107.172.163.27:6543",
    "http://himanshu_2SuPS:9012ABCabc__@dc.oxylabs.io:8004",
    "http://himanshu_2SuPS:9012ABCabc__@dc.oxylabs.io:8005",
    "http://himanshu_2SuPS:9012ABCabc__@dc.oxylabs.io:8002",
    "http://himanshu_2SuPS:9012ABCabc__@dc.oxylabs.io:8003",
    "http://himanshu_Rm9oO:9012ABCabc__@dc.oxylabs.io:8004"
]

# ✅ Proxy Round Robin Iterator (backup use)
proxy_cycle = itertools.cycle(PROXY_LIST)

def get_next_proxy():
    return next(proxy_cycle)

# ✅ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("instagram-scraper")

# ✅ Stats
STATS = {
    "proxy_success": 0,
    "proxy_fail": 0,
    "last_alerts": []
}

# ✅ Global HTTPX Client Pool
CLIENT_POOL = []

async def init_clients():
    global CLIENT_POOL
    for proxy in PROXY_LIST:
        client = httpx.AsyncClient(proxies={"http://": proxy, "https://": proxy}, timeout=10.0)
        CLIENT_POOL.append({"client": client, "proxy": proxy})

def get_random_client():
    return random.choice(CLIENT_POOL)

# ✅ Header pool
HEADERS_POOL = [
    {"x-ig-app-id": "936619743392459", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"},
    {"x-ig-app-id": "936619743392459", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) "
     "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"},
    {"x-ig-app-id": "936619743392459", "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.86 Safari/537.36"},
    {"x-ig-app-id": "936619743392459", "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) "
     "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1"},
    {"x-ig-app-id": "936619743392459", "User-Agent": "Mozilla/5.0 (Linux; Android 14; Pixel 7 Pro) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.105 Mobile Safari/537.36"},
    {"x-ig-app-id": "936619743392459", "User-Agent": "Mozilla/5.0 (Linux; Android 14; Pixel 6) "
     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36 "
     "Instagram 320.0.0.23.111 Android"},
]

def get_random_headers():
    return random.choice(HEADERS_POOL)

# ================= Utils =================
def format_error_message(username: str, proxy_url: str, attempt: int, error: str, status_code: int = None):
    base = f"❌ ERROR | User: {username}\n🔁 Attempt: {attempt}\n🌐 Proxy: {proxy_url}"
    if status_code:
        return f"{base}\n📡 Status: {status_code} ({error})"
    else:
        return f"{base}\n⚠️ Exception: {error}"

async def cache_cleaner():
    while True:
        now = time.time()
        expired_keys = [k for k, v in CACHE.items() if v["expiry"] < now]
        for k in expired_keys:
            CACHE.pop(k, None)
        await asyncio.sleep(60)

async def notify_telegram(message: str):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
        async with httpx.AsyncClient() as client:
            await client.post(url, data=payload)
        # store in stats
        STATS["last_alerts"].append({"time": time.time(), "msg": message})
        STATS["last_alerts"] = STATS["last_alerts"][-10:]  # keep last 10
    except Exception as e:
        logger.error(f"Failed to send Telegram notification: {e}")

async def handle_error(status_code: int, detail: str, notify_msg: str = None):
    if notify_msg:
        await notify_telegram(notify_msg)
    raise HTTPException(status_code=status_code, detail=detail)

# ================= Lifespan =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(cache_cleaner())
    await init_clients()
    yield

# ================= App Init =================
app = FastAPI(lifespan=lifespan)

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
        item = get_random_client()
        client = item["client"]
        proxy_url = item["proxy"]

        try:
            result = await client.get(url, headers=headers)

            if result.status_code == 200:
                try:
                    data = result.json()
                except json.JSONDecodeError:
                    continue

                user = data.get("data", {}).get("user")
                if not user:
                    await handle_error(404, "User not found", f"⚠️ User not found: {username}")

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
                STATS["proxy_success"] += 1
                return user_data

            elif result.status_code == 404:
                msg = format_error_message(username, proxy_url, attempt+1, "User Not Found", result.status_code)
                logger.warning(msg)
                STATS["proxy_fail"] += 1
                await handle_error(404, "User not found", f"⚠️ User not found: {username}")

            else:
                msg = format_error_message(username, proxy_url, attempt+1, "Request Failed", result.status_code)
                logger.warning(msg)
                STATS["proxy_fail"] += 1
                await notify_telegram(msg)

        except httpx.RequestError as e:
            msg = format_error_message(username, proxy_url, attempt+1, str(e))
            logger.warning(msg)
            STATS["proxy_fail"] += 1
            await notify_telegram(msg)

    await handle_error(502, "All proxies failed", f"🚨 All proxies failed for {username}")

# ================= Routes =================
@app.get("/scrape/{username}")
@limiter.limit("10/10minute")
async def get_user(username: str, request: Request):
    return await scrape_user(username)

@app.get("/proxy-image/")
@limiter.limit("10/10minute")
async def proxy_image(request: Request, url: str, max_retries: int = 2):
    for attempt in range(max_retries):
        item = get_random_client()
        client = item["client"]
        proxy_url = item["proxy"]

        try:
            resp = await client.get(url)

            if resp.status_code == 200:
                STATS["proxy_success"] += 1
                return StreamingResponse(io.BytesIO(resp.content), media_type=resp.headers.get("content-type", "image/jpeg"))
            elif resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Image not found")
            else:
                msg = format_error_message("proxy-image", proxy_url, attempt+1, "Image fetch failed", resp.status_code)
                logger.warning(msg)
                STATS["proxy_fail"] += 1
                await notify_telegram(msg)

        except httpx.RequestError as e:
            msg = format_error_message("proxy-image", proxy_url, attempt+1, str(e))
            logger.warning(msg)
            STATS["proxy_fail"] += 1
            await notify_telegram(msg)

    raise HTTPException(status_code=502, detail="All proxies failed for image fetch")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}

@app.head("/health")
async def health_check_head():
    return JSONResponse(content=None, status_code=200)

@app.get("/stats")
async def stats():
    return {
        "cache_size": len(CACHE),
        "proxy_success": STATS["proxy_success"],
        "proxy_fail": STATS["proxy_fail"],
        "last_alerts": STATS["last_alerts"]
    }




