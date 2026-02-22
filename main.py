import asyncio
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import re
import json
import os
import time
import chardet

START_URLS = [
    "https://www.idnes.cz/",
    "https://www.idnes.cz/fotbal/pohary/dortmundu-pomohl-k-remize-kolleruv-gol.A010911_225921_fot_pohary_bra"

]

OUTPUT_FILE = "articles.jsonl"
MAX_SIZE = 3 * 1024**3  # 3 GB

ALLOWED_DOMAINS = ("www.idnes.cz", "idnes.cz")
ARTICLE_RE = re.compile(r"A\d{6}_\d{6}")

visited_articles = set()
visited_pages = set()
queue = asyncio.Queue()
stored_urls = set()
saved_count = 0


def normalize(url: str) -> str:
    p = urlparse(url)
    return urlunparse((p.scheme or "https", p.netloc, p.path, "", "", ""))


def allowed(url: str) -> bool:
    p = urlparse(url)
    return p.netloc in ALLOWED_DOMAINS and p.scheme in ("http", "https")


def is_article(url: str) -> bool:
    if not ARTICLE_RE.search(url):
        return False
    path = urlparse(url).path.lower()


    if any(x in path for x in [
        "/foto", "/diskuse", "/wiki/", "/video", "/hry/", "/serialy/", "/premium/", "/epaper."
    ]):
        return False

    if re.search(r"\.(jpg|jpeg|png|gif|webp|svg|jfif)$", path):
        return False
    return True



def file_size() -> int:
    return os.path.getsize(OUTPUT_FILE) if os.path.exists(OUTPUT_FILE) else 0


async def save_record(record: dict):
    global saved_count
    async with asyncio.Lock():
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        saved_count += 1


def parse_article(url: str, soup: BeautifulSoup) -> dict | None:
    title_tag = soup.find("h1")
    if not title_tag:
        return None

    paragraphs = [p.get_text(" ", strip=True) for p in soup.select("div#art-text p")]
    content = "\n".join(paragraphs).strip()

    #datum článku
    date = None
    meta_date = soup.find("meta", {"property": "article:published_time"})
    if meta_date and meta_date.get("content"):
        date = meta_date.get("content")
    else:
        time_tag = soup.find("time")
        if time_tag and time_tag.get("datetime"):
            date = time_tag.get("datetime")

    #obrázky
    more_gallery = soup.select_one("div.more-gallery b")
    if more_gallery and more_gallery.text.isdigit():
        images_count = int(more_gallery.text)
    else:
        images_count = len({img.get("src") for img in soup.select("div#art-text img") if img.get("src")})

    #komentáře
    comments_count = 0
    discussion_link = soup.select_one("a.btndsc")
    if discussion_link:
        spans = discussion_link.find_all("span")
        if spans:
            text = spans[-1].get_text(strip=True)
            m = re.search(r"(\d+)", text)
            if m:
                comments_count = int(m.group(1))

    path_parts = urlparse(url).path.split("/")
    category = path_parts[1] if len(path_parts) > 1 and path_parts[1] else None

    return {
        "url": url,
        "title": title_tag.get_text(strip=True),
        "date": date,
        "category": category,
        "content": content,
        "images": images_count,
        "comments": comments_count,
    }


async def fetch(session: ClientSession, url: str):
    norm_url = normalize(url)

    try:
        async with session.get(norm_url, timeout=20) as resp:
            if resp.status != 200:
                return

            raw = await resp.read()
            try:
                html = raw.decode("utf-8")
            except UnicodeDecodeError:
                html = raw.decode("windows-1250", errors="ignore")

            soup = BeautifulSoup(html, "html.parser")

            if is_article(norm_url) and norm_url not in stored_urls:
                rec = parse_article(norm_url, soup)
                if rec and rec["title"].strip():
                    await save_record(rec)
                    stored_urls.add(norm_url)
                    print(f"[+] Uložen nový článek: {rec['title'][:100]} | soubor: {file_size()/1024/1024:.2f} MB")


            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                if not href or href.startswith(("javascript:", "#")):
                    continue

                joined = urljoin(norm_url, href)
                n = normalize(joined)

                if not allowed(n):
                    continue

                path = urlparse(n).path.lower()
                if (
                    "/wiki/" in path
                    or "/video/" in path
                    or path.endswith((".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".jfif"))
                ):
                    continue

                if is_article(n):
                    if n in visited_articles or n in stored_urls:
                        continue
                    visited_articles.add(n)
                    await queue.put(n)
                else:
                    if n in visited_pages:
                        continue
                    visited_pages.add(n)
                    await queue.put(n)

    except Exception as ex:
        print(f"Výjimka při stahování {norm_url}: {ex}")

async def worker(session: ClientSession):
    while file_size() < MAX_SIZE:
        try:
            url = await asyncio.wait_for(queue.get(), timeout=5)
        except asyncio.TimeoutError:
            break
        await fetch(session, url)
        queue.task_done()


async def load_stored_urls():
    if not os.path.exists(OUTPUT_FILE):
        return
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                if "url" in obj:
                    stored_urls.add(obj["url"])
            except:
                continue
    print(f"Načteno {len(stored_urls)} URL ze souboru.")


async def progress_report():
    while True:
        print(f"STATUS: ve frontě {queue.qsize()} | uložených článků {saved_count} | velikost {file_size()/1024/1024:.2f} MB")
        await asyncio.sleep(60)


async def main():
    await load_stored_urls()

    for url in START_URLS:
        await queue.put(url)

    async with aiohttp.ClientSession(cookies={"dCMP": "mafra=1111"}) as session:
        workers = [asyncio.create_task(worker(session)) for _ in range(40)]
        asyncio.create_task(progress_report())
        await asyncio.gather(*workers)


if __name__ == "__main__":
    print("Crawler běží...")
    start = time.time()
    asyncio.run(main())
    print(f"Crawling dokončen. Celkem uložených článků: {saved_count}")
