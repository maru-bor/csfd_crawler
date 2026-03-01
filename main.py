import asyncio
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import os
import re

START_URLS = [
    "https://www.csfd.cz/zebricky/filmy/nejlepsi/"
]

OUTPUT_FILE = "csfd_movies.jsonl"
MAX_SIZE = 1 * 1024**3

ALLOWED_DOMAIN = "www.csfd.cz"

visited = set()
queue = asyncio.Queue()
saved_urls = set()
saved_count = 0
lock = asyncio.Lock()


def normalize(url):
    p = urlparse(url)
    return f"https://{p.netloc}{p.path}"


def allowed(url):
    return ALLOWED_DOMAIN in urlparse(url).netloc


def canonical_movie_url(url: str) -> str | None:
    m = re.search(r"(https://www\.csfd\.cz/film/\d+)", url)
    if not m:
        return None
    return m.group(1) + "/"


def is_movie(url):
    return re.search(r"/film/\d+", url)


def file_size():
    return os.path.getsize(OUTPUT_FILE) if os.path.exists(OUTPUT_FILE) else 0


async def save_record(rec):
    global saved_count
    async with lock:
        with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        saved_count += 1


def parse_movie(url, soup):
    title_tag = soup.select_one("h1")
    if not title_tag:
        return None
    title = title_tag.get_text(strip=True)

    # rok
    year = None
    year_tag = soup.select_one(".origin")
    if year_tag:
        m = re.search(r"\d{4}", year_tag.text)
        if m:
            year = int(m.group())

    # hodnocení
    rating = None
    rating_tag = soup.select_one(".film-rating-average")
    if rating_tag:
        try:
            rating = int(rating_tag.text.replace("%","").strip())
        except:
            rating = None

    # votes
    votes = None
    votes_link = soup.select_one('a.tab-link[data-show-tab=".rating-users"] span.counter')
    if votes_link:
        text = votes_link.get_text(strip=True)
        m = re.search(r"\d[\d\s\u00A0]*", text)
        if m:
            clean_number = re.sub(r"[^\d]", "", m.group(0))  
            votes = int(clean_number)

    # žánry
    genres = []
    for g in soup.select(".genres a"):
        genres.append(g.text.strip())

    # popis
    description = None
    desc_tag = soup.select_one(".plot-full")
    if desc_tag:
        description = desc_tag.text.strip()

    return {
        "url": url,
        "title": title,
        "year": year,
        "rating": rating,
        "votes": votes,
        "genres": genres,
        "description": description
    }


async def fetch(session, url):
    norm = normalize(url)
    if norm in visited:
        return
    visited.add(norm)

    try:
        async with session.get(norm, timeout=20) as resp:
            if resp.status != 200:
                return

            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            if is_movie(norm):
                canon = canonical_movie_url(norm)
                if not canon or canon in saved_urls:
                    return

                rec = parse_movie(norm, soup)
                if rec:
                    rec["url"] = canon
                    await save_record(rec)
                    saved_urls.add(canon)
                    print(f"Uložen: {rec['title']} | votes: {rec['votes']} | rating: {rec['rating']}")

            for a in soup.find_all("a", href=True):
                href = a["href"]
                full = urljoin(norm, href)
                if not allowed(full):
                    continue

                canon = canonical_movie_url(full)
                target = canon if canon else normalize(full)
                if target not in visited:
                    await queue.put(target)

    except asyncio.TimeoutError:
        print(f"Timeout: {norm}")
    except Exception as e:
        print(f"Chyba: {norm} -> {e}")


async def worker(session):
    while True:
        try:
            url = await asyncio.wait_for(queue.get(), timeout=10)
        except asyncio.TimeoutError:
            break
        try:
            await fetch(session, url)
        except Exception as e:
            print(f"Worker chyba: {e}")
        queue.task_done()


async def progress_report():
    while True:
        print(f"STATUS: ve frontě {queue.qsize()} | uložených filmů {saved_count} | velikost {file_size()/1024/1024:.2f} MB")
        await asyncio.sleep(30)


async def main():
    for url in START_URLS:
        await queue.put(url)

    async with ClientSession() as session:
        workers = [asyncio.create_task(worker(session)) for _ in range(20)]
        asyncio.create_task(progress_report())
        await asyncio.gather(*workers)


if __name__ == "__main__":
    asyncio.run(main())
