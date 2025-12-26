import yaml
import re
from bs4 import BeautifulSoup, Comment
from pymongo import MongoClient, ASCENDING
import sys

REMOVE_TAGS = ["script","style","noscript","header","footer","nav","aside","form"]

def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(REMOVE_TAGS):
        tag.decompose()

    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        c.extract()

    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()

    return text

def main(cfg_path: str):
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    client = MongoClient(cfg["db"]["uri"])
    db = client[cfg["db"]["name"]]

    src = db["documents"]
    dst = db["documents_clean"]

    dst.create_index([("url_norm", ASCENDING)], unique=True)
    dst.create_index([("source", ASCENDING)])

    cur = src.find(
        {"raw_html": {"$exists": True}},
        {"url_norm": 1, "url": 1, "source": 1, "raw_html": 1, "fetched_at": 1}
    ).batch_size(200)

    processed = 0
    for doc in cur:
        url_norm = doc.get("url_norm")
        if not url_norm:
            continue

        html = doc.get("raw_html") or ""
        text = clean_html(html)

        dst.update_one(
            {"url_norm": url_norm},
            {"$set": {
                "url_norm": url_norm,
                "url": doc.get("url"),
                "source": doc.get("source"),
                "fetched_at": doc.get("fetched_at"),
                "clean_text": text
            }},
            upsert=True
        )

        processed += 1
        if processed % 500 == 0:
            print("cleaned:", processed)

    print("DONE. cleaned:", processed)

if __name__ == "__main__":
    main(sys.argv[1])
