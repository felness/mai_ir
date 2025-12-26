import time
import yaml
import hashlib
import requests
import re
from urllib.parse import urlsplit, urlunsplit, urldefrag, urljoin
from pymongo import MongoClient, ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import datetime
import traceback

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def normalize_url(url: str) -> str:
    url, _frag = urldefrag(url)
    parts = urlsplit(url)
    scheme = (parts.scheme or "https").lower()
    netloc = (parts.netloc or "").lower()
    path = parts.path or "/"
    query = parts.query
    return urlunsplit((scheme, netloc, path, query, ""))

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def now_ts() -> int:
    return int(time.time())

class ThreadSafeCounter:
    def __init__(self, initial=0):
        self.value = initial
        self._lock = threading.Lock()
    
    def increment(self, n=1):
        with self._lock:
            self.value += n
            return self.value
    
    def get(self):
        with self._lock:
            return self.value

def ensure_indexes(db):
    db.documents.create_index([("url_norm", ASCENDING)], unique=True)
    db.queue.create_index([("url_norm", ASCENDING)], unique=True)
    db.queue.create_index([("status", ASCENDING), ("next_fetch_at", ASCENDING)])
    db.queue.create_index([("source", ASCENDING), ("status", ASCENDING)])
    db.queue.create_index([("priority", ASCENDING), ("status", ASCENDING), ("next_fetch_at", ASCENDING)])

def queue_put(db, source: str, url: str, priority: int = 2, next_fetch_at: int | None = None):
    url_norm = normalize_url(url)
    doc = {
        "url_norm": url_norm,
        "url": url,
        "source": source,
        "priority": priority,
        "status": "pending",
        "attempts": 0,
        "updated_at": now_ts(),
        "next_fetch_at": float(next_fetch_at if next_fetch_at is not None else now_ts()),
    }
    try:
        db.queue.insert_one(doc)
        return True
    except DuplicateKeyError:
        return False

def get_next_job(db, source_limits: dict):
    current_time = float(now_ts())
    
    pipeline = [
        {
            "$match": {
                "status": "pending",
                "next_fetch_at": {"$lte": current_time}
            }
        },
        {
            "$addFields": {
                "can_fetch": {
                    "$lt": [
                        {"$ifNull": [f"$source_stats.{'$source'}", 0]},
                        {"$ifNull": [{"$arrayElemAt": [{"$objectToArray": source_limits}, 0]}, 999999]}
                    ]
                }
            }
        },
        {
            "$match": {"can_fetch": True}
        },
        {
            "$sort": {
                "priority": ASCENDING,
                "next_fetch_at": ASCENDING
            }
        },
        {
            "$limit": 1
        }
    ]
    
    for priority in [1, 2]:  
        for source, limit in source_limits.items():
            current_count = db.documents.count_documents({"source": source})
            if current_count >= limit:
                continue  
                
            job = db.queue.find_one_and_update(
                {
                    "status": "pending", 
                    "next_fetch_at": {"$lte": current_time},
                    "source": source,
                    "priority": priority
                },
                {"$set": {"status": "in_progress", "updated_at": now_ts()}},
                sort=[("next_fetch_at", ASCENDING)],
                return_document=ReturnDocument.AFTER,
            )
            
            if job:
                return job
    
    return db.queue.find_one_and_update(
        {"status": "pending", "next_fetch_at": {"$lte": current_time}},
        {"$set": {"status": "in_progress", "updated_at": now_ts()}},
        sort=[("priority", ASCENDING), ("next_fetch_at", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )

def mark_job(db, url_norm: str, ok: bool, retry_in: int = 30, recrawl_in: int = 60 * 60 * 24 * 30):
    if ok:
        db.queue.update_one(
            {"url_norm": url_norm},
            {"$set": {"status": "done", "updated_at": now_ts(), "next_fetch_at": float(now_ts() + recrawl_in)}}
        )
    else:
        q = db.queue.find_one({"url_norm": url_norm}) or {}
        attempts = int(q.get("attempts", 0)) + 1
        db.queue.update_one(
            {"url_norm": url_norm},
            {"$set": {
                "status": "pending",
                "attempts": attempts,
                "updated_at": now_ts(),
                "next_fetch_at": float(now_ts() + retry_in)
            }}
        )

def seed_wikisource_allpages(db, source_cfg, ua: str, limit: int = 40000):
    if db.queue.count_documents({"source": "wikisource_ru"}) > 0:
        logger.info("Wikisource seed уже выполнен, пропускаем")
        return 0
    
    api_url = source_cfg["seed"]["api_url"]
    params = {"action": "query", "format": "json", "list": "allpages"}
    params.update(source_cfg["seed"].get("params", {}))
    cont = {}
    
    base = source_cfg["url_builder"]["base"]
    priority = source_cfg.get("priority", 2)
    
    inserted = 0
    batch_size = 100
    
    with requests.Session() as s:
        s.headers.update({"User-Agent": ua})
        
        while inserted < limit:
            p = params.copy()
            if cont:
                p.update(cont)
            
            try:
                r = s.get(api_url, params=p, timeout=30)
                r.raise_for_status()
                data = r.json()
                
                pages = data.get("query", {}).get("allpages", [])
                for page in pages:
                    title = page["title"]
                    url = base + title.replace(" ", "_")
                    queue_put(db, source_cfg["name"], url, priority)
                    inserted += 1
                    
                    if inserted >= limit:
                        break
                    
                    if inserted % batch_size == 0:
                        logger.info(f"Wikisource: добавлено {inserted} URL")
                
                if inserted >= limit:
                    break
                    
                if "continue" in data:
                    cont = data["continue"]
                else:
                    break
                    
            except Exception as e:
                logger.error(f"Ошибка при seed wikisource: {e}")
                break
    
    logger.info(f"Добавлено {inserted} URL из wikisource")
    return inserted

def seed_libru_initial(db, source_cfg):
    if db.queue.count_documents({"source": "libru"}) > 0:
        logger.info("Lib.ru seed уже выполнен, пропускаем")
        return 0
    
    priority = source_cfg.get("priority", 2)
    added = 0
    
    for url in source_cfg["seed"].get("urls", []):
        if queue_put(db, source_cfg["name"], url, priority):
            added += 1
    
    logger.info(f"Добавлено {added} начальных URL из lib.ru")
    return added

def extract_links_from_html(html: str, base_url: str, source: str):
    links = set()
    
    for match in re.finditer(r'href="([^"]*)"', html, re.IGNORECASE):
        href = match.group(1)
        
        if not href or href.startswith(('#', 'javascript:', 'mailto:')):
            continue
        
        full_url = urljoin(base_url, href)
        if 'lib.ru' in full_url:
            norm_url = normalize_url(full_url)
            links.add(norm_url)
    
    return list(links)

def worker(worker_id, cfg, db, stop_event, stats, source_limits):
    session = requests.Session()
    ua = cfg["logic"]["user_agent"]
    timeout = cfg["logic"]["timeout_seconds"]
    max_retries = cfg["logic"]["max_retries"]
    delay = cfg["logic"]["delay_seconds"]
    
    empty_cycles = 0
    
    while not stop_event.is_set():
        try:
            job = get_next_job(db, source_limits)
            
            if not job:
                empty_cycles += 1
                if empty_cycles > 3:
                    time.sleep(5)
                else:
                    time.sleep(2)
                continue
            
            empty_cycles = 0
            
            url = job["url"]
            url_norm = job["url_norm"]
            source = job["source"]
            
            source_doc_count = db.documents.count_documents({"source": source})
            if source_doc_count >= source_limits.get(source, 999999):
                logger.debug(f"Worker {worker_id}: лимит для {source} достигнут")
                mark_job(db, url_norm, ok=False, retry_in=300)  
                continue
            
            prev = db.documents.find_one({"url_norm": url_norm}, 
                                        {"etag": 1, "last_modified": 1, "content_hash": 1})
            etag = prev.get("etag") if prev else None
            last_modified = prev.get("last_modified") if prev else None
            
            try:
                headers = {"User-Agent": ua}
                if etag:
                    headers["If-None-Match"] = etag
                if last_modified:
                    headers["If-Modified-Since"] = last_modified
                
                r = session.get(url, headers=headers, timeout=timeout)
                
                if r.status_code == 304:
                    db.documents.update_one(
                        {"url_norm": url_norm},
                        {"$set": {"fetched_at": now_ts()}},
                        upsert=True
                    )
                    mark_job(db, url_norm, ok=True)
                    stats[f"{source}_304"] = stats.get(f"{source}_304", 0) + 1
                    
                elif r.status_code == 200:
                    html = r.text
                    h = sha256_text(html)
                    
                    changed = (not prev) or (prev.get("content_hash") != h)
                    
                    doc_data = {
                        "url": url,
                        "url_norm": url_norm,
                        "source": source,
                        "fetched_at": now_ts(),
                        "raw_html": html,
                        "etag": r.headers.get("ETag"),
                        "last_modified": r.headers.get("Last-Modified"),
                        "content_hash": h,
                    }
                    
                    if changed:
                        db.documents.update_one(
                            {"url_norm": url_norm},
                            {"$set": doc_data},
                            upsert=True
                        )
                        stats[f"{source}_new"] = stats.get(f"{source}_new", 0) + 1
                        
                        if source == "libru" and "lib.ru" in url:
                            try:
                                new_links = extract_links_from_html(html, url, source)
                                added_links = 0
                                
                                for link_url in new_links:
                                    if queue_put(db, source, link_url, priority=2):
                                        added_links += 1
                                
                                if added_links > 0:
                                    stats[f"{source}_links"] = stats.get(f"{source}_links", 0) + added_links
                            except Exception as e:
                                logger.debug(f"Ошибка извлечения ссылок: {e}")
                    else:
                        db.documents.update_one(
                            {"url_norm": url_norm},
                            {"$set": {
                                "fetched_at": now_ts(),
                                "etag": r.headers.get("ETag"),
                                "last_modified": r.headers.get("Last-Modified"),
                            }}
                        )
                        stats[f"{source}_cached"] = stats.get(f"{source}_cached", 0) + 1
                    
                    mark_job(db, url_norm, ok=True)
                    
                else:
                    attempts = int(job.get("attempts", 0))
                    if attempts >= max_retries:
                        db.queue.update_one(
                            {"url_norm": url_norm},
                            {"$set": {"status": "error", "updated_at": now_ts(), 
                                     "error": f"HTTP {r.status_code}"}}
                        )
                        stats[f"{source}_error"] = stats.get(f"{source}_error", 0) + 1
                    else:
                        mark_job(db, url_norm, ok=False, retry_in=60)
                        stats[f"{source}_retry"] = stats.get(f"{source}_retry", 0) + 1
                        
            except Exception as e:
                attempts = int(job.get("attempts", 0))
                if attempts >= max_retries:
                    db.queue.update_one(
                        {"url_norm": url_norm},
                        {"$set": {"status": "error", "updated_at": now_ts(), "error": str(e)}}
                    )
                    stats[f"{source}_exception"] = stats.get(f"{source}_exception", 0) + 1
                else:
                    mark_job(db, url_norm, ok=False, retry_in=60)
                    stats[f"{source}_retry_ex"] = stats.get(f"{source}_retry_ex", 0) + 1
            
            time.sleep(delay)
            
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {e}")
            time.sleep(5)
    
    logger.info(f"Worker {worker_id} остановлен")

def main(cfg_path: str):
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    
    client = MongoClient(cfg["db"]["uri"])
    db = client[cfg["db"]["name"]]
    ensure_indexes(db)
    
    print("=" * 70)
    print("УНИВИРСАЛЬНЫЙ КРАУЛЕР - WIKISOURCE + LIB.RU")
    print("=" * 70)
    
    wikisource_target = cfg["logic"]["wikisource_target"]
    libru_target = cfg["logic"]["libru_target"]
    max_docs = wikisource_target + libru_target
    source_limits = {
        "wikisource_ru": wikisource_target,
        "libru": libru_target
    }
    
    print(f"Конфигурация:")
    print(f"   - Цель wikisource: {wikisource_target} документов")
    print(f"   - Цель lib.ru: {libru_target} документов")
    print(f"   - Всего цель: {max_docs} документов")
    print(f"   - Потоков: {cfg['logic'].get('threads', 1)}")
    print(f"   - Задержка: {cfg['logic']['delay_seconds']} сек")
    
    print("Текущее состояние:")
    
    total_docs = db.documents.count_documents({})
    wikisource_docs = db.documents.count_documents({"source": "wikisource_ru"})
    libru_docs = db.documents.count_documents({"source": "libru"})
    
    print(f"   Всего документов: {total_docs}")
    print(f"   - wikisource_ru: {wikisource_docs}/{wikisource_target}")
    print(f"   - lib.ru: {libru_docs}/{libru_target}")
    
    total_queue = db.queue.count_documents({})
    pending_wiki = db.queue.count_documents({"source": "wikisource_ru", "status": "pending"})
    pending_libru = db.queue.count_documents({"source": "libru", "status": "pending"})
    
    print(f"\nОчередь: {total_queue} задач")
    print(f"   - wikisource_ru pending: {pending_wiki}")
    print(f"   - lib.ru pending: {pending_libru}")
    
    need_seed = total_queue < 1000  
    
    if need_seed:
        print("\n Выполняем seed...")
        
        for s_cfg in cfg["sources"]:
            if s_cfg["name"] == "wikisource_ru":
                s_cfg["priority"] = 1
            elif s_cfg["name"] == "libru":
                s_cfg["priority"] = 2
        
        for s_cfg in cfg["sources"]:
            seed_type = s_cfg.get("seed", {}).get("type")
            
            if seed_type == "mediawiki_api_allpages" and wikisource_docs < wikisource_target:
                print(f"  Добавляем URL из wikisource...")
                seed_wikisource_allpages(db, s_cfg, cfg["logic"]["user_agent"], 
                                        limit=wikisource_target * 2)
                
            elif seed_type == "url_list" and libru_docs < libru_target:
                print(f"  Добавляем URL из lib.ru...")
                seed_libru_initial(db, s_cfg)
    
    pending_wiki = db.queue.count_documents({"source": "wikisource_ru", "status": "pending"})
    pending_libru = db.queue.count_documents({"source": "libru", "status": "pending"})
    
    print(f"\n Готово к работе:")
    print(f"   - В очереди wikisource: {pending_wiki}")
    print(f"   - В очереди lib.ru: {pending_libru}")
    
    num_threads = cfg["logic"].get("threads", 1)
    print(f"\n Запускаем {num_threads} воркеров...")
    
    stop_event = threading.Event()
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        stats_list = [{} for _ in range(num_threads)]
        
        for i in range(num_threads):
            future = executor.submit(worker, i, cfg, db, stop_event, 
                                   stats_list[i], source_limits)
            futures.append(future)
        
        try:
            last_stats_time = time.time()
            start_time = time.time()
            
            while not all(f.done() for f in futures):
                current_time = time.time()
                
                if current_time - last_stats_time > 15:
                    wikisource_current = db.documents.count_documents({"source": "wikisource_ru"})
                    libru_current = db.documents.count_documents({"source": "libru"})
                    total_current = wikisource_current + libru_current
                    
                    pending_total = db.queue.count_documents({"status": "pending"})
                    done_total = db.queue.count_documents({"status": "done"})
                    
                    elapsed = current_time - start_time
                    docs_per_hour = (total_current / elapsed) * 3600 if elapsed > 0 else 0
                    
                    print(f"\n Прогресс [{datetime.now().strftime('%H:%M:%S')}]:")
                    print(f"   wikisource: {wikisource_current}/{wikisource_target} "
                          f"({wikisource_current/wikisource_target*100:.1f}%)")
                    print(f"   lib.ru: {libru_current}/{libru_target} "
                          f"({libru_current/libru_target*100:.1f}%)")
                    print(f"   Всего: {total_current}/{max_docs} "
                          f"({total_current/max_docs*100:.1f}%)")
                    print(f"   Очередь: {pending_total} pending, {done_total} done")
                    print(f"   Скорость: {docs_per_hour:.1f} док/час")
                    
                    last_stats_time = current_time
                
                wikisource_current = db.documents.count_documents({"source": "wikisource_ru"})
                libru_current = db.documents.count_documents({"source": "libru"})
                total_current = wikisource_current + libru_current
                
                if total_current >= max_docs:
                    print(f"\nЦЕЛЬ ДОСТИГНУТА: {total_current}/{max_docs} документов!")
                    stop_event.set()
                    break
                
                if (wikisource_current >= wikisource_target * 0.9 and 
                    libru_current < libru_target and 
                    db.queue.count_documents({"source": "libru", "status": "pending"}) < 1000):
                    
                    print("Добавляем дополнительные seed URL для lib.ru...")
                    for s_cfg in cfg["sources"]:
                        if s_cfg["name"] == "libru":
                            seed_libru_initial(db, s_cfg)
                
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("\n⏸ Пауза ...")
            stop_event.set()
            
            wikisource_current = db.documents.count_documents({"source": "wikisource_ru"})
            libru_current = db.documents.count_documents({"source": "libru"})
            total_current = wikisource_current + libru_current
            
            print(f"\nСостояние сохранено:")
            print(f"   wikisource: {wikisource_current}/{wikisource_target}")
            print(f"   lib.ru: {libru_current}/{libru_target}")
            print(f"   Всего: {total_current}/{max_docs}")
            print("\nДля продолжения просто запустите скрипт снова!")
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Ошибка воркера: {e}")
    
    print("\n" + "=" * 70)
    print("ФИНАЛЬНАЯ СТАТИСТИКА")
    print("=" * 70)
    
    total_docs = db.documents.count_documents({})
    wikisource_docs = db.documents.count_documents({"source": "wikisource_ru"})
    libru_docs = db.documents.count_documents({"source": "libru"})
    
    print(f"Документы:")
    print(f"   Всего: {total_docs}")
    print(f"   - wikisource_ru: {wikisource_docs} "
          f"({wikisource_docs/wikisource_target*100:.1f}% от цели)")
    print(f"   - lib.ru: {libru_docs} "
          f"({libru_docs/libru_target*100:.1f}% от цели)")
    
    total_queue = db.queue.count_documents({})
    queue_by_status = db.queue.aggregate([
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])
    
    print(f"\n Очередь: {total_queue} задач")
    for stat in queue_by_status:
        print(f"   - {stat['_id']}: {stat['count']}")
    
    queue_by_source = db.queue.aggregate([
        {"$group": {"_id": "$source", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])
    
    print(f"\nПо источникам в очереди:")
    for stat in queue_by_source:
        print(f"   - {stat['_id']}: {stat['count']}")
    
    print(f"\n Статистика воркеров:")
    total_stats = {}
    for i, stats in enumerate(stats_list):
        print(f"   Воркер {i}: {stats}")
        for key, value in stats.items():
            total_stats[key] = total_stats.get(key, 0) + value
    
    if total_stats:
        print(f"\n Общая статистика обработки:")
        for key in sorted(total_stats.keys()):
            print(f"   {key}: {total_stats[key]}")
    
    print("\nГотово!.")
    print("=" * 70)
    
    client.close()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    
    try:
        main(sys.argv[1])
    except Exception as e:
        print(f" ошибка: {e}")
        traceback.print_exc()
        sys.exit(1)