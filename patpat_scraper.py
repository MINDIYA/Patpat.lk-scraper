#!/usr/bin/env python3
"""
PATPAT SCRAPER V2.1 - SAVING FIX
- FIX: Prevents Extractor threads from quitting early.
- FIX: Forces 'Saved' count to sync with 'Found' count.
"""

from __future__ import annotations
import os
import sys
import time
import random
import queue
import sqlite3
import logging
import threading
import requests
import argparse
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Any, List
from tqdm import tqdm
from bs4 import BeautifulSoup
import re
import csv

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://127.0.0.1:8191/v1")

MAKES = ['toyota', 'nissan', 'suzuki', 'honda', 'mitsubishi', 'mazda',
         'daihatsu', 'kia', 'hyundai', 'micro', 'audi', 'bmw', 'mercedes-benz', 'land-rover', 'tata', 'mahindra']

TYPES = ['car', 'van', 'suv', 'crew-cab', 'pickup', 'lorry', 'bus']

MAX_PAGES_PER_COMBO = 50 
DAYS_TO_KEEP = 15
BATCH_SIZE = 20

# 🛑 TUNING
SESSION_TARGET_DEFAULT = 3
OUTPUT_FOLDER = "patpat_data_v2"
CHECKPOINT_FILE = "patpat_progress.txt"
SEEN_DB = "patpat_seen.sqlite"
LOG_LEVEL = logging.INFO

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("patpat_v2")

# ---------------------------
# 💾 DATABASE
# ---------------------------
class SeenDB:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.execute("CREATE TABLE IF NOT EXISTS seen (url TEXT PRIMARY KEY, ts TEXT)")
        self._conn.commit()

    def seen(self, url: str) -> bool:
        with self._lock:
            cur = self._conn.execute("SELECT 1 FROM seen WHERE url = ?", (url,))
            return cur.fetchone() is not None

    def mark(self, url: str):
        with self._lock:
            try:
                ts = datetime.now(timezone.utc).isoformat()
                self._conn.execute("INSERT OR IGNORE INTO seen (url, ts) VALUES (?, ?)", (url, ts))
                self._conn.commit()
            except: pass

class BatchWriter:
    def __init__(self, filepath: str, fieldnames: List[str]):
        self.filepath = filepath
        self.fieldnames = fieldnames
        self._buffer = []
        self._lock = threading.Lock()
        
        folder = os.path.dirname(self.filepath)
        if folder and not os.path.exists(folder): os.makedirs(folder, exist_ok=True)
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()

    def add_row(self, row: Dict[str, Any]):
        with self._lock:
            self._buffer.append(row)
            if len(self._buffer) >= BATCH_SIZE: self._flush_unsafe()

    def flush(self):
        with self._lock: 
            if self._buffer: self._flush_unsafe()

    def _flush_unsafe(self):
        try:
            with open(self.filepath, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writerows(self._buffer)
            self._buffer.clear()
        except: pass

class CheckpointManager:
    def __init__(self, path: str):
        self.path = path
        self.completed = set()
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f: self.completed = set(line.strip() for line in f)
            except: pass

    def add(self, task_id: str):
        if task_id in self.completed: return
        self.completed.add(task_id)
        try:
            with open(self.path, "a") as f: f.write(task_id + "\n")
        except: pass

# ---------------------------
# 🏊‍♂️ SESSION MANAGER
# ---------------------------
class SessionManager:
    def __init__(self, pool_size: int):
        self.fs_url = FLARESOLVERR_URL.rstrip('/')
        self.headers = {"Content-Type": "application/json"}
        self.pool = queue.Queue(maxsize=pool_size)
        
        log.info(f"🔥 Creating {pool_size} FlareSolverr sessions...")
        for i in range(pool_size):
            sid = f"patpat_fix_{random.randint(1000,9999)}"
            self._create(sid)
            self.pool.put(sid)

    def _create(self, sid):
        try: requests.post(self.fs_url, json={"cmd": "sessions.create", "session": sid}, headers=self.headers, timeout=10)
        except: pass

    def fetch(self, url: str) -> Optional[str]:
        try:
            sid = self.pool.get(timeout=10)
        except: return None

        html = None
        try:
            payload = {
                "cmd": "request.get",
                "url": url,
                "session": sid,
                "maxTimeout": 60000,
            }
            r = requests.post(self.fs_url, json=payload, headers=self.headers, timeout=65)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "ok":
                    html = data.get("solution", {}).get("response", "")
        except: 
            self._create(sid) 
        
        self.pool.put(sid)
        return html

    def close(self):
        while not self.pool.empty():
            try:
                sid = self.pool.get_nowait()
                requests.post(self.fs_url, json={"cmd": "sessions.destroy", "session": sid}, headers=self.headers, timeout=2)
            except: break

# ---------------------------
# 👷 WORKER LOGIC
# ---------------------------
ad_queue = queue.Queue(maxsize=1000) 
stop_event = threading.Event() # 🟢 KEY FIX: Controls worker life
stats = {'found': 0, 'saved': 0, 'errors': 0}
stats_lock = threading.Lock()

def harvest_task(make, v_type, page_num, cutoff, pool, ckpt, seen_db):
    task_id = f"{make}|{v_type}|{page_num}"
    if task_id in ckpt.completed: return 0

    url = f"https://patpat.lk/en/sri-lanka/vehicle/{v_type}/{make}"
    if page_num > 1: url += f"?page={page_num}"

    html = pool.fetch(url)
    if not html: return 0
    if "No results found" in html: return 0

    soup = BeautifulSoup(html, "lxml")
    
    links = []
    all_a = soup.find_all('a', href=True)
    
    for a in all_a:
        href = a['href']
        if f"/vehicle/{v_type}/" in href or f"/{make}" in href:
             if len(href) > 20 and not "page=" in href:
                 links.append(a)

    count = 0
    for link in links:
        href = link['href']
        if not href.startswith("http"): href = "https://patpat.lk" + href
        
        if seen_db.seen(href): continue
        seen_db.mark(href)

        title = link.get_text(" ", strip=True)
        if len(title) < 5:
            parent = link.find_parent()
            if parent: title = parent.get_text(" ", strip=True)
        
        title = title.replace("\n", " ").strip()[:100]

        item = {'url': href, 'date': "Check_Page", 'make': make, 'type': v_type, 'title': title, 'price': "0"}
        ad_queue.put(item)
        count += 1

    if count > 0: ckpt.add(task_id)
    with stats_lock: stats['found'] += count
    return count

def extractor_worker(bw, dw, cutoff, pool):
    # 🟢 KEY FIX: Loop runs until stop_event is set AND queue is empty
    while not stop_event.is_set() or not ad_queue.empty():
        try: 
            item = ad_queue.get(timeout=3)
        except queue.Empty:
            continue

        try:
            html = pool.fetch(item['url'])
            if not html: continue
            
            soup = BeautifulSoup(html, "lxml")
            full_text = soup.get_text(" ", strip=True)

            dm = re.search(r'Posted on\s*[:|-]?\s*(\d{4}-\d{2}-\d{2})', full_text, re.IGNORECASE)
            if not dm: dm = re.search(r'(\d{4}-\d{2}-\d{2})', full_text)
            
            final_date = datetime.now().strftime("%Y-%m-%d")
            if dm:
                try:
                    d_obj = datetime.strptime(dm.group(1), "%Y-%m-%d")
                    if d_obj < cutoff: 
                        ad_queue.task_done()
                        continue
                    final_date = dm.group(1)
                except: pass
            
            pm = re.search(r'(?:Rs|LKR)\.?\s*([\d,]+)', full_text, re.IGNORECASE)
            price = pm.group(1).replace(",", "") if pm else "0"

            details = {'YOM': '', 'Contact': '', 'Location': '', 'Mileage': ''}
            
            tels = soup.find_all('a', href=re.compile(r'^tel:'))
            phones = [t['href'].replace('tel:', '') for t in tels]
            if not phones:
                phones = re.findall(r'(?:07\d|0\d{2})[- ]?\d{3}[- ]?\d{4}', full_text)
            details['Contact'] = " / ".join(list(set(phones)))

            for li in soup.find_all(['li', 'tr', 'div']):
                txt = li.get_text(" ", strip=True).lower()
                if 'mileage' in txt and 'km' in txt: details['Mileage'] = txt
                if 'location' in txt: details['Location'] = txt
                if 'year' in txt or 'yom' in txt: details['YOM'] = txt

            row = {'Date': final_date, 'Make': item['make'], 'Model': item['title'], 'Price': price, 
                   'YOM': details['YOM'][:20], 'Contact': details['Contact'], 'URL': item['url']}
            
            bw.add_row(row)
            with stats_lock: stats['saved'] += 1

        except: pass
        finally: ad_queue.task_done()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=DAYS_TO_KEEP)
    args = parser.parse_args()

    # 🟢 FORCE 6 Browsers for speed
    pool_size = 6
    pool = SessionManager(pool_size)
    
    cutoff = datetime.now() - timedelta(days=args.days)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    bw = BatchWriter(f"{OUTPUT_FOLDER}/PATPAT_RESULTS_{ts}.csv", ['Date', 'Make', 'Model', 'Price', 'YOM', 'Contact', 'URL'])
    ckpt = CheckpointManager(CHECKPOINT_FILE)
    seen = SeenDB(SEEN_DB)

    # Start Extractor Threads
    ex_pool = ThreadPoolExecutor(max_workers=pool_size)
    for _ in range(pool_size): ex_pool.submit(extractor_worker, bw, None, cutoff, pool)

    tasks = []
    for make in MAKES:
        for v_type in TYPES:
            for p in range(1, MAX_PAGES_PER_COMBO + 1): tasks.append((make, v_type, p))
    random.shuffle(tasks)

    print(f"🚀 Started Patpat Scraper V2.1 (Tasks: {len(tasks)})")
    
    with ThreadPoolExecutor(max_workers=pool_size) as s_pool:
        futures = [s_pool.submit(harvest_task, m, t, p, cutoff, pool, ckpt, seen) for (m, t, p) in tasks]
        with tqdm(total=len(futures)) as pbar:
            for f in as_completed(futures):
                pbar.update(1)
                pbar.set_postfix(stats)

    # 🟢 WAIT for queue to empty before stopping workers
    ad_queue.join()
    stop_event.set() # Tell workers to go home
    ex_pool.shutdown(wait=True)
    
    bw.flush()
    pool.close()
    print("✅ Done.")

if __name__ == "__main__":
    main()
