#!/usr/bin/env python3
"""
RSS Reader — painel moderno (similar ao Readwise Reader)
Versão em Python com FastAPI + SQLite + Feedparser + APScheduler + Tailwind (UI)

Como rodar:
  1) Salve como app.py
  2) python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
  3) pip install fastapi uvicorn feedparser apscheduler bleach
  4) python app.py
  5) Abra http://localhost:8000

Variáveis de ambiente opcionais:
  REFRESH_MINUTES=15
  SEED_FEEDS="https://www.theverge.com/rss/index.xml\nhttps://hnrss.org/frontpage"
"""
import os
import re
import time
import hashlib
import sqlite3
import json
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

import feedparser
from urllib.request import Request, urlopen
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import bleach

PORT = int(os.getenv("PORT", 8000))
REFRESH_MINUTES = int(os.getenv("REFRESH_MINUTES", 15))
DEFAULT_FEEDS = [u for u in (os.getenv("SEED_FEEDS", """
https://www.theverge.com/rss/index.xml
https://www.wired.com/feed/rss
https://feeds.feedburner.com/TechCrunch/
https://hnrss.org/frontpage
""").strip().splitlines()) if u.strip()]

DB_PATH = "rss.db"

app = FastAPI(title="RSS Reader")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------
# DB helpers
# ------------------------------

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS feeds (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT UNIQUE NOT NULL,
          title TEXT,
          site_url TEXT,
          last_checked INTEGER
        );
        CREATE TABLE IF NOT EXISTS items (
          id TEXT PRIMARY KEY,
          feed_id INTEGER NOT NULL,
          title TEXT,
          link TEXT,
          author TEXT,
          pub_date INTEGER,
          content TEXT,
          summary TEXT,
          image TEXT,
          read INTEGER DEFAULT 0,
          starred INTEGER DEFAULT 0,
          created_at INTEGER DEFAULT (strftime('%s','now')),
          FOREIGN KEY(feed_id) REFERENCES feeds(id)
        );
        CREATE INDEX IF NOT EXISTS idx_items_feed ON items(feed_id);
        CREATE INDEX IF NOT EXISTS idx_items_pub ON items(pub_date DESC);
        CREATE INDEX IF NOT EXISTS idx_items_read ON items(read);
        CREATE INDEX IF NOT EXISTS idx_items_star ON items(starred);
        """
    )
    conn.commit()
    conn.close()


# ------------------------------
# RSS helpers
# ------------------------------

ALLOWED_TAGS = bleach.sanitizer.ALLOWED_TAGS.union(
    {"img", "figure", "figcaption"}
)
ALLOWED_ATTRS = {
    **bleach.sanitizer.ALLOWED_ATTRIBUTES,
    "a": ["href", "title", "target", "rel"],
    "img": ["src", "alt", "title", "loading"],
}


def sanitize_html(html: str) -> str:
    if not html:
        return ""
    return bleach.clean(
        html,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRS,
        protocols=["http", "https", "data", "mailto"],
        strip=True,
    )


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


IMG_RE = re.compile(r"<img[^>]+src=['\"]([^'\"]+)['\"]", re.I)
FEED_LINK_RE = re.compile(
    r'<link[^>]+type=["\']application/(?:rss|atom)\+xml["\'][^>]*>|'
    r'<link[^>]+type=["\']application/(?:json|feed\+json)["\'][^>]*>',
    re.I,
)
HREF_RE = re.compile(r"href=['\"]([^'\"]+)['\"]", re.I)


def extract_image(entry, content_html: str) -> Optional[str]:
    # media:content
    media = entry.get("media_content") or entry.get("media_thumbnail")
    if isinstance(media, list) and media:
        u = media[0].get("url")
        if u:
            return u
    # enclosure
    enclosures = entry.get("enclosures")
    if isinstance(enclosures, list) and enclosures:
        u = enclosures[0].get("href") or enclosures[0].get("url")
        if u:
            return u
    # html
    m = IMG_RE.search(content_html or "")
    return m.group(1) if m else None


def to_ts(struct_time) -> int:
    if not struct_time:
        return int(time.time())
    try:
        return int(time.mktime(struct_time))
    except Exception:
        return int(time.time())


def parse_iso_ts(s: Optional[str]) -> int:
    if not s:
        return int(time.time())
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return int(time.time())


def upsert_feed(conn, url: str, title: str, site_url: Optional[str]):
    conn.execute(
        """
        INSERT INTO feeds(url, title, site_url, last_checked)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET title=excluded.title, site_url=excluded.site_url, last_checked=excluded.last_checked
        """,
        (url, title, site_url, int(time.time())),
    )


def get_feed_by_url(conn, url: str):
    cur = conn.execute("SELECT * FROM feeds WHERE url=?", (url,))
    return cur.fetchone()


def get_feed_by_id(conn, fid: int):
    cur = conn.execute("SELECT * FROM feeds WHERE id=?", (fid,))
    return cur.fetchone()


def insert_item(conn, data: dict):
    conn.execute(
        """
        INSERT OR IGNORE INTO items(
          id, feed_id, title, link, author, pub_date, content, summary, image
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["id"],
            data["feed_id"],
            data.get("title"),
            data.get("link"),
            data.get("author"),
            data.get("pub_date"),
            data.get("content"),
            data.get("summary"),
            data.get("image"),
        ),
    )


def parse_feed_url(feed_url: str):
    parsed = feedparser.parse(feed_url)
    if parsed.feed and parsed.entries:
        ftype = "atom" if "atom" in (parsed.version or "").lower() else "rss"
        title = parsed.feed.get("title", feed_url)
        return {"url": feed_url, "title": title, "type": ftype, "parsed": parsed}
    try:
        req = Request(feed_url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=10) as resp:
            data = resp.read()
        js = json.loads(data)
        if isinstance(js, dict) and js.get("items"):
            title = js.get("title", feed_url)
            return {"url": feed_url, "title": title, "type": "json", "parsed": js}
    except Exception:
        pass
    return None


def discover_feeds(url: str):
    candidate = parse_feed_url(url)
    if candidate:
        return [candidate]
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", "ignore")
            base_url = resp.geturl()
    except Exception:
        raise ValueError("Feed não encontrado")
    feeds = []
    for tag in FEED_LINK_RE.findall(html):
        href_match = HREF_RE.search(tag)
        if not href_match:
            continue
        feed_url = urljoin(base_url, href_match.group(1))
        parsed = parse_feed_url(feed_url)
        if parsed:
            feeds.append(parsed)
    if not feeds:
        raise ValueError("Feed não encontrado")
    return feeds


def discover_feed_url(url: str):
    feeds = discover_feeds(url)
    if feeds:
        f = feeds[0]
        return f["url"], f["parsed"], f["type"]
    raise ValueError("Feed não encontrado")


def fetch_feed(url: str):
    feed_url, parsed, ftype = discover_feed_url(url)
    conn = get_db()
    try:
        if ftype == "json":
            title = parsed.get("title", feed_url)
            site_url = parsed.get("home_page_url") or url
            upsert_feed(conn, feed_url, title, site_url)
            feed_row = get_feed_by_url(conn, feed_url)
            for e in parsed.get("items", []):
                guid = e.get("id") or e.get("url") or (e.get("title", "") + (e.get("date_published", "") or ""))
                item_id = sha1(guid)
                content_html = e.get("content_html") or e.get("content_text") or ""
                content_clean = sanitize_html(content_html)
                summary = (bleach.clean(e.get("summary") or e.get("content_text") or "", strip=True) or "")[:500]
                ts = parse_iso_ts(e.get("date_published") or e.get("date_modified"))
                author = e.get("author", "")
                if isinstance(author, dict):
                    author = author.get("name", "")
                image = e.get("image")
                insert_item(
                    conn,
                    {
                        "id": item_id,
                        "feed_id": feed_row["id"],
                        "title": e.get("title") or "(sem título)",
                        "link": e.get("url", "#"),
                        "author": author,
                        "pub_date": ts,
                        "content": content_clean,
                        "summary": summary,
                        "image": image,
                    },
                )
        else:
            title = parsed.feed.get("title", feed_url)
            site_url = parsed.feed.get("link") or url
            upsert_feed(conn, feed_url, title, site_url)
            feed_row = get_feed_by_url(conn, feed_url)
            for e in parsed.entries:
                guid = e.get("id") or e.get("guid") or e.get("link") or (e.get("title", "") + str(e.get("published", "")))
                item_id = sha1(guid)
                content_html = ""
                if e.get("content") and isinstance(e.get("content"), list):
                    content_html = e["content"][0].get("value", "")
                else:
                    content_html = e.get("summary", "")
                content_clean = sanitize_html(content_html)
                summary = (bleach.clean(e.get("summary", ""), strip=True) or "")[:500]
                ts = to_ts(e.get("published_parsed") or e.get("updated_parsed"))
                image = extract_image(e, content_html)
                insert_item(
                    conn,
                    {
                        "id": item_id,
                        "feed_id": feed_row["id"],
                        "title": e.get("title") or "(sem título)",
                        "link": e.get("link", "#"),
                        "author": e.get("author", ""),
                        "pub_date": ts,
                        "content": content_clean,
                        "summary": summary,
                        "image": image,
                    },
                )
        conn.commit()
    finally:
        conn.close()


def refresh_all_feeds():
    conn = get_db()
    try:
        feeds = conn.execute("SELECT * FROM feeds").fetchall()
    finally:
        conn.close()
    for f in feeds:
        try:
            fetch_feed(f["url"])  # noqa
            print("✓ Atualizado:", f["url"])  # noqa
        except Exception as e:
            print("✗ Falha ao atualizar", f["url"], str(e))  # noqa


scheduler = BackgroundScheduler(daemon=True)


def seed_if_empty():
    conn = get_db()
    try:
        c = conn.execute("SELECT COUNT(1) FROM feeds").fetchone()[0]
        if c == 0:
            print("Sem feeds. Inserindo alguns de exemplo…")
            for u in DEFAULT_FEEDS:
                try:
                    fetch_feed(u)
                except Exception as e:
                    print("Falha no seed", u, str(e))
    finally:
        conn.close()


# ------------------------------
# API
# ------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML


@app.get("/api/feeds")
async def api_feeds():
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT f.*, (
              SELECT COUNT(1) FROM items i WHERE i.feed_id=f.id AND i.read=0
            ) as unread_count
            FROM feeds f
            ORDER BY title COLLATE NOCASE ASC
            """
        ).fetchall()
        return JSONResponse([dict(r) for r in rows])
    finally:
        conn.close()


@app.get("/api/discover")
async def api_discover(url: str):
    try:
        feeds = discover_feeds(url)
        return JSONResponse([{ "url": f["url"], "title": f["title"], "type": f["type"] } for f in feeds])
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@app.post("/api/feeds")
async def api_add_feed(payload: dict):
    url = (payload or {}).get("url")
    if not url:
        return JSONResponse({"error": "Informe url"}, status_code=400)
    try:
        fetch_feed(url)
        return JSONResponse({"ok": True})
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/refresh")
async def api_refresh():
    # dispara atualização em background (rápido)
    scheduler.add_job(refresh_all_feeds)
    return JSONResponse({"ok": True})


@app.get("/api/items")
async def api_items(
    feed_id: Optional[int] = None,
    only_unread: int = 0,
    only_starred: int = 0,
    q: str = "",
    limit: int = 30,
    offset: int = 0,
):
    limit = min(max(1, limit), 100)
    conn = get_db()
    try:
        sql_where = ["1=1"]
        params = []
        if feed_id is not None:
            sql_where.append("feed_id = ?")
            params.append(feed_id)
        if only_unread:
            sql_where.append("read = 0")
        if only_starred:
            sql_where.append("starred = 1")
        if q:
            sql_where.append("(title LIKE ? OR summary LIKE ? OR content LIKE ?)")
            like = f"%{q}%"
            params.extend([like, like, like])
        where = " AND ".join(sql_where)
        items = conn.execute(
            f"SELECT * FROM items WHERE {where} ORDER BY pub_date DESC, created_at DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
        total = conn.execute(
            f"SELECT COUNT(1) FROM items WHERE {where}",
            (*params,),
        ).fetchone()[0]
        return JSONResponse({"items": [dict(i) for i in items], "total": total})
    finally:
        conn.close()


@app.get("/api/items/{item_id}")
async def api_item(item_id: str):
    conn = get_db()
    try:
        it = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if not it:
            return JSONResponse({"error": "Item não encontrado"}, status_code=404)
        return JSONResponse(dict(it))
    finally:
        conn.close()


@app.post("/api/items/{item_id}/read")
async def api_mark_read(item_id: str, payload: Optional[dict] = None):
    read = 1
    if payload and "read" in payload:
        read = 1 if payload["read"] else 0
    conn = get_db()
    try:
        cur = conn.execute("UPDATE items SET read=? WHERE id=?", (read, item_id))
        conn.commit()
        return JSONResponse({"ok": True, "changes": cur.rowcount})
    finally:
        conn.close()


@app.post("/api/items/{item_id}/star")
async def api_toggle_star(item_id: str):
    conn = get_db()
    try:
        cur = conn.execute(
            "UPDATE items SET starred=CASE WHEN starred=1 THEN 0 ELSE 1 END WHERE id=?",
            (item_id,),
        )
        conn.commit()
        return JSONResponse({"ok": True, "changes": cur.rowcount})
    finally:
        conn.close()


# ------------------------------
# UI (SPA em HTML inline)
# ------------------------------
HTML = """<!doctype html>
<html lang=\"pt-br\" class=\"h-full\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>RSS Reader — Painel</title>
  <script src=\"https://cdn.tailwindcss.com\"></script>
  <script>tailwind.config = { darkMode: 'class' };</script>
  <link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">
  <link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>
  <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap\" rel=\"stylesheet\">
  <style>
    html { font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, 'Helvetica Neue', Arial; }
    .line-clamp-2 { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
    .scroll-thin { scrollbar-width: thin; }
  </style>
  <script src=\"https://unpkg.com/lucide@latest\"></script>
</head>
<body class=\"h-full bg-gray-50 text-gray-900 dark:bg-gray-950 dark:text-gray-100\">
  <div id=\"app\" class=\"h-full grid grid-rows-[auto,1fr]\">
    <header class=\"sticky top-0 z-20 backdrop-blur supports-[backdrop-filter]:bg-white/70 dark:supports-[backdrop-filter]:bg-gray-900/70 border-b border-gray-200 dark:border-gray-800\">
      <div class=\"mx-auto max-w-[1400px] px-4 py-3 flex items-center gap-3\">
        <div class=\"flex items-center gap-2\">
          <div class=\"w-8 h-8 rounded-xl bg-gradient-to-br from-violet-500 to-fuchsia-500\"></div>
          <span class=\"font-semibold\">RSS Reader</span>
          <span class=\"text-xs text-gray-500\">python</span>
        </div>
        <div class=\"flex-1\"></div>
        <div class=\"relative w-full max-w-xl\">
          <input id=\"search\" placeholder=\"Buscar…\" class=\"w-full rounded-2xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 px-4 py-2 pl-10 focus:outline-none focus:ring-2 focus:ring-violet-500\" />
          <div class=\"absolute left-3 top-1/2 -translate-y-1/2 text-gray-400\" data-lucide=\"search\"></div>
        </div>
        <button id=\"refreshBtn\" class=\"ml-3 rounded-xl border border-gray-200 dark:border-gray-800 px-3 py-2 hover:bg-gray-100 dark:hover:bg-gray-800 flex items-center gap-2\">
          <i data-lucide=\"rotate-cw\" class=\"w-4 h-4\"></i><span class=\"hidden sm:inline\">Atualizar</span>
        </button>
        <button id=\"themeBtn\" class=\"ml-2 rounded-xl border border-gray-200 dark:border-gray-800 px-3 py-2 hover:bg-gray-100 dark:hover:bg-gray-800\" title=\"Tema claro/escuro\">
          <i data-lucide=\"moon\" class=\"w-4 h-4\"></i>
        </button>
      </div>
    </header>

    <main class=\"mx-auto max-w-[1400px] w-full grid grid-cols-12 gap-4 p-4\">
      <aside class=\"col-span-12 md:col-span-3 lg:col-span-2 space-y-4\">
        <div class=\"rounded-2xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-3\">
          <div class=\"flex items-center justify-between mb-2\">
            <h3 class=\"font-semibold\">Feeds</h3>
            <button id=\"addFeedBtn\" class=\"text-violet-600 hover:text-violet-700\">+ Adicionar</button>
          </div>
          <nav id=\"feedList\" class=\"space-y-1 max-h-[60vh] overflow-auto scroll-thin pr-1\"></nav>
        </div>
        <div class=\"rounded-2xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-3\">
          <h3 class=\"font-semibold mb-2\">Filtros</h3>
          <div class=\"flex flex-col gap-1 text-sm\">
            <label class=\"inline-flex items-center gap-2 cursor-pointer\">
              <input type=\"radio\" name=\"filter\" value=\"all\" class=\"accent-violet-600\" checked>
              <span>Todos</span>
            </label>
            <label class=\"inline-flex items-center gap-2 cursor-pointer\">
              <input type=\"radio\" name=\"filter\" value=\"unread\" class=\"accent-violet-600\">
              <span>Não lidos</span>
            </label>
            <label class=\"inline-flex items-center gap-2 cursor-pointer\">
              <input type=\"radio\" name=\"filter\" value=\"starred\" class=\"accent-violet-600\">
              <span>Favoritos</span>
            </label>
          </div>
        </div>
      </aside>

      <section class=\"col-span-12 md:col-span-5 lg:col-span-5\">
        <div class=\"rounded-2xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900\">
          <div class=\"flex items-center justify-between p-3 border-b border-gray-200 dark:border-gray-800\">
            <h3 class=\"font-semibold\">Artigos</h3>
            <div class=\"flex items-center gap-2\">
              <select id=\"sortSel\" class=\"rounded-xl border border-gray-200 dark:border-gray-800 bg-transparent px-3 py-1 text-sm\">
                <option value=\"new\">Mais recentes</option>
                <option value=\"old\">Mais antigos</option>
              </select>
              <button id=\"markAllReadBtn\" class=\"text-xs rounded-lg px-3 py-1 border border-gray-200 dark:border-gray-800 hover:bg-gray-100 dark:hover:bg-gray-800\">Marcar página como lida</button>
            </div>
          </div>
          <div id=\"itemList\" class=\"divide-y divide-gray-200 dark:divide-gray-800 max-h-[75vh] overflow-auto scroll-thin\"></div>
          <div class=\"p-3 flex justify-center\">
            <button id=\"loadMoreBtn\" class=\"rounded-xl border border-gray-200 dark:border-gray-800 px-4 py-2 hover:bg-gray-100 dark:hover:bg-gray-800\">Carregar mais</button>
          </div>
        </div>
      </section>

      <section class=\"col-span-12 md:col-span-4 lg:col-span-5\">
        <article id=\"reader\" class=\"rounded-2xl border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-5 min-h-[60vh]\">
          <div class=\"text-center text-gray-500\">Selecione um artigo para ler</div>
        </article>
      </section>
    </main>
  </div>

  <div id=\"addFeedModal\" class=\"fixed inset-0 bg-black/50 hidden items-center justify-center\">
    <div class=\"bg-white dark:bg-gray-900 rounded-xl p-4 w-full max-w-md\">
      <h3 class=\"font-semibold mb-2\">Adicionar Feed</h3>
      <input id=\"addFeedInput\" placeholder=\"URL da página\" class=\"w-full rounded-xl border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-800 px-3 py-2 mb-2\" />
      <div id=\"feedOptions\" class=\"max-h-60 overflow-auto space-y-1 text-sm\"></div>
      <div class=\"text-right mt-3\">
        <button id=\"closeAddFeed\" class=\"px-3 py-1 rounded-xl border border-gray-300 dark:border-gray-700\">Fechar</button>
      </div>
    </div>
  </div>

  <script>
    const themeBtn = document.getElementById('themeBtn');
    const userPref = localStorage.getItem('theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    if (userPref === 'dark') document.documentElement.classList.add('dark');
    themeBtn.addEventListener('click', () => {
      document.documentElement.classList.toggle('dark');
      localStorage.setItem('theme', document.documentElement.classList.contains('dark') ? 'dark' : 'light');
    });

    let state = { feeds: [], selectedFeed: null, filter: 'all', q: '', items: [], offset: 0, limit: 30, selectedItem: null, sort: 'new' };
    const feedList = document.getElementById('feedList');
    const itemList = document.getElementById('itemList');
    const reader = document.getElementById('reader');

    function fmtDate(ts){ if(!ts) return ''; const d=new Date(ts*1000); return d.toLocaleString(); }
    function icon(name, extra='w-4 h-4'){ return `<i data-lucide="${name}" class="${extra}"></i>`; }

    function renderFeeds(){
      feedList.innerHTML='';
      const allBtn = document.createElement('button');
      allBtn.className='w-full flex items-center justify-between rounded-xl px-3 py-2 hover:bg-gray-100 dark:hover:bg-gray-800';
      allBtn.innerHTML=`<span class="flex items-center gap-2">${icon('inbox')} Todos</span>`;
      allBtn.onclick=()=>{ state.selectedFeed=null; state.offset=0; loadItems(true); highlightSelectedFeed(null); };
      feedList.appendChild(allBtn);

      state.feeds.forEach(f=>{
        const b=document.createElement('button');
        b.className='w-full flex items-center justify-between rounded-xl px-3 py-2 hover:bg-gray-100 dark:hover:bg-gray-800';
        b.dataset.id=f.id;
        b.innerHTML=`<span class="flex items-center gap-2">${icon('rss')} <span class="truncate max-w-[140px]" title="${f.title||f.url}">${f.title||f.url}</span></span>`+
                     `<span class="text-xs rounded-full bg-gray-100 dark:bg-gray-800 px-2 py-0.5">${f.unread_count}</span>`;
        b.onclick=()=>{ state.selectedFeed=f.id; state.offset=0; loadItems(true); highlightSelectedFeed(f.id); };
        feedList.appendChild(b);
      });
      lucide.createIcons();
      highlightSelectedFeed(state.selectedFeed);
    }

    function highlightSelectedFeed(id){
      [...feedList.children].forEach(el=>{
        if(!el.dataset) return;
        if((id===null && !el.dataset.id) || (String(el.dataset.id||'')===String(id||''))) el.classList.add('bg-gray-100','dark:bg-gray-800');
        else el.classList.remove('bg-gray-100','dark:bg-gray-800');
      });
    }

    function renderItems(append=false){
      if(!append) itemList.innerHTML='';
      state.items.forEach(it=>{
        const card=document.createElement('article');
        card.className='p-4 hover:bg-gray-50 dark:hover:bg-gray-800 cursor-pointer';
        card.innerHTML=`
          <div class="flex items-start gap-3">
            <div class="flex-1 min-w-0">
              <div class="flex items-center gap-2">
                <h4 class="font-semibold ${it.read?'text-gray-500':''} truncate">${it.title||'(sem título)'}</h4>
                ${it.starred?`<span class="text-amber-500">${icon('star','w-4 h-4 fill-current')}</span>`:''}
              </div>
              <div class="text-xs text-gray-500 flex items-center gap-2 mt-1">
                <span>${fmtDate(it.pub_date)}</span>
                <span>•</span>
                <a class="hover:underline" href="${it.link}" target="_blank">Abrir fonte</a>
              </div>
              <p class="text-sm text-gray-600 dark:text-gray-300 line-clamp-2 mt-2">${(it.summary||'').replace(/</g,'&lt;')}</p>
            </div>
            ${it.image?`<img src="${it.image}" class="w-20 h-20 rounded-xl object-cover hidden sm:block"/>`:''}
          </div>`;
        card.onclick=()=>openItem(it.id);
        itemList.appendChild(card);
      });
      lucide.createIcons();
    }

    async function openItem(id){
      const r=await fetch('/api/items/'+id); const it=await r.json();
      state.selectedItem=it;
      const buttons=`<div class="flex items-center gap-2">
        <button id="toggleStarBtn" class="rounded-xl border border-gray-200 dark:border-gray-800 px-3 py-2 hover:bg-gray-100 dark:hover:bg-gray-800 flex items-center gap-2">${icon('star')} Favoritar</button>
        <a href="${it.link}" target="_blank" class="rounded-xl border border-gray-200 dark:border-gray-800 px-3 py-2 hover:bg-gray-100 dark:hover:bg-gray-800 flex items-center gap-2">${icon('external-link')} Abrir</a>
      </div>`;
      reader.innerHTML=`
        <header class="mb-4">
          <h1 class="text-2xl font-semibold leading-tight">${it.title||'(sem título)'}</h1>
          <div class="text-sm text-gray-500 flex items-center gap-2 mt-1">
            <span>${fmtDate(it.pub_date)}</span>
            ${it.author?`<span>•</span><span>${it.author}</span>`:''}
          </div>
          <div class="mt-3">${buttons}</div>
        </header>
        <div class="prose prose-neutral dark:prose-invert max-w-none">${it.content||'<p><em>Sem conteúdo.</em></p>'}</div>`;
      lucide.createIcons();
      fetch('/api/items/'+id+'/read',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({read:1})}).then(()=>{
        const itLocal=state.items.find(x=>x.id===id); if(itLocal) itLocal.read=1; renderItems();
      });
      document.getElementById('toggleStarBtn').onclick=async()=>{
        await fetch('/api/items/'+id+'/star',{method:'POST'});
        const itLocal=state.items.find(x=>x.id===id); if(itLocal) itLocal.starred=itLocal.starred?0:1; openItem(id);
      };
    }

    async function loadFeeds(){ const r=await fetch('/api/feeds'); state.feeds=await r.json(); renderFeeds(); }
    async function loadItems(reset=false){
      if(reset){ state.items=[]; state.offset=0; itemList.innerHTML=''; }
      const params=new URLSearchParams();
      if(state.selectedFeed) params.set('feed_id', state.selectedFeed);
      if(state.filter==='unread') params.set('only_unread','1');
      if(state.filter==='starred') params.set('only_starred','1');
      if(state.q) params.set('q', state.q);
      params.set('limit', state.limit); params.set('offset', state.offset);
      let r=await fetch('/api/items?'+params.toString()); let data=await r.json();
      let arr=data.items||[]; if(state.sort==='old') arr=arr.slice().reverse();
      state.items=state.items.concat(arr); state.offset+=state.limit; renderItems(true);
    }

    const addFeedModal=document.getElementById('addFeedModal');
    const addFeedInput=document.getElementById('addFeedInput');
    const feedOptions=document.getElementById('feedOptions');
    const closeAddFeed=document.getElementById('closeAddFeed');
    document.getElementById('addFeedBtn').onclick=()=>{ addFeedModal.classList.remove('hidden'); addFeedModal.classList.add('flex'); addFeedInput.value=''; feedOptions.innerHTML=''; addFeedInput.focus(); };
    closeAddFeed.onclick=()=>{ addFeedModal.classList.add('hidden'); addFeedModal.classList.remove('flex'); };
    async function discoverFeeds(url){
      feedOptions.innerHTML='<div class="text-gray-500 text-sm">Buscando...</div>';
      try{
        const resp=await fetch('/api/discover?url='+encodeURIComponent(url));
        if(!resp.ok){ feedOptions.innerHTML='<div class="text-sm text-red-500">Nenhum feed encontrado</div>'; return; }
        const feeds=await resp.json();
        if(!feeds.length){ feedOptions.innerHTML='<div class="text-sm text-red-500">Nenhum feed encontrado</div>'; return; }
        feedOptions.innerHTML='';
        feeds.forEach(f=>{
          const b=document.createElement('button');
          b.className='w-full text-left px-3 py-2 rounded-xl hover:bg-gray-100 dark:hover:bg-gray-800';
          b.innerHTML=`<div class="font-medium">${f.title}</div><div class="text-xs text-gray-500">${f.type.toUpperCase()} • ${f.url}</div>`;
          b.onclick=async()=>{ const resp=await fetch('/api/feeds',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:f.url})}); if(resp.ok){ closeAddFeed.onclick(); await loadFeeds(); await loadItems(true); } else { alert('Erro ao adicionar feed'); } };
          feedOptions.appendChild(b);
        });
      }catch(e){ feedOptions.innerHTML='<div class="text-sm text-red-500">Erro</div>'; }
    }
    addFeedInput.addEventListener('input',e=>{ const url=e.target.value.trim(); if(url) discoverFeeds(url); else feedOptions.innerHTML=''; });
    document.getElementById('refreshBtn').onclick=async()=>{
      document.getElementById('refreshBtn').classList.add('animate-pulse');
      await fetch('/api/refresh',{method:'POST'});
      await new Promise(r=>setTimeout(r,1200));
      await loadFeeds(); await loadItems(true);
      document.getElementById('refreshBtn').classList.remove('animate-pulse');
    };
    document.getElementById('search').addEventListener('input',(e)=>{ state.q=e.target.value.trim(); state.offset=0; loadItems(true); });
    document.getElementById('loadMoreBtn').onclick=()=>loadItems(false);
    document.getElementById('markAllReadBtn').onclick=async()=>{
      const ids=state.items.map(it=>it.id);
      await Promise.all(ids.map(id=>fetch('/api/items/'+id+'/read',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({read:1})})));
      loadItems(true); loadFeeds();
    };
    document.querySelectorAll('input[name="filter"]').forEach(radio=>{
      radio.addEventListener('change',()=>{ state.filter=radio.value; state.offset=0; loadItems(true); });
    });
    document.getElementById('sortSel').addEventListener('change',(e)=>{ state.sort=e.target.value; renderItems(); });

    (async function init(){ await loadFeeds(); await loadItems(true); lucide.createIcons(); })();
  </script>
</body>
</html>"""


if __name__ == "__main__":
    init_db()
    seed_if_empty()
    # scheduler
    scheduler.start()
    scheduler.add_job(refresh_all_feeds, 'interval', minutes=REFRESH_MINUTES, next_run_time=None)
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
