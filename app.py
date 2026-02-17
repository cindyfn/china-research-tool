import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from io import BytesIO

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request, send_file
from openai import OpenAI
from fpdf import FPDF

load_dotenv()

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "history.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT DEFAULT '',
            chinese_text TEXT NOT NULL,
            english_text TEXT NOT NULL,
            summary TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            highlights_json TEXT DEFAULT '{}',
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS folders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT DEFAULT '',
            credibility_tier TEXT DEFAULT '',
            language TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT DEFAULT '',
            client_name_cn TEXT DEFAULT '',
            client_name_en TEXT DEFAULT '',
            industry TEXT DEFAULT '',
            status TEXT DEFAULT 'Active',
            notes TEXT DEFAULT '',
            due_by TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    # Add columns to existing tables (migration-safe)
    migrations = [
        "ALTER TABLE history ADD COLUMN summary TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN folder_id INTEGER DEFAULT NULL",
        "ALTER TABLE history ADD COLUMN position INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE history ADD COLUMN title TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN source_id INTEGER DEFAULT NULL",
        "ALTER TABLE history ADD COLUMN project_id INTEGER DEFAULT NULL",
        "ALTER TABLE history ADD COLUMN scraped_source_name TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN pub_date TEXT DEFAULT ''",
        "ALTER TABLE projects ADD COLUMN due_by TEXT DEFAULT ''",
        "ALTER TABLE projects ADD COLUMN project_name TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN article_title_en TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN source_name_en TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN author TEXT DEFAULT ''",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()
    conn.close()


init_db()

client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url="https://api.deepseek.com",
)


def _extract_infzm_content_id(url):
    """Extract infzm.com content ID from various URL formats."""
    # /wap/#/content/123 (SPA)
    m = re.search(r'infzm\.com/wap/?#/content/(\d+)', url)
    if m:
        return m.group(1)
    # /contents/123 (server-rendered)
    m = re.search(r'infzm\.com/contents/(\d+)', url)
    if m:
        return m.group(1)
    return None


def _fetch_infzm_article(content_id):
    """Fetch article from infzm.com's mobile API. Returns dict or None."""
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        resp = requests.get(
            f"https://api.infzm.com/mobile/contents/{content_id}",
            headers=headers, timeout=15,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        c = data.get("data", {}).get("content", {})
        if not c:
            return None

        fulltext_html = c.get("fulltext", "")
        introtext = c.get("introtext", "")

        # Parse HTML content
        from bs4 import BeautifulSoup
        text = ""
        if fulltext_html:
            soup = BeautifulSoup(fulltext_html, "html.parser")
            text = soup.get_text(separator="\n", strip=True)

        # Check if paywalled (truncated)
        word_count = c.get("word_count", 0)
        pay_prop = c.get("pay_property", {}) or {}
        is_paid = pay_prop.get("mode") in ("meterage", "pay")
        if is_paid and word_count and len(text) < word_count * 0.5:
            text += f"\n\n[Note: This article is behind a paywall. Only a preview (~{len(text)} of ~{word_count} characters) is available.]"

        pub_date = (c.get("publish_time") or "")[:10]

        return {
            "text": _clean_article_text(text) if text else "",
            "title": c.get("subject", ""),
            "source_name": "南方周末",
            "author": c.get("author", ""),
            "pub_date": pub_date,
        }
    except Exception:
        return None


def fetch_article_from_url(url):
    """Fetch and extract text content + metadata from a Chinese article URL.

    Returns a dict: {text, title, source_name, pub_date}
    """
    # Site-specific handlers
    infzm_id = _extract_infzm_content_id(url)
    if infzm_id:
        result = _fetch_infzm_article(infzm_id)
        if result and result.get("text"):
            return result
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.encoding = resp.apparent_encoding
    soup = BeautifulSoup(resp.text, "html.parser")

    # --- Strategy 1: Extract from embedded JSON (Next.js, nuxt, etc.) ---
    result = _try_extract_json_content(soup)
    if result:
        # result is now a dict with text + metadata
        return result

    # --- Strategy 2: HTML-based extraction ---
    # Extract metadata from HTML before stripping tags
    meta = _extract_html_metadata(soup)

    # Remove non-content elements
    for tag in soup(["nav", "header", "footer", "aside",
                      "noscript", "iframe", "svg", "form", "button"]):
        tag.decompose()

    # Remove boilerplate by role (collect first to avoid mutation during iteration)
    to_remove = [
        el for el in soup.find_all(attrs={"role": True})
        if el.attrs and el.attrs.get("role") in (
            "navigation", "banner", "complementary", "contentinfo")
    ]
    for el in to_remove:
        el.decompose()

    # Remove elements whose class token exactly matches boilerplate names
    boilerplate_tokens = {
        "nav", "navbar", "menu", "sidebar", "footer", "header", "banner",
        "breadcrumb", "comments", "share", "social", "related", "recommend",
        "ad", "ads", "advertisement", "app-download", "copyright",
        "disclaimer", "legal", "privacy", "feedback", "login", "signup",
    }
    to_remove = [
        el for el in soup.find_all(attrs={"class": True})
        if {c.lower() for c in (el.get("class") or [])} & boilerplate_tokens
    ]
    for el in to_remove:
        el.decompose()
    to_remove = [
        el for el in soup.find_all(attrs={"id": True})
        if (el.get("id") or "").lower() in boilerplate_tokens
    ]
    for el in to_remove:
        el.decompose()

    # Find article container
    container = soup.find("article")

    if not container:
        selectors = [
            {"class_": "article-content"}, {"class_": "article_content"},
            {"class_": "news_txt"}, {"class_": "news-content"},
            {"class_": "post_body"}, {"class_": "post_text"},
            {"id": "artibody"}, {"id": "article"},
        ]
        for sel in selectors:
            container = soup.find(**sel)
            if container:
                break

    # Match hashed class names like "cententWrap__xxx"
    if not container:
        content_class_re = re.compile(
            r"(?:content|article|news.?(?:txt|body|detail)|"
            r"centet|text.?wrap|post.?body).*__",
            re.IGNORECASE,
        )
        for div in soup.find_all("div", class_=True):
            classes = " ".join(div.get("class", []))
            if content_class_re.search(classes):
                container = div
                break

    # Fallback: narrowest div with substantial Chinese text
    if not container:
        candidates = []
        for div in soup.find_all("div"):
            txt = div.get_text(strip=True)
            cn_chars = sum(1 for c in txt if '\u4e00' <= c <= '\u9fff')
            if cn_chars > 100:
                candidates.append((div, cn_chars, len(txt)))
        if candidates:
            max_cn = max(c[1] for c in candidates)
            threshold = max_cn * 0.6
            candidates = [c for c in candidates if c[1] >= threshold]
            candidates.sort(key=lambda c: c[2])
            container = candidates[0][0]

    if container:
        raw = container.get_text(separator="\n", strip=True)
    else:
        raw = soup.get_text(separator="\n", strip=True)

    return {
        "text": _clean_article_text(raw),
        "title": meta.get("title", ""),
        "source_name": meta.get("source_name", ""),
        "author": meta.get("author", ""),
        "pub_date": meta.get("pub_date", ""),
    }


def _extract_html_metadata(soup):
    """Extract title, publication name, and date from HTML meta tags."""
    meta = {}

    # Title: og:title > <title> tag
    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content", "").strip():
        meta["title"] = og_title["content"].strip()
    else:
        title_tag = soup.find("title")
        if title_tag:
            raw = title_tag.get_text(strip=True)
            # Strip common suffixes like " - 新浪网" or "_腾讯新闻"
            raw = re.split(r'\s*[_\-|–—]\s*(?!.*[_\-|–—])', raw, maxsplit=1)[0].strip()
            if raw:
                meta["title"] = raw

    # Source/publication: og:site_name > meta[name=source/publisher]
    og_site = soup.find("meta", property="og:site_name")
    if og_site and og_site.get("content", "").strip():
        meta["source_name"] = og_site["content"].strip()
    else:
        for attr in ("source", "publisher"):
            tag = soup.find("meta", attrs={"name": attr})
            if tag and tag.get("content", "").strip():
                meta["source_name"] = tag["content"].strip()
                break

    # Author: meta[name=author] > article:author
    for attr in ("author",):
        tag = soup.find("meta", attrs={"name": attr})
        if tag and tag.get("content", "").strip():
            meta["author"] = tag["content"].strip()
            break
    if "author" not in meta:
        tag = soup.find("meta", property="article:author")
        if tag and tag.get("content", "").strip():
            meta["author"] = tag["content"].strip()

    # Date: article:published_time > meta[name=publishdate]
    for prop in ("article:published_time", "og:article:published_time"):
        tag = soup.find("meta", property=prop)
        if tag and tag.get("content", "").strip():
            meta["pub_date"] = tag["content"].strip()[:10]
            break
    if "pub_date" not in meta:
        for name in ("publishdate", "publish_date", "date", "PubDate"):
            tag = soup.find("meta", attrs={"name": name})
            if tag and tag.get("content", "").strip():
                meta["pub_date"] = tag["content"].strip()[:10]
                break

    return meta


def _try_extract_json_content(soup):
    """Extract article from embedded JSON data (__NEXT_DATA__, etc.).

    Returns a dict {text, title, source_name, pub_date} or None.
    """
    # Next.js sites (thepaper.cn, etc.)
    script = soup.find("script", id="__NEXT_DATA__")
    if script and script.string:
        try:
            data = json.loads(script.string)
            return _extract_from_nextdata(data)
        except (json.JSONDecodeError, KeyError):
            pass

    # Generic: look for large JSON blobs in script tags
    for script in soup.find_all("script"):
        txt = script.string or ""
        if "contentDetail" in txt or "articleBody" in txt:
            # Try to find JSON object
            match = re.search(r'\{.*"content(?:Detail|_detail)".*\}', txt,
                              re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group())
                    body = (data.get("contentDetail", {}).get("content")
                            or data.get("content_detail", {}).get("content")
                            or "")
                    if body:
                        inner = BeautifulSoup(body, "html.parser")
                        return {
                            "text": _clean_article_text(
                                inner.get_text(separator="\n", strip=True)),
                            "title": "",
                            "source_name": "",
                            "author": "",
                            "pub_date": "",
                        }
                except (json.JSONDecodeError, AttributeError):
                    pass
    return None


def _extract_from_nextdata(data):
    """Walk __NEXT_DATA__ to find article content.

    Returns a dict {text, title, source_name, pub_date} or None.
    """
    props = data.get("props", {}).get("pageProps", {})

    # The Paper (thepaper.cn)
    detail = props.get("detailData", {}).get("contentDetail", {})
    if detail:
        title = detail.get("name", "")
        author = (detail.get("author") or "").strip()
        source = (detail.get("source") or "").strip()
        pub_time = (detail.get("pubTimeLong") or detail.get("pubTime") or "")
        content_html = detail.get("content", "")
        inner = BeautifulSoup(content_html, "html.parser")
        body = inner.get_text(separator="\n", strip=True)

        # Build text with metadata header
        parts = []
        if title:
            parts.append(title)
        meta_line = " | ".join(filter(None, [author, source, str(pub_time)]))
        if meta_line:
            parts.append(meta_line)
        if body:
            parts.append(body)

        # Parse pub_date to YYYY-MM-DD if possible
        pub_date = ""
        if pub_time:
            pub_str = str(pub_time).strip()
            date_match = re.match(r"(\d{4}[-/]\d{1,2}[-/]\d{1,2})", pub_str)
            if date_match:
                pub_date = date_match.group(1).replace("/", "-")

        return {
            "text": "\n".join(parts),
            "title": title,
            "source_name": source or "",
            "author": author or "",
            "pub_date": pub_date,
        }

    # Generic Next.js: look for common article fields
    for key in ("article", "post", "news", "detail", "data"):
        obj = props.get(key, {})
        if isinstance(obj, dict):
            content = (obj.get("content") or obj.get("body")
                       or obj.get("text") or "")
            if content and len(content) > 50:
                inner = BeautifulSoup(content, "html.parser")
                title = obj.get("title", "")
                body = inner.get_text(separator="\n", strip=True)
                return {
                    "text": (title + "\n" + body) if title else body,
                    "title": title,
                    "source_name": obj.get("source", "") or "",
                    "author": obj.get("author", "") or "",
                    "pub_date": (obj.get("publishTime", "") or obj.get("pubDate", "") or "")[:10],
                }
    return None


def _clean_article_text(raw):
    """Remove footer boilerplate and UI junk from extracted text."""
    footer_markers = [
        "特别声明", "免责声明", "版权声明", "责任编辑",
        "原标题：", "阅读原文", "返回搜狐", "举报/反馈",
        "扫码下载", "下载客户端", "关于我们", "联系我们",
        "©", "ICP备", "ICP证", "京公网安备", "沪公网安备",
    ]

    lines = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        # Stop at footer boilerplate
        if any(marker in line for marker in footer_markers):
            break
        # Skip very short non-Chinese lines (UI remnants like "+1")
        if len(line) <= 4 and not any('\u4e00' <= c <= '\u9fff' for c in line):
            continue
        lines.append(line)

    return "\n".join(lines)


def translate_chinese_to_english(text):
    """Translate Chinese text to English using DeepSeek API."""
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a professional Chinese-to-English translator "
                    "specializing in news articles, legal documents, and "
                    "adverse media reports. Translate the following Chinese "
                    "text into clear, accurate English. Preserve paragraph "
                    "structure. Do not add commentary — only output the "
                    "translation."
                ),
            },
            {"role": "user", "content": text},
        ],
        temperature=0.3,
    )
    return response.choices[0].message.content


def generate_summary(english_text):
    """Generate an executive summary of the translated article using DeepSeek."""
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an analyst producing concise executive summaries of "
                    "adverse media articles for compliance and due-diligence teams. "
                    "Given the English translation of a Chinese article, produce a "
                    "structured summary in this exact format:\n\n"
                    "OVERVIEW: 2-3 sentence summary of the article.\n\n"
                    "KEY ENTITIES: Bullet list of people, companies, or organizations mentioned. "
                    "Write each name in both Chinese and English, e.g. '张三 (Zhang San)' or '新华社 (Xinhua News Agency)'.\n\n"
                    "KEY CLAIMS: Bullet list of the main allegations, findings, or events.\n\n"
                    "KEY DATES: Bullet list of any significant dates mentioned.\n\n"
                    "Use plain text with bullet points (- ). Be concise and factual. "
                    "Do not add commentary beyond what is in the article."
                ),
            },
            {"role": "user", "content": english_text},
        ],
        temperature=0.3,
    )
    return response.choices[0].message.content


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search")
def search_history():
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify([])
    conn = get_db()
    like = f"%{q}%"
    rows = conn.execute(
        "SELECT id, url, title, chinese_text, summary, folder_id, project_id, pub_date, created_at "
        "FROM history WHERE title LIKE ? OR summary LIKE ? OR chinese_text LIKE ? OR english_text LIKE ? "
        "ORDER BY created_at DESC LIMIT 50",
        (like, like, like, like),
    ).fetchall()
    conn.close()
    return jsonify([{
        "id": r["id"],
        "url": r["url"],
        "title": r["title"] or "",
        "preview": r["chinese_text"][:80],
        "folder_id": r["folder_id"],
        "project_id": r["project_id"],
        "pub_date": r["pub_date"] or "",
        "created_at": r["created_at"],
    } for r in rows])


@app.route("/history/check-url")
def check_duplicate_url():
    url = (request.args.get("url") or "").strip()
    if not url:
        return jsonify({"exists": False})
    conn = get_db()
    row = conn.execute(
        "SELECT id, title, created_at FROM history WHERE url = ? LIMIT 1", (url,)
    ).fetchone()
    conn.close()
    if row:
        return jsonify({
            "exists": True,
            "id": row["id"],
            "title": row["title"] or "",
            "created_at": row["created_at"],
        })
    return jsonify({"exists": False})


@app.route("/translate", methods=["POST"])
def translate():
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid request. Please try again."}), 400
    input_text = (data.get("text") or "").strip()
    input_url = (data.get("url") or "").strip()
    project_id = data.get("project_id")  # None, 0, or positive int

    if not input_text and not input_url:
        return jsonify({"error": "Please provide text or a URL."}), 400

    try:
        # Get source text
        article_title = ""
        article_source_name = ""
        article_author = ""
        article_pub_date = ""
        if input_url:
            result = fetch_article_from_url(input_url)
            if isinstance(result, dict):
                chinese_text = result.get("text", "")
                article_title = result.get("title", "")
                article_source_name = result.get("source_name", "")
                article_author = result.get("author", "")
                article_pub_date = result.get("pub_date", "")
            else:
                chinese_text = result
        else:
            chinese_text = input_text

        if not chinese_text:
            return jsonify({"error": "No text content found."}), 400

        # Translate
        english_text = translate_chinese_to_english(chinese_text)

        # Generate executive summary
        summary = generate_summary(english_text)

        # Auto-save to history (prepend to Unfiled)
        conn = get_db()
        conn.execute(
            "UPDATE history SET position = position + 1 WHERE folder_id IS NULL"
        )
        cur = conn.execute(
            "INSERT INTO history (url, chinese_text, english_text, summary, title, scraped_source_name, pub_date, author, folder_id, position, project_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, ?, ?)",
            (input_url, chinese_text, english_text, summary, article_title, article_source_name, article_pub_date, article_author, project_id, datetime.now(timezone.utc).isoformat()),
        )
        history_id = cur.lastrowid
        conn.commit()
        conn.close()

        return jsonify({
            "id": history_id,
            "chinese": chinese_text,
            "english": english_text,
            "summary": summary,
            "project_id": project_id,
            "title": article_title,
            "source_name": article_source_name,
            "author": article_author,
            "pub_date": article_pub_date,
        })

    except requests.RequestException as e:
        return jsonify({"error": f"Failed to fetch URL: {e}"}), 400
    except Exception as e:
        return jsonify({"error": f"Translation failed: {e}"}), 500


@app.route("/history")
def history_list():
    conn = get_db()
    rows = conn.execute(
        "SELECT id, url, chinese_text, title, folder_id, position, project_id, created_at FROM history ORDER BY folder_id, position"
    ).fetchall()
    conn.close()
    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "url": r["url"],
            "title": r["title"] or "",
            "preview": r["chinese_text"][:80],
            "folder_id": r["folder_id"],
            "position": r["position"],
            "project_id": r["project_id"],
            "created_at": r["created_at"],
        })
    return jsonify(items)


@app.route("/history/<int:entry_id>")
def history_get(entry_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM history WHERE id = ?", (entry_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify({
        "id": row["id"],
        "url": row["url"],
        "title": row["title"] or "",
        "article_title_en": row["article_title_en"] or "",
        "chinese_text": row["chinese_text"],
        "english_text": row["english_text"],
        "summary": row["summary"] or "",
        "notes": row["notes"],
        "highlights_json": row["highlights_json"],
        "source_id": row["source_id"],
        "project_id": row["project_id"],
        "scraped_source_name": row["scraped_source_name"] or "",
        "source_name_en": row["source_name_en"] or "",
        "author": row["author"] or "",
        "pub_date": row["pub_date"] or "",
        "created_at": row["created_at"],
    })


@app.route("/history/<int:entry_id>", methods=["PUT"])
def history_update(entry_id):
    data = request.get_json(force=True, silent=True) or {}
    conn = get_db()
    row = conn.execute("SELECT id FROM history WHERE id = ?", (entry_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    conn.execute(
        "UPDATE history SET notes = ?, highlights_json = ? WHERE id = ?",
        (data.get("notes", ""), data.get("highlights_json", "{}"), entry_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/history/<int:entry_id>/rename", methods=["PUT"])
def history_rename(entry_id):
    data = request.get_json(force=True, silent=True) or {}
    title = (data.get("title") or "").strip()
    conn = get_db()
    row = conn.execute("SELECT id FROM history WHERE id = ?", (entry_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    conn.execute("UPDATE history SET title = ? WHERE id = ?", (title, entry_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/history/<int:entry_id>", methods=["DELETE"])
def history_delete(entry_id):
    conn = get_db()
    conn.execute("DELETE FROM history WHERE id = ?", (entry_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/folders")
def folders_list():
    conn = get_db()
    folders = conn.execute("SELECT * FROM folders ORDER BY position").fetchall()
    # Get entry counts per folder
    counts = {}
    for row in conn.execute("SELECT folder_id, COUNT(*) as cnt FROM history WHERE folder_id IS NOT NULL GROUP BY folder_id").fetchall():
        counts[row["folder_id"]] = row["cnt"]
    conn.close()
    return jsonify([{
        "id": f["id"],
        "name": f["name"],
        "position": f["position"],
        "entry_count": counts.get(f["id"], 0),
    } for f in folders])


@app.route("/folders", methods=["POST"])
def folder_create():
    data = request.get_json(force=True, silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Name is required"}), 400
    conn = get_db()
    max_pos = conn.execute("SELECT COALESCE(MAX(position), -1) FROM folders").fetchone()[0]
    cur = conn.execute("INSERT INTO folders (name, position) VALUES (?, ?)", (name, max_pos + 1))
    folder_id = cur.lastrowid
    conn.commit()
    conn.close()
    return jsonify({"id": folder_id, "name": name, "position": max_pos + 1})


@app.route("/folders/<int:folder_id>", methods=["PUT"])
def folder_update(folder_id):
    data = request.get_json(force=True, silent=True) or {}
    conn = get_db()
    row = conn.execute("SELECT id FROM folders WHERE id = ?", (folder_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    updates = []
    params = []
    if "name" in data:
        updates.append("name = ?")
        params.append(data["name"])
    if "position" in data:
        updates.append("position = ?")
        params.append(data["position"])
    if updates:
        params.append(folder_id)
        conn.execute(f"UPDATE folders SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/folders/<int:folder_id>", methods=["DELETE"])
def folder_delete(folder_id):
    conn = get_db()
    # Move entries back to unfiled
    conn.execute("UPDATE history SET folder_id = NULL WHERE folder_id = ?", (folder_id,))
    conn.execute("DELETE FROM folders WHERE id = ?", (folder_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/folders/reorder", methods=["PUT"])
def folders_reorder():
    data = request.get_json(force=True, silent=True) or {}
    order = data.get("order", [])
    conn = get_db()
    for pos, fid in enumerate(order):
        conn.execute("UPDATE folders SET position = ? WHERE id = ?", (pos, fid))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/history/<int:entry_id>/move", methods=["PUT"])
def history_move(entry_id):
    data = request.get_json(force=True, silent=True) or {}
    folder_id = data.get("folder_id")  # None = unfiled
    position = data.get("position", 0)
    conn = get_db()
    row = conn.execute("SELECT id, folder_id FROM history WHERE id = ?", (entry_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    # Shift positions in target folder to make room
    if folder_id is None:
        conn.execute(
            "UPDATE history SET position = position + 1 WHERE folder_id IS NULL AND position >= ?",
            (position,),
        )
    else:
        conn.execute(
            "UPDATE history SET position = position + 1 WHERE folder_id = ? AND position >= ?",
            (folder_id, position),
        )
    conn.execute(
        "UPDATE history SET folder_id = ?, position = ? WHERE id = ?",
        (folder_id, position, entry_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/sources")
def sources_list():
    conn = get_db()
    rows = conn.execute("SELECT * FROM sources ORDER BY name").fetchall()
    # Get article counts per source
    counts = {}
    for row in conn.execute(
        "SELECT source_id, COUNT(*) as cnt FROM history WHERE source_id IS NOT NULL GROUP BY source_id"
    ).fetchall():
        counts[row["source_id"]] = row["cnt"]
    conn.close()
    return jsonify([{
        "id": r["id"],
        "name": r["name"],
        "type": r["type"],
        "credibility_tier": r["credibility_tier"],
        "language": r["language"],
        "notes": r["notes"],
        "article_count": counts.get(r["id"], 0),
        "created_at": r["created_at"],
    } for r in rows])


@app.route("/sources", methods=["POST"])
def source_create():
    data = request.get_json(force=True, silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "Name is required"}), 400
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO sources (name, type, credibility_tier, language, notes, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (
            name,
            (data.get("type") or "").strip(),
            (data.get("credibility_tier") or "").strip(),
            (data.get("language") or "").strip(),
            (data.get("notes") or "").strip(),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    source_id = cur.lastrowid
    conn.commit()
    conn.close()
    return jsonify({"id": source_id, "name": name})


@app.route("/sources/<int:source_id>", methods=["PUT"])
def source_update(source_id):
    data = request.get_json(force=True, silent=True) or {}
    conn = get_db()
    row = conn.execute("SELECT id FROM sources WHERE id = ?", (source_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    updates = []
    params = []
    for field in ("name", "type", "credibility_tier", "language", "notes"):
        if field in data:
            updates.append(f"{field} = ?")
            params.append((data[field] or "").strip())
    if updates:
        params.append(source_id)
        conn.execute(f"UPDATE sources SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/sources/<int:source_id>", methods=["DELETE"])
def source_delete(source_id):
    conn = get_db()
    conn.execute("UPDATE history SET source_id = NULL WHERE source_id = ?", (source_id,))
    conn.execute("DELETE FROM sources WHERE id = ?", (source_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/history/<int:entry_id>/source", methods=["PUT"])
def history_set_source(entry_id):
    data = request.get_json(force=True, silent=True) or {}
    source_id = data.get("source_id")  # int or None
    conn = get_db()
    row = conn.execute("SELECT id FROM history WHERE id = ?", (entry_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    conn.execute("UPDATE history SET source_id = ? WHERE id = ?", (source_id, entry_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/history/<int:entry_id>/metadata", methods=["PUT"])
def history_update_metadata(entry_id):
    data = request.get_json(force=True, silent=True) or {}
    conn = get_db()
    row = conn.execute("SELECT id FROM history WHERE id = ?", (entry_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    updates = []
    params = []
    for field in ("title", "article_title_en", "scraped_source_name", "source_name_en", "author", "pub_date", "url"):
        if field in data:
            updates.append(f"{field} = ?")
            params.append((data[field] or "").strip())
    if updates:
        params.append(entry_id)
        conn.execute(f"UPDATE history SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/projects")
def projects_list():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM projects ORDER BY CASE WHEN due_by IS NULL OR due_by = '' THEN 1 ELSE 0 END, due_by ASC, created_at DESC"
    ).fetchall()
    counts = {}
    for row in conn.execute(
        "SELECT project_id, COUNT(*) as cnt FROM history WHERE project_id IS NOT NULL AND project_id > 0 GROUP BY project_id"
    ).fetchall():
        counts[row["project_id"]] = row["cnt"]
    conn.close()
    return jsonify([{
        "id": r["id"],
        "project_name": r["project_name"] or "",
        "client_name_cn": r["client_name_cn"],
        "client_name_en": r["client_name_en"],
        "industry": r["industry"],
        "status": r["status"],
        "notes": r["notes"],
        "due_by": r["due_by"] or "",
        "article_count": counts.get(r["id"], 0),
        "created_at": r["created_at"],
    } for r in rows])


@app.route("/projects", methods=["POST"])
def project_create():
    data = request.get_json(force=True, silent=True) or {}
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO projects (project_name, client_name_cn, client_name_en, industry, status, notes, due_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            (data.get("project_name") or "").strip(),
            (data.get("client_name_cn") or "").strip(),
            (data.get("client_name_en") or "").strip(),
            (data.get("industry") or "").strip(),
            (data.get("status") or "Active").strip(),
            (data.get("notes") or "").strip(),
            (data.get("due_by") or "").strip(),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    project_id = cur.lastrowid
    conn.commit()
    conn.close()
    return jsonify({"id": project_id})


@app.route("/projects/<int:project_id>")
def project_get(project_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Not found"}), 404
    return jsonify({
        "id": row["id"],
        "project_name": row["project_name"] or "",
        "client_name_cn": row["client_name_cn"],
        "client_name_en": row["client_name_en"],
        "industry": row["industry"],
        "status": row["status"],
        "notes": row["notes"],
        "due_by": row["due_by"] or "",
        "created_at": row["created_at"],
    })


@app.route("/projects/<int:project_id>", methods=["PUT"])
def project_update(project_id):
    data = request.get_json(force=True, silent=True) or {}
    conn = get_db()
    row = conn.execute("SELECT id FROM projects WHERE id = ?", (project_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    updates = []
    params = []
    for field in ("project_name", "client_name_cn", "client_name_en", "industry", "status", "notes", "due_by"):
        if field in data:
            updates.append(f"{field} = ?")
            params.append((data[field] or "").strip())
    if updates:
        params.append(project_id)
        conn.execute(f"UPDATE projects SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/projects/<int:project_id>", methods=["DELETE"])
def project_delete(project_id):
    conn = get_db()
    # Move linked articles to unfiled (project_id = 0)
    conn.execute("UPDATE history SET project_id = 0 WHERE project_id = ?", (project_id,))
    conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/projects/<int:project_id>/articles")
def project_articles(project_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT id, url, chinese_text, english_text, summary, title, source_id, highlights_json, pub_date, created_at FROM history WHERE project_id = ? ORDER BY created_at DESC",
        (project_id,),
    ).fetchall()
    conn.close()
    items = []
    for r in rows:
        hl_count = 0
        try:
            hl_data = json.loads(r["highlights_json"] or "{}")
            for panel_html in hl_data.values():
                hl_count += panel_html.count("<mark")
        except (json.JSONDecodeError, AttributeError):
            pass
        items.append({
            "id": r["id"],
            "url": r["url"],
            "title": r["title"] or "",
            "preview": r["chinese_text"][:80],
            "summary": r["summary"] or "",
            "source_id": r["source_id"],
            "highlight_count": hl_count,
            "pub_date": r["pub_date"] or "",
            "created_at": r["created_at"],
        })
    return jsonify(items)


@app.route("/projects/<int:project_id>/entities")
def project_entities(project_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT summary FROM history WHERE project_id = ? AND summary IS NOT NULL AND summary != ''",
        (project_id,),
    ).fetchall()
    conn.close()
    summaries = [r["summary"] for r in rows]
    if not summaries:
        return jsonify([])

    combined = "\n\n".join(f"Article {i+1}:\n{s}" for i, s in enumerate(summaries))
    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": (
                    "You are an entity extraction assistant. Extract all key entities (people, companies, organizations, government bodies, locations, laws/regulations) "
                    "from the provided article summaries. For each entity, count how many DISTINCT articles it appears in. "
                    "Return ONLY a JSON array sorted by count descending, like: [{\"entity\": \"Name\", \"type\": \"person\", \"count\": 3}, ...]. "
                    "Use these types: person, company, organization, government, location, regulation, other. "
                    "Include both Chinese and English names if both appear — combine them as one entry using the format 'English Name (中文名)'. "
                    "Do not include generic terms. Only return the JSON array, no other text."
                )},
                {"role": "user", "content": combined},
            ],
            max_tokens=2000,
        )
        raw = resp.choices[0].message.content.strip()
        # Extract JSON from response (handle markdown code blocks)
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        entities = json.loads(raw)
        return jsonify(entities)
    except Exception as e:
        print(f"Entity extraction error: {e}")
        return jsonify({"error": "Entity extraction failed"}), 500


@app.route("/history/<int:entry_id>/project", methods=["PUT"])
def history_set_project(entry_id):
    data = request.get_json(force=True, silent=True) or {}
    project_id = data.get("project_id")  # int, 0, or None
    conn = get_db()
    row = conn.execute("SELECT id FROM history WHERE id = ?", (entry_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    conn.execute("UPDATE history SET project_id = ? WHERE id = ?", (project_id, entry_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/unfiled")
def unfiled_articles():
    conn = get_db()
    rows = conn.execute(
        "SELECT id, url, chinese_text, title, summary, created_at FROM history WHERE project_id IS NULL OR project_id = 0 ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return jsonify([{
        "id": r["id"],
        "url": r["url"],
        "title": r["title"] or "",
        "preview": r["chinese_text"][:80],
        "summary": r["summary"] or "",
        "created_at": r["created_at"],
    } for r in rows])


def _build_citation(style, title, source_name, date, url):
    """Generate a citation string in the given style."""
    pub = source_name or "Unknown Source"
    title = title or "(Untitled Article)"
    date = date or "n.d."
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if style == "footnote":
        parts = f'"{title}," {pub} (Chinese), {date}'
        if url:
            parts += f", {url}"
        return parts + f" [Accessed: {today}]"
    elif style == "short":
        parts = f'{pub} (CN), {date} — "{title}"'
        if url:
            parts += f"\n{url}"
        return parts
    else:  # inline (default)
        parts = f'Source: {pub}, "{title}", {date}.'
        if url:
            parts += f"\nAvailable at: {url}"
        return parts


@app.route("/projects/<int:project_id>/export-pdf", methods=["POST"])
def project_export_pdf(project_id):
    data = request.get_json(force=True, silent=True) or {}
    citation_style = data.get("citation_style", "inline")

    conn = get_db()
    project = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    if not project:
        conn.close()
        return jsonify({"error": "Not found"}), 404

    articles = conn.execute(
        "SELECT id, url, title, summary, source_id, pub_date, created_at FROM history WHERE project_id = ? ORDER BY created_at",
        (project_id,),
    ).fetchall()

    # Build source lookup
    source_ids = set(a["source_id"] for a in articles if a["source_id"])
    sources = {}
    if source_ids:
        placeholders = ",".join("?" * len(source_ids))
        for s in conn.execute(f"SELECT * FROM sources WHERE id IN ({placeholders})", list(source_ids)).fetchall():
            sources[s["id"]] = dict(s)
    conn.close()

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    cjk_font = None
    if CJK_FONT_PATH:
        pdf.add_font("CJK", "", CJK_FONT_PATH, uni=True)
        pdf.add_font("CJK", "B", CJK_FONT_PATH, uni=True)
        cjk_font = "CJK"

    # Title — project name + "Report"
    proj_title = project["project_name"] or project["client_name_en"] or project["client_name_cn"] or "Project"
    report_title = f"{proj_title} Report"
    _pdf_set_font(pdf, cjk_font, "B", 18, report_title)
    pdf.set_text_color(26, 26, 26)
    pdf.multi_cell(0, 12, report_title)
    pdf.set_draw_color(74, 144, 217)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)

    # Date
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(136, 136, 136)
    pdf.cell(0, PDF_LINE_H_SMALL, f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d')}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    # Client Profile
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(74, 144, 217)
    pdf.cell(0, 8, "Client Profile", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_text_color(51, 51, 51)

    profile_lines = []
    if project["project_name"]:
        profile_lines.append(f"Project: {project['project_name']}")
    if project["client_name_en"]:
        profile_lines.append(f"Client (EN): {project['client_name_en']}")
    if project["client_name_cn"]:
        profile_lines.append(f"Name (CN): {project['client_name_cn']}")
    if project["industry"]:
        profile_lines.append(f"Industry: {project['industry']}")
    if project["status"]:
        profile_lines.append(f"Status: {project['status']}")
    if project["due_by"]:
        profile_lines.append(f"Due by: {project['due_by']}")
    if project["notes"]:
        profile_lines.append(f"Notes: {project['notes']}")

    pdf.set_fill_color(245, 248, 255)
    profile_text = "\n".join(profile_lines)
    _pdf_set_font(pdf, cjk_font, "", 10, profile_text)
    pdf.multi_cell(0, PDF_LINE_H, profile_text, fill=True)
    pdf.ln(6)

    # Timeline (sorted by pub_date, fallback to created_at)
    if articles:
        sorted_articles = sorted(
            articles,
            key=lambda a: (a["pub_date"] or a["created_at"] or "")[:10],
        )
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_text_color(74, 144, 217)
        pdf.cell(0, 8, "Chronological Timeline", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        pdf.set_text_color(51, 51, 51)
        for a in sorted_articles:
            date_str = (a["pub_date"] or a["created_at"] or "")[:10]
            title = a["title"] or a["url"] or "(untitled)"
            _pdf_set_font(pdf, cjk_font, "", 10, title)
            pdf.cell(0, PDF_LINE_H, f"{date_str}  -  {title}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(6)

    # Article Details
    if articles:
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_text_color(74, 144, 217)
        pdf.cell(0, 8, "Article Details", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        for a in articles:
            title = a["title"] or a["url"] or "(untitled)"
            _pdf_set_font(pdf, cjk_font, "B", 11, title)
            pdf.set_text_color(26, 26, 26)
            pdf.cell(0, PDF_LINE_H, title, new_x="LMARGIN", new_y="NEXT")

            pdf.set_text_color(136, 136, 136)
            meta_parts = [a["created_at"][:10] if a["created_at"] else ""]
            if a["source_id"] and a["source_id"] in sources:
                s = sources[a["source_id"]]
                meta_parts.append(f"{s['name']} ({s.get('credibility_tier', '')})")
            meta_text = " | ".join(filter(None, meta_parts))
            _pdf_set_font(pdf, cjk_font, "", 9, meta_text)
            pdf.cell(0, PDF_LINE_H_SMALL, meta_text, new_x="LMARGIN", new_y="NEXT")

            if a["summary"]:
                pdf.set_text_color(51, 51, 51)
                pdf.set_fill_color(245, 248, 255)
                _pdf_write_paragraphs(pdf, a["summary"], cjk_font, fill=True)

            # Citation
            source_name = ""
            if a["source_id"] and a["source_id"] in sources:
                source_name = sources[a["source_id"]]["name"]
            cit = _build_citation(
                citation_style,
                a["title"] or "",
                source_name,
                a["created_at"][:10] if a["created_at"] else "",
                a["url"] or "",
            )
            pdf.ln(2)
            _pdf_set_font(pdf, cjk_font, "", 9, cit)
            pdf.set_text_color(100, 100, 100)
            pdf.multi_cell(0, PDF_LINE_H_SMALL, cit)
            pdf.ln(4)

    buf = BytesIO(pdf.output())
    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"project-report-{project_id}-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.pdf",
    )


def _find_cjk_font():
    """Find a CJK-capable TTF font on macOS."""
    candidates = [
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
        "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


CJK_FONT_PATH = _find_cjk_font()

PDF_LINE_H = 6
PDF_LINE_H_SMALL = 5


def _has_cjk(text):
    """Check if text contains CJK characters."""
    return any('\u4e00' <= c <= '\u9fff' for c in text)


def _pdf_set_font(pdf, cjk_font, style, size, text=""):
    """Set font to Helvetica for English or CJK font for Chinese text."""
    if cjk_font and _has_cjk(text):
        pdf.set_font(cjk_font, style, size)
    else:
        pdf.set_font("Helvetica", style, size)


def _pdf_write_paragraphs(pdf, text, cjk_font, size=10, line_h=PDF_LINE_H, fill=False):
    """Write text with clean paragraph breaks. Each paragraph separated by spacing."""
    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    for i, para in enumerate(paragraphs):
        _pdf_set_font(pdf, cjk_font, "", size, para)
        pdf.multi_cell(0, line_h, para, fill=fill)
        if i < len(paragraphs) - 1:
            pdf.ln(line_h * 0.5)


HIGHLIGHT_COLORS = {
    "yellow": (254, 240, 138),
    "green": (187, 247, 208),
    "blue": (191, 219, 254),
    "red": (254, 202, 202),
    "purple": (233, 213, 255),
}


@app.route("/export-pdf", methods=["POST"])
def export_pdf():
    data = request.get_json(force=True, silent=True) or {}
    chinese = data.get("chinese", "")
    english = data.get("english", "")
    summary = data.get("summary", "")
    notes = data.get("notes", "")
    highlights = data.get("highlights", [])
    url = data.get("url", "")
    citation = data.get("citation", "")
    article_title = data.get("title", "")

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Register CJK font if available
    cjk_font = None
    if CJK_FONT_PATH:
        pdf.add_font("CJK", "", CJK_FONT_PATH, uni=True)
        pdf.add_font("CJK", "B", CJK_FONT_PATH, uni=True)
        cjk_font = "CJK"

    # Title — article name + "Report"
    report_title = f"{article_title} Report" if article_title else "Research Report"
    _pdf_set_font(pdf, cjk_font, "B", 18, report_title)
    pdf.set_text_color(26, 26, 26)
    pdf.multi_cell(0, 12, report_title)
    pdf.set_draw_color(74, 144, 217)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)

    # Date + source
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(136, 136, 136)
    pdf.cell(0, PDF_LINE_H_SMALL, f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d')}", new_x="LMARGIN", new_y="NEXT")
    if url:
        pdf.cell(0, PDF_LINE_H_SMALL, f"Source: {url}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)

    # Executive Summary
    if summary.strip():
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_text_color(74, 144, 217)
        pdf.cell(0, 8, "Executive Summary", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        pdf.set_text_color(51, 51, 51)
        pdf.set_fill_color(245, 248, 255)
        _pdf_write_paragraphs(pdf, summary, cjk_font, fill=True)
        pdf.ln(6)

    # Chinese section
    _pdf_set_font(pdf, cjk_font, "B", 13, "Original Chinese 原文")
    pdf.set_text_color(74, 144, 217)
    pdf.cell(0, 8, "Original Chinese", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_text_color(51, 51, 51)
    _pdf_write_paragraphs(pdf, chinese, cjk_font)
    pdf.ln(6)

    # English section
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(74, 144, 217)
    pdf.cell(0, 8, "English Translation", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_text_color(51, 51, 51)
    _pdf_write_paragraphs(pdf, english, cjk_font)
    pdf.ln(6)

    # Highlights
    if highlights:
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_text_color(74, 144, 217)
        pdf.cell(0, 8, "Highlights", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        for h in highlights:
            color_name = h.get("color", "yellow")
            rgb = HIGHLIGHT_COLORS.get(color_name, (254, 240, 138))
            pdf.set_fill_color(*rgb)
            pdf.set_text_color(51, 51, 51)
            text = h.get("text", "")
            _pdf_set_font(pdf, cjk_font, "", 10, text)
            pdf.multi_cell(0, PDF_LINE_H, f"[{color_name.upper()}] {text}", fill=True)
            pdf.ln(1)
        pdf.ln(4)

    # Notes
    if notes.strip():
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_text_color(74, 144, 217)
        pdf.cell(0, 8, "Research Notes", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        pdf.set_text_color(51, 51, 51)
        pdf.set_fill_color(248, 248, 248)
        _pdf_write_paragraphs(pdf, notes, cjk_font, fill=True)

    # Citation
    if citation.strip():
        pdf.ln(6)
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_text_color(74, 144, 217)
        pdf.cell(0, 8, "Citation", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)
        pdf.set_text_color(51, 51, 51)
        pdf.set_fill_color(245, 248, 255)
        _pdf_set_font(pdf, cjk_font, "", 10, citation)
        pdf.multi_cell(0, PDF_LINE_H, citation, fill=True)

    buf = BytesIO(pdf.output())
    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"research-report-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.pdf",
    )


if __name__ == "__main__":
    import webbrowser, threading, os
    if os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        threading.Timer(1.5, lambda: webbrowser.open("http://127.0.0.1:5001")).start()
    app.run(debug=True, port=5001)
