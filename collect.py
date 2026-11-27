"""
Daily snapshot collector for public ATS job boards (Greenhouse, Lever, Ashby).

Writes immutable raw snapshots; all processing (dedup, extraction, scoring)
happens downstream on these files, so a bug there never costs you a day of data.

Usage:
  python collect.py probe companies_seed.txt   # find which ATS each name uses -> boards.csv
  python collect.py snapshot                   # fetch every board in boards.csv -> data/snapshots/YYYY-MM-DD/
  python collect.py stats [YYYY-MM-DD]         # quick counts for a snapshot

Optional: set CONTACT_EMAIL in your environment so the User-Agent tells
board operators who to reach if they have a concern.

Stdlib only (no pip install needed). Python 3.9+.
"""
import csv
import datetime as dt
import gzip
import hashlib
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).parent
DATA = ROOT / "data" / "snapshots"
BOARDS = ROOT / "boards.csv"
_contact = os.environ.get("CONTACT_EMAIL", "")
UA = "listing-quality-research/0.1" + (f" (contact: {_contact})" if _contact else "")
WORKERS = 6          # keep it polite
TIMEOUT = 20
RETRIES = 3

# ---------------------------------------------------------------- HTTP

def fetch_json(url):
    """Return (status, parsed_json_or_None). Retries on 429/5xx/network errors."""
    for attempt in range(RETRIES):
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return r.status, json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (404, 400, 403):
                return e.code, None
            if attempt == RETRIES - 1:
                return e.code, None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            if attempt == RETRIES - 1:
                return 0, None
        time.sleep(2 ** attempt)
    return 0, None

# ---------------------------------------------------------------- text utils

TAG = re.compile(r"<[^>]+>")
WS = re.compile(r"\s+")

def html_to_text(s):
    if not s:
        return ""
    s = html.unescape(html.unescape(s))          # Greenhouse double-escapes content
    s = re.sub(r"(?i)<\s*(br|/p|/li|/h\d|/div)\s*/?>", "\n", s)
    s = TAG.sub(" ", s)
    return WS.sub(" ", s).strip()

def content_hash(title, text):
    norm = WS.sub(" ", f"{title}\n{text}".lower()).strip()
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()

def ms_to_iso(ms):
    if not ms:
        return None
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat()

# ---------------------------------------------------------------- per-ATS adapters
# Each returns (http_status, list_of_normalized_postings or None if the board doesn't exist).

def gh(slug):
    status, d = fetch_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true")
    if d is None or "jobs" not in d:
        return status, None
    out = []
    for j in d["jobs"]:
        text = html_to_text(j.get("content"))
        out.append({
            "source": "greenhouse", "company": slug, "job_id": str(j.get("id")),
            "title": j.get("title", ""),
            "location": (j.get("location") or {}).get("name", ""),
            "url": j.get("absolute_url"),
            "first_published": j.get("first_published"),
            "updated_at": j.get("updated_at"),
            "workplace_type": None,
            "department": ", ".join(x.get("name", "") for x in j.get("departments") or []),
            "text": text,
            "salary_raw": None,
            "hash": content_hash(j.get("title", ""), text),
        })
    return status, out

def lever(slug):
    out, skip, status = [], 0, 0
    while True:
        status, d = fetch_json(f"https://api.lever.co/v0/postings/{slug}?mode=json&limit=100&skip={skip}")
        if d is None or not isinstance(d, list):
            return status, (out or None)
        for j in d:
            cats = j.get("categories") or {}
            lists = " ".join(f"{l.get('text','')}: {html_to_text(l.get('content'))}" for l in j.get("lists") or [])
            text = " ".join(x for x in [j.get("descriptionPlain"), lists, j.get("additionalPlain")] if x)
            sal = j.get("salaryRange")
            out.append({
                "source": "lever", "company": slug, "job_id": j.get("id"),
                "title": j.get("text", ""),
                "location": cats.get("location", "") or ", ".join(cats.get("allLocations") or []),
                "url": j.get("hostedUrl"),
                "first_published": ms_to_iso(j.get("createdAt")),
                "updated_at": None,
                "workplace_type": j.get("workplaceType"),
                "department": cats.get("team", ""),
                "text": WS.sub(" ", text).strip(),
                "salary_raw": json.dumps(sal) if sal else None,
                "hash": content_hash(j.get("text", ""), text),
            })
        if len(d) < 100:
            return status, out
        skip += 100

def ashby(slug):
    status, d = fetch_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true")
    if d is None or "jobs" not in d:
        return status, None
    out = []
    for j in d["jobs"]:
        text = j.get("descriptionPlain") or html_to_text(j.get("descriptionHtml"))
        locs = [j.get("location") or ""] + [
            (x.get("location") if isinstance(x, dict) else str(x)) or "" for x in j.get("secondaryLocations") or []
        ]
        comp = j.get("compensation")
        out.append({
            "source": "ashby", "company": slug, "job_id": j.get("id"),
            "title": j.get("title", ""),
            "location": "; ".join(l for l in locs if l),
            "url": j.get("jobUrl"),
            "first_published": j.get("publishedAt"),
            "updated_at": None,
            "workplace_type": j.get("workplaceType") or ("Remote" if j.get("isRemote") else None),
            "department": j.get("department") or j.get("team") or "",
            "text": WS.sub(" ", text or "").strip(),
            "salary_raw": json.dumps(comp) if comp else None,
            "hash": content_hash(j.get("title", ""), text or ""),
        })
    return status, out

ADAPTERS = {"greenhouse": gh, "lever": lever, "ashby": ashby}

# ---------------------------------------------------------------- commands

def slugify(name):
    return re.sub(r"[^a-z0-9]", "", name.lower())

def probe(seed_file):
    names = [l.strip() for l in open(seed_file, encoding="utf-8") if l.strip() and not l.startswith("#")]
    tasks = [(src, slugify(n)) for n in names for src in ADAPTERS]
    found = {}
    with ThreadPoolExecutor(WORKERS) as ex:
        futs = {ex.submit(ADAPTERS[src], slug): (src, slug) for src, slug in tasks}
        for f in as_completed(futs):
            src, slug = futs[f]
            _, jobs = f.result()
            if jobs:  # board exists and has at least one posting
                found.setdefault(slug, []).append((src, len(jobs)))
    with open(BOARDS, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["source", "slug", "n_jobs_at_probe"])
        for slug, hits in sorted(found.items()):
            for src, n in hits:
                w.writerow([src, slug, n])
    total = sum(n for hits in found.values() for _, n in hits)
    print(f"{len(found)}/{len(names)} names matched a board; {total} postings visible. Wrote {BOARDS}")

def snapshot():
    boards = list(csv.DictReader(open(BOARDS)))
    day = dt.date.today().isoformat()
    outdir = DATA / day
    outdir.mkdir(parents=True, exist_ok=True)
    fetched_at = dt.datetime.now(dt.timezone.utc).isoformat()
    manifest, n_total = [], 0
    with ThreadPoolExecutor(WORKERS) as ex:
        futs = {ex.submit(ADAPTERS[b["source"]], b["slug"]): b for b in boards}
        for f in as_completed(futs):
            b = futs[f]
            status, jobs = f.result()
            jobs = jobs or []
            with gzip.open(outdir / f"{b['source']}__{b['slug']}.jsonl.gz", "wt", encoding="utf-8") as fh:
                for j in jobs:
                    j["fetched_at"] = fetched_at
                    fh.write(json.dumps(j, ensure_ascii=False) + "\n")
            manifest.append({"source": b["source"], "slug": b["slug"], "status": status, "n": len(jobs)})
            n_total += len(jobs)
    (outdir / "manifest.json").write_text(json.dumps({"fetched_at": fetched_at, "boards": manifest}, indent=1))
    bad = [m for m in manifest if m["n"] == 0]
    print(f"{day}: {n_total} postings from {len(boards) - len(bad)}/{len(boards)} boards -> {outdir}")
    if bad:
        print(f"  {len(bad)} boards returned nothing (see manifest.json)")

def load_day(day):
    for p in sorted((DATA / day).glob("*.jsonl.gz")):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            for line in fh:
                yield json.loads(line)

def stats(day=None):
    day = day or sorted(p.name for p in DATA.iterdir() if p.is_dir())[-1]
    rows = list(load_day(day))
    by_src = {}
    for r in rows:
        by_src[r["source"]] = by_src.get(r["source"], 0) + 1
    hashes = [r["hash"] for r in rows]
    print(f"{day}: {len(rows)} postings {by_src}; exact-duplicate texts: {len(hashes) - len(set(hashes))}")
    print(f"  with salary info: {sum(1 for r in rows if r['salary_raw'])}; "
          f"with publish date: {sum(1 for r in rows if r['first_published'])}")

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "snapshot"
    if cmd == "probe":
        probe(sys.argv[2] if len(sys.argv) > 2 else "companies_seed.txt")
    elif cmd == "snapshot":
        snapshot()
    elif cmd == "stats":
        stats(sys.argv[2] if len(sys.argv) > 2 else None)
    else:
        print(__doc__)
