"""
backfill.py — fetch historical posts for all accounts in bluesky_sources.csv
and insert them into feed_database.db.

Usage:
    python backfill.py

Optional flags:
    python backfill.py --limit 50      # only fetch last 50 posts per account
    python backfill.py --bias neutral  # only backfill accounts with a specific bias
"""

import argparse
import csv
import time
from datetime import datetime, timezone

import requests

from server.database import Post

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CSV_PATH = "bluesky_sources.csv"
API_BASE = "https://public.api.bsky.app/xrpc"
POSTS_PER_PAGE = 100  # max allowed by the API
SLEEP_BETWEEN_ACCOUNTS = 0.5  # seconds — be polite to the API


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def fetch_author_feed(did: str, limit: int | None = None) -> list[dict]:
    """Fetch all posts for a given DID, paginating through full history."""
    posts = []
    cursor = None
    page = 0

    while True:
        params = {
            "actor": did,
            "limit": POSTS_PER_PAGE,
            "filter": "posts_no_replies",  # skip replies, only original posts
        }
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(
                f"{API_BASE}/app.bsky.feed.getAuthorFeed",
                params=params,
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"    [!] Request error for {did}: {e}")
            break

        feed = data.get("feed", [])
        if not feed:
            break

        for item in feed:
            post = item.get("post", {})
            uri = post.get("uri", "")
            cid = post.get("cid", "")
            record = post.get("record", {})
            reply_ref = record.get("reply", None)
            reply_parent = reply_ref["parent"]["uri"] if reply_ref else None
            reply_root = reply_ref["root"]["uri"] if reply_ref else None
            indexed_at = post.get("indexedAt", datetime.now(timezone.utc).isoformat())

            if uri and cid:
                posts.append(
                    {
                        "uri": uri,
                        "cid": cid,
                        "reply_parent": reply_parent,
                        "reply_root": reply_root,
                        "indexed_at": indexed_at,
                    }
                )

        page += 1
        cursor = data.get("cursor")
        print(
            f"    page {page}: {len(feed)} posts fetched (total so far: {len(posts)})"
        )

        # Stop if we've hit the user-specified limit
        if limit and len(posts) >= limit:
            posts = posts[:limit]
            break

        # Stop if no more pages
        if not cursor:
            break

        time.sleep(0.1)  # small pause between pages

    return posts


def load_sources(csv_path: str, bias_filter: str | None = None) -> list[dict]:
    sources = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            did = row.get("did", "").strip()
            bias = row.get("bias", "").strip()
            handle = row.get("handle", "").strip()
            if not did:
                continue
            if bias_filter and bias != bias_filter:
                continue
            sources.append({"did": did, "bias": bias, "handle": handle})
    # deduplicate by DID
    seen = set()
    unique = []
    for s in sources:
        if s["did"] not in seen:
            seen.add(s["did"])
            unique.append(s)
    return unique


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max posts to fetch per account (default: all)",
    )
    parser.add_argument(
        "--bias",
        type=str,
        default=None,
        help="Only backfill accounts with this bias label",
    )
    args = parser.parse_args()

    sources = load_sources(CSV_PATH, bias_filter=args.bias)
    print(
        f"Backfilling {len(sources)} accounts"
        + (f" (bias={args.bias})" if args.bias else "")
        + (f" (limit={args.limit} posts each)" if args.limit else "")
    )
    print()

    total_inserted = 0

    for i, source in enumerate(sources, 1):
        did = source["did"]
        handle = source["handle"]
        print(f"[{i}/{len(sources)}] {handle} ({did})")

        posts = fetch_author_feed(did, limit=args.limit)

        if not posts:
            print(f"    no posts found, skipping")
            time.sleep(SLEEP_BETWEEN_ACCOUNTS)
            continue

        # Bulk insert, ignore duplicates
        inserted = 0
        batch_size = 100
        for j in range(0, len(posts), batch_size):
            batch = posts[j : j + batch_size]
            rows = Post.insert_many(batch).on_conflict_ignore().execute()
            inserted += len(batch)

        total_inserted += inserted
        print(f"    inserted {inserted} posts (db total: {Post.select().count()})")
        time.sleep(SLEEP_BETWEEN_ACCOUNTS)

    print()
    print(f"Done. Total posts inserted this run: {total_inserted}")
    print(f"Total posts in database: {Post.select().count()}")


if __name__ == "__main__":
    main()
