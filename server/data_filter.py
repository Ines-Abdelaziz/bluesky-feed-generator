import csv
import os
from datetime import datetime, timezone

from server.logger import logger

# ---------------------------------------------------------------------------
# Load allowed source DIDs from CSV at startup
# ---------------------------------------------------------------------------


def _load_sources(csv_path: str) -> dict[str, str]:
    """
    Returns a dict of {did: bias} for all accounts in the CSV.
    Adjust csv_path to wherever you place your bluesky_sources.csv.
    """
    sources = {}
    if not os.path.exists(csv_path):
        logger.warning(f"Sources CSV not found at {csv_path}")
        return sources
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            did = row.get("did", "").strip()
            bias = row.get("bias", "unknown").strip()
            if did:
                sources[did] = bias
    logger.info(f"Loaded {len(sources)} source DIDs from {csv_path}")
    return sources


# Path to your CSV — adjust if needed
CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "bluesky_sources.csv")
ALLOWED_SOURCES: dict[str, str] = _load_sources(CSV_PATH)

# ---------------------------------------------------------------------------
# Optional: filter by bias label
# Set to None to include all bias categories, or e.g. {'neutral', 'left-center'}
# ---------------------------------------------------------------------------
ALLOWED_BIASES: set[str] | None = None  # e.g. {'neutral'} to restrict

# ---------------------------------------------------------------------------
# Main callback — called for every firehose event
# ---------------------------------------------------------------------------


def operations_callback(ops: dict) -> None:
    from server.database import Post  # local import to avoid circular deps

    posts_to_create = []

    for post in ops.get("posts", {}).get("creates", []):
        author_did = post.get("author", "")
        uri = post.get("uri", "")
        cid = post.get("cid", "")

        # Skip if author is not in our source list
        if author_did not in ALLOWED_SOURCES:
            continue

        # Optional: filter by bias category
        if ALLOWED_BIASES is not None:
            bias = ALLOWED_SOURCES[author_did]
            if bias not in ALLOWED_BIASES:
                continue

        posts_to_create.append(
            {
                "uri": uri,
                "cid": cid,
                "indexed_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    if posts_to_create:
        logger.debug(f"Indexing {len(posts_to_create)} posts from allowed sources")
        Post.insert_many(posts_to_create).on_conflict_ignore().execute()
