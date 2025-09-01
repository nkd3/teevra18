# C:\teevra18\scripts\notion_reset_autodoc.py
"""
Reset AUTODOC marker paragraphs across pages in your Notion DB.

Default behavior:
- DRY RUN: shows what would be deleted/kept.
- Keeps the first AUTODOC paragraph found; deletes all other AUTODOC paragraphs.
- Optionally creates a new AUTODOC paragraph if none exists (--create-if-missing).

Usage:
  python notion_reset_autodoc.py [--page PAGE_ID] [--prefix "AUTODOC: "] [--create-if-missing] [--limit N] [--dry-run | --apply]

Examples:
  # Dry-run entire DB
  python notion_reset_autodoc.py --dry-run

  # Actually apply on entire DB (be careful)
  python notion_reset_autodoc.py --apply

  # Only one page
  python notion_reset_autodoc.py --page 25ce38d1-33c7-819a-897b-c1f09dc938ea --apply

  # Use a custom prefix (must match your watcher)
  python notion_reset_autodoc.py --prefix "AUTODOC: " --apply
"""
import os, sys, argparse
from dotenv import load_dotenv
from notion_client import Client
from notion_client.errors import APIResponseError

def parse_args():
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--page", default=None, help="Operate on a single page id")
    ap.add_argument("--prefix", default="AUTODOC: ", help="Marker prefix to match (default: 'AUTODOC: ')")
    ap.add_argument("--create-if-missing", action="store_true", help="Create one AUTODOC paragraph if none present")
    ap.add_argument("--limit", type=int, default=1000, help="Max pages to scan from DB (default 1000)")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Do not change anything (default)")
    mode.add_argument("--apply", action="store_true", help="Make changes (delete duplicates / create if missing)")
    return ap.parse_args()

def load_client():
    load_dotenv(r"C:\teevra18\.env", override=True)
    token = os.getenv("NOTION_TOKEN")
    dbid  = os.getenv("NOTION_DB")
    if not token or not dbid:
        raise SystemExit("Missing NOTION_TOKEN/NOTION_DB in C:\\teevra18\\.env")
    return Client(auth=token), dbid

def list_db_pages(client: Client, database_id: str, limit: int):
    pages = []
    cursor = None
    while True:
        payload = {"database_id": database_id, "page_size": min(100, max(1, limit - len(pages)))}
        if cursor: payload["start_cursor"] = cursor
        resp = client.databases.query(**payload)
        pages.extend(resp.get("results", []))
        if len(pages) >= limit or not resp.get("has_more"): break
        cursor = resp.get("next_cursor")
    return pages[:limit]

def get_page_children(client: Client, page_id: str, page_size=100):
    kids = []
    cursor = None
    while True:
        payload = {"block_id": page_id, "page_size": page_size}
        if cursor: payload["start_cursor"] = cursor
        resp = client.blocks.children.list(**payload)
        kids.extend(resp.get("results", []))
        if not resp.get("has_more"): break
        cursor = resp.get("next_cursor")
    return kids

def rt_plain_text(rt_list):
    return "".join(rt.get("plain_text", "") for rt in (rt_list or []))

def reset_page(client: Client, page_id: str, prefix: str, create_if_missing: bool, apply: bool):
    kept_id = None
    dupes   = []
    none_found = True

    # fetch children
    try:
        kids = get_page_children(client, page_id)
    except APIResponseError as e:
        print(f"[{page_id}] FAILED to list children: {e}")
        return

    # scan for paragraph blocks that start with prefix
    for b in kids:
        if b.get("type") != "paragraph": 
            continue
        plain = rt_plain_text(b["paragraph"].get("rich_text", []))
        if plain.startswith(prefix):
            if kept_id is None:
                kept_id = b["id"]
            else:
                dupes.append(b["id"])
            none_found = False

    # Report
    print(f"[{page_id}] AUTODOC blocks -> keep: {kept_id or '-'}  delete_count: {len(dupes)}  none_found: {none_found}")

    # delete duplicates
    for bid in dupes:
        if apply:
            try:
                client.blocks.delete(block_id=bid)
                print(f"  deleted duplicate block: {bid}")
            except APIResponseError as e:
                print(f"  delete fail {bid}: {e}")
        else:
            print(f"  (dry-run) would delete: {bid}")

    # create new one if none and requested
    if none_found and create_if_missing:
        if apply:
            try:
                client.blocks.children.append(
                    block_id=page_id,
                    children=[{
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{"type": "text", "text": {"content": prefix + "(initialized)"}}]
                        },
                    }],
                )
                print("  created initial AUTODOC paragraph")
            except APIResponseError as e:
                print(f"  append fail: {e}")
        else:
            print("  (dry-run) would create initial AUTODOC paragraph")

def main():
    args = parse_args()
    client, dbid = load_client()
    apply = bool(args.apply)
    # default to dry-run if neither flag provided
    if not args.dry_run and not args.apply:
        print("(no mode specified) defaulting to --dry-run")
    print(f"MODE: {'APPLY' if apply else 'DRY-RUN'} | prefix={args.prefix!r} | create_if_missing={args.create_if_missing}")

    if args.page:
        reset_page(client, args.page, args.prefix, args.create_if_missing, apply)
        return

    # whole DB
    pages = list_db_pages(client, dbid, args.limit)
    print(f"Scanning {len(pages)} pages...")
    for pg in pages:
        pid = pg["id"]
        reset_page(client, pid, args.prefix, args.create_if_missing, apply)

if __name__ == "__main__":
    main()
