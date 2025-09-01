import os, datetime
from notion_client import Client
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Config
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "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")
DATABASE_ID = os.getenv("NOTION_DB", "25ce38d133c78026bafed8d9c431de57")
ROOT_DIR = r"C:\teevra18"

notion = Client(auth=NOTION_TOKEN)

class ChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory:
            filepath = event.src_path
            relpath = os.path.relpath(filepath, ROOT_DIR)
            with open(filepath, "r", errors="ignore") as f:
                content = f.read()[:2000]  # Notion text limit safeguard
            notion.pages.create(
                parent={"database_id": DATABASE_ID},
                properties={
                    "Stage": {"title": [{"text": {"content": relpath}}]},
                    "File": {"rich_text": [{"text": {"content": relpath}}]},
                    "Content": {"rich_text": [{"text": {"content": content}}]},
                    "Timestamp": {"date": {"start": datetime.datetime.now().isoformat()}}
                }
            )
            print(f"Updated Notion: {relpath}")

if __name__ == "__main__":
    event_handler = ChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, ROOT_DIR, recursive=True)
    observer.start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
