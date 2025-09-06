# -*- coding: utf-8 -*-
from app.ui.components.auth import upsert_user

# >>> CHANGE THESE SAFELY <<<
USERNAME = "<ADMIN_USERNAME>"  # e.g., "admin"
PASSWORD = "<STRONG_PASSWORD_12+>"  # e.g., "ChangeMe!42"

if __name__ == "__main__":
    upsert_user(USERNAME, PASSWORD)
    print(f"[OK] Upserted user: {USERNAME}")
