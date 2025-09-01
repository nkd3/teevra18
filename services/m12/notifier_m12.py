import os, sqlite3, time, json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import urllib.request, urllib.parse

DB_PATH = Path(os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db"))
BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def now_utc_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def send_telegram(text: str) -> bool:
    if not BOT or not CHAT_ID:
        print("[WARN] Telegram env missing; skipping send.")
        return False
    base = f"https://api.telegram.org/bot{BOT}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": text}).encode("utf-8")
    try:
        with urllib.request.urlopen(base, data=data, timeout=10) as r:
            _ = r.read()
        return True
    except Exception as e:
        print("[ERROR] Telegram send failed:", e)
        return False

def main():
    print("[INFO] M12 Notifier start @", now_utc_str())
    with sqlite3.connect(DB_PATH) as conn:
        # find due, pending
        due = pd.read_sql("""
            SELECT id, instrument, ts_utc, prob_up, exp_move_abs, pre_alert_at, created_at
            FROM signals_m11
            WHERE status='PENDING'
              AND pre_alert_at <= strftime('%Y-%m-%d %H:%M:%S','now')
            ORDER BY pre_alert_at ASC, prob_up DESC
            LIMIT 25;
        """, conn)

        if due.empty:
            print("[INFO] No due signals.")
            return

        sent_ids = []
        for r in due.itertuples(index=False):
            msg = (
                f"📣 TeeVra18 PRE-ALERT\n"
                f"• Instrument: {r.instrument}\n"
                f"• Bar: {r.ts_utc} UTC\n"
                f"• Prob↑: {float(r.prob_up):.2%}\n"
                f"• Exp Move: {'' if pd.isna(r.exp_move_abs) else round(float(r.exp_move_abs),2)}\n"
                f"• Created: {r.created_at} UTC\n"
                f"• Pre-Alert At: {r.pre_alert_at} UTC\n"
            )
            if send_telegram(msg):
                sent_ids.append(int(r.id))

        if sent_ids:
            qmarks = ",".join("?" for _ in sent_ids)
            conn.execute(f"UPDATE signals_m11 SET status='ALERTED' WHERE id IN ({qmarks})", sent_ids)
            conn.commit()
            print(f"[OK] Marked ALERTED: {len(sent_ids)} rows")

if __name__ == "__main__":
    main()
