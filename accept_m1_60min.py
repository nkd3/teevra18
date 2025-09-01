import os, time, csv, sqlite3, datetime as dt

DB = os.getenv("DB_PATH", r"C:\teevra18\data\teevra18.db")
SAMPLE_SECS   = float(os.getenv("M1_SAMPLE_SECS", "5"))
DURATION_SECS = int(os.getenv("M1_ACCEPT_SECS", str(60*60)))  # default 60 mins
CPU_LIMIT     = float(os.getenv("CPU_ACCEPT_LIMIT", "40"))

start_utc = dt.datetime.utcnow()

log_dir = os.getenv("LOG_DIR", r"C:\teevra18\logs")
os.makedirs(log_dir, exist_ok=True)
stamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
out_csv = os.path.join(log_dir, f"m1_accept_{stamp}.csv")

conn = sqlite3.connect(DB, timeout=5)
conn.execute("PRAGMA journal_mode=WAL")

fields = ["utc_ts","rows","status","cpu","gap_warning"]
f = open(out_csv, "w", newline="")
w = csv.writer(f)
w.writerow(fields)
f.flush()

high_cpu_cnt = 0
gap_warn_cnt = 0
max_gap_streak = 0.0
cur_gap_streak = 0.0
sample_count = 0

def get_val(k, default=None):
    row = conn.execute("SELECT value FROM health WHERE key=?", (k,)).fetchone()
    return row[0] if row else default

# mark log start to filter ops_log later
conn.execute("INSERT INTO ops_log(level, area, msg, ts_utc) VALUES(?,?,?,datetime('now'))",
             ("INFO","accept","M1 acceptance start"))
conn.commit()

t_end = time.perf_counter() + DURATION_SECS
next_tick = time.perf_counter()

while time.perf_counter() < t_end:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute("SELECT COUNT(1) FROM ticks_raw").fetchone()[0]
    st   = get_val("m1_status","(none)")
    cpu  = get_val("m1_cpu","0")   # string like '12.3'
    gap  = get_val("m1_gap_warning","0")

    # write sample
    w.writerow([now, rows, st, cpu, gap]); f.flush()
    sample_count += 1

    # counters
    try:
        if float(cpu) > CPU_LIMIT:
            high_cpu_cnt += 1
    except:
        pass

    if str(gap) == "1":
        gap_warn_cnt += 1
        cur_gap_streak += SAMPLE_SECS
        if cur_gap_streak > max_gap_streak:
            max_gap_streak = cur_gap_streak
    else:
        cur_gap_streak = 0.0

    # pace to every SAMPLE_SECS from our own schedule
    next_tick += SAMPLE_SECS
    sleep_for = max(0.0, next_tick - time.perf_counter())
    time.sleep(sleep_for)

f.close()

expected_samples = int(DURATION_SECS // SAMPLE_SECS)
cpu_over_pct = (100.0 * high_cpu_cnt / sample_count) if sample_count else 0.0

summary = {
    "expected_samples": expected_samples,
    "actual_samples": sample_count,
    "cpu_over_limit_count": high_cpu_cnt,
    "cpu_over_limit_pct": round(cpu_over_pct,2),
    "max_gap_streak_secs": round(max_gap_streak,1),
    "gap_warn_total_samples": gap_warn_cnt,
    "csv": out_csv
}

# collect WARN/ERROR since start
ops = conn.execute("""
SELECT ts_utc, level, area, msg
FROM ops_log
WHERE ts_utc >= ?
  AND level IN ('WARN','ERROR')
ORDER BY ts_utc
""", (start_utc.strftime("%Y-%m-%d %H:%M:%S"),)).fetchall()

print("=== M1 Acceptance Summary ===")
for k,v in summary.items():
    print(f"{k}: {v}")
if ops:
    print("\nWARN/ERROR during run:")
    for r in ops:
        print(" ", r)

# basic pass/fail signal:
# - got (?) all samples,
# - CPU not above limit >5% of the time,
# - no gap streak >3s,
# - no WARN/ERROR in ops_log
ok_count = (sample_count >= max(1, int(expected_samples*0.95)))
ok_cpu   = (cpu_over_pct <= 5.0)
ok_gap   = (max_gap_streak <= 3.0)
ok_ops   = (len(ops) == 0)
passed   = ok_count and ok_cpu and ok_gap and ok_ops
print(f"\nPASS: {passed}")
