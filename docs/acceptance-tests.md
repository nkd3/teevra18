# Acceptance Tests (Auto)
Stage | Criteria (from plan) | Status | Last Verified
----- | -------------------- | ------ | -------------
1 | Continuous ingestion, no gaps > 3s, reconnects logged | Complete | 2025-08-14T21:57:24.703Z
2 | Deterministic 1m/5m candles idempotent on ts_exch+symbol | Complete | 2025-08-14T21:57:24.703Z
3 | Deterministic signals, PAUSE on validator fault, RESUME cmd | In Progress | 2025-08-14T21:57:24.703Z
4 | ≥3 green paper days, Max DD ≤ cap; backtest ↔ paper coherent | In Progress | 2025-08-14T21:57:24.703Z
5 | Single alerts (idempotent nonce), commands reliable | Complete | 2025-08-14T21:57:24.703Z
6 | Dashboard updates 2–5 min; EOD Telegram summary | In Progress | 2025-08-14T21:57:24.703Z
7 | Drift/anomaly triggers → PAUSE; chaos tests pass | In Progress | 2025-08-14T21:57:24.703Z
8 | 5 live sessions within risk caps; clean logs | In Progress | 2025-08-14T21:57:24.703Z
9 | Swap to DhanHQ via adapter; rerun Stage 1–2 acceptance | In Progress | 2025-08-14T21:57:24.703Z
