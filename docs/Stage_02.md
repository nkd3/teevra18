\# Stage 2 — Ingestion (DhanHQ → Sheets)



\## Status

✅ Completed



\## Acceptance Criteria

\- Continuous ingestion ≥60 min, no >3s gaps

\- ticks\_raw growing, health updated (last\_ingest, gap\_s)

\- ops\_log shows INGEST\_OK events; GAP\_WARN if >3s

\- JWT‑protected /ingest; Telegram alerts optional

\- Auto‑restart script ready (.ps1)



\## Evidence (recent ops\_log)

2025-08-24T16:21:53+05:30	AUTODOC	Stage 2 completion pushed



Generated at: <put timestamp>



