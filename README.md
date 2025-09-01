# Teevra18

Local-only trading research stack. Public repo excludes secrets, databases, and raw data.

**Runtime defaults**
- DhanHQ = market data
- Zerodha = manual execution only
- Max trades/day ≤ 5
- SL ≤ ₹1000/lot
- R:R ≥ 1:2

**Folders**
- \pp\ — Streamlit UI
- \services\ — microservices
- \scripts\ — helpers/migrations
- \config\ — non-secret config samples
- \data\, \logs\ — ignored (local only)
- \docs\ — runbooks/design notes
- \	ools\ — helper scripts

**Never commit secrets**. Keep \.env\ local; share values in chat only when needed.
