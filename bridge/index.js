const express = require('express');
const axios = require('axios');
require('dotenv').config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8787;
const ENV  = process.env.ENV || 'dev';
const SHEETS_ENDPOINT = process.env.SHEETS_ENDPOINT; // Apps Script Web App URL
const SHEETS_JWT      = process.env.SHEETS_JWT;      // JWT signed with Script Properties JWT_SECRET

app.get('/health', (_req, res) => {
  res.json({ ok: true, service: 'Teevra18 Bridge', env: ENV, time: new Date().toISOString() });
});

app.post('/ping', async (_req, res) => {
  try {
    if (!SHEETS_ENDPOINT || !SHEETS_JWT) throw new Error('SHEETS_ENDPOINT or SHEETS_JWT missing');
    const body = {
      jwt: SHEETS_JWT,
      type: 'heartbeat',
      data: { source: 'bridge', note: 'bridge ping' }
    };
    const r = await axios.post(SHEETS_ENDPOINT, body, { timeout: 10000 });
    return res.json({ ok: true, sheets: r.data });
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err) });
  }
});

app.listen(PORT, () => console.log(`Bridge up on :${PORT}`));

