/** Teevra18 Apps Script — Stage 0 (heartbeat + Telegram)
 * Web App endpoint: POST JSON { jwt, type, data }
 * JWT: HS256 signed; secret in Script Properties as JWT_SECRET
 */

const SPROPS = PropertiesService.getScriptProperties();

function onOpen() {
  SpreadsheetApp.getUi().createMenu('Teevra18')
    .addItem('Send Telegram Test', 'sendTelegramTest')
    .addItem('Rotate JWT Secret', 'rotateJwtSecret')
    .addToUi();
}

function getProp(k){ return SPROPS.getProperty(k); }
function setProp(k,v){ SPROPS.setProperty(k, v); }

function sendTelegram(text) {
  const token = getProp('TELEGRAM_BOT_TOKEN');
  const chatId = getProp('TELEGRAM_CHAT_ID');
  if (!token || !chatId) throw new Error('Telegram token/chatId missing');
  const url = 'https://api.telegram.org/bot' + token + '/sendMessage';
  const payload = { chat_id: chatId, text: text };
  UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
}

function sendTelegramTest() {
  sendTelegram('Teevra18: Telegram OK @ ' + new Date().toISOString());
  SpreadsheetApp.getActive().getSheetByName('logs')
    .appendRow([new Date().toISOString(), 'INFO', 'sendTelegramTest', 'Telegram test sent', '']);
}

function rotateJwtSecret() {
  const newSecret = Utilities.getUuid().replace(/-/g,'');
  setProp('JWT_SECRET', newSecret);
  SpreadsheetApp.getActive().getSheetByName('logs')
    .appendRow([new Date().toISOString(), 'WARN', 'rotateJwtSecret', 'JWT secret rotated', '']);
  SpreadsheetApp.getUi().alert('New JWT_SECRET set. Update the Bridge token.');
}

function doGet(e){
  const payload = {
    ok: true,
    service: 'Teevra18 Apps Script',
    time: new Date().toISOString(),
    env: getProp('ENV') || 'dev'
  };
  return ContentService.createTextOutput(JSON.stringify(payload))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e){
  try {
    if (!e || !e.postData || !e.postData.contents) throw new Error('Empty body');
    const body = JSON.parse(e.postData.contents);

    const secret = getProp('JWT_SECRET');
    if (!secret) throw new Error('JWT secret not configured');

    const claims = verifyJwtHS256(body.jwt, secret);
    if (!claims) throw new Error('JWT verification failed');

    const type = body.type || 'unknown';
    const data = body.data || {};

    if (type === 'heartbeat') {
      const sh = SpreadsheetApp.getActive().getSheetByName('heartbeat');
      sh.appendRow([new Date().toISOString(), data.source || 'bridge', data.note || 'OK']);
      // optional: notify Telegram the first time or on errors only (keep quiet for heartbeat spam)
    } else {
      const sh = SpreadsheetApp.getActive().getSheetByName('logs');
      sh.appendRow([new Date().toISOString(), 'INFO', 'doPost', 'Unhandled type', JSON.stringify({type, data}).slice(0, 3000)]);
    }

    return json200({ok:true});
  } catch (err) {
    logError('doPost', err);
    return json400({ok:false, error:String(err)});
  }
}

/* ==== helpers ==== */
function json200(obj){
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
function json400(obj){
  const out = ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
  // Apps Script can’t set 400 explicitly in Web Apps; caller must inspect body.
  return out;
}
function logError(where, err){
  const sh = SpreadsheetApp.getActive().getSheetByName('logs');
  sh.appendRow([new Date().toISOString(), 'ERROR', where, String(err), '']);
}

/* ==== JWT (HS256) verify ==== */
function verifyJwtHS256(token, secret){
  if (!token || typeof token !== 'string') return null;
  const parts = token.split('.');
  if (parts.length !== 3) return null;

  const [hB, pB, sB] = parts;
  const signingInput = hB + '.' + pB;

  // HMAC SHA-256
  const rawSig = Utilities.computeHmacSha256Signature(signingInput, secret, Utilities.Charset.UTF_8);
  const calcSig = Utilities.base64EncodeWebSafe(rawSig).replace(/=+$/,'');

  if (calcSig !== sB) return null;

  const payloadJson = base64urlToString(pB);
  const claims = JSON.parse(payloadJson);

  const now = Math.floor(Date.now()/1000);
  if (claims.exp && now > claims.exp) return null;
  if (claims.nbf && now < claims.nbf) return null;

  return claims;
}

function base64urlToString(b64url){
  // Apps Script supports websafe decode directly
  const bytes = Utilities.base64DecodeWebSafe(b64url);
  return Utilities.newBlob(bytes).getDataAsString();
}

