/*****  TEEVRA 18 AUTODOCS — Path 1 (Sheets + Apps Script + GitHub)
 *  What this script does:
 *  - Ensures admin tabs exist and have headers
 *  - Infers Stage statuses from Sheet evidence (heuristics you can adjust)
 *  - Generates Markdown docs: README, acceptance table, per-stage pages, daily changelog
 *  - Commits docs to GitHub via REST API
 *  - Exposes a /change webhook for GitHub Actions to log "cause of change" & bump version
 *
 *  Required Script Properties (File > Project Properties > Script Properties):
 *   GITHUB_TOKEN  = <GitHub Personal Access Token with contents:write>
 *   GH_REPO       = <owner/repo>   e.g.,  CyberCrispsLtd/teevra18
 *   GH_BRANCH     = main
 *   DOCS_JWT      = <any long random string>  (shared secret for webhook)
 *   TIMEZONE      = Asia/Kolkata   (or your zone)
 *
 *  Optional (only if sending Telegram messages from docs jobs):
 *   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
 ****/

// ------------------ CONFIG ------------------
const CFG = {
  DOCS_DIR: "docs",
  CHANGELOG_DIR: "CHANGELOG",
  DIAGRAMS_DIR: "diagrams",
  STAGES: [
    {id:0, name:"Foundations"},
    {id:1, name:"Ingestion"},
    {id:2, name:"Candle Builder"},
    {id:3, name:"Strategy+Validator+Breaker"},
    {id:4, name:"Backtesting & Paper"},
    {id:5, name:"Alerts & Observability"},
    {id:6, name:"Dashboards & EOD"},
    {id:7, name:"Guardrails & Chaos"},
    {id:8, name:"Live (Manual Zerodha)"},
    {id:9, name:"DhanHQ Swap"}
  ],
  STATUS: { COMPLETE:"Complete", INPROG:"In Progress", BLOCKED:"Blocked", UNKNOWN:"Unknown" }
};

const SHEET_ID = PropertiesService.getScriptProperties().getProperty("SHEET_ID");
const SS = SpreadsheetApp.openById(SHEET_ID);
const TZ = PropertiesService.getScriptProperties().getProperty("TIMEZONE") || Session.getScriptTimeZone();

// ------------------ ENTRYPOINTS ------------------

// Run this once after pasting code
function seedDocs() {
  ensureAdminTabs_();
  updateDocMetaFromEvidence_();
  const md = buildAllMarkdown_();
  commitMarkdownToGitHub_(md, "docs: initial seed from autodocs");
}

// Schedule this (every 10 minutes during market + once at EOD)
function cronUpdateDocs() {
  ensureAdminTabs_();
  updateDocMetaFromEvidence_();
  const md = buildAllMarkdown_();
  commitMarkdownToGitHub_(md, "docs: auto-refresh from Sheet evidence");
}

// EOD rollup (daily changelog append)
function eodRollup() {
  ensureAdminTabs_();
  const md = buildAllMarkdown_(/*forceDaily=*/true);
  commitMarkdownToGitHub_(md, "docs: EOD changelog & acceptance refresh");
}

// GitHub → Apps Script webhook (on PR merge or push to main)
function doPost(e) {
  try {
    const props = PropertiesService.getScriptProperties();
    const ok = e && e.postData && e.postData.contents;
    if (!ok) return ContentService.createTextOutput("no_body");
    const body = JSON.parse(e.postData.contents);
    const jwt = (body.jwt || e.parameter.jwt || "");
    if (jwt !== props.getProperty("DOCS_JWT")) return ContentService.createTextOutput("401");

    // Expected fields in body (see GitHub Action below)
    const row = [
      body.change_id || Utilities.getUuid(),
      new Date(),
      body.stage ?? "",
      body.area ?? "",
      body.change_type ?? "",
      body.cause_of_change ?? "",
      body.trigger_src ?? "github",
      body.summary ?? "",
      (body.issue_ids || []).join(","),
      body.pr ?? "",
      body.commit_sha ?? "",
      "", /* version_after (we compute/bump) */
      body.kpi_before ?? "",
      body.kpi_after ?? "",
      body.acceptance_ref ?? "",
      body.evidence_link ?? "",
      body.operator ?? ""
    ];
    appendRow_("change_log", row);

    // bump semver from change_type (feat -> minor; fix -> patch; breaking -> major)
    const after = bumpVersionFromChangeType_(body.change_type || "", /*breaking=*/body.breaking || false);
    setMeta_("current_version", after);

    // rebuild docs quickly
    const md = buildAllMarkdown_();
    commitMarkdownToGitHub_(md, `docs: ${body.change_type||"change"} — ${body.summary||""}`);

    return ContentService.createTextOutput("OK");
  } catch (err) {
    return ContentService.createTextOutput("ERR: " + err);
  }
}

// ------------------ ADMIN TABS ------------------
function ensureAdminTabs_() {
  ensureTabHeaders_("doc_meta", ["key","value","notes","last_updated"]);
  ensureTabHeaders_("pm_board", ["item_id","title","stage","type","priority","effort","status","assignee","due","links","cause_ref","notes"]);
  ensureTabHeaders_("change_log", ["change_id","ts","stage","area","change_type","cause_of_change","trigger_src","summary","issue_ids","pr","commit_sha","version_after","kpi_before","kpi_after","acceptance_ref","evidence_link","operator"]);
  if (!getMeta_("current_version")) setMeta_("current_version","0.1.0");
}

// ------------------ EVIDENCE → STATUS ------------------
function updateDocMetaFromEvidence_() {
  const stages = CFG.STAGES.map(s => inferStage_(s.id));
  const sh = getSheet_("doc_meta");
  // Clear previous stage rows (rows where key like stage_X_status)
  const data = sh.getDataRange().getValues();
  const toKeep = data.filter((r,i)=> i===0 || !String(r[0]).match(/^stage_\d+_status$/));
  sh.clear();
  sh.getRange(1,1,toKeep.length,toKeep[0].length).setValues(toKeep);

  stages.forEach(s => {
    setMeta_(`stage_${s.id}_status`, s.status);
    setMeta_(`stage_${s.id}_evidence`, s.evidence);
    setMeta_(`stage_${s.id}_notes`, s.notes||"");
  });
  setMeta_("last_docs_refresh", new Date().toISOString());
}

function inferStage_(id) {
  // Minimal, safe heuristics. You can improve anytime.
  try {
    switch(id) {
      case 1: // Ingestion: ticks_raw flowing; health logs present (no >3s gap check simplified)
        return hasRows_("ticks_raw", 100) && hasRows_("health", 1)
          ? stat(id, CFG.STATUS.COMPLETE, "ticks_raw+health recent")
          : stat(id, CFG.STATUS.INPROG, "waiting for ticks/health");
      case 2: // Candles: candles_1m present & recent
        return hasRows_("candles_1m", 50)
          ? stat(id, CFG.STATUS.COMPLETE, "candles_1m updating")
          : stat(id, CFG.STATUS.INPROG, "waiting for candles_1m");
      case 3: // Signals + breaker: signals recent & ops_log has PAUSE/RESUME at least once
        return hasRows_("signals", 5) && containsText_("ops_log","PAUSE")
          ? stat(id, CFG.STATUS.COMPLETE, "signals+ops_log ok")
          : stat(id, CFG.STATUS.INPROG, "waiting for signals/ops_log");
      case 4: // Backtest & Paper: backtest_orders exists; paper_orders recent
        return hasRows_("backtest_orders", 50) && hasRows_("paper_orders", 10)
          ? stat(id, CFG.STATUS.COMPLETE, "backtest+paper evidence")
          : stat(id, CFG.STATUS.INPROG, "need backtest/paper evidence");
      case 5: // Alerts & Observability: ops_log, health non-empty
        return hasRows_("ops_log", 10) && hasRows_("health", 10)
          ? stat(id, CFG.STATUS.COMPLETE, "ops_log+health populated")
          : stat(id, CFG.STATUS.INPROG, "need alert/ops evidence");
      case 6: // Dashboards & EOD: assume Complete if any ops EOD summary present
        return containsText_("ops_log","EOD") ? stat(id, CFG.STATUS.COMPLETE,"EOD summaries present")
          : stat(id, CFG.STATUS.INPROG,"add EOD ops summaries");
      case 7: // Guardrails: look for DRIFT/ANOMALY events
        return (containsText_("ops_log","DRIFT") || containsText_("ops_log","ANOMALY"))
          ? stat(id, CFG.STATUS.COMPLETE, "guardrail events logged")
          : stat(id, CFG.STATUS.INPROG, "run guardrail tests");
      case 8: // Live: live_journal entries exist
        return hasRows_("live_journal", 1)
          ? stat(id, CFG.STATUS.COMPLETE, "live_journal entries")
          : stat(id, CFG.STATUS.INPROG, "no live_journal yet");
      case 9: // DhanHQ swap: config adapter = dhan
        return getCfg_("adapter")==="dhan"
          ? stat(id, CFG.STATUS.COMPLETE, "adapter=dhan")
          : stat(id, CFG.STATUS.INPROG, "adapter not swapped");
      default:
        return stat(id, CFG.STATUS.UNKNOWN, "no rule, manual set");
    }
  } catch (e) { return stat(id, CFG.STATUS.UNKNOWN, "error: "+e); }
}

function stat(id,status,notes){ return {id,status,evidence: evidenceLink_(id), notes}; }
function evidenceLink_(id){ return `https://docs.google.com/spreadsheets/d/${SS.getId()}/edit#gid=0`; }


/** Return today's rows from the change_log tab (in script timezone). */
function recentChangeLog_() {
  const sh = getSheet_("change_log");
  const data = sh.getDataRange().getValues(); // header + rows
  if (!data || data.length <= 1) return [];

  const todayYMD = Utilities.formatDate(new Date(), TZ, "yyyy-MM-dd");

  // Headers: ["change_id","ts","stage","area","change_type","cause_of_change",
  //           "trigger_src","summary","issue_ids","pr","commit_sha","version_after",
  //           "kpi_before","kpi_after","acceptance_ref","evidence_link","operator"]
  // We filter by column index 1 (ts)
  const out = [];
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const ts = row[1];
    if (!ts) continue;
    const d = (ts instanceof Date) ? ts : new Date(ts);
    if (isNaN(d)) continue;
    const ymd = Utilities.formatDate(d, TZ, "yyyy-MM-dd");
    if (ymd === todayYMD) out.push(row);
  }
  return out;
}




// ------------------ MARKDOWN RENDER ------------------
function buildAllMarkdown_(forceDaily) {
  const version = getMeta_("current_version") || "0.1.0";
  const now = new Date(); const ymd = Utilities.formatDate(now, TZ, "yyyy-MM-dd");

  const stageTable = CFG.STAGES.map(s=>{
    const st = getMeta_(`stage_${s.id}_status`) || "Unknown";
    const ev = getMeta_(`stage_${s.id}_evidence`) || "";
    return `| ${s.id} | ${s.name} | ${st} | [evidence](${ev}) |`;
  }).join("\n");

  const README =
`# Teevra 18 — Live Documentation (v${version})
A living record built from Google Sheets evidence + GitHub changes.

## Stage Map
| Stage | Name | Status | Evidence |
|---:|---|---|---|
${stageTable}

## What is Teevra 18?
Strategy system staged from ingestion → candles → signals → backtest/paper → alerts → dashboards → guardrails → manual-live → DhanHQ swap. See the Stage docs for acceptance details.

*This repo is auto-updated by Apps Script; do not edit generated files by hand.*`;

  const ACCEPT =
`# Acceptance Tests (Auto)
Stage | Criteria (from plan) | Status | Last Verified
----- | -------------------- | ------ | -------------
1 | Continuous ingestion, no gaps > 3s, reconnects logged | ${getMeta_("stage_1_status")||"Unknown"} | ${getMeta_("last_docs_refresh")||""}
2 | Deterministic 1m/5m candles idempotent on ts_exch+symbol | ${getMeta_("stage_2_status")||"Unknown"} | ${getMeta_("last_docs_refresh")||""}
3 | Deterministic signals, PAUSE on validator fault, RESUME cmd | ${getMeta_("stage_3_status")||"Unknown"} | ${getMeta_("last_docs_refresh")||""}
4 | ≥3 green paper days, Max DD ≤ cap; backtest ↔ paper coherent | ${getMeta_("stage_4_status")||"Unknown"} | ${getMeta_("last_docs_refresh")||""}
5 | Single alerts (idempotent nonce), commands reliable | ${getMeta_("stage_5_status")||"Unknown"} | ${getMeta_("last_docs_refresh")||""}
6 | Dashboard updates 2–5 min; EOD Telegram summary | ${getMeta_("stage_6_status")||"Unknown"} | ${getMeta_("last_docs_refresh")||""}
7 | Drift/anomaly triggers → PAUSE; chaos tests pass | ${getMeta_("stage_7_status")||"Unknown"} | ${getMeta_("last_docs_refresh")||""}
8 | 5 live sessions within risk caps; clean logs | ${getMeta_("stage_8_status")||"Unknown"} | ${getMeta_("last_docs_refresh")||""}
9 | Swap to DhanHQ via adapter; rerun Stage 1–2 acceptance | ${getMeta_("stage_9_status")||"Unknown"} | ${getMeta_("last_docs_refresh")||""}
`;

  const perStage = {};
  CFG.STAGES.forEach(s=>{
    const st = getMeta_(`stage_${s.id}_status`)||"Unknown";
    const ev = getMeta_(`stage_${s.id}_evidence`)||"";
    const notes = getMeta_(`stage_${s.id}_notes`)||"";
    perStage[`${CFG.DOCS_DIR}/stage-${s.id}-${slug_(s.name)}.md`] =
`# Stage ${s.id} — ${s.name}
**Status:** ${st}  
**Evidence:** ${ev}

## Summary
This page summarizes evidence from Sheets tabs (ticks_raw, candles_1m/5m, signals, ops_log, health, backtest_orders, paper_orders, live_journal) and maps them to the acceptance criteria defined in the Teevra 18 plan.

## Acceptance Criteria (from plan)
- See acceptance table in /docs/acceptance-tests.md

## What changed recently?
(See the daily changelog in /CHANGELOG)

## Notes
${notes || "_(none)_"}

*Auto-generated.*`;
  });

  // Daily changelog page
  const changelogPath = `${CFG.CHANGELOG_DIR}/${Utilities.formatDate(now, TZ, "yyyy-MM-dd")}.md`;
  const clogs = recentChangeLog_().map(r=>`- ${fmtDate_(r[1])} — [${r[4]}] ${r[7]} (cause: ${r[5]}) PR:${r[9]} SHA:${r[10]}`).join("\n") || "- No changes logged today.";
  const CHANGELOG =
`# ${Utilities.formatDate(now, TZ, "yyyy-MM-dd")}
Version: ${getMeta_("current_version")}

${clogs}

*Auto-generated from change_log tab.*`;

  const out = {};
  out[`${CFG.DOCS_DIR}/README.md`] = README;
  out[`${CFG.DOCS_DIR}/acceptance-tests.md`] = ACCEPT;
  Object.keys(perStage).forEach(k=> out[k] = perStage[k]);
  out[changelogPath] = CHANGELOG;
  return out;
}

// ------------------ GITHUB COMMIT ------------------
function commitMarkdownToGitHub_(filesMap, message) {
  const props = PropertiesService.getScriptProperties();
  const repo = props.getProperty("GH_REPO");
  const branch = props.getProperty("GH_BRANCH") || "main";
  if (!repo) throw new Error("Set GH_REPO in Script Properties");

  Object.keys(filesMap).forEach(path=>{
    putFile_(repo, branch, path, filesMap[path], message);
  });
}

function putFile_(repo, branch, path, content, message) {
  const existing = getFileSha_(repo, branch, path); // to update if exists
  const url = `https://api.github.com/repos/${repo}/contents/${encodeURI(path)}`;
  const payload = {
    message,
    content: Utilities.base64Encode(content, Utilities.Charset.UTF_8),
    branch
  };
  if (existing) payload.sha = existing;

  const res = UrlFetchApp.fetch(url, {
    method: "put",
    muteHttpExceptions: true,
    headers: { Authorization: "token " + PropertiesService.getScriptProperties().getProperty("GITHUB_TOKEN"),
               "User-Agent": "teevra18-autodocs" },
    payload: JSON.stringify(payload),
    contentType: "application/json"
  });
  const code = res.getResponseCode();
  if (code>=200 && code<300) {
    const obj = JSON.parse(res.getContentText());
    setMeta_("last_docs_commit_sha", obj.commit && obj.commit.sha || "");
  } else {
    throw new Error("GitHub PUT failed "+code+": "+res.getContentText());
  }
}

function getFileSha_(repo, branch, path) {
  try {
    const url = `https://api.github.com/repos/${repo}/contents/${encodeURI(path)}?ref=${branch}`;
    const res = UrlFetchApp.fetch(url, { headers: { Authorization: "token "+PropertiesService.getScriptProperties().getProperty("GITHUB_TOKEN"),
                                                    "User-Agent":"teevra18-autodocs" }});
    if (res.getResponseCode()===200) {
      const j = JSON.parse(res.getContentText());
      return j.sha;
    }
  } catch(e){ /* file not found on first run */ }
  return null;
}

// ------------------ HELPERS ------------------
function ensureTabHeaders_(name, headers) {
  const sh = getSheet_(name);
  const rng = sh.getRange(1,1,1,headers.length);
  const vals = rng.getValues()[0];
  const same = vals.length===headers.length && headers.every((h,i)=> String(vals[i]||"").trim()===h);
  if (!same) rng.setValues([headers]);
}

function hasRows_(tab, minRows) {
  const sh = SS.getSheetByName(tab);
  if (!sh) return false;
  const r = sh.getLastRow()-1; // exclude header
  return r >= (minRows||1);
}

function containsText_(tab, needle) {
  const sh = SS.getSheetByName(tab);
  if (!sh) return false;
  const txt = sh.getDataRange().getDisplayValues().flat().join(" ");
  return txt.indexOf(needle) >= 0;
}

function getSheet_(name) {
  let sh = SS.getSheetByName(name);
  if (!sh) sh = SS.insertSheet(name);
  return sh;
}

function appendRow_(tab, row) {
  const sh = getSheet_(tab);
  sh.appendRow(row);
}

function getMeta_(key) {
  const sh = getSheet_("doc_meta");
  const data = sh.getDataRange().getValues();
  for (let i=1;i<data.length;i++){
    if ((data[i][0]||"")===key) return data[i][1]||"";
  }
  return "";
}
function setMeta_(key, value){
  const sh = getSheet_("doc_meta");
  const data = sh.getDataRange().getValues();
  for (let i=1;i<data.length;i++){
    if ((data[i][0]||"")===key){ sh.getRange(i+1,2).setValue(value); sh.getRange(i+1,4).setValue(new Date()); return; }
  }
  sh.appendRow([key, value, "", new Date()]);
}

function getCfg_(key){
  const sh = SS.getSheetByName("config");
  if (!sh) return "";
  const data = sh.getDataRange().getValues();
  for (let i=1;i<data.length;i++){
    if ((data[i][0]||"")===key) return data[i][1]||"";
  }
  return "";
}

function bumpVersionFromChangeType_(type, breaking){
  // semver: major.minor.patch
  const cur = (getMeta_("current_version") || "0.1.0").split(".").map(x=>parseInt(x,10));
  if (breaking) { cur[0]++; cur[1]=0; cur[2]=0; }
  else if (/^feat/i.test(type||"")) { cur[1]++; cur[2]=0; }
  else { cur[2]++; }
  const v = cur.join(".");
  setMeta_("current_version", v);
  return v;
}

function slug_(s){ return String(s).toLowerCase().replace(/[^a-z0-9]+/g,"-"); }
function fmtDate_(d){ return Utilities.formatDate(new Date(d), TZ, "yyyy-MM-dd HH:mm"); }

// Optional: Telegram notify (if you set TELEGRAM vars)
function notify_(text){
  const p = PropertiesService.getScriptProperties();
  const tok = p.getProperty("TELEGRAM_BOT_TOKEN");
  const chat = p.getProperty("TELEGRAM_CHAT_ID");
  if (!tok || !chat) return;
  const url = `https://api.telegram.org/bot${tok}/sendMessage`;
  UrlFetchApp.fetch(url, {method:"post", payload:{ chat_id: chat, text }});
}
