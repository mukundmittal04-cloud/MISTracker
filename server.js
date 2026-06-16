// ============================================================
// FIDATO MIS SERVER v2.8.19 — master control panel at /api/panel (tabs+buttons behind Basic Auth lock). Base: v2.8.18 endpoint lock.
// v2.8.16 — clear stale Chromium SingletonLock on boot so redeploys self-heal (volume no longer causes 'profile in use' Code 21)
// Adds: smart first-message parsing (extracts company/account from free-form text),
//       multi-amount detection that forces one-at-a-time discipline,
//       vision read confirmation before posting (kills filename-misread bugs),
//       SHOW + EDIT mid-flow commands.
// All v2.6 features preserved unchanged (reverse-scan, MORE commands, top-N report).
// ============================================================
const express = require('express');
const { google } = require('googleapis');
const fetch = require('node-fetch');
const cron = require('node-cron');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const puppeteer = require('puppeteer');
const fs = require('fs');
const app = express();
app.use(express.json());
const CONFIG = {
  SHEET_ID: process.env.SHEET_ID || '1JDoDEk2smAJu0S3RO1WLPZ4MzGZD-_Kn1pP9K8U0J5w',
  GOOGLE_CREDENTIALS: process.env.GOOGLE_CREDENTIALS,
  WHATSAPP_GROUP_JID: process.env.WHATSAPP_GROUP_JID || '120363425432126351@g.us',
  APPROVAL_GROUP_JID: process.env.APPROVAL_GROUP_JID || '120363408304471879@g.us',
  BOT_ENABLED: process.env.BOT_ENABLED !== 'false',
  CLAUDE_API_KEY: process.env.CLAUDE_API_KEY,
  PORT: process.env.PORT || 3000,
  MM_PHONE: '919873095398',
  SM_PHONE: '919873429794',
  ACCOUNTANT_PHONES: ['919873574112','919873574180','919873574192','919873574103','919773592304'],
  TEST_PHONES: ['917838537000'],
  LID_WHITELIST: ['86960253214761@lid'],
  MM_NAMES: ['madhur', 'madhur mittal'],
  SM_NAMES: ['sumit', 'sumit mittal'],
};

// ── v2.6 NEW: Reverse-scan + report tuning constants ─────────────────────────
// Floor amount below which Ledger OUT entries are ignored (treated as petty cash).
var REVERSE_SCAN_MIN_AMOUNT = parseInt(process.env.REVERSE_SCAN_MIN_AMOUNT) || 50000;
// Reverse-scan window in days (default 3).
var REVERSE_SCAN_WINDOW_DAYS = parseInt(process.env.REVERSE_SCAN_WINDOW_DAYS) || 3;
// Top-N caps for the daily report (each section shows this many; rest rolled up).
var REPORT_TOP_N = parseInt(process.env.RECON_TOP_N) || 3;
var STALE_TOP_N = parseInt(process.env.STALE_TOP_N) || 3;
// "Recent" window for stale section in hours (default 72h).
var STALE_RECENT_HOURS = parseInt(process.env.STALE_RECENT_HOURS) || 72;

// Recurring-vendor patterns — Ledger entries matching these are suppressed from
// the "payment without approval" anomaly list. Tune by adding patterns.
var RECURRING_PATTERNS = [
  /bank charges?/i, /\btds\b/i,
  /(mm|sm)\s+drawing/i, /\bmm\s+pdc\b/i, /\bsm\s+pdc\b/i, /drawing\s+(mm|sm)/i,
  /\bpf\b/i, /\besic?\b/i,
  /cash\s*withdrawal/i, /internal transfer/i, /\bcontra\b/i,
  /electricity\s+bill/i, /water\s+bill/i, /property\s+tax/i,
];
// v2.8 Module 6: salary and GST removed from suppression — they always need an
// approval, so an unapproved salary/GST payment is flagged like any other.
// EMI rule: "auto debit" in the description => informational auto-debit (suppressed,
// highlighted separately). EMI WITHOUT "auto debit" => manual payment that needed
// approval => flagged if no approval found.
var EMI_PATTERN = /\bcar\s*emi\b|\bhome\s*loan\b|\bemi\b/i;
var AUTO_DEBIT_PATTERN = /auto\s*-?\s*debit|\becs\b|\bnach\b|standing\s+instruction/i;
function isAutoDebitEntry(le){
  if(!le) return false;
  var text = ((le.description||'') + ' ' + (le.head||'') + ' ' + (le.tag||'') + ' ' + (le.person||'')).toLowerCase();
  return AUTO_DEBIT_PATTERN.test(text);
}
function isRecurringPattern(le) {
  if(!le) return false;
  var text = ((le.description||'') + ' ' + (le.head||'') + ' ' + (le.tag||'') + ' ' + (le.person||'')).toLowerCase();
  if(EMI_PATTERN.test(text)) return AUTO_DEBIT_PATTERN.test(text);
  for(var i=0; i<RECURRING_PATTERNS.length; i++){
    if(RECURRING_PATTERNS[i].test(text)) return true;
  }
  return false;
}

// ── Google Sheets ─────────────────────────────────────────────────────────────
var sheetsApi = null;
function initGoogleSheets() {
  if (!CONFIG.GOOGLE_CREDENTIALS) { console.log('No GOOGLE_CREDENTIALS.'); return; }
  try {
    var creds = JSON.parse(CONFIG.GOOGLE_CREDENTIALS);
    var auth = new google.auth.GoogleAuth({ credentials: creds, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
    sheetsApi = google.sheets({ version: 'v4', auth: auth });
    console.log('Google Sheets API initialized.');
  } catch (e) { console.error('Sheets init failed:', e.message); }
}
async function readSheet(range) {
  if (!sheetsApi) throw new Error('Google Sheets not initialized');
  var r = await sheetsApi.spreadsheets.values.get({ spreadsheetId: CONFIG.SHEET_ID, range: range });
  return r.data.values || [];
}
// ── WhatsApp ──────────────────────────────────────────────────────────────────
var waClient = null, waReady = false, latestQR = null, latestQRDataUrl = null;
function clearStaleChromiumLocks() {
  // v2.8.16: with a persistent volume, a redeploy leaves the previous container's
  // Chromium lock files behind. The new container then refuses to launch
  // ("profile appears to be in use", Code 21). These lock files are safe to
  // delete on boot — they are NOT the session; the WhatsApp auth lives in
  // ./wa_auth/session/Default and is preserved. This self-heals every redeploy.
  var sessionDir = './wa_auth/session';
  var lockNames = ['SingletonLock','SingletonSocket','SingletonCookie'];
  try {
    lockNames.forEach(function(name){
      var p = sessionDir + '/' + name;
      try { if (fs.existsSync(p) || fs.lstatSync(p)) { fs.rmSync(p, { force: true }); console.log('[WA] cleared stale lock', name); } }
      catch(e){ /* lstat throws if absent — ignore */ }
    });
    // Also clear the Default-profile lock if present.
    ['./wa_auth/session/Default/SingletonLock'].forEach(function(p){
      try { fs.rmSync(p, { force: true }); } catch(e){}
    });
  } catch(e) { console.error('[WA] lock cleanup:', e.message); }
}
function createWhatsAppClient() {
  clearStaleChromiumLocks();
  waClient = new Client({
    authStrategy: new LocalAuth({ dataPath: './wa_auth' }),
    puppeteer: { headless: true, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-accelerated-2d-canvas','--no-first-run','--no-zygote','--single-process','--disable-gpu','--disable-extensions'] },
  });
  waClient.on('qr', function(qr) { latestQR = qr; qrcode.toDataURL(qr, function(err, url) { if (!err) latestQRDataUrl = url; }); console.log('QR generated.'); });
  waClient.on('ready', function() { waReady = true; latestQR = null; latestQRDataUrl = null; console.log('WhatsApp ready!'); });
  waClient.on('authenticated', function() { console.log('WhatsApp authenticated.'); });
  waClient.on('auth_failure', function(msg) { console.error('Auth failure:', msg); waReady = false; });
  waClient.on('disconnected', function(reason) {
    console.log('[WA] Disconnected:', reason);
    waReady = false;
    if (!global._waLogoutLog) global._waLogoutLog = [];
    var now = Date.now();
    if (reason === 'LOGOUT') {
      global._waLogoutLog.push(now);
      global._waLogoutLog = global._waLogoutLog.filter(function(t){ return now - t < 5*60*1000; });
      console.log('[WA] LOGOUT count in 5min window:', global._waLogoutLog.length);
      if (global._waLogoutLog.length >= 3) {
        console.log('[WA] 3+ LOGOUTs in 5min — clearing wa_auth and restarting fresh');
        global._waLogoutLog = [];
        setTimeout(function() {
          try { if (fs.existsSync('./wa_auth')) { fs.rmSync('./wa_auth', { recursive: true, force: true }); console.log('[WA] wa_auth cleared'); } } catch(e) { console.error('[WA] Clear failed:', e.message); }
          createWhatsAppClient();
        }, 5000);
        return;
      }
    }
    setTimeout(function() {
      try { waClient.initialize().catch(function(e) { console.error('[WA] reinit failed:', e.message); }); }
      catch(e) { console.error('[WA] reinit threw:', e.message); }
    }, 10000);
  });
  waClient.on('message', function(msg) {
    handlePromoterVerdicts(msg).then(function(handled){
      if(!handled) return handleAccountantDM(msg);
    }).catch(function(e){ console.error('[Msg handler]', e.message); });
  });
  waClient.initialize().catch(function(e) { console.error('WA init failed:', e.message); });
}
// ── Puppeteer ─────────────────────────────────────────────────────────────────
var browserInstance = null;
async function htmlToImage(html, width, height) {
  if (!browserInstance) { browserInstance = await puppeteer.launch({ headless: 'new', args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage'] }); }
  var page = await browserInstance.newPage();
  await page.setViewport({ width: width || 800, height: height || 600 });
  await page.setContent(html, { waitUntil: 'networkidle0' });
  var body = await page.$('body');
  var box = await body.boundingBox();
  var shot = await page.screenshot({ clip: { x: 0, y: 0, width: box.width, height: box.height }, type: 'png' });
  await page.close();
  return shot;
}
// ── Helpers ───────────────────────────────────────────────────────────────────
function parseSheetDate(val) {
  if (!val) return null; if (val instanceof Date) return val;
  var s = val.toString().trim(), p = s.split(/[\/\.\-]/);
  if (p.length === 3) { var d=parseInt(p[0]),m=parseInt(p[1]),y=parseInt(p[2]); if(y<100)y+=2000; if(d>0&&d<=31&&m>0&&m<=12) return new Date(y,m-1,d); }
  var x = new Date(val); return isNaN(x.getTime()) ? null : x;
}
function parseAmount(val) { if(typeof val==='number')return val; if(!val)return 0; var n=parseFloat(String(val).replace(/,/g,'').replace(/[^0-9.\-]/g,'')); return isNaN(n)?0:n; }
function formatINR(num) {
  if(!num)return '0'; var neg=num<0; num=Math.abs(Math.round(num)); var s=num.toString();
  var l=s.substring(s.length-3), o=s.substring(0,s.length-3);
  if(o!=='')l=','+l; return (neg?'-':'')+o.replace(/\B(?=(\d{2})+(?!\d))/g,',')+l;
}
// ── Sender identification ─────────────────────────────────────────────────────
async function identifySender(rawSender) {
  var role = 'unknown', contactName = '';
  try {
    var contact = await waClient.getContactById(rawSender);
    if (contact) {
      contactName = (contact.pushname || contact.name || contact.shortName || '').toLowerCase().trim();
      for (var i = 0; i < CONFIG.MM_NAMES.length; i++) {
        if (contactName === CONFIG.MM_NAMES[i] || contactName.indexOf(CONFIG.MM_NAMES[i]) >= 0) {
          var isSM = false;
          for (var s = 0; s < CONFIG.SM_NAMES.length; s++) { if (contactName === CONFIG.SM_NAMES[s]) { isSM = true; break; } }
          if (!isSM) { role = 'mm'; break; }
        }
      }
      if (role === 'unknown') {
        for (var j = 0; j < CONFIG.SM_NAMES.length; j++) {
          if (contactName === CONFIG.SM_NAMES[j] || contactName.indexOf(CONFIG.SM_NAMES[j]) >= 0) { role = 'sm'; break; }
        }
      }
    }
  } catch (e) { /* skip */ }
  return { role: role, contactName: contactName };
}
// ── Response parsing ──────────────────────────────────────────────────────────
function parseResponse(text) {
  if(!text)return 'pending'; var l=text.toLowerCase().trim();
  var qWords=['advance to','for whom','kis ke liye','kya','kab','kaun','kitna'];
  for(var q=0;q<qWords.length;q++){if(l.indexOf(qWords[q])>=0)return 'question';}
  if((l.indexOf('?')>=0)&&l.length<40)return 'question';
  var yesExact=['yes','ok','okay','o','approved','done','haan','ha','han','theek hai','thik hai','kar do','karo','y','yep','yea','yeah','sure','fine','agreed','confirmed','sahi hai','bilkul'];
  for(var i=0;i<yesExact.length;i++){if(l===yesExact[i])return 'yes';}
  if(l.indexOf('\u{1F44D}')>=0||l.indexOf('\u2705')>=0||l.indexOf('\u{1F44C}')>=0)return 'yes';
  var yesContains=['ok','okay','approved','haan','theek','kar do','go ahead','proceed','done'];
  for(var ic=0;ic<yesContains.length;ic++){if(l.indexOf(yesContains[ic])>=0)return 'yes';}
  var no=['no','nahi','nah','rejected','cancel','mat karo','nope','deny','denied','reject','nhi','mat','band karo'];
  for(var j=0;j<no.length;j++){if(l===no[j]||l.indexOf(no[j])>=0)return 'no';}
  if(l.indexOf('\u274C')>=0||l.indexOf('\u{1F44E}')>=0)return 'no';
  var hold=['hold','wait','ruko','later','baad mein','not now','pending','rukko','abhi nahi','bad me','kal'];
  for(var k=0;k<hold.length;k++){if(l===hold[k]||l.indexOf(hold[k])>=0)return 'hold';}
  return 'other';
}
// ── Expense message parsing ───────────────────────────────────────────────────
function extractLineAmount(line, strict) {
  if(!line) return 0;
  var am=line.match(/(\d[\d,]*\.?\d*)\s*(?:lakhs?|lacs?|l\b|cr|crore)/i);
  if(am){var a=parseFloat(am[1].replace(/,/g,'')); return /cr|crore/i.test(am[0])?a*10000000:a*100000;}
  var km=line.match(/(\d[\d,]*\.?\d*)\s*k\b/i);
  if(km) return parseFloat(km[1].replace(/,/g,''))*1000;
  var tm=line.match(/(\d[\d,]*\.?\d*)\s*(?:thousands?|hazaar|hazar)\b/i);
  if(tm) return parseFloat(tm[1].replace(/,/g,''))*1000;
  var hm=line.match(/(\d[\d,]*\.?\d*)\s*hundreds?\b/i);
  if(hm) return parseFloat(hm[1].replace(/,/g,''))*100;
  var pm=line.match(/(\d[\d,]{3,})\s*\/\s*\-?/);
  if(pm) return parseFloat(pm[1].replace(/,/g,''));
  var rm=line.match(/(?:rs\.?\s*|inr\s*|\u20B9\s*)(\d[\d,]*\.?\d*)/i);
  if(rm) return parseFloat(rm[1].replace(/,/g,''));
  if(strict) return 0;
  var lm = line.match(/\b(\d{1,3}(?:,\d{2,3}){1,3})\b/);
  if(lm){
    var v = parseFloat(lm[1].replace(/,/g,''));
    if(v >= 100 && v < 1000000000) return v;
  }
  if(/\d{1,2}[\-\/](?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|\d{1,2})[\-\/]\d{2,4}/i.test(line)) return 0;
  var lm2 = line.match(/\b(\d{4,9})\b/);
  if(lm2){
    var v2 = parseFloat(lm2[1]);
    if(v2 >= 100 && v2 < 1000000000){
      if(lm2[1].length === 10 && /^[6-9]/.test(lm2[1])) return 0;
      if(lm2[1].length === 4 && v2 >= 1900 && v2 <= 2100){
        if(!/(amount|paid|payment|approve|due|invoice|bill|expense|cost|fee|rs|inr|\u20B9|\$|lac|lakh|cr|crore|k\b)/i.test(line)) return 0;
      }
      return v2;
    }
  }
  return 0;
}
function extractLineVendor(line) {
  return line
    .replace(/(\d[\d,]*\.?\d*)\s*(?:lakhs?|lacs?|l\b|cr|crore)/i, '')
    .replace(/(\d[\d,]*\.?\d*)\s*k\b/i, '')
    .replace(/(\d[\d,]{3,})\s*\/\s*\-?/, '')
    .replace(/(?:rs\.?\s*|inr\s*|\u20B9\s*)(\d[\d,]*\.?\d*)/i, '')
    .replace(/^\s*(please approve|kindly approve|approve|for|to|on account of|on account|payment to|pay to)\s*/i, '')
    .replace(/\s+/g,' ').trim();
}
// v2.7 NEW: clean up a free-form expense message into a tidy "Details" string.
// Drops field-prefix lines (Amount:/Company:/From:/etc), strips the approval
// preamble ("kindly approve 10 lakh rs for"), removes leftover amount/currency
// tokens, and collapses whitespace. Keeps the meaningful purpose text.
function cleanDetails(body) {
  if(!body) return '';
  var lines = body.split('\n').map(function(l){ return l.trim(); }).filter(Boolean);
  var kept = [];
  lines.forEach(function(line){
    // Skip lines that are purely a structured field we capture elsewhere
    if(/^\s*(amount|company|from|account|a\/c|bank)\s*[:\-]/i.test(line)) return;
    kept.push(line);
  });
  var text = kept.join(' ');
  // Strip leading approval-request phrases (repeat to catch "please kindly approve")
  for(var i=0;i<3;i++){
    text = text.replace(/^\s*(please|kindly|pls|plz|request(?:ing)?(?:\s+(?:you|u))?|i\s+request|sir|ji)\s+/i, '');
    text = text.replace(/^\s*(approve|approval|pay|payment|release|sanction)\s+/i, '');
  }
  // Remove amount + unit tokens. Units ordered LONGEST-FIRST so "crore" is not
  // partially matched as "cr" (which used to leave "ore"). Covers crore(s)/cr,
  // lakh(s)/lac(s), thousand(s), hundred(s), hazaar/hazar (Hindi), k, l.
  // Repeated twice to catch compound amounts like "2 crore 50 lakh".
  for(var u=0;u<2;u++){
    text = text
      .replace(/(?:rs\.?|inr|\u20B9)\s*\d[\d,]*\.?\d*\s*(?:crores?|cr|lakhs?|lacs?|thousands?|hundreds?|hazaar|hazar|k|l)?\/?\-?/gi, '')
      .replace(/\b\d[\d,]*\.?\d*\s*(?:crores?|cr|lakhs?|lacs?|thousands?|hundreds?|hazaar|hazar)\b/gi, '')
      .replace(/\b\d+\.?\d*\s*l\b/gi, '')
      .replace(/\b\d[\d,]*\.?\d*\s*k\b/gi, '')
      .replace(/\b\d{1,3}(?:,\d{2,3}){1,3}\s*\/?\-?/g, '')
      .replace(/\b\d{4,9}\b\s*\/?\-?/g, '');
  }
  text = text.replace(/\brs\b\.?/gi, '');
  // Tidy connector words left dangling at the start ("for legal expenses" -> "legal expenses")
  for(var j=0;j<3;j++){
    text = text.replace(/^\s*(payment|paid|pay)\s+/i, '');
    text = text.replace(/^\s*(for|to|towards|of|on account of|on account|against)\s+/i, '');
  }
  // Collapse whitespace and stray punctuation
  text = text.replace(/\s{2,}/g, ' ').replace(/\s+([,.])/g, '$1').replace(/^[\s,.\-]+|[\s,.\-]+$/g, '').trim();
  // Capitalize first letter for a tidy look
  if(text.length > 0) text = text.charAt(0).toUpperCase() + text.slice(1);
  return text.substring(0, 250);
}
function parseExpenseMessage(body) {
  if(!body) return [{vendor:'',amount:0}];
  // v2.8.13: strip a leading WhatsApp @number mention (e.g. "@77975215149179 ")
  // so free-text "pay" requests posted straight to the group still parse cleanly.
  body = body.replace(/^\s*@\d{6,}\s*/,'').trim();
  // v2.8: the accountant bot posts a structured "*EXPENSE REQUEST*" block.
  // Read its Details:/Amount: lines instead of grabbing the header as vendor.
  if(/^\s*\*?EXPENSE REQUEST\*?/i.test(body)){
    var dl = body.match(/Details:\s*(.+)/i);
    var subRe = /^\s*-\s*(.+?)\s+Rs\.?\s*([\d,\.]+)/gim, subM, subs=[];
    while((subM = subRe.exec(body))!==null){ subs.push({vendor:subM[1].trim(), amount:parseAmount(subM[2])}); }
    if(subs.length>1) return subs;
    var am = body.match(/Amount:\s*Rs\.?\s*([\d,\.]+)/i);
    var vend = dl ? dl[1].trim() : '';
    var amt = am ? parseAmount(am[1]) : 0;
    if(!amt){ // fall through to scan if Amount line missing
      var t2=0; body.split('\n').forEach(function(l){ t2+=extractLineAmount(l,false); });
      amt=t2;
    }
    return [{vendor:vend.substring(0,150), amount:amt}];
  }
  var lines=body.split('\n').map(function(l){return l.trim();}).filter(Boolean);
  var itemLines=[];
  for(var i=0;i<lines.length;i++){
    var a=extractLineAmount(lines[i], true);
    if(a>0){var v=extractLineVendor(lines[i]); if(v&&v.length>1)itemLines.push({vendor:v,amount:a});}
  }
  if(itemLines.length>1) return itemLines;
  // v2.8.13: handle inline sums like "pay 31,200+5,000" on a single line — add the
  // parts rather than capturing only the first number.
  var total=0;
  for(var j=0;j<lines.length;j++){
    var plusMatch = lines[j].match(/(\d[\d,]*)\s*\+\s*(\d[\d,]*)/);
    if(plusMatch){
      total += parseAmount(plusMatch[1]) + parseAmount(plusMatch[2]);
    } else {
      total += extractLineAmount(lines[j], false);
    }
  }
  var rawVendor = extractLineVendor(lines[0]) || lines[0].substring(0,150);
  var vendor = (typeof cleanDetails==='function') ? (cleanDetails(rawVendor)||rawVendor) : rawVendor;
  return [{vendor:vendor,amount:total}];
}
// ── Vision (image + PDF) ──────────────────────────────────────────────────────
const visionCache = new Map();
async function extractFromImage(media, msgId) {
  if (msgId && visionCache.has(msgId)) return visionCache.get(msgId);
  if (!CONFIG.CLAUDE_API_KEY) { console.error('[Vision] CLAUDE_API_KEY missing'); return null; }
  if (!media || !media.data) return null;
  var mime = (media.mimetype || '').toLowerCase();
  if (mime.indexOf('image/') !== 0 && mime !== 'application/pdf') return null;
  try {
    var isPDF = mime === 'application/pdf';
    var mediaBlock = isPDF
      ? { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: media.data } }
      : { type: 'image', source: { type: 'base64', media_type: mime, data: media.data } };
    var prompt = isPDF
      ? 'This PDF is attached to an expense approval request. It is likely an invoice, PO, bill, or payment challan. Extract: (1) vendor/payee name, (2) total amount in INR as a number, (3) brief purpose max 10 words, (4) amountCount = how many DISTINCT separate payable amounts/invoices appear (1 for a single bill, 2+ if multiple separate bills or payment items). Do NOT treat a single itemized invoice as multiple — that is amountCount 1 with the grand total. Reply ONLY with JSON on one line: {"vendor":"","amount":0,"purpose":"","imageType":"invoice","confidence":"high","amountCount":1}.'
      : 'This image is attached to an expense approval request. Classify it: imageType = "cheque" (any bank cheque even cancelled), "invoice" (printed bill), "receipt", "screenshot", or "other". For CHEQUES: set vendor to "" and amount to 0 — they are shared as bank reference only, not expense amounts. For printed INVOICES/RECEIPTS: extract vendor, total amount in INR, and purpose. Also set amountCount = how many DISTINCT separate payable amounts/invoices appear in the image (1 for a single bill, 2+ if it shows multiple separate bills or a list of multiple payments). A single itemized invoice with line items is amountCount 1 (the grand total). Set confidence to "high" if clearly printed, "low" if handwritten or blurry. Reply ONLY with JSON on one line: {"vendor":"","amount":0,"purpose":"","imageType":"cheque","confidence":"low","amountCount":1}.';
    var resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': CONFIG.CLAUDE_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 300, messages: [{ role: 'user', content: [mediaBlock, { type: 'text', text: prompt }] }] })
    });
    if (!resp.ok) { console.error('[Vision] API error', resp.status); return null; }
    var data = await resp.json();
    var text = '';
    if (data.content) { for (var i=0;i<data.content.length;i++) { if(data.content[i].type==='text'){text=data.content[i].text;break;} } }
    if (!text) return null;
    text = text.replace(/```json|```/g, '').trim();
    var parsed; try { parsed = JSON.parse(text); } catch(e) { var m=text.match(/\{[^}]*\}/); if(m){try{parsed=JSON.parse(m[0]);}catch(e2){return null;}} else return null; }
    var result = { vendor:(parsed.vendor||'').toString().substring(0,150), amount:parseAmount(parsed.amount), purpose:(parsed.purpose||'').toString().substring(0,200), imageType:parsed.imageType||'other', confidence:parsed.confidence||'low', amountCount:parseInt(parsed.amountCount)||1 };
    if (msgId) visionCache.set(msgId, result);
    console.log('[Vision] Parsed', msgId, '->', JSON.stringify(result));
    return result;
  } catch (e) { console.error('[Vision] Exception:', e.message); return null; }
}
// ── Approval audit ────────────────────────────────────────────────────────────
async function fetchApprovalMessages(days) {
  if(!waReady||!waClient)throw new Error('WhatsApp not connected');
  var chat = await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);
  var allMessages = [];
  var limits = [100,200,500,1000];
  for(var i=0;i<limits.length;i++){
    try{allMessages=await chat.fetchMessages({limit:limits[i]}); if(allMessages.length<limits[i])break;}catch(e){break;}
  }
  var cutoff=new Date(); cutoff.setDate(cutoff.getDate()-(days||15));
  return allMessages.filter(function(m){return new Date(m.timestamp*1000)>=cutoff;});
}
async function buildApprovalAudit(days) {
  var messages = await fetchApprovalMessages(days||15);
  var expenses = [], replyMap = {};
  var questionMessages = {};
  var answerMap = {};
  var digestQuotedApprovals = []; // v2.8.6: swipe-replies that quoted a digest/reminder
  for(var i=0;i<messages.length;i++){
    var msg=messages[i];
    var rawSender=msg.author||msg.from||'';
    var msgDate=new Date(msg.timestamp*1000);
    var body=(msg.body||'').trim();
    var hasMedia=msg.hasMedia||false;
    var senderInfo = await identifySender(rawSender);
    var thisMsgId = msg.id._serialized||msg.id.id;
    var quotedMsgId=null, quotedBody=null;
    if(msg.hasQuotedMsg){try{var q=await msg.getQuotedMessage();quotedMsgId=q.id._serialized||q.id.id;quotedBody=q.body||'';}catch(e){}}
    // v2.8.5: if the reply quotes one of the bot's own digest/reminder messages, the
    // promoter is approving the expense(s) that message was ABOUT — redirect the
    // quotedMsgId to the real expense id so a swipe-"ok" on the reminder registers.
    if(quotedMsgId && quotedBody && /PENDING APPROVALS|🔔|\[BOT REMINDER\]|⚡\s*URGENT|URGENT\s*—\s*approval needed/i.test(quotedBody)){
      // Defer resolution: stash the approval with the quoted digest text, and after all
      // expenses are built, match it to a live expense by content (id-drift safe).
      digestQuotedApprovals.push({ role: senderInfo.role, resp: parseResponse(body), raw: body, date: msgDate, name: senderInfo.contactName, quotedBody: quotedBody });
      quotedMsgId = null; // don't process via the normal id path
    }
    if(quotedMsgId){
      var resp=parseResponse(body);
      if(questionMessages[quotedMsgId]){
        var qInfo = questionMessages[quotedMsgId];
        var answerFromOtherPromoter = (qInfo.role==='mm' && senderInfo.role==='sm') || (qInfo.role==='sm' && senderInfo.role==='mm');
        var answerFromAccountant = senderInfo.role!=='mm' && senderInfo.role!=='sm';
        if(answerFromOtherPromoter || answerFromAccountant){
          var promoterReplyShort = answerFromOtherPromoter && (parseResponse(body)==='yes' || parseResponse(body)==='no' || parseResponse(body)==='hold');
          if(!promoterReplyShort){
            answerMap[qInfo.expenseId] = {
              role: qInfo.role,
              question: qInfo.question,
              questionDate: qInfo.date,
              answer: body,
              answerDate: msgDate,
              answerBy: senderInfo.contactName || rawSender,
              answerByRole: answerFromOtherPromoter ? (senderInfo.role==='mm' ? 'M' : 'S') : 'accountant'
            };
            continue;
          }
        }
      }
      if(!replyMap[quotedMsgId])replyMap[quotedMsgId]={mm:null,sm:null};
      if(senderInfo.role==='mm'){
        replyMap[quotedMsgId].mm={response:resp,date:msgDate,raw:body,name:senderInfo.contactName,msgId:thisMsgId};
        if(resp==='question'){
          questionMessages[thisMsgId] = { expenseId: quotedMsgId, role: 'mm', question: body, date: msgDate, name: senderInfo.contactName };
        }
      } else if(senderInfo.role==='sm'){
        replyMap[quotedMsgId].sm={response:resp,date:msgDate,raw:body,name:senderInfo.contactName,msgId:thisMsgId};
        if(resp==='question'){
          questionMessages[thisMsgId] = { expenseId: quotedMsgId, role: 'sm', question: body, date: msgDate, name: senderInfo.contactName };
        }
      }
    } else {
      // v2.8: never ingest the bot's own posts as expenses (digest, urgent, reminder).
      if(body.indexOf('[BOT REMINDER]')===0){continue;}
      if(body.indexOf('🔔')===0 || /PENDING APPROVALS/i.test(body)){continue;}
      if(/^\s*\*?URGENT\b/i.test(body) && /please review/i.test(body)){continue;}
      if(/^\s*Recorded from [MS]:/i.test(body)){continue;}
      if(senderInfo.role!=='mm'&&senderInfo.role!=='sm'){
        var msgId=thisMsgId;
        var parsedItems=parseExpenseMessage(body);
        var vendor=parsedItems[0].vendor, amount=parsedItems[0].amount, purpose='', visionResult=null;
        var subItems=parsedItems.length>1?parsedItems:null;
        if(hasMedia){
          try{
            var media=await msg.downloadMedia();
            if(media&&media.data){
              visionResult=await extractFromImage(media,msgId);
              if(visionResult){
                var isCheque=visionResult.imageType==='cheque', isLow=visionResult.confidence==='low';
                if(!isCheque){
                  if(amount===0 && visionResult.amount>0) amount=visionResult.amount;
                  if(!isLow && (!vendor||body.length<15) && visionResult.vendor) vendor=visionResult.vendor;
                  if(!isLow && visionResult.purpose) purpose=visionResult.purpose;
                }
              }
            }
          }catch(e){console.error('[Vision] Failed for',msgId,e.message);}
        }
        // v2.8.17: only capture as an expense if it is a structured EXPENSE REQUEST,
        // OR carries a real amount, OR is media that vision parsed into an amount.
        // Zero-amount free text (leave notes, "From X account" orphans, bare images
        // with no parse) is NOT an expense and is skipped at intake.
        var isExpenseRequest = /^\s*\*?EXPENSE REQUEST\*?/i.test(body);
        var hasParsedMediaAmount = hasMedia && visionResult && visionResult.amount > 0;
        var capture = isExpenseRequest || amount > 0 || hasParsedMediaAmount;
        if(capture){
          expenses.push({id:msgId,date:msgDate,body:body||(hasMedia?'[Image attached]':'[Empty]'),sender:senderInfo.contactName||rawSender,vendor:vendor||(hasMedia?'[See image]':''),amount:amount,purpose:purpose,subItems:subItems,hasMedia:hasMedia,visionParsed:visionResult?true:false,mmApproval:null,smApproval:null,status:{mm:'pending',sm:'pending'},queryAnswer:null});
        }
      }
    }
  }
  // v2.8.11: resolve bot-post-quoted replies against the actual expenses now that
  // they're all built. Parse the quoted post into its item(s); resolve each item by
  // label words + amount. Digest quotes apply to every item they listed; single-item
  // posts (reminder/urgent) must resolve to exactly one expense or are skipped.
  // Only clear verdicts (yes/no/hold) are applied retroactively from history.
  var DQ_STOP = ['the','and','for','with','from','this','that','please','approve','kindly','payment','amount','pending','approvals','approval','request','requests','needs','reply','number','yes','hold','later','reject','rs','inr','total','both'];
  function dqMatchExpenses(label, amount){
    var w = (label||'').toLowerCase().replace(/…\s*$/,'').split(/[^a-z0-9]+/).filter(function(x){ return x.length>=4 && DQ_STOP.indexOf(x)<0; });
    if(!w.length) return [];
    var cands = expenses.filter(function(e){
      var ev = ((e.vendor||'')+' '+(e.body||'')).toLowerCase();
      return w.some(function(word){ return ev.indexOf(word)>=0; });
    });
    if(cands.length>1 && amount>0){
      var nar = cands.filter(function(e){ return e.amount===amount; });
      if(nar.length) cands = nar;
    }
    return cands;
  }
  digestQuotedApprovals.forEach(function(dq){
    if(dq.role!=='mm' && dq.role!=='sm') return;
    if(dq.resp!=='yes' && dq.resp!=='no' && dq.resp!=='hold') return;
    var post = parseBotPostItems(dq.quotedBody);
    var resolvedIds = [];
    if(post && post.items && post.items.length){
      post.items.forEach(function(qi){
        var c = dqMatchExpenses(qi.label, qi.amount);
        if(c.length===1 && resolvedIds.indexOf(c[0].id)<0) resolvedIds.push(c[0].id);
      });
    } else {
      // Fallback (unparseable bot post): whole-body words, require exactly one match.
      var qWords = (dq.quotedBody||'').toLowerCase().split(/[^a-z0-9]+/).filter(function(w){ return w.length>=4 && DQ_STOP.indexOf(w)<0; });
      if(qWords.length){
        var matches = expenses.filter(function(e){
          var ev = ((e.vendor||'')+' '+(e.body||'')).toLowerCase();
          return qWords.some(function(w){ return ev.indexOf(w)>=0; });
        });
        if(matches.length>1){
          var amtM = (dq.quotedBody||'').match(/rs\.?\s*([\d,]+)/ig) || [];
          var qAmts = amtM.map(function(s){ return parseAmount(s); }).filter(function(n){ return n>0; });
          if(qAmts.length){
            var narrowed = matches.filter(function(e){ return qAmts.indexOf(e.amount)>=0; });
            if(narrowed.length===1) matches = narrowed;
          }
        }
        if(matches.length===1) resolvedIds.push(matches[0].id);
      }
    }
    resolvedIds.forEach(function(eid){
      if(!replyMap[eid]) replyMap[eid]={mm:null,sm:null};
      if(dq.role==='mm' && !replyMap[eid].mm) replyMap[eid].mm={response:dq.resp,date:dq.date,raw:dq.raw,name:dq.name,viaDigest:true};
      if(dq.role==='sm' && !replyMap[eid].sm) replyMap[eid].sm={response:dq.resp,date:dq.date,raw:dq.raw,name:dq.name,viaDigest:true};
    });
  });
  for(var j=0;j<expenses.length;j++){
    var rep=replyMap[expenses[j].id];
    if(rep){expenses[j].mmApproval=rep.mm;expenses[j].smApproval=rep.sm;expenses[j].status.mm=rep.mm?rep.mm.response:'pending';expenses[j].status.sm=rep.sm?rep.sm.response:'pending';}
    if(answerMap[expenses[j].id]){
      expenses[j].queryAnswer = answerMap[expenses[j].id];
    }
  }
  function descriptionsRelate(visionData, textBody) {
    if(!visionData || !textBody) return false;
    var visionText = ((visionData.purpose||'') + ' ' + (visionData.vendor||'')).toLowerCase();
    var stopWords = ['the','and','for','with','from','this','that','please','approve','kindly','payment','amount','rs','inr','total'];
    var textWords = textBody.toLowerCase().split(/[^a-z0-9]+/).filter(function(w){
      return w.length >= 3 && stopWords.indexOf(w) < 0;
    });
    var visionWords = visionText.split(/[^a-z0-9]+/);
    for(var w=0; w<textWords.length; w++) {
      if(visionWords.indexOf(textWords[w]) >= 0) return true;
    }
    return false;
  }
  var dedupedIds = {};
  for(var di=0; di<expenses.length; di++) {
    var mediaExp = expenses[di];
    if(!mediaExp.hasMedia || !mediaExp.visionParsed || mediaExp.amount <= 0) continue;
    if(mediaExp.mmApproval || mediaExp.smApproval) continue;
    for(var dj=0; dj<expenses.length; dj++) {
      if(di === dj) continue;
      var textExp = expenses[dj];
      if(textExp.hasMedia && (!textExp.body || textExp.body.length < 10)) continue;
      if(textExp.sender !== mediaExp.sender) continue;
      if(Math.abs(textExp.amount - mediaExp.amount) > 1) continue;
      var timeDelta = Math.abs(textExp.date.getTime() - mediaExp.date.getTime());
      if(timeDelta > 10 * 60 * 1000) continue;
      var cached = visionCache.get(mediaExp.id);
      if(cached && !descriptionsRelate(cached, textExp.body)) continue;
      if(!textExp.supportingDocs) textExp.supportingDocs = [];
      textExp.supportingDocs.push({
        filename: mediaExp.body || '[Attachment]',
        amount: mediaExp.amount,
        vendor: cached ? cached.vendor : '',
        purpose: cached ? cached.purpose : (mediaExp.purpose || ''),
        msgId: mediaExp.id
      });
      dedupedIds[mediaExp.id] = textExp.id;
      console.log('[Dedup] Merged', mediaExp.id, '->', textExp.id, '(', mediaExp.amount, ')');
      break;
    }
  }
  var consolidatedExpenses = expenses.filter(function(e){ return !dedupedIds[e.id]; });
  applyVerdictOverrides(consolidatedExpenses); // v2.8: numbered-reply verdicts + state/approvedAmount
  assignClusters(consolidatedExpenses);        // v2.8: silent re-ask clustering
  var result={fullyApproved:[],partialApproval:[],noApproval:[],onHold:[],rejected:[],allExpenses:consolidatedExpenses,totalExpenses:consolidatedExpenses.length,totalMessages:messages.length,fetchedDays:days||15,visionCacheSize:visionCache.size,dedupedCount:Object.keys(dedupedIds).length};
  for(var k=0;k<consolidatedExpenses.length;k++){
    var e=consolidatedExpenses[k],mm=e.status.mm,sm=e.status.sm;
    if(mm==='no'||sm==='no')result.rejected.push(e);
    else if(mm==='hold'||sm==='hold')result.onHold.push(e);
    else if(mm==='yes'&&sm==='yes')result.fullyApproved.push(e);
    else if(mm==='yes'||sm==='yes')result.partialApproval.push(e);
    else result.noApproval.push(e);
  }
  return result;
}
// ── Pending reminders ─────────────────────────────────────────────────────────
function getDaysPending(date) {
  return Math.max(1, Math.floor((new Date() - date) / (1000*60*60*24)));
}
function buildReminderText(expense) {
  var mm=expense.status.mm, sm=expense.status.sm;
  var bothPending=mm==='pending'&&sm==='pending';
  var mmOnly=mm==='pending'&&sm==='yes';
  var smOnly=sm==='pending'&&mm==='yes';
  var queryMM=mm==='question', querySM=sm==='question';
  var queryAnswered = expense.queryAnswer ? true : false;
  var header;
  if((queryMM||querySM) && queryAnswered) header='[BOT REMINDER] - Query answered - awaiting M+S approval';
  else if(queryMM||querySM) header='[BOT REMINDER] - Query unanswered - '+getDaysPending(expense.date)+' day(s) pending';
  else if(bothPending) header='[BOT REMINDER] - Approval needed';
  else if(mmOnly) header='[BOT REMINDER] - M approval needed';
  else if(smOnly) header='[BOT REMINDER] - S approval needed';
  else return null;
  var lines=[header,''];
  var vendor=expense.vendor||expense.body.substring(0,60);
  lines.push(vendor);
  if(expense.subItems&&expense.subItems.length>1){
    var total=expense.subItems.reduce(function(s,it){return s+it.amount;},0);
    lines.push('Amount: Rs.'+formatINR(total)+' total');
    expense.subItems.forEach(function(si){lines.push('  - '+si.vendor+' Rs.'+formatINR(si.amount));});
  } else if(expense.amount>0){
    lines.push('Amount: Rs.'+formatINR(expense.amount));
  }
  var d=expense.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
  var t=expense.date.toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',timeZone:'Asia/Kolkata'});
  lines.push('Requested by: '+expense.sender+' - '+d+', '+t);
  if(expense.supportingDocs && expense.supportingDocs.length > 0){
    var docNames = expense.supportingDocs.map(function(d){ return d.filename; }).join(', ');
    lines.push('Supporting docs: '+docNames);
  }
  lines.push('');
  var mmLabel=mm==='yes'?'M: Ok':mm==='question'?'M: query raised':'M: pending';
  var smLabel=sm==='yes'?'S: Ok':sm==='question'?'S: query raised':'S: pending';
  lines.push(mmLabel+' | '+smLabel);
  if((queryMM||querySM) && queryAnswered){
    var ans = expense.queryAnswer;
    var who = ans.role==='mm'?'M':'S';
    var answerLabel = ans.answerByRole && ans.answerByRole !== 'accountant' ? (ans.answerByRole + ' (' + ans.answerBy + ')') : ans.answerBy;
    lines.push('');
    lines.push(who+' asked:');
    lines.push('"'+ans.question+'"');
    lines.push('');
    lines.push(answerLabel+' answered:');
    lines.push('"'+ans.answer+'"');
    lines.push('');
    lines.push(who==='M'?'Madhur sir, please confirm to approve':'Sumit sir, please confirm to approve');
  }
  else if(queryMM&&expense.mmApproval){lines.push('');lines.push('M asked:');lines.push('"'+expense.mmApproval.raw+'"');lines.push('');lines.push('Please answer M\'s query to proceed');}
  else if(querySM&&expense.smApproval){lines.push('');lines.push('S asked:');lines.push('"'+expense.smApproval.raw+'"');lines.push('');lines.push('Please answer S\'s query to proceed');}
  else if(mmOnly){lines.push('');lines.push('Madhur sir, please swipe-reply Ok to approve');}
  else if(smOnly){lines.push('');lines.push('Sumit sir, please swipe-reply Ok to approve');}
  else{lines.push('');lines.push('Please swipe-reply Ok to approve');}
  return lines.join('\n');
}
async function sendPendingReminders() {
  if(!waReady){console.log('[Reminders] WA not connected');return 0;}
  if(loadSilentMode()){console.log('[Reminders] silent mode ON — skipping group reminders');return 0;}
  try {
    var audit=await buildApprovalAudit(15);
    var toRemind=audit.partialApproval.concat(audit.noApproval).filter(function(e){
      return e.amount>0||(e.subItems&&e.subItems.length>0);
    });
    console.log('[Reminders] Sending',toRemind.length,'reminders');
    var delay=function(ms){return new Promise(function(r){setTimeout(r,ms);});};
    for(var i=0;i<toRemind.length;i++){
      var text=buildReminderText(toRemind[i]);
      if(!text)continue;
      await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID,text);
      await delay(2000);
    }
    console.log('[Reminders] Done');
    return toRemind.length;
  } catch(e){console.error('[Reminders] Error:',e.message);return 0;}
}

// ── v2.7.2 NEW: twice-daily consolidated approval reminder digest ────────────
// Posts ONE message to the approval group listing all pending approvals that are
// 0–14 days old with a real amount. Tags M and S once at the bottom.
// Bypasses silent mode (these are the scheduled digests the user explicitly wants).
var REMINDER_MAX_AGE_DAYS = parseInt(process.env.REMINDER_MAX_AGE_DAYS) || 14;
// v2.7.7 NEW: fresh-start cutoff. Requests posted BEFORE this date are excluded
// from the reminder digest and urgent lists (they remain in the audit + sheet).
// Set via Railway env var REPORT_START_DATE (YYYY-MM-DD, IST). Default: 2026-06-10.
var REPORT_START_DATE = process.env.REPORT_START_DATE || '2026-06-10';
var REPORT_START_MS = new Date(REPORT_START_DATE + 'T00:00:00+05:30').getTime();
// v2.7.3 NEW: detect M/S capital-contribution entries — these are not vendor
// payments needing approval, so they must not appear in the reminder digest.
// Matches "contribution" / "contributiin" (common typo) / "capital" together with
// an M/S / promoter / drawing reference, OR an explicit "MM/SM contribution".
function isContributionEntry(e) {
  var text = ((e.vendor||'') + ' ' + (e.body||'') + ' ' + (e.details||'') + ' ' + (e.purpose||'')).toLowerCase();
  var mentionsContribution = /contribut|capital\s+contribut/.test(text);
  if(!mentionsContribution) return false;
  // Confirm it's tied to the promoters / their own capital, not a vendor named "...contribution"
  var promoterRef = /\bmm\b|\bsm\b|\bm\b|\bs\b|madhur|sumit|partner|promoter|drawing|own/.test(text);
  return mentionsContribution && promoterRef;
}
// ══ v2.8 — verdict state (numbered-reply approvals) ══════════════════════════
var VERDICT_FILE = './wa_auth/verdict_overrides.json';   // {expenseId:{mm:{verdict,amount,raw,at},sm:{...}}}
var DIGEST_MAP_FILE = './wa_auth/digest_map.json';       // {at, items:[{n,id,label,amount,sender}]}
function loadVerdicts(){ try{ if(fs.existsSync(VERDICT_FILE)) return JSON.parse(fs.readFileSync(VERDICT_FILE,'utf8')); }catch(e){} return {}; }
function saveVerdicts(v){ try{ fs.writeFileSync(VERDICT_FILE, JSON.stringify(v,null,1)); }catch(e){ console.error('[Verdicts] save:',e.message);} }
function loadDigestMap(){ try{ if(fs.existsSync(DIGEST_MAP_FILE)) return JSON.parse(fs.readFileSync(DIGEST_MAP_FILE,'utf8')); }catch(e){} return null; }
function saveDigestMap(m){ try{ fs.writeFileSync(DIGEST_MAP_FILE, JSON.stringify(m,null,1)); }catch(e){ console.error('[DigestMap] save:',e.message);} }

// v2.8.11: parse any of the bot's own posts into the item(s) it was about.
// Returns {kind:'digest'|'single', items:[{label,amount}]} or null if not a bot post.
// - digest: '🔔 *PENDING APPROVALS*' with numbered '*N.* label' lines + '*Rs.X*' lines
// - single: '[BOT REMINDER] - ...' or '*⚡ URGENT — approval needed*' (one expense:
//   vendor line after header + 'Amount: Rs.X')
function parseBotPostItems(text){
  if(!text) return null;
  if(/PENDING APPROVALS|🔔/i.test(text)){
    var items=[]; var lines=text.split('\n');
    for(var i=0;i<lines.length;i++){
      var m=lines[i].match(/^\s*\*?(\d+)\.\*?\s+(.+)$/);
      if(m){
        var amt=0;
        var am=(lines[i+1]||'').match(/rs\.?\s*([\d,]+)/i);
        if(am) amt=parseAmount(am[1]);
        items.push({label:m[2].replace(/…\s*$/,'').trim(), amount:amt});
      }
    }
    return items.length ? {kind:'digest', items:items} : {kind:'digest', items:[]};
  }
  if(/^\s*\[BOT REMINDER\]/i.test(text) || /⚡\s*URGENT|URGENT\s*—\s*approval needed/i.test(text)){
    var ls=text.split('\n').map(function(x){return x.trim();}).filter(Boolean);
    var label=ls[1]||'';
    var amt2=0;
    for(var j=0;j<ls.length;j++){
      var am2=ls[j].match(/^Amount:\s*rs\.?\s*([\d,]+)/i);
      if(am2){ amt2=parseAmount(am2[1]); break; }
    }
    return {kind:'single', items:[{label:label.replace(/…\s*$/,'').trim(), amount:amt2}]};
  }
  return null;
}

// Overlay numbered-reply verdicts onto audit expenses + compute state/approvedAmount.
// Rule A: an amend counts as that person's yes at the amended amount; the other
// party's plain yes carries to it. Stricter signal wins (no > hold > question > yes).
// ── v2.8 Module 4: silent re-ask clustering ──────────────────────────────────
// Same amount (±Rs.1) + at least one significant shared word (5+ chars) + within
// 30 days → same clusterId. Cross-sender included (team re-asks). When any member
// is paid, reconciliation retires the whole cluster from awaiting-payment.
var CLUSTER_STOPWORDS = ['kindly','approve','approval','please','payment','amount','request','expense','account','towards','against'];
function clusterWords(e){
  var t = ((e.vendor||'')+' '+(e.body||'')).toLowerCase();
  return t.split(/[^a-z]+/).filter(function(w){ return w.length>=5 && CLUSTER_STOPWORDS.indexOf(w)<0; });
}
function assignClusters(expenses){
  var THIRTY_D = 30*86400000;
  for(var i=0;i<expenses.length;i++){
    var a = expenses[i];
    if(!a.clusterId) a.clusterId = a.id;
    if(!(a.amount>0)) continue;
    var aw = clusterWords(a);
    if(aw.length===0) continue;
    for(var j=i+1;j<expenses.length;j++){
      var b = expenses[j];
      if(!(b.amount>0)) continue;
      if(Math.abs(a.amount-b.amount)>1) continue;
      if(Math.abs(a.date.getTime()-b.date.getTime())>THIRTY_D) continue;
      var bw = clusterWords(b);
      var shared = aw.some(function(w){ return bw.indexOf(w)>=0; });
      if(shared){ b.clusterId = a.clusterId; }
    }
  }
}

function applyVerdictOverrides(expenses){
  var v = loadVerdicts();
  // v2.8.12: build an id->expense map, plus a content reconciler so a verdict stored
  // under a digest-map id that has since drifted still finds its expense by label+amount.
  var byId = {};
  expenses.forEach(function(e){ byId[e.id] = e; });
  function vStop(s){ return ['the','and','for','with','from','this','that','please','approve','kindly','payment','amount','pending','approvals','approval','request','requests','needs','reply','number','yes','hold','later','reject','rs','inr','total','both'].indexOf(s)>=0; }
  function reconcileByContent(label, amount){
    var w = (label||'').toLowerCase().replace(/…\s*$/,'').split(/[^a-z0-9]+/).filter(function(x){ return x.length>=4 && !vStop(x); });
    if(!w.length) return null;
    var cands = expenses.filter(function(e){
      var ev = ((e.vendor||'')+' '+(e.body||'')).toLowerCase();
      return w.some(function(word){ return ev.indexOf(word)>=0; });
    });
    if(cands.length>1 && amount>0){ var nar=cands.filter(function(e){ return e.amount===amount; }); if(nar.length) cands=nar; }
    return cands.length===1 ? cands[0] : null;
  }
  // Resolve each verdict-store key to a live expense (direct id, else content fallback).
  Object.keys(v).forEach(function(key){
    var ov = v[key];
    if(!ov || (!ov.mm && !ov.sm)) return;
    var target = byId[key];
    if(!target && (ov._label || ov._amount!=null)){
      target = reconcileByContent(ov._label, ov._amount);
    }
    if(target && !target._verdictApplied){
      // attach the override onto the resolved expense (so the loop below uses it)
      target._resolvedVerdict = ov;
    }
  });
  expenses.forEach(function(e){
    var ov = v[e.id] || e._resolvedVerdict;
    e.requestedAmount = e.amount;
    e.approvedAmount = e.amount;
    if(ov){
      ['mm','sm'].forEach(function(r){
        if(!ov[r]) return;
        var vd = ov[r].verdict;
        if(vd==='yes') e.status[r]='yes';
        else if(vd==='no') e.status[r]='no';
        else if(vd==='hold') e.status[r]='hold';
        else if(vd==='question') e.status[r]='question';
        else if(vd==='amend'){ e.status[r]='yes'; if(ov[r].amount>0 && ov[r].amount<e.approvedAmount) e.approvedAmount=ov[r].amount; }
      });
      e.verdictLog = ov;
    }
    var st = e.status;
    if(st.mm==='no'||st.sm==='no') e.state='rejected';
    else if(st.mm==='hold'||st.sm==='hold') e.state='held';
    else if(st.mm==='question'||st.sm==='question') e.state='query';
    else if(st.mm==='yes'&&st.sm==='yes') e.state=(e.approvedAmount!==e.requestedAmount)?'amended':'approved';
    else e.state='pending';
  });
}

// ── v2.8 grouped digest: scoreboard + numbered items + held section ──────────
async function buildApprovalReminderDigest() {
  var audit = await buildApprovalAudit(REMINDER_MAX_AGE_DAYS + 1);
  var nowMs = Date.now();
  var cutoffMs = REMINDER_MAX_AGE_DAYS * 86400000;
  var base = audit.partialApproval.concat(audit.noApproval).filter(function(e){
    var hasAmt = e.amount > 0 || (e.subItems && e.subItems.length > 0);
    var ageOk = (nowMs - e.date.getTime()) <= cutoffMs;
    var afterStart = e.date.getTime() >= REPORT_START_MS;
    return hasAmt && ageOk && afterStart && !isContributionEntry(e);
  });
  // states were applied in buildApprovalAudit; partition
  var onM=[], onS=[], onBoth=[];
  var held = (audit.onHold||[]).filter(function(e){
    return (e.amount>0) && ((nowMs - e.date.getTime()) <= cutoffMs) && (e.date.getTime() >= REPORT_START_MS) && !isContributionEntry(e);
  });
  base.forEach(function(e){
    if(e.state==='rejected'||e.state==='approved'||e.state==='amended') return;
    if(e.state==='held') return;
    var mDone = e.status.mm==='yes', sDone = e.status.sm==='yes';
    if(sDone && !mDone) onM.push(e);
    else if(mDone && !sDone) onS.push(e);
    else onBoth.push(e);
  });
  if(onM.length+onS.length+onBoth.length+held.length === 0) return null;
  var byOldest = function(a,b){ return a.date.getTime()-b.date.getTime(); };
  onM.sort(byOldest); onS.sort(byOldest); onBoth.sort(byOldest);
  function lakh(n){ return n>=100000 ? (Math.round(n/10000)/10)+'L' : formatINR(n); }
  function sum(arr){ return arr.reduce(function(s,e){return s+(e.approvedAmount||e.amount);},0); }
  function age(e){ var h=Math.floor((nowMs-e.date.getTime())/3600000); return h>=24?Math.floor(h/24)+'d':h+'h'; }
  function label(e){
    var raw = e.vendor || e.body || '';
    var cleaned = (typeof cleanDetails==='function') ? cleanDetails(raw) : raw;
    var l = (cleaned && cleaned.length>1 ? cleaned : raw).replace(/\n/g,' ').trim();
    return l.length>48 ? l.substring(0,48)+'…' : l;
  }
  var d = new Date(nowMs+5.5*3600000);
  var totalCount = onM.length+onS.length+onBoth.length;
  var totalAmt = sum(onM)+sum(onS)+sum(onBoth);
  var lines = ['🔔 *PENDING APPROVALS — '+d.getUTCDate()+' '+['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][d.getUTCMonth()]+'*'];
  lines.push(totalCount+(totalCount===1?' request':' requests')+' · Rs.'+formatINR(totalAmt));
  var items=[], n=0;
  // Continuous numbering across sections so "1 yes" always maps correctly.
  function section(title, note, arr){
    if(arr.length===0) return;
    lines.push('');
    lines.push('*'+title+'*'+(note?' _('+note+')_':''));
    lines.push('');
    arr.forEach(function(e){
      n++;
      var q = e.state==='query' ? ' _(query open)_' : '';
      lines.push('*'+n+'.* '+label(e));
      lines.push('*Rs.'+formatINR(e.approvedAmount||e.amount)+'* _('+age(e)+' ago)_'+q);
      items.push({n:n, id:e.id, label:label(e), amount:e.approvedAmount||e.amount, sender:e.sender});
    });
  }
  section('Needs M approval', 'S already approved', onM);
  section('Needs S approval', 'M already approved', onS);
  section('Needs M & S approval', '', onBoth);
  if(held.length){
    lines.push('');
    lines.push('*On hold* _(awaiting resolution)_');
    lines.push('');
    held.forEach(function(e){
      var who = e.status.mm==='hold'?'M':'S';
      n++;
      lines.push('*'+n+'.* '+label(e));
      lines.push('*Rs.'+formatINR(e.amount)+'* _(held by '+who+', '+age(e)+')_');
      items.push({n:n, id:e.id, label:label(e), amount:e.approvedAmount||e.amount, sender:e.sender});
    });
  }
  lines.push('');
  lines.push('_Reply with the number:_');
  lines.push('*1 yes* — approve');
  lines.push('*1 no* — reject');
  lines.push('*1 hold* — later');
  if(totalCount+held.length > 1) lines.push('');
  if(totalCount+held.length > 1) lines.push('_Many at once: 1 yes 2 yes 3 no_');
  var mentionJids=[];
  if(onM.length||onBoth.length) mentionJids.push(CONFIG.MM_PHONE+'@c.us');
  if(onS.length||onBoth.length) mentionJids.push(CONFIG.SM_PHONE+'@c.us');
  return { text: lines.join('\n'), mentionJids: mentionJids, count: totalCount, items: items };
}
async function sendApprovalReminderDigest() {
  if(!waReady){ console.log('[Digest] WA not connected'); return 0; }
  if(!CONFIG.BOT_ENABLED){ console.log('[Digest] bot disabled'); return 0; }
  try {
    var digest = await buildApprovalReminderDigest();
    if(!digest){ console.log('[Digest] nothing pending in window'); return 0; }
    var sentMsg = await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, digest.text, { mentions: digest.mentionJids });
    var digestMsgId = sentMsg && sentMsg.id ? (sentMsg.id._serialized || sentMsg.id.id) : null;
    saveDigestMap({ at: new Date().toISOString(), msgId: digestMsgId, items: digest.items });
    console.log('[Digest] posted', digest.count, 'pending; map saved with', digest.items.length, 'items');
    return digest.count;
  } catch(e){ console.error('[Digest] error:', e.message); return 0; }
}

// ── v2.8 verdict parsing ──────────────────────────────────────────────────────
// Parses "1 yes 2 yes 3 no 4 hold", "1-4 yes", "all ok", "3 ok 50000", "2 why".
function parseVerdictMessage(body, maxN){
  if(!body) return null;
  var t = body.toLowerCase().replace(/;+/g,' ').replace(/,(?=\s|$)/g,' ').replace(/\s+/g,' ').trim();
  if(!/\d|all/.test(t)) return null;
  if(!/(yes|ok|okay|approve|no|reject|hold|why|reason)/.test(t)) return null;
  var out = [];
  function push(n, vd, amt){ if(n>=1 && n<=maxN) out.push({n:n, verdict:vd, amount:amt||0}); }
  function vmap(w){ if(/^(yes|ok|okay|approved?|approve)$/.test(w)) return 'yes'; if(/^(no|rejected?|reject)$/.test(w)) return 'no'; if(w==='hold') return 'hold'; if(/^(why|reason)$/.test(w)) return 'question'; return null; }
  var rest = t;
  // ranges: "1-4 yes" / "1 to 4 yes"
  rest = rest.replace(/(\d+)\s*(?:-|to)\s*(\d+)\s+(yes|ok|okay|approved?|no|rejected?|hold)/g, function(_,a,b,w){
    var vd=vmap(w); for(var i=parseInt(a);i<=parseInt(b);i++) push(i,vd);
    return ' ';
  });
  // all: "all yes"
  rest = rest.replace(/\ball\s+(yes|ok|okay|approved?|no|rejected?|hold)\b/g, function(_,w){
    var vd=vmap(w); for(var i=1;i<=maxN;i++) push(i,vd);
    return ' ';
  });
  // per-item with optional amount: "3 ok 50000" / "3 yes 50k" / "2 why".
  // Amount must look like an amount (unit, 4+ digits, comma format, or Rs prefix)
  // so a bare following item number ("1 yes 2 yes") is never eaten as an amount.
  var re = /(\d+)\s*[:.\)]?\s*(yes|ok|okay|approved?|approve|no|rejected?|reject|hold|why|reason)\b\s*\??\s*((?:rs\.?\s*)?\d[\d,]*\.?\d*\s*(?:k|lakhs?|lacs?|crores?|cr)\b|rs\.?\s*\d[\d,]*\.?\d*|\d{4,}[\d,]*|\d{1,3}(?:,\d{2,3})+)?/g;
  var m;
  while((m = re.exec(rest)) !== null){
    var vd = vmap(m[2]);
    if(!vd) continue;
    var amt = 0;
    if(m[3]){ amt = extractLineAmount(m[3], false) || parseAmount(m[3]); }
    if(vd==='yes' && amt>0) push(parseInt(m[1]),'amend',amt);
    else push(parseInt(m[1]),vd,amt);
  }
  if(out.length===0) return null;
  // last verdict for an item wins within one message
  var byN = {}; out.forEach(function(o){ byN[o.n]=o; });
  return Object.keys(byN).map(function(k){ return byN[k]; });
}

// Sonnet fallback for messy verdict messages.
async function aiParseVerdicts(body, items){
  if(!CONFIG.CLAUDE_API_KEY) return null;
  var list = items.map(function(it){ return it.n+'. '+it.label+' Rs.'+formatINR(it.amount); }).join('\n');
  var prompt = 'A company promoter replied to this numbered approval list:\n'+list+'\n\nTheir reply: "'+body.substring(0,400)+'"\n\nExtract per-item verdicts. verdict is one of yes/no/hold/question/amend (amend = approved at a different amount; include amount in rupees). Reply ONLY strict JSON: {"verdicts":[{"n":1,"verdict":"yes","amount":0}]} — empty array if the reply is not about these items.';
  try{
    var resp = await fetch('https://api.anthropic.com/v1/messages', { method:'POST', headers:{'Content-Type':'application/json','x-api-key':CONFIG.CLAUDE_API_KEY,'anthropic-version':'2023-06-01'}, body: JSON.stringify({ model:'claude-sonnet-4-6', max_tokens:400, messages:[{role:'user',content:prompt}] }) });
    if(!resp.ok) return null;
    var data = await resp.json(); var text='';
    if(data.content) for(var i=0;i<data.content.length;i++) if(data.content[i].type==='text'){ text=data.content[i].text; break; }
    text = text.replace(/```json|```/g,'').trim();
    var p; try{ p=JSON.parse(text); }catch(e){ var mm=text.match(/\{[\s\S]*\}/); if(!mm) return null; p=JSON.parse(mm[0]); }
    if(!p.verdicts || !p.verdicts.length) return null;
    return p.verdicts.filter(function(v){ return v.n>=1 && v.n<=items.length && /^(yes|no|hold|question|amend)$/.test(v.verdict); }).map(function(v){ return {n:v.n, verdict:v.verdict, amount:parseAmount(v.amount)||0}; });
  }catch(e){ console.error('[AI verdicts]', e.message); return null; }
}

// Handle group messages from M or S as numbered verdicts against the latest digest.
async function handlePromoterVerdicts(msg){
  try{
    if(msg.from !== CONFIG.APPROVAL_GROUP_JID) return false;
    var author = (msg.author||'');
    // v2.8.13: WhatsApp now delivers group authors as @lid IDs (e.g. 102469514330302@lid),
    // not phone numbers — so a raw phone-prefix check fails. Resolve role via
    // identifySender (name-based, same as the rest of the bot), with a phone fallback.
    var role = author.indexOf(CONFIG.MM_PHONE)===0 ? 'mm' : (author.indexOf(CONFIG.SM_PHONE)===0 ? 'sm' : null);
    if(!role){
      var who2 = await identifySender(author);
      if(who2 && (who2.role==='mm'||who2.role==='sm')) role = who2.role;
    }
    if(!role) return false;
    var body = (msg.body||'').trim();
    if(!body) return false;
    var map = loadDigestMap();
    if(!map || !map.items || !map.items.length) return false;
    var verdicts = parseVerdictMessage(body, map.items.length);
    // v2.8.11: M/S swipe-reply ANY bot post (digest, [BOT REMINDER], ⚡ URGENT) with a
    // bare "ok/yes/no/hold". Resolve per item the quoted post actually listed:
    // digest → every listed item; reminder/urgent → exactly that one expense.
    // A single-item post can NEVER blanket-approve the whole list.
    if(!verdicts){
      var quotedId=null, quotedBodyV=null;
      if(msg.hasQuotedMsg){ try{ var q=await msg.getQuotedMessage(); quotedId=q.id._serialized||q.id.id; quotedBodyV=q.body||''; }catch(e){} }
      var botPost = parseBotPostItems(quotedBodyV);
      var repliedToBotPost = quotedId && (botPost || (map.msgId && quotedId===map.msgId));
      var bare = body.toLowerCase().trim();
      var bareVerdict = /^(yes|ok|okay|approved?|approve|done|haan|theek)$/.test(bare) ? 'yes'
                      : /^(no|reject(ed)?|nahi)$/.test(bare) ? 'no'
                      : /^hold$/.test(bare) ? 'hold' : null;
      var allVerdict = /^all\s+(yes|ok|okay|approved?|approve)$/.test(bare) ? 'yes'
                      : /^all\s+(no|reject(ed)?)$/.test(bare) ? 'no' : null;
      if(repliedToBotPost && (allVerdict || bareVerdict)){
        var vd = allVerdict || bareVerdict;
        function normLabel(s){ return (s||'').toLowerCase().replace(/…\s*$/,'').trim(); }
        function resolveQuotedItem(qi){
          // exact label first (digest labels are generated by the same code as the map)
          var exact = map.items.filter(function(it){ return normLabel(it.label)===normLabel(qi.label); });
          var cands = exact.length ? exact : map.items.filter(function(it){
            var w = normLabel(qi.label).split(/[^a-z0-9]+/).filter(function(x){return x.length>=4;});
            var il = normLabel(it.label);
            return w.length && w.some(function(word){ return il.indexOf(word)>=0; });
          });
          if(cands.length>1 && qi.amount>0){
            var nar = cands.filter(function(it){ return it.amount===qi.amount; });
            if(nar.length) cands = nar;
          }
          return cands.length===1 ? cands[0] : null;
        }
        var targets = [];
        if(botPost && botPost.items.length){
          botPost.items.forEach(function(qi){
            var hit = resolveQuotedItem(qi);
            if(hit && targets.indexOf(hit)<0) targets.push(hit);
          });
        } else if(map.msgId && quotedId===map.msgId){
          targets = map.items.slice(); // reply to the exact latest digest, quoted body unavailable
        }
        if(targets.length){
          verdicts = targets.map(function(it){ return {n:it.n, verdict:vd, amount:0}; });
        } else {
          // Quoted a bot post but nothing safely resolved — never guess, never blanket-apply.
          await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, "Couldn't match that "+(botPost&&botPost.kind==='single'?'reminder':'digest')+" to the current pending list. Please reply on the latest digest with the number, e.g. \"1 "+vd+"\".");
          return true;
        }
      }
    }
    if(!verdicts && /\d/.test(body) && /(yes|ok|no|hold|why|reason|approve|reject)/i.test(body)){
      verdicts = await aiParseVerdicts(body, map.items);
    }
    if(!verdicts || !verdicts.length) return false;
    var store = loadVerdicts();
    var who = role==='mm' ? 'M' : 'S';
    // v2.8.17: no per-reply echo. Record verdicts silently; only announce an item
    // once BOTH M and S have approved it. Partial/hold/reject/query states are
    // reported by the 10 AM / 7 PM digests, so they need no real-time echo.
    var nowApproved = [];   // items that reached full M+S approval on THIS reply
    verdicts.forEach(function(v){
      var item = map.items[v.n-1];
      if(!item || item.n!==v.n){ item = null; map.items.forEach(function(it){ if(it.n===v.n) item=it; }); }
      if(!item){ return; }
      if(!store[item.id]) store[item.id] = {};
      store[item.id]._label = item.label;   // v2.8.12: enable id-drift-safe reconciliation
      store[item.id]._amount = item.amount;
      store[item.id][role] = { verdict:v.verdict, amount:v.amount||0, raw:body.substring(0,200), at:new Date().toISOString() };
      // Is this item now approved by BOTH parties (per the verdict store)?
      var mmV = store[item.id].mm, smV = store[item.id].sm;
      var isYes = function(x){ return x && (x.verdict==='yes' || x.verdict==='amend'); };
      if(isYes(mmV) && isYes(smV)){
        var amt = item.amount;
        if(mmV.verdict==='amend' && mmV.amount>0) amt = mmV.amount;
        if(smV.verdict==='amend' && smV.amount>0 && (!amt || smV.amount<amt)) amt = smV.amount;
        nowApproved.push({ label:item.label, amount:amt });
      }
    });
    saveVerdicts(store);
    if(nowApproved.length){
      var conf = nowApproved.map(function(a){ return '✓ Approved (M+S): '+a.label+' Rs.'+formatINR(a.amount); });
      await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, conf.join('\n'));
    }
    console.log('[Verdicts]', who, 'recorded', verdicts.length, 'item(s);', nowApproved.length, 'now fully approved');
    return true;
  }catch(e){ console.error('[Verdicts]', e.message); return false; }
}
// ── DM Relay state ───────────────────────────────────────────────────────────
var DM_STATE_FILE = './wa_auth/dm_state.json';
function loadDMState() {
  try {
    if(fs.existsSync(DM_STATE_FILE)){
      return JSON.parse(fs.readFileSync(DM_STATE_FILE, 'utf8'));
    }
  } catch(e) { console.error('[DM] state load failed:', e.message); }
  return { pending: {} };
}
function saveDMState(state) {
  try {
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth', { recursive: true });
    fs.writeFileSync(DM_STATE_FILE, JSON.stringify(state, null, 2));
  } catch(e) { console.error('[DM] state save failed:', e.message); }
}
function pruneStaleDMState(state) {
  var now = Date.now();
  Object.keys(state.pending).forEach(function(jid){
    var entry = state.pending[jid];
    if(now - new Date(entry.lastUpdate).getTime() > 30*60*1000){
      delete state.pending[jid];
    }
  });
}
async function isAuthorisedAccountant(rawJid, contactName) {
  if(!rawJid) return false;
  if(rawJid.indexOf('@g.us') >= 0) return false;
  var phoneOnly = null;
  if(rawJid.indexOf('@c.us') >= 0){
    phoneOnly = rawJid.split('@')[0].replace(/[^0-9]/g, '');
  } else if(rawJid.indexOf('@lid') >= 0){
    var resolvedPhone = null;
    try {
      var contact = await waClient.getContactById(rawJid);
      if(contact){
        if(contact.number){
          var n = String(contact.number).replace(/[^0-9]/g, '');
          if(n.length >= 10 && n.length <= 13) resolvedPhone = n;
        }
        if(!resolvedPhone){
          var candidates = [contact.pushname, contact.name, contact.shortName, contact.verifiedName];
          for(var ci=0; ci<candidates.length; ci++){
            var cn = String(candidates[ci] || '');
            var phoneMatch = cn.match(/\+?(91)?[\s\-]?(\d{5})[\s\-]?(\d{5})/);
            if(phoneMatch){
              var maybe = '91' + phoneMatch[2] + phoneMatch[3];
              if(maybe.length === 12){ resolvedPhone = maybe; break; }
            }
          }
        }
      }
      if(!resolvedPhone){
        try {
          var chat = await waClient.getChatById(rawJid);
          if(chat && chat.name){
            var phoneMatch2 = String(chat.name).match(/\+?(91)?[\s\-]?(\d{5})[\s\-]?(\d{5})/);
            if(phoneMatch2){
              var maybe2 = '91' + phoneMatch2[2] + phoneMatch2[3];
              if(maybe2.length === 12) resolvedPhone = maybe2;
            }
          }
        } catch(e) {}
      }
    } catch(e) {
      console.log('[Auth] LID resolve failed for', rawJid, ':', e.message);
    }
    if(!resolvedPhone){
      if(CONFIG.LID_WHITELIST && CONFIG.LID_WHITELIST.indexOf(rawJid) >= 0){
        console.log('[Auth] LID', rawJid, 'allowed via LID_WHITELIST');
        return true;
      }
      console.log('[Auth] LID could not be resolved to a phone:', rawJid, '(name:', contactName, ')');
      return false;
    }
    phoneOnly = resolvedPhone;
    console.log('[Auth] LID', rawJid, 'resolved to phone', phoneOnly);
  } else {
    return false;
  }
  if(!phoneOnly) return false;
  var whitelist = CONFIG.ACCOUNTANT_PHONES.concat([CONFIG.MM_PHONE, CONFIG.SM_PHONE]).concat(CONFIG.TEST_PHONES || []);
  if(whitelist.indexOf(phoneOnly) >= 0){
    console.log('[Auth] allow:', phoneOnly, '(', contactName, ')');
    return true;
  }
  console.log('[Auth] reject:', phoneOnly, '(', contactName, ') - not in whitelist');
  return false;
}
function buildGroupPostFromDM(entry, posterName) {
  var lines = ['*EXPENSE REQUEST*', ''];
  if(entry.details) lines.push('Details: ' + entry.details);
  if(entry.subItems && entry.subItems.length > 1){
    var total = entry.subItems.reduce(function(s,it){return s+it.amount;},0);
    lines.push('Amount: Rs.' + formatINR(total) + ' total');
    entry.subItems.forEach(function(si){ lines.push('  - ' + si.vendor + ' Rs.' + formatINR(si.amount)); });
  } else if(entry.amount > 0) {
    lines.push('Amount: Rs.' + formatINR(entry.amount));
  }
  if(entry.company) lines.push('Company: ' + entry.company);
  if(entry.fromAC) lines.push('From: ' + entry.fromAC);
  lines.push('Posted by: ' + posterName);
  lines.push('');
  lines.push('M/S please review.');
  return lines.join('\n');
}

// ── v2.7 NEW: smart free-form parser helpers ─────────────────────────────────
// Extract company + account references from anywhere in a free-form DM message
// by matching against the live Fund Position list. Returns { companyMatches, acMatches }.
// Used to avoid asking the accountant for fields they already mentioned.
async function smartExtractCompanyAccount(text) {
  if(!text) return { companyMatches: [], acMatches: [] };
  var lower = text.toLowerCase();
  var fp;
  try { fp = await getFundPosition(); } catch(e) { return { companyMatches: [], acMatches: [] }; }
  var companies = [];
  var accounts = [];
  fp.forEach(function(a){
    if(a.company && companies.indexOf(a.company) < 0) companies.push(a.company);
    if(a.bankAC && accounts.indexOf(a.bankAC) < 0) accounts.push(a.bankAC);
  });
  // Find companies whose full name appears in the text (case-insensitive)
  var companyMatches = companies.filter(function(c){
    return c.length >= 4 && lower.indexOf(c.toLowerCase()) >= 0;
  });
  // Find bank accounts whose full name appears in the text
  var acMatches = accounts.filter(function(a){
    return a.length >= 4 && lower.indexOf(a.toLowerCase()) >= 0;
  });
  // Also try partial matches: any unique 2+ word token from a company/account name found in text
  // This catches "hansaflon" matching "Hansaflon Buildcon"
  if(companyMatches.length === 0){
    companies.forEach(function(c){
      var firstWord = c.split(/\s+/)[0];
      if(firstWord && firstWord.length >= 5 && lower.indexOf(firstWord.toLowerCase()) >= 0){
        companyMatches.push(c);
      }
    });
  }
  if(acMatches.length === 0){
    accounts.forEach(function(a){
      var tokens = a.split(/\s+/).filter(function(t){ return t.length >= 4; });
      // Match if ALL tokens of the account name appear in the text
      var allFound = tokens.length > 0 && tokens.every(function(t){ return lower.indexOf(t.toLowerCase()) >= 0; });
      if(allFound) acMatches.push(a);
    });
  }
  // Dedupe
  companyMatches = Array.from(new Set(companyMatches));
  acMatches = Array.from(new Set(acMatches));
  return { companyMatches: companyMatches, acMatches: acMatches };
}

// Count how many distinct amount patterns are in a free-form message body.
// Used to enforce "one expense at a time" — refuses multi-amount requests.
function countAmountPatterns(body) {
  if(!body) return 0;
  var found = [];
  // Pattern A: <number> lakh/lac/cr/crore/thousand/hundred/hazaar/l (with currency unit)
  var unitMatches = body.match(/\d[\d,]*\.?\d*\s*(?:crores?|cr|lakhs?|lacs?|thousands?|hundreds?|hazaar|hazar|l\b)/gi) || [];
  unitMatches.forEach(function(m){ found.push(m); });
  // Pattern B: <number> k (thousand)
  var kMatches = body.match(/\d[\d,]*\.?\d*\s*k\b/gi) || [];
  kMatches.forEach(function(m){ found.push(m); });
  // Pattern C: Rs./INR/₹ prefix
  var rsMatches = body.match(/(?:rs\.?\s*|inr\s*|\u20B9\s*)\d[\d,]*\.?\d*/gi) || [];
  rsMatches.forEach(function(m){ found.push(m); });
  // Pattern D: Indian-format commas (e.g. 7,08,708) — only if not already covered by Rs prefix
  var commaMatches = body.match(/\b\d{1,3}(?:,\d{2,3}){1,3}\b/g) || [];
  commaMatches.forEach(function(m){
    // Skip if this match is part of a Rs-prefixed match we already counted
    var alreadyCounted = found.some(function(f){ return f.indexOf(m) >= 0; });
    if(!alreadyCounted) found.push(m);
  });
  return found.length;
}

// ── v2.7.2 NEW: AI arbiter for multi-amount messages ─────────────────────────
// Distinguishes a single approval request that happens to mention several numbers
// (cost breakdown / payment plan — e.g. Umesh's granite message) from a genuine
// multi-payment request (e.g. "one cheque for Ajit Singh Rs.X and one for Suresh Rs.Y").
// Returns one of:
//   { kind:'single', approvalAmount:<n>, details:'<clean context summary>' }
//   { kind:'multiple', count:<n> }
//   { kind:'unclear' }                      (fall back to the blunt block)
// Best-effort: any error → 'unclear'.
async function aiParseExpenseIntent(body) {
  if(!CONFIG.CLAUDE_API_KEY || !body) return { kind: 'unclear' };
  var prompt = 'An accountant sent this expense message in a real-estate firm. It may contain several numbers (totals, amounts already paid, balances, instalment splits) but usually only ONE amount is actually being requested for approval right now.\n\n' +
    'MESSAGE:\n"""\n' + body.substring(0, 1500) + '\n"""\n\n' +
    'Decide:\n' +
    '- If this is ONE approval request (even if it lists cost breakdown / payment plan / past payments as context), return the single amount being requested for approval NOW, plus a clean one-line context summary preserving the useful details (vendor, total, paid, balance, instalment plan).\n' +
    '- If this is genuinely MULTIPLE separate payment requests (e.g. distinct cheques to different parties each needing approval), return kind "multiple" with the count.\n\n' +
    'The amount being requested is usually the one attached to words like "approve", "pay", "release", "kindly approve". Totals, amounts already paid, and balances are context, NOT the ask.\n\n' +
    'Reply ONLY with strict JSON on one line:\n' +
    '{"kind":"single","approvalAmount":<number>,"details":"<clean summary>"}  OR  {"kind":"multiple","count":<number>}';
  try {
    var resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': CONFIG.CLAUDE_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: 'claude-sonnet-4-6', max_tokens: 400, messages: [{ role: 'user', content: prompt }] })
    });
    if(!resp.ok){ console.error('[AI parse] HTTP', resp.status); return { kind: 'unclear' }; }
    var data = await resp.json();
    var text = '';
    if(data.content){ for(var i=0;i<data.content.length;i++){ if(data.content[i].type==='text'){ text=data.content[i].text; break; } } }
    if(!text) return { kind: 'unclear' };
    text = text.replace(/```json|```/g,'').trim();
    var parsed;
    try { parsed = JSON.parse(text); } catch(e){ var m=text.match(/\{[\s\S]*\}/); if(m){ try{ parsed=JSON.parse(m[0]); }catch(e2){ return { kind:'unclear' }; } } else return { kind:'unclear' }; }
    if(parsed.kind === 'single'){
      var amt = parseAmount(parsed.approvalAmount);
      if(amt > 0) return { kind:'single', approvalAmount: amt, details: (parsed.details||'').toString().substring(0,250) };
      return { kind:'unclear' };
    }
    if(parsed.kind === 'multiple'){
      return { kind:'multiple', count: parseInt(parsed.count) || 2 };
    }
    return { kind:'unclear' };
  } catch(e){ console.error('[AI parse] exception:', e.message); return { kind:'unclear' }; }
}

// ── Handle accountant DM ─────────────────────────────────────────────────────
async function handleAccountantDM(msg) {
  if(!msg || !waReady) return false;
  var rawFrom = msg.from || '';
  if(rawFrom.indexOf('@g.us') >= 0) return false;
  if(rawFrom.indexOf('@c.us') < 0 && rawFrom.indexOf('@lid') < 0) return false;
  if(msg.fromMe) return false;
  var senderInfo = await identifySender(rawFrom);
  if(!(await isAuthorisedAccountant(rawFrom, senderInfo.contactName))){
    console.log('[DM] unauthorised sender (silent):', rawFrom, senderInfo.contactName);
    if(process.env.REPLY_TO_UNAUTHORISED === 'true'){
      if(!global._unauthorisedNotified) global._unauthorisedNotified = {};
      var dayKey = rawFrom + '_' + new Date().toISOString().split('T')[0];
      if(!global._unauthorisedNotified[dayKey]){
        global._unauthorisedNotified[dayKey] = true;
        try {
          await waClient.sendMessage(rawFrom, 'This is an automated assistant. Your number is not authorised. Please contact admin if you need access.');
        } catch(e) { /* ignore */ }
      }
    }
    return false;
  }
  var body = (msg.body || '').trim();
  var hasMedia = msg.hasMedia || false;
  var thisMsgId = msg.id._serialized || msg.id.id;
  console.log('[DM] from', senderInfo.contactName || rawFrom, ':', body.substring(0,80), hasMedia?'[+media]':'');
  var state = loadDMState();
  pruneStaleDMState(state);
  if(/^\s*(cancel|reset|clear)\s*$/i.test(body)){
    delete state.pending[rawFrom];
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'Pending request cleared. Send a new expense to start over.');
    return true;
  }
  var phoneOfSender = rawFrom.indexOf('@c.us')>=0 ? rawFrom.split('@')[0] : null;
  if(!phoneOfSender){
    try {
      var c = await waClient.getContactById(rawFrom);
      var nameStr = String((c && (c.pushname||c.name||c.shortName)) || '');
      var pm = nameStr.match(/\+?(91)?[\s\-]?(\d{5})[\s\-]?(\d{5})/);
      if(pm) phoneOfSender = '91' + pm[2] + pm[3];
    } catch(e){}
  }
  if(/^\s*silent\s+on\s*$/i.test(body)){
    if(phoneOfSender !== SILENT_OBSERVER){
      await waClient.sendMessage(rawFrom, 'Only the silent-mode observer ('+SILENT_OBSERVER+') can toggle this.');
      return true;
    }
    saveSilentMode(true);
    await waClient.sendMessage(rawFrom, 'Silent mode ON. I will stop posting reminders to the approval group. Daily summary at 7 PM will come to you privately.');
    return true;
  }
  if(/^\s*silent\s+off\s*$/i.test(body)){
    if(phoneOfSender !== SILENT_OBSERVER){
      await waClient.sendMessage(rawFrom, 'Only the silent-mode observer ('+SILENT_OBSERVER+') can toggle this.');
      return true;
    }
    saveSilentMode(false);
    await waClient.sendMessage(rawFrom, 'Silent mode OFF. Resuming group reminders and evening report to Day Book group.');
    return true;
  }
  if(/^\s*silent\s+status\s*$/i.test(body)){
    var s = loadSilentMode();
    await waClient.sendMessage(rawFrom, 'Silent mode is currently '+(s?'ON':'OFF')+'.');
    return true;
  }
  // ── v2.8 Module 7: unapproved-payment alert toggle (Day Book group) ───────
  if(/^\s*unapproved\s+(on|off|status)\s*$/i.test(body)){
    if(phoneOfSender !== SILENT_OBSERVER){
      await waClient.sendMessage(rawFrom, 'Only the observer can toggle this.');
      return true;
    }
    var uw = body.match(/^\s*unapproved\s+(on|off|status)\s*$/i)[1].toLowerCase();
    if(uw==='status'){
      await waClient.sendMessage(rawFrom, 'Unapproved-payment alert to Day Book group is '+(loadUnapprovedAlert()?'ON':'OFF')+'. It always appears in your private summary regardless.');
    } else {
      saveUnapprovedAlert(uw==='on');
      await waClient.sendMessage(rawFrom, 'Unapproved-payment Day Book alert '+(uw==='on'?'ON — flagged payments will post to the Day Book group with the 7 PM cycle.':'OFF — findings stay in your private summary only.'));
    }
    return true;
  }
  // ── Manual intervention commands (only from SILENT_OBSERVER) ─────────────
  if(phoneOfSender === SILENT_OBSERVER){
    var cmdMatch = body.match(/^\s*(\d+)\s+(ok|confirm|reject|flag|ignore|chase|paid)(?:\s+(.+))?\s*$/i);
    if(cmdMatch){
      var idx = parseInt(cmdMatch[1]);
      var verb = cmdMatch[2].toLowerCase();
      var arg = cmdMatch[3] ? cmdMatch[3].trim() : '';
      var dmState = loadDMState();
      var lastOutliers = (dmState.lastOutliers && dmState.lastOutliers.items) || [];
      var target = lastOutliers.find(function(o){ return o.id === idx; });
      if(!target){
        await waClient.sendMessage(rawFrom, 'No outlier #'+idx+' in the most recent report. Run /api/outliers to see current list.');
        return true;
      }
      var cache = loadMatchCache();
      if(!cache.matches) cache.matches = {};
      if(!cache.rejected) cache.rejected = {};
      if(!cache.manualPaid) cache.manualPaid = {};
      if(verb === 'ok' || verb === 'confirm'){
        if(target.ledgerHash){
          cache.matches[target.expenseId] = Object.assign(cache.matches[target.expenseId] || {}, {
            ledgerHash: target.ledgerHash, stage: 'manual', confidence: 'manual', manuallyConfirmed: true, ts: new Date().toISOString()
          });
        }
        saveMatchCache(cache);
        await waClient.sendMessage(rawFrom, '✓ Outlier #'+idx+' confirmed and learned. Future similar matches will auto-pass.');
        return true;
      }
      if(verb === 'reject' || verb === 'flag'){
        if(target.ledgerHash){
          if(!cache.rejected[target.expenseId]) cache.rejected[target.expenseId] = [];
          if(cache.rejected[target.expenseId].indexOf(target.ledgerHash) < 0) cache.rejected[target.expenseId].push(target.ledgerHash);
          if(cache.matches[target.expenseId] && cache.matches[target.expenseId].ledgerHash === target.ledgerHash){
            cache.matches[target.expenseId].manuallyRejected = true;
          }
        }
        saveMatchCache(cache);
        await waClient.sendMessage(rawFrom, '✗ Outlier #'+idx+' rejected. The matcher will not suggest this pairing again.');
        return true;
      }
      if(verb === 'ignore'){
        if(!cache.ignored) cache.ignored = {};
        cache.ignored[target.expenseId] = true;
        saveMatchCache(cache);
        await waClient.sendMessage(rawFrom, '⏭ Outlier #'+idx+' will be hidden from future reports.');
        return true;
      }
      if(verb === 'paid'){
        cache.manualPaid[target.expenseId] = { paidDate: arg || new Date().toISOString().split('T')[0], ts: new Date().toISOString() };
        saveMatchCache(cache);
        await waClient.sendMessage(rawFrom, '✓ Outlier #'+idx+' marked as paid'+(arg?' on '+arg:'')+'. The matcher will reflect this in the next report.');
        return true;
      }
      if(verb === 'chase'){
        try {
          await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, '[BOT NUDGE] Reminder — please log Ledger entry for: ' + (target.expenseId || ''));
          await waClient.sendMessage(rawFrom, '📣 Nudge posted in approval group for outlier #'+idx+'.');
        } catch(e){
          await waClient.sendMessage(rawFrom, 'Could not post nudge: '+e.message);
        }
        return true;
      }
    }
  }
  // ── v2.6 NEW: MORE X drill-down commands (only from SILENT_OBSERVER) ─────
  // MORE STALE | MORE PAID | MORE TOLERANCE | MORE AWAITING | MORE MISMATCH
  // MORE UNMATCHED | MORE RECURRING — each dumps the full bucket as chunked text.
  if(phoneOfSender === SILENT_OBSERVER){
    var moreMatch = body.match(/^\s*more\s+(stale|paid|tolerance|awaiting|mismatch|unmatched|recurring)\s*$/i);
    if(moreMatch){
      var which = moreMatch[1].toLowerCase();
      try {
        var sendChunked = async function(jid, text){
          var maxLen = 3500;
          if(text.length <= maxLen){ await waClient.sendMessage(jid, text); return; }
          var chunks = [];
          var lines = text.split('\n');
          var cur = '';
          for(var i=0; i<lines.length; i++){
            if((cur + lines[i] + '\n').length > maxLen){ chunks.push(cur); cur = ''; }
            cur += lines[i] + '\n';
          }
          if(cur) chunks.push(cur);
          for(var ci=0; ci<chunks.length; ci++){
            await waClient.sendMessage(jid, '(' + (ci+1) + '/' + chunks.length + ')\n' + chunks[ci]);
            if(ci < chunks.length-1) await new Promise(function(r){setTimeout(r,1200);});
          }
        };
        if(which === 'stale'){
          var audit = await buildApprovalAudit(15);
          var allStale = audit.partialApproval.concat(
            audit.noApproval.filter(function(e){ return e.amount > 0; })
          );
          allStale.sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
          if(allStale.length === 0){
            await waClient.sendMessage(rawFrom, 'No stale pending approvals.');
          } else {
            var total = 0;
            var ls = ['FULL STALE PENDING LIST (' + allStale.length + '):', ''];
            allStale.forEach(function(e){
              var status = e.status.mm === 'yes' ? 'S pending' : e.status.sm === 'yes' ? 'M pending' : e.status.mm === 'question' ? 'Query open' : e.status.sm === 'question' ? 'Query open' : 'Both pending';
              var age = Math.floor((Date.now() - e.date.getTime()) / (60*60*1000));
              ls.push('- ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + ' [' + status + ', ' + age + 'h]');
              total += e.amount;
            });
            ls.push('');
            ls.push('Total: Rs.' + formatINR(total));
            await sendChunked(rawFrom, ls.join('\n'));
          }
          return true;
        }
        var rec = await buildReconciliation(30);
        if(which === 'paid'){
          if(rec.paid.length === 0){ await waClient.sendMessage(rawFrom, 'No fully-matched paid items.'); return true; }
          var sorted = rec.paid.slice().sort(function(a,b){
            var aD = (a.matchResult && a.matchResult.match) ? a.matchResult.match.date.getTime() : 0;
            var bD = (b.matchResult && b.matchResult.match) ? b.matchResult.match.date.getTime() : 0;
            return bD - aD;
          });
          var lp = ['FULL PAID LIST (' + sorted.length + '):', ''];
          sorted.forEach(function(e){
            var m = e.matchResult ? e.matchResult.match : null;
            var dt = m ? m.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'}) : '';
            lp.push('  ✓ ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + (m ? ' → ' + dt + ', ' + (m.bankAC||'-') : ''));
          });
          lp.push('');
          lp.push('Total paid: Rs.' + formatINR(rec.summary.totalPaid));
          await sendChunked(rawFrom, lp.join('\n'));
          return true;
        }
        if(which === 'tolerance'){
          if(rec.paidWithTolerance.length === 0){ await waClient.sendMessage(rawFrom, 'No items in the tolerance bucket.'); return true; }
          var lt = ['FULL TOLERANCE LIST (' + rec.paidWithTolerance.length + '):', ''];
          rec.paidWithTolerance.forEach(function(e){
            var m = e.matchResult ? e.matchResult.match : null;
            lt.push('  ~ ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + ' (paid Rs.' + formatINR(m ? m.amount : 0) + ')');
          });
          await sendChunked(rawFrom, lt.join('\n'));
          return true;
        }
        if(which === 'awaiting'){
          if(rec.awaitingPayment.length === 0){ await waClient.sendMessage(rawFrom, 'Nothing currently awaiting payment.'); return true; }
          var sortedA = rec.awaitingPayment.slice().sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
          var la = ['FULL AWAITING LIST (' + sortedA.length + '):', ''];
          sortedA.forEach(function(e){
            var d = e.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
            la.push('  ⏳ ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + ' (approved ' + d + ')');
          });
          la.push('');
          la.push('Total awaiting: Rs.' + formatINR(rec.summary.totalAwaiting));
          await sendChunked(rawFrom, la.join('\n'));
          return true;
        }
        if(which === 'mismatch'){
          if(rec.possibleMatch.length === 0){ await waClient.sendMessage(rawFrom, 'No possible mismatches.'); return true; }
          var lm2 = ['FULL MISMATCH LIST (' + rec.possibleMatch.length + '):', ''];
          rec.possibleMatch.forEach(function(e){
            var m = e.matchResult ? e.matchResult.match : null;
            lm2.push('  ⚠ ' + (e.vendor || e.body.substring(0,40)) + ' approved Rs.' + formatINR(e.amount) + ' vs Ledger Rs.' + formatINR(m ? m.amount : 0));
          });
          await sendChunked(rawFrom, lm2.join('\n'));
          return true;
        }
        if(which === 'unmatched'){
          if(!rec.ledgerWithoutApproval || rec.ledgerWithoutApproval.length === 0){
            await waClient.sendMessage(rawFrom, 'No unmatched Ledger payments — every recent payment has an approval.');
            return true;
          }
          var lu = ['LEDGER PAYMENTS WITHOUT APPROVAL (' + rec.ledgerWithoutApproval.length + '):', '', '(Last ' + REVERSE_SCAN_WINDOW_DAYS + ' days, floor Rs.' + formatINR(REVERSE_SCAN_MIN_AMOUNT) + ')', ''];
          rec.ledgerWithoutApproval.forEach(function(le){
            var dt = le.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
            var desc = (le.description||le.entity||'').substring(0,50);
            lu.push('  ❗ ' + desc + ' Rs.' + formatINR(le.amount) + ' (' + (le.bankAC||'-') + ', ' + dt + ')');
          });
          lu.push('');
          lu.push('Total: Rs.' + formatINR(rec.summary.totalUnmatchedLedger || 0));
          await sendChunked(rawFrom, lu.join('\n'));
          return true;
        }
        if(which === 'recurring'){
          if(!rec.ledgerRecurring || rec.ledgerRecurring.length === 0){
            await waClient.sendMessage(rawFrom, 'No recurring/auto Ledger items in the recent window.');
            return true;
          }
          var lr = ['RECURRING/AUTO LEDGER ITEMS (' + rec.ledgerRecurring.length + '):', '', '(Suppressed from unmatched list)', ''];
          rec.ledgerRecurring.forEach(function(le){
            var dt = le.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
            var desc = (le.description||le.entity||'').substring(0,50);
            lr.push('  ℹ ' + desc + ' Rs.' + formatINR(le.amount) + ' (' + (le.bankAC||'-') + ', ' + dt + ')');
          });
          await sendChunked(rawFrom, lr.join('\n'));
          return true;
        }
      } catch(e) {
        console.error('[MORE cmd]', e.message);
        await waClient.sendMessage(rawFrom, 'Failed to fetch list: ' + e.message);
        return true;
      }
    }
  }
  if(/^\s*help\s*$/i.test(body)){
    var helpLines = [
      'Send me an expense request and I will post it in the approval group.',
      '',
      'I need 4 things:',
      '1. Details (what the payment is for)',
      '2. Amount',
      '3. Company',
      '4. Bank A/C to pay from',
      '',
      'You can send them all at once like:',
      'Details: TMT bar payment slab 110-118',
      'Amount: 7,08,708',
      'Company: Hansaflon Buildcon',
      'From: Hansaflon JKB',
      '',
      'Or just write naturally — I will pick out company/account names if you mention them, and ask for whatever is missing.',
      '',
      'IMPORTANT: One expense per message. If you have multiple amounts, send them one at a time.',
      '',
      'Attachments (PDF/image): I will read them and confirm the amount with you BEFORE posting — so filename/OCR errors do not slip through.',
      '',
      'Commands:',
      '  show              see your current draft',
      '  edit <field>: x   change a field (details/amount/company/from)',
      '  edit from: 7      pick bank A/C #7 from the last list shown',
      '  cancel            clear and start over',
      '  yes               post the draft to the group',
      '  show              see your current draft',
      '  urgent            list your pending requests',
      '  urgent <n>        push request #n to the group urgently (tags M/S)'
    ];
    if(phoneOfSender === SILENT_OBSERVER){
      helpLines.push('');
      helpLines.push('— Observer commands —');
      helpLines.push('MORE STALE       full pending list');
      helpLines.push('MORE PAID        full matched/paid list');
      helpLines.push('MORE AWAITING    full not-yet-paid list');
      helpLines.push('MORE MISMATCH    full possible-mismatch list');
      helpLines.push('MORE UNMATCHED   Ledger payments without approval');
      helpLines.push('MORE RECURRING   suppressed auto/recurring items');
      helpLines.push('MORE TOLERANCE   paid within 5% tolerance');
      helpLines.push('silent on/off/status');
      helpLines.push('<n> ok | reject | flag | chase | paid <date>  (outlier action)');
    }
    await waClient.sendMessage(rawFrom, helpLines.join('\n'));
    return true;
  }

  // ── v2.7 NEW: SHOW command — display current draft ────────────────────────
  if(/^\s*show\s*$/i.test(body)){
    var draft = state.pending[rawFrom];
    if(!draft){
      await waClient.sendMessage(rawFrom, 'No pending draft. Send an expense request to start one.');
      return true;
    }
    var showLines = ['Your current draft:', ''];
    showLines.push('Details: ' + (draft.details || '(not set)'));
    showLines.push('Amount:  ' + (draft.amount > 0 ? 'Rs.' + formatINR(draft.amount) : '(not set)'));
    showLines.push('Company: ' + (draft.company || '(not set)'));
    showLines.push('From:    ' + (draft.fromAC || '(not set)'));
    if(draft.mediaFiles && draft.mediaFiles.length) showLines.push('Attachments: ' + draft.mediaFiles.length);
    if(draft.askedFor) showLines.push('\nWaiting for: ' + draft.askedFor);
    showLines.push('\nReply yes to post, edit <field>: <value> to change, or cancel to clear.');
    await waClient.sendMessage(rawFrom, showLines.join('\n'));
    return true;
  }

  // ── v2.7.2 NEW: URGENT trigger — re-notify a pending request before scheduled digests ──
  // `urgent`        → list this accountant's pending requests (0–14 days), numbered
  // `urgent <n>`    → immediately post that item to the approval group as URGENT,
  //                   tagging whoever hasn't approved. 2-hour cooldown per expense.
  var urgentMatch = body.match(/^\s*urgent\s*(\d+)?\s*$/i);
  if(urgentMatch){
    var myName = (senderInfo.contactName || '').toLowerCase();
    var uAudit = await buildApprovalAudit(REMINDER_MAX_AGE_DAYS + 1);
    var nowU = Date.now();
    var cutoffU = REMINDER_MAX_AGE_DAYS * 86400000;
    // This accountant's own pending requests, within window, real amount
    var myPending = uAudit.partialApproval.concat(uAudit.noApproval).filter(function(e){
      var mine = (e.sender||'').toLowerCase() === myName;
      var hasAmt = e.amount > 0 || (e.subItems && e.subItems.length > 0);
      var ageOk = (nowU - e.date.getTime()) <= cutoffU;
      var afterStartU = e.date.getTime() >= REPORT_START_MS;
      return mine && hasAmt && ageOk && afterStartU;
    });
    myPending.sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
    if(myPending.length === 0){
      await waClient.sendMessage(rawFrom, 'You have no pending requests in the last '+REMINDER_MAX_AGE_DAYS+' days. (If a request is older, please post it again as a fresh request.)');
      return true;
    }
    var pickNum = urgentMatch[1] ? parseInt(urgentMatch[1]) : null;
    if(!pickNum){
      // List them
      var ulines = ['Your pending requests — reply "urgent <number>" to push one now:', ''];
      myPending.forEach(function(e, i){
        var ageHrs = Math.floor((nowU - e.date.getTime())/(60*60*1000));
        var ageStr = ageHrs >= 24 ? Math.floor(ageHrs/24)+'d' : ageHrs+'h';
        var who = e.status.mm==='yes' ? 'S pending' : e.status.sm==='yes' ? 'M pending' : 'both pending';
        ulines.push((i+1)+'. '+(e.vendor||(e.body||'').substring(0,40))+' — Rs.'+formatINR(e.amount)+' ('+who+', '+ageStr+')');
      });
      await waClient.sendMessage(rawFrom, ulines.join('\n'));
      return true;
    }
    // urgent <n> → push that item
    if(pickNum < 1 || pickNum > myPending.length){
      await waClient.sendMessage(rawFrom, 'No item #'+pickNum+'. Reply "urgent" to see your list.');
      return true;
    }
    var target = myPending[pickNum - 1];
    // 2-hour cooldown per expense
    var urgState = loadDMState();
    if(!urgState.urgentCooldown) urgState.urgentCooldown = {};
    var lastUrgent = urgState.urgentCooldown[target.id];
    if(lastUrgent && (nowU - new Date(lastUrgent).getTime()) < 2*60*60*1000){
      var mins = Math.ceil((2*60*60*1000 - (nowU - new Date(lastUrgent).getTime()))/60000);
      await waClient.sendMessage(rawFrom, 'I already pushed this one recently. Please wait '+mins+' more minutes before pushing it again.');
      return true;
    }
    // Build the urgent post
    var needMu = target.status.mm !== 'yes';
    var needSu = target.status.sm !== 'yes';
    var uTags = [], uMentions = [];
    if(needMu){ uTags.push('@'+CONFIG.MM_PHONE); uMentions.push(CONFIG.MM_PHONE+'@c.us'); }
    if(needSu){ uTags.push('@'+CONFIG.SM_PHONE); uMentions.push(CONFIG.SM_PHONE+'@c.us'); }
    var ageHrsU = Math.floor((nowU - target.date.getTime())/(60*60*1000));
    var ageStrU = ageHrsU >= 24 ? Math.floor(ageHrsU/24)+'d' : ageHrsU+'h';
    var upost = ['*⚡ URGENT — approval needed*', ''];
    upost.push((target.vendor || (target.body||'').substring(0,60)));
    upost.push('Amount: Rs.'+formatINR(target.amount));
    upost.push('Pending '+ageStrU+' · requested by '+target.sender);
    upost.push('');
    upost.push(uTags.join(' ')+' please review urgently.');
    try {
      await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, upost.join('\n'), { mentions: uMentions });
      urgState.urgentCooldown[target.id] = new Date().toISOString();
      saveDMState(urgState);
      await waClient.sendMessage(rawFrom, 'Pushed as urgent to the approval group, tagging '+(needMu&&needSu?'M and S':needMu?'M':'S')+'.');
    } catch(e){
      await waClient.sendMessage(rawFrom, 'Could not post: '+e.message);
    }
    return true;
  }

  // 2+ amount patterns no longer auto-reject. AI decides: single ask with context
  // (Umesh's payment-plan style) vs genuine multiple requests (Ajit Singh style).
  var existingDraft = state.pending[rawFrom];
  var isAnsweringQuestion = existingDraft && existingDraft.askedFor;
  var isVisionConfirm = existingDraft && existingDraft.awaitingVisionConfirm;
  var isYesPost = /^\s*(yes|post|send|y)\s*$/i.test(body);
  var isEditCmd = /^\s*edit\s+/i.test(body);
  var isAiAmountConfirm = existingDraft && existingDraft.awaitingAiAmountConfirm;
  if(!isAnsweringQuestion && !isVisionConfirm && !isYesPost && !isEditCmd && !isAiAmountConfirm && body){
    var amtCount = countAmountPatterns(body);
    if(amtCount >= 2){
      var intent = await aiParseExpenseIntent(body);
      if(intent.kind === 'multiple'){
        await waClient.sendMessage(rawFrom, [
          'This looks like ' + intent.count + ' separate payment requests.',
          '',
          'Please send one expense request at a time. M and S review each separately, and Ledger matching stays clean.',
          '',
          'Send the first one by itself, then the next after it is posted.',
          '',
          'Reply cancel to clear.'
        ].join('\n'));
        return true;
      } else if(intent.kind === 'single'){
        // One ask + context. Pre-fill the draft and confirm the amount before posting.
        if(!state.pending[rawFrom]) state.pending[rawFrom] = { details: '', amount: 0, company: '', fromAC: '', mediaIds: [], lastUpdate: new Date().toISOString(), askedFor: null, posterName: senderInfo.contactName || rawFrom, subItems: null, companyOptions: [], fromACOptions: [] };
        var d0 = state.pending[rawFrom];
        d0.lastUpdate = new Date().toISOString();
        d0.posterName = senderInfo.contactName || d0.posterName;
        if(intent.details && !d0.details) d0.details = intent.details;
        d0.amount = intent.approvalAmount;
        d0.awaitingAiAmountConfirm = true;
        // Pull company/account out of the text if mentioned
        try {
          var sm0 = await smartExtractCompanyAccount(body);
          if(sm0.companyMatches.length === 1 && !d0.company) d0.company = sm0.companyMatches[0];
          if(sm0.acMatches.length === 1 && !d0.fromAC) d0.fromAC = sm0.acMatches[0];
        } catch(e){}
        saveDMState(state);
        var aiLines = ['I read this as ONE request for approval:'];
        aiLines.push('  Amount: Rs.' + formatINR(intent.approvalAmount));
        if(intent.details) aiLines.push('  Details: ' + intent.details);
        aiLines.push('');
        aiLines.push('The full breakdown will be included for M/S.');
        aiLines.push('');
        aiLines.push('Confirm: reply "yes" if Rs.' + formatINR(intent.approvalAmount) + ' is the amount to approve, or send the correct amount.');
        await waClient.sendMessage(rawFrom, aiLines.join('\n'));
        return true;
      } else {
        // unclear → safe fallback: block, ask to simplify
        await waClient.sendMessage(rawFrom, [
          'I see ' + amtCount + ' amounts here and I am not sure which one needs approval.',
          '',
          'Please send just the amount to approve, like: "approve 46,200 for granite balance freight".',
          '',
          'Reply cancel to clear.'
        ].join('\n'));
        return true;
      }
    }
  }

  // ── v2.7.2: handle reply to AI amount confirmation ──────────────────────
  if(existingDraft && existingDraft.awaitingAiAmountConfirm){
    var d1 = existingDraft;
    if(/^\s*(yes|y|ok|correct|sahi)\s*$/i.test(body)){
      d1.awaitingAiAmountConfirm = false;
      saveDMState(state);
      // fall through to normal validate-required-fields flow below
    } else {
      // Try to parse a corrected amount from their reply
      var corr = parseExpenseMessage(body);
      if(corr[0].amount > 0){
        d1.amount = corr[0].amount;
        d1.awaitingAiAmountConfirm = false;
        saveDMState(state);
        await waClient.sendMessage(rawFrom, 'Updated to Rs.' + formatINR(d1.amount) + '.');
        // fall through
      } else {
        await waClient.sendMessage(rawFrom, 'Reply "yes" to confirm Rs.' + formatINR(d1.amount) + ', or send the correct amount (e.g. "46,200" or "46200").');
        return true;
      }
    }
  }

  if(!state.pending[rawFrom]) state.pending[rawFrom] = { details: '', amount: 0, company: '', fromAC: '', mediaIds: [], lastUpdate: new Date().toISOString(), askedFor: null, posterName: senderInfo.contactName || rawFrom, subItems: null, companyOptions: [], fromACOptions: [] };
  var entry = state.pending[rawFrom];
  entry.lastUpdate = new Date().toISOString();
  entry.posterName = senderInfo.contactName || entry.posterName;
  function parseStructuredFields(text) {
    var result = {};
    if(!text) return result;
    var lines = text.split('\n');
    lines.forEach(function(line) {
      var m = line.match(/^\s*(details?|amount|company|from|account|reason|vendor|to)\s*[:\-]\s*(.+)$/i);
      if(m){
        var key = m[1].toLowerCase();
        var val = m[2].trim();
        if(key === 'detail' || key === 'details' || key === 'reason' || key === 'vendor' || key === 'to') result.details = val;
        else if(key === 'amount'){
          var p = parseExpenseMessage(val);
          if(p[0].amount > 0) result.amount = p[0].amount;
        }
        else if(key === 'company') result.company = val;
        else if(key === 'from' || key === 'account') result.fromAC = val;
      }
    });
    return result;
  }
  var structured = parseStructuredFields(body);
  if(structured.details && !entry.details) entry.details = cleanDetails(structured.details) || structured.details;
  if(structured.amount && entry.amount === 0) entry.amount = structured.amount;
  if(structured.company && !entry.company) entry.company = structured.company;
  if(structured.fromAC && !entry.fromAC) entry.fromAC = structured.fromAC;
  var hadAnyStructured = structured.details || structured.amount || structured.company || structured.fromAC;
  if(entry.askedFor && body && !hadAnyStructured){
    if(entry.askedFor === 'details'){
      entry.details = body;
      entry.askedFor = null;
    } else if(entry.askedFor === 'amount'){
      var pa = parseExpenseMessage(body);
      if(pa.length > 1){
        entry.subItems = pa;
        entry.amount = pa.reduce(function(s,p){return s+p.amount;},0);
        entry.askedFor = null;
      } else if(pa[0].amount > 0){
        entry.amount = pa[0].amount;
        entry.askedFor = null;
      } else {
        await waClient.sendMessage(rawFrom, 'I could not read an amount in "' + body.substring(0,50) + '". Try one of these formats:\n  • 3 lac\n  • 3,00,000\n  • 300000\n  • 10k\n  • 1.5 cr');
        saveDMState(state);
        return true;
      }
    } else if(entry.askedFor === 'company'){
      var trimmed = body.trim();
      if(/^\d+$/.test(trimmed) && entry.companyOptions && entry.companyOptions.length){
        var idx = parseInt(trimmed) - 1;
        if(idx >= 0 && idx < entry.companyOptions.length) entry.company = entry.companyOptions[idx];
        else entry.company = trimmed;
      } else {
        entry.company = trimmed;
      }
      entry.askedFor = null;
    } else if(entry.askedFor === 'fromAC'){
      var trimmed2 = body.trim();
      if(/^\d+$/.test(trimmed2) && entry.fromACOptions && entry.fromACOptions.length){
        var idx2 = parseInt(trimmed2) - 1;
        if(idx2 >= 0 && idx2 < entry.fromACOptions.length) entry.fromAC = entry.fromACOptions[idx2];
        else entry.fromAC = trimmed2;
      } else {
        entry.fromAC = trimmed2;
      }
      entry.askedFor = null;
    }
  } else if(body && !hadAnyStructured && !entry.askedFor){
    // v2.7: If there's a pendingConfirm (we asked "did you mean X?") and the user just replied with
    // yes/no/numeric, resolve it here BEFORE doing fresh free-form parse.
    if(entry.pendingConfirm){
      var pc = entry.pendingConfirm;
      var trimmedPC = body.trim().toLowerCase();
      if(trimmedPC === 'yes' || trimmedPC === 'y' || trimmedPC === 'confirm'){
        if(pc.type === 'company') entry.company = pc.value;
        else if(pc.type === 'fromAC') entry.fromAC = pc.value;
        entry.pendingConfirm = null;
        saveDMState(state);
        // Fall through — re-evaluate what's still missing in the next pass
      } else if(/^\d+$/.test(trimmedPC)){
        // User typed a number — defer to the standard numbered-pick flow below.
        // Clear pendingConfirm and let the askedFor logic re-prompt with full list.
        entry.pendingConfirm = null;
        // Continue — askedFor logic will kick in once we reach the validate-required-fields stage
      } else if(trimmedPC === 'no' || trimmedPC === 'n'){
        entry.pendingConfirm = null;
        saveDMState(state);
        // Fall through — will re-prompt below
      }
    }
    var parsed = parseExpenseMessage(body);
    if(parsed.length > 1){
      entry.subItems = parsed;
      if(entry.amount === 0) entry.amount = parsed.reduce(function(s,p){return s+p.amount;},0);
      if(!entry.details) entry.details = parsed.map(function(p){return p.vendor+' '+formatINR(p.amount);}).join(', ');
    } else {
      var p = parsed[0];
      if(p.amount > 0 && entry.amount === 0) entry.amount = p.amount;
      if(!entry.details && body.length > 0){
        var cleaned = cleanDetails(body);
        entry.details = cleaned.length > 1 ? cleaned : body.substring(0, 250);
      }
    }
    // v2.7 NEW: smart free-form extraction of company + account from anywhere in the text.
    // Only pre-fill if the field is currently empty (don't overwrite explicit user input).
    try {
      var smart = await smartExtractCompanyAccount(body);
      if(!entry.company && smart.companyMatches.length === 1){
        // Single confident match — confirm before locking in
        if(!entry.pendingConfirm){
          entry.pendingConfirm = { type: 'company', value: smart.companyMatches[0] };
        }
      } else if(!entry.company && smart.companyMatches.length > 1){
        // Multiple matches — let the standard askedFor flow ask via numbered list
      }
      if(!entry.fromAC && smart.acMatches.length === 1){
        if(!entry.pendingConfirm){
          entry.pendingConfirm = { type: 'fromAC', value: smart.acMatches[0] };
        }
      }
    } catch(e) { /* smart extract is best-effort */ }
  }
  if(hasMedia){
    try {
      var media = await msg.downloadMedia();
      if(media && media.data){
        var visionResult = await extractFromImage(media, thisMsgId);
        if(visionResult){
          // v2.7.2 NEW: multi-amount image guard — mirror the text rule.
          // If the image shows 2+ distinct payable amounts, refuse and ask for one at a time.
          // Cheques are exempt (they carry no expense amount).
          if(visionResult.imageType !== 'cheque' && visionResult.amountCount >= 2){
            await waClient.sendMessage(rawFrom, [
              'This image looks like it has ' + visionResult.amountCount + ' separate payments in it.',
              '',
              'Please send one expense per image — one bill/invoice at a time. Send the first one on its own, then the next after it is posted.',
              '',
              'Reply cancel to clear.'
            ].join('\n'));
            return true;
          }
          entry.mediaIds.push(thisMsgId);
          if(!entry.mediaFiles) entry.mediaFiles = [];
          entry.mediaFiles.push({ msgId: thisMsgId, filename: body || ('attachment_'+entry.mediaFiles.length+'.'+(media.mimetype||'').split('/')[1]), mimetype: media.mimetype, dataB64: media.data });
          // Cheques: no expense info to extract; just attach the media
          if(visionResult.imageType === 'cheque'){
            // No-op for amount/details from cheques
          } else {
            // v2.7 NEW: Vision confirmation step — never auto-fill amount from vision.
            // Always DM the parsed values back to the accountant and require explicit confirmation.
            entry.visionParsed = {
              vendor: visionResult.vendor || '',
              amount: visionResult.amount || 0,
              purpose: visionResult.purpose || '',
              confidence: visionResult.confidence || 'low'
            };
            // Purpose/vendor still feed into details (as before) since they're descriptive context.
            // Amount is the only thing we hold back for human confirmation.
            if(!entry.details && visionResult.confidence !== 'low'){
              var dParts = [];
              if(visionResult.vendor) dParts.push(visionResult.vendor);
              if(visionResult.purpose) dParts.push(visionResult.purpose);
              if(dParts.length > 0) entry.details = dParts.join(' - ').substring(0,250);
            }
            // If we haven't yet captured an amount AND vision extracted one, hold it for confirmation.
            if(entry.amount === 0 && visionResult.amount > 0){
              entry.awaitingVisionConfirm = true;
              saveDMState(state);
              var confLines = ['I read this attachment as:'];
              if(visionResult.vendor) confLines.push('  Vendor: ' + visionResult.vendor);
              confLines.push('  Amount: Rs.' + formatINR(visionResult.amount));
              if(visionResult.purpose) confLines.push('  Purpose: ' + visionResult.purpose);
              if(visionResult.confidence === 'low') confLines.push('  (vision confidence: LOW)');
              confLines.push('');
              confLines.push('Confirm the amount before I post:');
              confLines.push('  • Reply "yes" if Rs.' + formatINR(visionResult.amount) + ' is correct');
              confLines.push('  • Or reply with the correct amount (e.g. "1.5 lac" or "150000")');
              await waClient.sendMessage(rawFrom, confLines.join('\n'));
              return true;
            }
          }
        }
      }
    } catch(e) { console.error('[DM] media download failed:', e.message); }
  }
  // v2.7 NEW: handle response to vision confirmation
  if(entry.awaitingVisionConfirm && body){
    var trimmedVC = body.trim().toLowerCase();
    if(trimmedVC === 'yes' || trimmedVC === 'y' || trimmedVC === 'confirm' || trimmedVC === 'ok'){
      // Accept vision's amount as-is
      if(entry.visionParsed && entry.visionParsed.amount > 0){
        entry.amount = entry.visionParsed.amount;
      }
      entry.awaitingVisionConfirm = false;
      saveDMState(state);
      await waClient.sendMessage(rawFrom, 'Confirmed Rs.' + formatINR(entry.amount) + '. Continuing...');
      // Fall through — required-fields validation will pick up next
    } else {
      // Try parsing a corrected amount
      var pcVC = parseExpenseMessage(body);
      if(pcVC[0].amount > 0){
        entry.amount = pcVC[0].amount;
        entry.awaitingVisionConfirm = false;
        saveDMState(state);
        await waClient.sendMessage(rawFrom, 'Updated amount to Rs.' + formatINR(entry.amount) + '. Continuing...');
        // Fall through
      } else {
        await waClient.sendMessage(rawFrom, 'I could not read an amount in "' + body.substring(0,50) + '". Reply "yes" to accept Rs.' + formatINR(entry.visionParsed.amount) + ' or send the correct amount (e.g. "1.5 lac").');
        saveDMState(state);
        return true;
      }
    }
  }
  // v2.7 NEW: if we have a pendingConfirm (smart-detected company/account), ask before list-ask
  if(entry.pendingConfirm && !entry.askedFor){
    var pcEntry = entry.pendingConfirm;
    saveDMState(state);
    var label = pcEntry.type === 'company' ? 'company' : 'bank A/C';
    await waClient.sendMessage(rawFrom, 'Did you mean ' + label + ': *' + pcEntry.value + '*?\n\nReply "yes" to confirm, "no" to pick from the full list, or just type the correct name.');
    return true;
  }
  if(!entry.details){
    entry.askedFor = 'details';
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'What is this payment for? Reply with the details (vendor name + reason).');
    return true;
  }
  if(entry.amount <= 0){
    entry.askedFor = 'amount';
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'How much is this expense for? Reply with amount (e.g. "3 lac", "300000", or "10k").');
    return true;
  }
  if(!entry.company){
    entry.askedFor = 'company';
    var companies = '';
    try {
      var fp = await getFundPosition();
      var uniqueCompanies = [];
      fp.forEach(function(a){ if(a.company && uniqueCompanies.indexOf(a.company) < 0) uniqueCompanies.push(a.company); });
      entry.companyOptions = uniqueCompanies;
      if(uniqueCompanies.length > 0) companies = '\n\nReply with a number or the name:\n' + uniqueCompanies.map(function(c,i){return (i+1)+'. '+c;}).join('\n');
    } catch(e) { entry.companyOptions = []; }
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'Which company is this expense for?' + companies);
    return true;
  }
  if(!entry.fromAC){
    entry.askedFor = 'fromAC';
    var accounts = '';
    try {
      var fp2 = await getFundPosition();
      var allAccounts = [];
      fp2.forEach(function(a){ if(a.bankAC && allAccounts.indexOf(a.bankAC) < 0) allAccounts.push(a.bankAC); });
      entry.fromACOptions = allAccounts;
      if(allAccounts.length > 0) accounts = '\n\nReply with a number or the name:\n' + allAccounts.map(function(c,i){return (i+1)+'. '+c;}).join('\n');
    } catch(e) { entry.fromACOptions = []; }
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'Which bank A/C should we pay from?' + accounts);
    return true;
  }
  var preview = buildGroupPostFromDM(entry, entry.posterName);
  var confirmText = 'Ready to post in the approval group:\n\n' + preview + '\n\nReply:\n  yes                          to post\n  edit <field>: <new value>    to change details/amount/company/from\n  show                         to see this draft again\n  cancel                       to clear and start over';
  if(body.toLowerCase() === 'yes' || body.toLowerCase() === 'post' || body.toLowerCase() === 'send'){
    try {
      var groupText = buildGroupPostFromDM(entry, entry.posterName);
      var postedMsg;
      if(entry.mediaFiles && entry.mediaFiles.length > 0){
        var firstMedia = entry.mediaFiles[0];
        var mm = new MessageMedia(firstMedia.mimetype, firstMedia.dataB64, firstMedia.filename);
        postedMsg = await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, mm, { caption: groupText });
        for(var mi=1; mi<entry.mediaFiles.length; mi++){
          var nm = new MessageMedia(entry.mediaFiles[mi].mimetype, entry.mediaFiles[mi].dataB64, entry.mediaFiles[mi].filename);
          await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, nm);
          await new Promise(function(r){setTimeout(r,1500);});
        }
      } else {
        postedMsg = await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, groupText);
      }
      delete state.pending[rawFrom];
      saveDMState(state);
      await waClient.sendMessage(rawFrom, 'Posted to approval group. M/S will review and respond.');
      console.log('[DM] posted to group from', entry.posterName);
    } catch(e) {
      console.error('[DM] post failed:', e.message);
      await waClient.sendMessage(rawFrom, 'Failed to post: ' + e.message + '. Reply "yes" to retry or "cancel" to clear.');
    }
    return true;
  }
  var editMatch = body.match(/^\s*edit\s+(details|amount|company|from|account)\s*:\s*(.+)$/i);
  if(editMatch){
    var fld = editMatch[1].toLowerCase();
    var val = editMatch[2].trim();
    if(fld === 'details') entry.details = val;
    else if(fld === 'amount'){
      var pe = parseExpenseMessage(val);
      if(pe[0].amount > 0) entry.amount = pe[0].amount;
      else {
        await waClient.sendMessage(rawFrom, 'I could not read an amount in "' + val.substring(0,50) + '". Try "3 lac", "3,00,000", or "300000".');
        saveDMState(state);
        return true;
      }
    }
    else if(fld === 'company'){
      // v2.7 NEW: support serial-number pick (e.g. "edit company: 3")
      if(/^\d+$/.test(val) && entry.companyOptions && entry.companyOptions.length){
        var ci = parseInt(val) - 1;
        if(ci >= 0 && ci < entry.companyOptions.length) entry.company = entry.companyOptions[ci];
        else entry.company = val;
      } else {
        entry.company = val;
      }
    }
    else if(fld === 'from' || fld === 'account'){
      // v2.7 NEW: support serial-number pick (e.g. "edit from: 7")
      if(/^\d+$/.test(val) && entry.fromACOptions && entry.fromACOptions.length){
        var fi = parseInt(val) - 1;
        if(fi >= 0 && fi < entry.fromACOptions.length) entry.fromAC = entry.fromACOptions[fi];
        else entry.fromAC = val;
      } else {
        entry.fromAC = val;
      }
    }
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'Updated. Here is the current draft:\n\n' + buildGroupPostFromDM(entry, entry.posterName) + '\n\nReply "yes" to post, or "edit <field>: <value>" to keep editing.');
    return true;
  }
  saveDMState(state);
  await waClient.sendMessage(rawFrom, confirmText);
  return true;
}
// ── Stale-pending scanner ────────────────────────────────────────────────────
var STALE_STATE_FILE = './wa_auth/reminder_state.json';
function loadStaleState() {
  try {
    if(fs.existsSync(STALE_STATE_FILE)){
      return JSON.parse(fs.readFileSync(STALE_STATE_FILE, 'utf8'));
    }
  } catch(e) { console.error('[Stale] state load failed:', e.message); }
  return { reminded: {} };
}
function saveStaleState(state) {
  try {
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth', { recursive: true });
    fs.writeFileSync(STALE_STATE_FILE, JSON.stringify(state, null, 2));
  } catch(e) { console.error('[Stale] state save failed:', e.message); }
}
// ── Silent mode toggle ───────────────────────────────────────────────────────
var SILENT_STATE_FILE = './wa_auth/silent_mode.json';
var SILENT_OBSERVER = '917838537000';
// ── v2.8 Module 7: unapproved-payment Day Book alert (toggle, default OFF) ───
var UNAPPROVED_ALERT_FILE = './wa_auth/unapproved_alert.json';
function loadUnapprovedAlert(){ try{ if(fs.existsSync(UNAPPROVED_ALERT_FILE)){ return JSON.parse(fs.readFileSync(UNAPPROVED_ALERT_FILE,'utf8')).enabled===true; } }catch(e){} return false; }
function saveUnapprovedAlert(on){ try{ if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth',{recursive:true}); fs.writeFileSync(UNAPPROVED_ALERT_FILE, JSON.stringify({enabled:!!on})); }catch(e){} }
// Posts flagged unapproved payments to the Day Book group when the toggle is ON.
// The same findings ALWAYS appear in the private EOD summary regardless.
async function postUnapprovedAlertIfEnabled(rec){
  try{
    if(!loadUnapprovedAlert()) return 0;
    if(!waReady || !rec || !rec.ledgerWithoutApproval || rec.ledgerWithoutApproval.length===0) return 0;
    var items = rec.ledgerWithoutApproval.slice(0,10);
    var lines = ['*PAYMENTS WITHOUT M/S APPROVAL*',''];
    var tot=0;
    items.forEach(function(le,i){
      var dt = le.date ? le.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'}) : '';
      lines.push((i+1)+'. '+(le.description||'(no description)').substring(0,60)+' — Rs.'+formatINR(le.amount)+' · '+(le.bankAC||'-')+' · '+dt);
      tot+=le.amount;
    });
    if(rec.ledgerWithoutApproval.length>10) lines.push('+'+(rec.ledgerWithoutApproval.length-10)+' more');
    lines.push('');
    lines.push(rec.ledgerWithoutApproval.length+' payment(s) · Rs.'+formatINR(tot)+' — please verify these were authorised.');
    await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID, lines.join('\n'));
    console.log('[UnapprovedAlert] posted', items.length, 'to Day Book');
    return items.length;
  }catch(e){ console.error('[UnapprovedAlert]', e.message); return 0; }
}
function loadSilentMode() {
  try {
    if(fs.existsSync(SILENT_STATE_FILE)){
      var s = JSON.parse(fs.readFileSync(SILENT_STATE_FILE, 'utf8'));
      return s.enabled === true;
    }
  } catch(e) { console.error('[Silent] load failed:', e.message); }
  return true;
}
function saveSilentMode(enabled) {
  try {
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth', { recursive: true });
    fs.writeFileSync(SILENT_STATE_FILE, JSON.stringify({ enabled: enabled, updatedAt: new Date().toISOString() }, null, 2));
    console.log('[Silent] mode set to:', enabled ? 'ON (silent)' : 'OFF (group reminders active)');
  } catch(e) { console.error('[Silent] save failed:', e.message); }
}
function getSilentObserverJid() {
  return SILENT_OBSERVER + '@c.us';
}
function buildStaleReminderText(expense, now) {
  var mm = expense.status.mm, sm = expense.status.sm;
  var bothPending = mm === 'pending' && sm === 'pending';
  var mmOnly = mm === 'pending' && sm === 'yes';
  var smOnly = sm === 'pending' && mm === 'yes';
  var queryMM = mm === 'question', querySM = sm === 'question';
  var queryAnswered = expense.queryAnswer ? true : false;
  var mentionJids = [];
  var mentionTags = [];
  if(mm === 'pending' || (queryMM && queryAnswered)) {
    mentionJids.push(CONFIG.MM_PHONE + '@c.us');
    mentionTags.push('@' + CONFIG.MM_PHONE);
  }
  if(sm === 'pending' || (querySM && queryAnswered)) {
    mentionJids.push(CONFIG.SM_PHONE + '@c.us');
    mentionTags.push('@' + CONFIG.SM_PHONE);
  }
  var queryUnanswered = (queryMM || querySM) && !queryAnswered;
  if(queryUnanswered) {
    mentionJids = []; mentionTags = [];
    CONFIG.ACCOUNTANT_PHONES.forEach(function(p){
      mentionJids.push(p + '@c.us');
      mentionTags.push('@' + p);
    });
  }
  if(mentionJids.length === 0) return null;
  var minutesPending = Math.floor((now - expense.date.getTime()) / (60*1000));
  var lines = [];
  var header;
  if(queryUnanswered) header = '[BOT REMINDER] Query unanswered - pending ' + minutesPending + ' min';
  else if(queryMM || querySM) header = '[BOT REMINDER] Query answered - awaiting M/S - pending ' + minutesPending + ' min';
  else if(bothPending) header = '[BOT REMINDER] Approval needed - pending ' + minutesPending + ' min';
  else if(mmOnly) header = '[BOT REMINDER] M approval needed - pending ' + minutesPending + ' min';
  else if(smOnly) header = '[BOT REMINDER] S approval needed - pending ' + minutesPending + ' min';
  else return null;
  lines.push(header);
  lines.push('');
  var vendor = expense.vendor || expense.body.substring(0, 60);
  lines.push(vendor);
  if(expense.subItems && expense.subItems.length > 1){
    var total = expense.subItems.reduce(function(s, it){ return s + it.amount; }, 0);
    lines.push('Amount: Rs.' + formatINR(total) + ' total');
    expense.subItems.forEach(function(si){ lines.push('  - ' + si.vendor + ' Rs.' + formatINR(si.amount)); });
  } else if(expense.amount > 0){
    lines.push('Amount: Rs.' + formatINR(expense.amount));
  }
  var d = expense.date.toLocaleDateString('en-IN', {day:'numeric', month:'short', timeZone:'Asia/Kolkata'});
  var t = expense.date.toLocaleTimeString('en-IN', {hour:'2-digit', minute:'2-digit', timeZone:'Asia/Kolkata'});
  lines.push('Requested by: ' + expense.sender + ' - ' + d + ', ' + t);
  if(expense.supportingDocs && expense.supportingDocs.length > 0){
    var docNames = expense.supportingDocs.map(function(dd){ return dd.filename; }).join(', ');
    lines.push('Supporting docs: ' + docNames);
  }
  lines.push('');
  var mmLabel = mm==='yes'?'M: Ok':mm==='question'?'M: query raised':'M: pending';
  var smLabel = sm==='yes'?'S: Ok':sm==='question'?'S: query raised':'S: pending';
  lines.push(mmLabel + ' | ' + smLabel);
  if((queryMM||querySM) && queryAnswered){
    var ans = expense.queryAnswer;
    var who = ans.role === 'mm' ? 'M' : 'S';
    var answerLabel = ans.answerByRole && ans.answerByRole !== 'accountant' ? (ans.answerByRole + ' (' + ans.answerBy + ')') : ans.answerBy;
    lines.push('');
    lines.push(who + ' asked:');
    lines.push('"' + ans.question + '"');
    lines.push('');
    lines.push(answerLabel + ' answered:');
    lines.push('"' + ans.answer + '"');
  }
  lines.push('');
  lines.push(mentionTags.join(' ') + ' please reply: Yes / No / Hold / or reason');
  return { text: lines.join('\n'), mentionJids: mentionJids };
}
async function scanStalePendings() {
  if(!waReady || !CONFIG.BOT_ENABLED) return 0;
  if(loadSilentMode()){console.log('[Stale] silent mode ON — skipping group scan');return 0;}
  try {
    var state = loadStaleState();
    var audit = await buildApprovalAudit(2);
    var now = Date.now();
    var THIRTY_MIN = 30 * 60 * 1000;
    var nowIST = new Date(now);
    var hourIST = parseInt(nowIST.toLocaleString('en-IN', { hour: '2-digit', hour12: false, timeZone: 'Asia/Kolkata' }));
    if(hourIST >= 21 || hourIST < 9) {
      console.log('[Stale] quiet hours (IST '+hourIST+'h), skipping scan');
      return 0;
    }
    var candidates = audit.partialApproval.concat(
      audit.noApproval.filter(function(e){ return e.amount > 0 || (e.subItems && e.subItems.length > 0); })
    );
    var sentCount = 0;
    var delay = function(ms){ return new Promise(function(r){ setTimeout(r, ms); }); };
    for(var i=0; i<candidates.length; i++) {
      var expense = candidates[i];
      var expenseAge = now - expense.date.getTime();
      if(expenseAge < THIRTY_MIN) continue;
      if(state.reminded[expense.id]) continue;
      var built = buildStaleReminderText(expense, now);
      if(!built) continue;
      try {
        await waClient.sendMessage(CONFIG.APPROVAL_GROUP_JID, built.text, { mentions: built.mentionJids });
        state.reminded[expense.id] = { sentAt: new Date().toISOString(), missing: [] };
        if(expense.status.mm === 'pending') state.reminded[expense.id].missing.push('mm');
        if(expense.status.sm === 'pending') state.reminded[expense.id].missing.push('sm');
        sentCount++;
        console.log('[Stale] reminded', expense.id, '(', expense.amount, ')');
        await delay(2000);
      } catch(e) {
        console.error('[Stale] send failed for', expense.id, e.message);
      }
    }
    if(sentCount > 0) saveStaleState(state);
    return sentCount;
  } catch(e) {
    console.error('[Stale] scan failed:', e.message);
    return 0;
  }
}
// ── v2.6 REWRITE: buildStalePendingSection — top-N recent + older rollup ──
async function buildStalePendingSection() {
  try {
    var audit = await buildApprovalAudit(7);
    var stillPending = audit.partialApproval.concat(
      audit.noApproval.filter(function(e){ return e.amount > 0; })
    ).filter(function(e){ return e.date.getTime() >= REPORT_START_MS; }); // v2.8 cutoff
    if(stillPending.length === 0) return '';
    var nowMs = Date.now();
    stillPending.sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
    var recent = [];
    var older = [];
    stillPending.forEach(function(e){
      var ageHrs = (nowMs - e.date.getTime()) / (60*60*1000);
      if(ageHrs <= STALE_RECENT_HOURS) recent.push(e);
      else older.push(e);
    });
    if(recent.length > STALE_TOP_N){
      older = recent.slice(STALE_TOP_N).concat(older);
      recent = recent.slice(0, STALE_TOP_N);
    }
    var lines = [''];
    lines.push('--- STALE PENDING (recent) ---');
    lines.push('');
    var totalRecent = 0;
    recent.forEach(function(e){
      var status = e.status.mm === 'yes' ? 'S pending' : e.status.sm === 'yes' ? 'M pending' : e.status.mm === 'question' ? 'Query open' : e.status.sm === 'question' ? 'Query open' : 'Both pending';
      var age = Math.floor((nowMs - e.date.getTime()) / (60*60*1000));
      lines.push('- ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + ' [' + status + ', ' + age + 'h]');
      totalRecent += e.amount;
    });
    lines.push('');
    lines.push('Total recent: Rs.' + formatINR(totalRecent));
    if(older.length > 0){
      var totalOlder = older.reduce(function(s,e){return s + e.amount;}, 0);
      lines.push('+ ' + older.length + ' older items totalling Rs.' + formatINR(totalOlder) + ' — reply MORE STALE to see all');
    }
    return lines.join('\n');
  } catch(e) {
    console.error('[StaleSection] failed:', e.message);
    return '';
  }
}
// ── Ledger + Fund Position ────────────────────────────────────────────────────
async function getLedgerData(dateStr) {
  var rows=await readSheet('Ledger!A:L');
  var target=dateStr||new Date().toISOString().split('T')[0], entries=[];
  for(var i=0;i<rows.length;i++){
    var row=rows[i]; if(!row[0]||!row[5])continue;
    var cd=parseSheetDate(row[0]); if(!cd)continue;
    if(cd.toISOString().split('T')[0]===target){
      entries.push({date:cd,entity:row[1]||'',head:row[2]||'',description:row[3]||'',tag:row[4]||'',inOut:row[5]||'',amount:parseAmount(row[6]),mode:row[7]||'',person:row[8]||'',bankAC:row[9]||'',transferTo:row[10]||'',notes:row[11]||''});
    }
  }
  return entries;
}
async function getFundPosition() {
  var rows=await readSheet('Fund Position!A4:J27'), accounts=[];
  for(var i=1;i<rows.length;i++){
    var r=rows[i]; if(!r[1]||r[1]==='TOTAL')continue;
    accounts.push({num:r[0]||'',company:r[1]||'',bankAC:r[2]||'',opening:parseAmount(r[3]),todayIn:parseAmount(r[4]),todayOut:parseAmount(r[5]),closing:parseAmount(r[6]),cheques:parseAmount(r[7]),netBal:parseAmount(r[8]),status:r[9]||'Usable'});
  }
  return accounts;
}
async function getLedgerRange(startDate, endDate) {
  var rows = await readSheet('Ledger!A:L');
  var entries = [];
  var startMs = startDate ? startDate.getTime() : 0;
  var endMs = endDate ? endDate.getTime() : Date.now() + 86400000;
  for(var i=0; i<rows.length; i++){
    var row = rows[i]; if(!row[0] || !row[5]) continue;
    var cd = parseSheetDate(row[0]); if(!cd) continue;
    var t = cd.getTime();
    if(t < startMs || t > endMs) continue;
    entries.push({
      date: cd,
      entity: row[1]||'', head: row[2]||'', description: row[3]||'',
      tag: row[4]||'', inOut: row[5]||'', amount: parseAmount(row[6]),
      mode: row[7]||'', person: row[8]||'', bankAC: row[9]||'',
      transferTo: row[10]||'', notes: row[11]||''
    });
  }
  return entries;
}
// ── Stage 2: Fuzzy matching helpers ──────────────────────────────────────────
function levenshtein(a, b) {
  if(!a) return b ? b.length : 0;
  if(!b) return a.length;
  a = a.toLowerCase(); b = b.toLowerCase();
  if(a === b) return 0;
  var m = a.length, n = b.length;
  var prev = new Array(n+1), curr = new Array(n+1);
  for(var i=0; i<=n; i++) prev[i] = i;
  for(var i2=1; i2<=m; i2++){
    curr[0] = i2;
    for(var j=1; j<=n; j++){
      var cost = a[i2-1] === b[j-1] ? 0 : 1;
      curr[j] = Math.min(curr[j-1]+1, prev[j]+1, prev[j-1]+cost);
    }
    var tmp = prev; prev = curr; curr = tmp;
  }
  return prev[n];
}
function soundex(s) {
  if(!s) return '';
  s = s.toUpperCase().replace(/[^A-Z]/g, '');
  if(!s) return '';
  var first = s[0];
  var map = { B:1,F:1,P:1,V:1, C:2,G:2,J:2,K:2,Q:2,S:2,X:2,Z:2, D:3,T:3, L:4, M:5,N:5, R:6 };
  var code = '';
  var prev = map[first] || '';
  for(var i=1; i<s.length && code.length<3; i++){
    var c = map[s[i]];
    if(c && c !== prev) code += c;
    if(s[i] !== 'H' && s[i] !== 'W') prev = c || '';
  }
  return (first + code + '000').substring(0,4);
}
function fuzzyMatch(a, b) {
  if(!a || !b) return false;
  a = a.toLowerCase(); b = b.toLowerCase();
  if(a === b) return true;
  if(a.indexOf(b) >= 0 || b.indexOf(a) >= 0) return true;
  var maxLen = Math.max(a.length, b.length);
  var threshold = maxLen <= 8 ? 3 : 4;
  var dist = levenshtein(a, b);
  if(dist <= threshold) return true;
  if(/^[a-z]+$/.test(a) && /^[a-z]+$/.test(b)){
    if(soundex(a) === soundex(b)) return true;
  }
  return false;
}
function ledgerFuzzyWordOverlap(ledgerEntry, expense) {
  if(!ledgerEntry || !expense) return null;
  var ledgerText = ((ledgerEntry.description||'') + ' ' + (ledgerEntry.entity||'') + ' ' + (ledgerEntry.person||'')).toLowerCase();
  var expenseTextRaw = (expense.vendor||'') + ' ' + (expense.body||'');
  if(expense.supportingDocs && expense.supportingDocs.length){
    expense.supportingDocs.forEach(function(d){ expenseTextRaw += ' ' + (d.vendor||'') + ' ' + (d.purpose||''); });
  }
  var expenseText = expenseTextRaw.toLowerCase();
  var stopWords = ['the','and','for','with','from','this','that','please','approve','kindly','payment','amount','rs','inr','total','expense','expenses','final'];
  var expWords = expenseText.split(/[^a-z0-9]+/).filter(function(w){
    return w.length >= 4 && stopWords.indexOf(w) < 0;
  });
  var ledgerWords = ledgerText.split(/[^a-z0-9]+/).filter(function(w){ return w.length >= 4; });
  for(var i=0; i<expWords.length; i++){
    for(var j=0; j<ledgerWords.length; j++){
      if(fuzzyMatch(expWords[i], ledgerWords[j])){
        return { matchedExpWord: expWords[i], matchedLedgerWord: ledgerWords[j] };
      }
    }
  }
  return null;
}
function ledgerWordOverlap(ledgerEntry, expense) {
  if(!ledgerEntry || !expense) return false;
  var ledgerText = ((ledgerEntry.description||'') + ' ' + (ledgerEntry.entity||'') + ' ' + (ledgerEntry.person||'')).toLowerCase();
  var expenseTextRaw = (expense.vendor||'') + ' ' + (expense.body||'');
  if(expense.supportingDocs && expense.supportingDocs.length){
    expense.supportingDocs.forEach(function(d){ expenseTextRaw += ' ' + (d.vendor||'') + ' ' + (d.purpose||''); });
  }
  var expenseText = expenseTextRaw.toLowerCase();
  var stopWords = ['the','and','for','with','from','this','that','please','approve','kindly','payment','amount','rs','inr','total','expense','expenses','final'];
  var expWords = expenseText.split(/[^a-z0-9]+/).filter(function(w){
    return w.length >= 3 && stopWords.indexOf(w) < 0;
  });
  var ledgerWords = ledgerText.split(/[^a-z0-9]+/);
  for(var w=0; w<expWords.length; w++){
    if(ledgerWords.indexOf(expWords[w]) >= 0) return true;
  }
  return false;
}
// ── Match Cache ──────────────────────────────────────────────────────────────
var MATCH_CACHE_FILE = './wa_auth/match_cache.json';
function loadMatchCache() {
  try {
    if(fs.existsSync(MATCH_CACHE_FILE)){
      return JSON.parse(fs.readFileSync(MATCH_CACHE_FILE, 'utf8'));
    }
  } catch(e) { console.error('[MatchCache] load failed:', e.message); }
  return { matches: {}, rejected: {}, manualPaid: {} };
}
function saveMatchCache(cache) {
  try {
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth', { recursive: true });
    fs.writeFileSync(MATCH_CACHE_FILE, JSON.stringify(cache, null, 2));
  } catch(e) { console.error('[MatchCache] save failed:', e.message); }
}
function ledgerEntryHash(le) {
  if(!le) return null;
  var d = le.date ? le.date.toISOString().split('T')[0] : '';
  return d + '|' + le.amount + '|' + (le.bankAC||'') + '|' + (le.description||'').substring(0,50);
}
// ── Stage 3: Haiku semantic matcher ──────────────────────────────────────────
async function haikuSemanticMatch(expense, candidates) {
  if(!CONFIG.CLAUDE_API_KEY) return null;
  if(!candidates || candidates.length === 0) return null;
  var top5 = candidates.slice(0,5);
  var expenseSummary = (expense.vendor || '') + (expense.body && expense.body !== expense.vendor ? ' · ' + expense.body : '');
  if(expense.supportingDocs && expense.supportingDocs.length){
    expense.supportingDocs.forEach(function(d){
      if(d.vendor) expenseSummary += ' · doc vendor: ' + d.vendor;
      if(d.purpose) expenseSummary += ' · doc purpose: ' + d.purpose;
    });
  }
  var candidateLines = top5.map(function(c, i){
    var dt = c.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',year:'numeric'});
    return (i) + '. ' + dt + ' · Rs.' + c.amount + ' · ' + (c.bankAC||'-') + ' · ' + (c.description||'') + (c.entity?' · '+c.entity:'') + (c.person?' · person:'+c.person:'');
  }).join('\n');
  var prompt = 'You are matching an approved expense request to its actual Ledger payment.\n\n' +
    'APPROVED EXPENSE:\n' +
    'Date: ' + expense.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',year:'numeric'}) + '\n' +
    'Amount: Rs.' + expense.amount + '\n' +
    'Description: ' + expenseSummary + '\n\n' +
    'CANDIDATE LEDGER ENTRIES:\n' + candidateLines + '\n\n' +
    'Which candidate (if any) is the actual payment for this approval? ' +
    'Consider synonyms (e.g. TMT bar = Steel rods), informal vs formal names, Hindi/English mix, abbreviations. ' +
    'Reply ONLY with strict JSON: {"match": <index 0-' + (top5.length-1) + '|null>, "confidence": <0.0-1.0>, "reasoning": "<one sentence>"}';
  try {
    var resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': CONFIG.CLAUDE_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 200,
        messages: [{ role: 'user', content: prompt }]
      })
    });
    if(!resp.ok){ console.error('[Haiku] HTTP', resp.status); return null; }
    var data = await resp.json();
    var text = data.content && data.content[0] && data.content[0].text || '';
    var jsonMatch = text.match(/\{[\s\S]*\}/);
    if(!jsonMatch) return null;
    var parsed = JSON.parse(jsonMatch[0]);
    if(parsed.match === null || parsed.match === undefined) return { match: null, confidence: 0, reasoning: parsed.reasoning };
    var idx = parseInt(parsed.match);
    if(isNaN(idx) || idx < 0 || idx >= top5.length) return null;
    return {
      match: top5[idx],
      confidence: parseFloat(parsed.confidence) || 0,
      reasoning: parsed.reasoning || '',
      candidatesShown: top5.length
    };
  } catch(e) {
    console.error('[Haiku] semantic match failed:', e.message);
    return null;
  }
}
function matchLedgerEntry(expense, ledgerEntries) {
  // v2.8: reconcile against the APPROVED amount (Rule A amendments), not the requested one.
  var effAmt = (expense && (expense.approvedAmount || expense.amount)) || 0;
  if(!expense || effAmt <= 0) return null;
  var approvalDate = expense.date.getTime();
  var fourteenDaysMs = 14 * 24 * 60 * 60 * 1000;
  var sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
  var strictMatches = [];
  var tolMatches = [];
  var amountOnlyCandidates = [];
  for(var i=0; i<ledgerEntries.length; i++){
    var le = ledgerEntries[i];
    if(le.inOut !== 'OUT') continue;
    var ledgerMs = le.date.getTime();
    var dateDiff = ledgerMs - approvalDate;
    if(dateDiff < -86400000) continue;
    if(dateDiff > fourteenDaysMs) continue;
    var amtDiff = Math.abs(le.amount - effAmt);
    var pctDiff = amtDiff / effAmt;
    var strictWords = ledgerWordOverlap(le, expense);
    var fuzzyWordsRes = strictWords ? null : ledgerFuzzyWordOverlap(le, expense);
    var fuzzyWords = fuzzyWordsRes ? true : false;
    if(amtDiff <= 1 && strictWords){
      strictMatches.push({ entry: le, dateDiffDays: Math.round(dateDiff/86400000) });
      continue;
    }
    if(pctDiff <= 0.05 && strictWords){
      tolMatches.push({ entry: le, dateDiffDays: Math.round(dateDiff/86400000), pctDiff: pctDiff });
      continue;
    }
    // v2.8: fuzzy and date-proximity cases no longer auto-match (they produced the
    // BSES/YEIDA false pairings). They become candidates for the Sonnet matcher.
    if(pctDiff <= 0.10 || ((strictWords || fuzzyWords) && Math.abs(dateDiff) <= sevenDaysMs)){
      amountOnlyCandidates.push(le);
    }
  }
  if(strictMatches.length > 0){
    return { status: 'paid', confidence: 'high', stage: 'exact', match: strictMatches[0].entry, dateDiffDays: strictMatches[0].dateDiffDays };
  }
  if(tolMatches.length > 0){
    return { status: 'paid_with_tolerance', confidence: 'medium', stage: 'exact_tolerance', match: tolMatches[0].entry, dateDiffDays: tolMatches[0].dateDiffDays, pctDiff: tolMatches[0].pctDiff };
  }
  if(amountOnlyCandidates.length > 0){
    return { status: 'awaiting_payment', confidence: null, stage: 'needs_ai', match: null, candidates: amountOnlyCandidates };
  }
  return { status: 'awaiting_payment', confidence: null, stage: 'no_match', match: null };
}
async function matchLedgerEntryWithAI(expense, ledgerEntries, cache) {
  if(!cache) cache = loadMatchCache();
  var cached = cache.matches[expense.id];
  if(cached){
    var matchedEntry = ledgerEntries.find(function(le){ return ledgerEntryHash(le) === cached.ledgerHash; });
    if(matchedEntry){
      return {
        status: cached.manuallyRejected ? 'awaiting_payment' : 'paid',
        confidence: cached.manuallyConfirmed ? 'manual' : (cached.confidence || 'cached'),
        stage: 'cached',
        match: matchedEntry,
        cachedFromStage: cached.stage,
        manuallyConfirmed: cached.manuallyConfirmed || false
      };
    }
  }
  if(cache.manualPaid && cache.manualPaid[expense.id]){
    return { status: 'paid', confidence: 'manual', stage: 'manual_paid', match: null, manualPaidDate: cache.manualPaid[expense.id].paidDate };
  }
  var result = matchLedgerEntry(expense, ledgerEntries);
  if(!result) return null;
  if(result.status === 'paid' || result.status === 'paid_with_tolerance'){
    if(result.match){
      cache.matches[expense.id] = {
        ledgerHash: ledgerEntryHash(result.match),
        stage: result.stage,
        confidence: result.confidence,
        approvedAt: expense.date.toISOString(),
        paidAt: result.match.date ? result.match.date.toISOString() : null,
        gapDays: typeof result.dateDiffDays==='number' ? result.dateDiffDays : null,
        ts: new Date().toISOString()
      };
      saveMatchCache(cache);
    }
    return result;
  }
  if(result.stage === 'needs_ai' && result.candidates && result.candidates.length > 0 && CONFIG.CLAUDE_API_KEY){
    var rejectedHashes = (cache.rejected[expense.id]) || [];
    var validCandidates = result.candidates.filter(function(c){ return rejectedHashes.indexOf(ledgerEntryHash(c)) < 0; });
    if(validCandidates.length === 0){
      return { status: 'awaiting_payment', confidence: null, stage: 'no_match', match: null };
    }
    var aiResult = await haikuSemanticMatch(expense, validCandidates);
    if(aiResult && aiResult.match){
      var aiMatch = aiResult.match;
      var status, confidence;
      if(aiResult.confidence >= 0.85){ status = 'paid'; confidence = 'ai_high'; }
      else if(aiResult.confidence >= 0.55){ status = 'possible_match'; confidence = 'ai_medium'; }
      else { return { status: 'awaiting_payment', confidence: null, stage: 'ai_rejected', aiReasoning: aiResult.reasoning }; }
      if(confidence === 'ai_high'){
        cache.matches[expense.id] = {
          ledgerHash: ledgerEntryHash(aiMatch),
          stage: 'ai',
          confidence: confidence,
          aiConfidence: aiResult.confidence,
          aiReasoning: aiResult.reasoning,
          approvedAt: expense.date.toISOString(),
          paidAt: aiMatch.date ? aiMatch.date.toISOString() : null,
          gapDays: aiMatch.date ? Math.round((aiMatch.date.getTime()-expense.date.getTime())/86400000) : null,
          ts: new Date().toISOString()
        };
        saveMatchCache(cache);
      }
      return {
        status: status,
        confidence: confidence,
        stage: 'ai',
        match: aiMatch,
        aiConfidence: aiResult.confidence,
        aiReasoning: aiResult.reasoning
      };
    }
  }
  return result;
}
// ── v2.6 EXTENDED: buildReconciliation with reverse-direction scan ───────────
async function buildReconciliation(days) {
  var audit = await buildApprovalAudit(days || 30);
  var approved = audit.fullyApproved;
  // Even if no approvals, we still want to run the reverse pass so a Ledger
  // payment with no approval at all gets flagged. So we always continue.
  var earliest = approved.length > 0
    ? approved.reduce(function(min, e){ return e.date.getTime() < min ? e.date.getTime() : min; }, Date.now())
    : Date.now() - (REVERSE_SCAN_WINDOW_DAYS * 86400000);
  var startDate = new Date(earliest - 86400000);
  var endDate = new Date(Date.now() + 86400000);
  var ledgerEntries = await getLedgerRange(startDate, endDate);
  var paid = [], paidWithTolerance = [], possibleMatch = [], awaitingPayment = [];
  var totalApproved = 0, totalPaid = 0, totalAwaiting = 0;
  var cache = loadMatchCache();
  var matcherStats = { exact: 0, exact_tolerance: 0, fuzzy: 0, ai: 0, cached: 0, manual: 0, awaiting: 0, possible: 0 };
  // FORWARD pass: each approved expense -> find Ledger match
  for(var ei=0; ei<approved.length; ei++){
    var expense = approved[ei];
    totalApproved += expense.amount;
    if(expense.subItems && expense.subItems.length > 1){
      var allMatched = true;
      var subResults = [];
      for(var si_i=0; si_i<expense.subItems.length; si_i++){
        var si = expense.subItems[si_i];
        var subExpense = { id: expense.id+':sub'+si_i, amount: si.amount, vendor: si.vendor, body: si.vendor, date: expense.date, supportingDocs: expense.supportingDocs };
        var subMatch = await matchLedgerEntryWithAI(subExpense, ledgerEntries, cache);
        subResults.push({ subItem: si, match: subMatch });
        if(!subMatch || subMatch.status === 'awaiting_payment') allMatched = false;
        if(subMatch && subMatch.stage) matcherStats[subMatch.stage] = (matcherStats[subMatch.stage]||0) + 1;
      }
      var combined = Object.assign({}, expense, { subItemResults: subResults, allSubItemsMatched: allMatched });
      if(allMatched){ paid.push(combined); totalPaid += expense.amount; }
      else { awaitingPayment.push(combined); totalAwaiting += expense.amount; }
      continue;
    }
    var result = await matchLedgerEntryWithAI(expense, ledgerEntries, cache);
    var withMatch = Object.assign({}, expense, { matchResult: result });
    if(result && result.stage) matcherStats[result.stage] = (matcherStats[result.stage]||0) + 1;
    if(!result){
      awaitingPayment.push(withMatch); totalAwaiting += expense.amount;
    } else if(result.status === 'paid'){
      paid.push(withMatch); totalPaid += expense.amount;
    } else if(result.status === 'paid_with_tolerance'){
      paidWithTolerance.push(withMatch); totalPaid += expense.amount;
    } else if(result.status === 'possible_match'){
      possibleMatch.push(withMatch);
    } else {
      awaitingPayment.push(withMatch); totalAwaiting += expense.amount;
    }
  }
  // v2.8 Module 4: cluster retirement — when any member of a re-ask cluster is
  // paid, its siblings are the same logical expense and leave awaiting-payment.
  var paidClusters = {};
  paid.concat(paidWithTolerance).forEach(function(it){ if(it.clusterId) paidClusters[it.clusterId]=true; });
  var clusterResolved = [];
  awaitingPayment = awaitingPayment.filter(function(it){
    if(it.clusterId && paidClusters[it.clusterId]){
      clusterResolved.push(it);
      totalAwaiting -= it.amount;
      return false;
    }
    return true;
  });
  // v2.6 REVERSE pass: Ledger entries with no matching approval
  // Collect every Ledger entry that was matched in the forward pass
  var matchedHashes = {};
  function _addMatched(items){
    items.forEach(function(item){
      if(item.matchResult && item.matchResult.match){
        matchedHashes[ledgerEntryHash(item.matchResult.match)] = true;
      }
      if(item.subItemResults){
        item.subItemResults.forEach(function(sr){
          if(sr.match && sr.match.match){
            matchedHashes[ledgerEntryHash(sr.match.match)] = true;
          }
        });
      }
    });
  }
  _addMatched(paid);
  _addMatched(paidWithTolerance);
  _addMatched(possibleMatch);
  // Walk Ledger OUT entries within the reverse-scan window
  var nowMs = Date.now();
  var windowStartMs = nowMs - (REVERSE_SCAN_WINDOW_DAYS * 86400000);
  var ledgerWithoutApproval = [];
  var ledgerRecurring = [];
  for(var li=0; li<ledgerEntries.length; li++){
    var le = ledgerEntries[li];
    if(le.inOut !== 'OUT') continue;
    if(le.amount < REVERSE_SCAN_MIN_AMOUNT) continue;
    if(le.date.getTime() < windowStartMs) continue;
    if(matchedHashes[ledgerEntryHash(le)]) continue;
    if(isRecurringPattern(le)){
      le.autoDebit = isAutoDebitEntry(le); // v2.8: highlight auto-debited EMIs informationally
      ledgerRecurring.push(le);
    } else {
      ledgerWithoutApproval.push(le);
    }
  }
  ledgerWithoutApproval.sort(function(a,b){
    var dDiff = b.date.getTime() - a.date.getTime();
    if(dDiff !== 0) return dDiff;
    return b.amount - a.amount;
  });
  ledgerRecurring.sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
  return {
    paid: paid,
    paidWithTolerance: paidWithTolerance,
    possibleMatch: possibleMatch,
    awaitingPayment: awaitingPayment,
    clusterResolved: clusterResolved,
    ledgerWithoutApproval: ledgerWithoutApproval,
    ledgerRecurring: ledgerRecurring,
    matcherStats: matcherStats,
    cacheSize: Object.keys(cache.matches).length,
    summary: {
      totalApproved: totalApproved,
      totalPaid: totalPaid,
      totalAwaiting: totalAwaiting,
      totalPossible: possibleMatch.reduce(function(s,e){return s+e.amount;},0),
      totalUnmatchedLedger: ledgerWithoutApproval.reduce(function(s,e){return s+e.amount;},0),
      totalRecurringLedger: ledgerRecurring.reduce(function(s,e){return s+e.amount;},0),
      countApproved: approved.length,
      countPaid: paid.length,
      countPaidTolerance: paidWithTolerance.length,
      countPossible: possibleMatch.length,
      countAwaiting: awaitingPayment.length,
      countUnmatchedLedger: ledgerWithoutApproval.length,
      countRecurringLedger: ledgerRecurring.length
    }
  };
}
// ── Outlier Detection ────────────────────────────────────────────────────────
async function buildOutliers(rec) {
  var outliers = [];
  var SEVEN_DAYS_MS = 7 * 86400000;
  var now = Date.now();
  rec.paidWithTolerance.forEach(function(e){
    var m = e.matchResult ? e.matchResult.match : null;
    if(!m) return;
    var diff = m.amount - e.amount;
    if(Math.abs(diff) >= 500){
      outliers.push({
        type: 'amount_mismatch',
        expense: e,
        ledger: m,
        approved: e.amount,
        actualPaid: m.amount,
        diff: diff,
        pctDiff: e.matchResult.pctDiff,
        likelyTDS: diff < 0 && Math.abs(diff/e.amount) < 0.05
      });
    }
  });
  rec.possibleMatch.forEach(function(e){
    if(!e.matchResult) return;
    if(e.matchResult.stage === 'ai' || e.matchResult.confidence === 'ai_medium'){
      outliers.push({
        type: 'ai_low_confidence',
        expense: e,
        ledger: e.matchResult.match,
        aiConfidence: e.matchResult.aiConfidence,
        aiReasoning: e.matchResult.aiReasoning
      });
    } else {
      outliers.push({
        type: 'possible_match',
        expense: e,
        ledger: e.matchResult.match,
        approved: e.amount,
        actualPaid: e.matchResult.match ? e.matchResult.match.amount : 0
      });
    }
  });
  rec.awaitingPayment.forEach(function(e){
    var ageMs = now - e.date.getTime();
    if(ageMs >= SEVEN_DAYS_MS){
      outliers.push({
        type: 'stale_awaiting',
        expense: e,
        ageDays: Math.floor(ageMs / 86400000)
      });
    }
  });
  rec.paid.forEach(function(e){
    if(!e.matchResult || !e.matchResult.dateDiffDays) return;
    if(e.matchResult.dateDiffDays >= 10){
      outliers.push({
        type: 'date_drift',
        expense: e,
        ledger: e.matchResult.match,
        daysAfterApproval: e.matchResult.dateDiffDays
      });
    }
  });
  outliers.forEach(function(o, i){ o.id = i + 1; });
  return outliers;
}
// ── v2.6 REWRITE: buildReconciliationSection — top-N per section + reverse-scan ──
async function buildReconciliationSection() {
  try {
    var rec = await buildReconciliation(30);
    if(rec.summary.countApproved === 0 && (!rec.ledgerWithoutApproval || rec.ledgerWithoutApproval.length === 0)) return '';
    function sortPaidByLedgerDate(arr){
      var copy = arr.slice();
      copy.sort(function(a,b){
        var aD = (a.matchResult && a.matchResult.match) ? a.matchResult.match.date.getTime() : 0;
        var bD = (b.matchResult && b.matchResult.match) ? b.matchResult.match.date.getTime() : 0;
        return bD - aD;
      });
      return copy;
    }
    function sortByApprovalDateDesc(arr){
      var copy = arr.slice();
      copy.sort(function(a,b){ return b.date.getTime() - a.date.getTime(); });
      return copy;
    }
    var lines = [''];
    lines.push('--- APPROVED EXPENSES STATUS ---');
    lines.push('');
    if(rec.paid.length > 0){
      var sortedPaid = sortPaidByLedgerDate(rec.paid);
      var shown = sortedPaid.slice(0, REPORT_TOP_N);
      lines.push('Recent paid (top ' + shown.length + ' of ' + rec.paid.length + '):');
      shown.forEach(function(e){
        var m = e.matchResult ? e.matchResult.match : null;
        var bankAC = m ? m.bankAC : '';
        var dt = m ? m.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'}) : '';
        lines.push('  ✓ ' + (e.vendor || e.body.substring(0,30)) + ' Rs.' + formatINR(e.amount) + (m ? ' → ' + dt + ', ' + bankAC : ''));
      });
      if(rec.paid.length > REPORT_TOP_N){
        lines.push('  + ' + (rec.paid.length - REPORT_TOP_N) + ' more paid items — reply MORE PAID to see all');
      }
      lines.push('');
    }
    if(rec.paidWithTolerance.length > 0){
      var sortedTol = sortPaidByLedgerDate(rec.paidWithTolerance);
      var shownTol = sortedTol.slice(0, REPORT_TOP_N);
      lines.push('Paid (within 5% tolerance) (top ' + shownTol.length + ' of ' + rec.paidWithTolerance.length + '):');
      shownTol.forEach(function(e){
        var m = e.matchResult ? e.matchResult.match : null;
        lines.push('  ~ ' + (e.vendor || e.body.substring(0,30)) + ' Rs.' + formatINR(e.amount) + ' (paid Rs.' + formatINR(m ? m.amount : 0) + ')');
      });
      if(rec.paidWithTolerance.length > REPORT_TOP_N){
        lines.push('  + ' + (rec.paidWithTolerance.length - REPORT_TOP_N) + ' more — reply MORE TOLERANCE to see all');
      }
      lines.push('');
    }
    if(rec.awaitingPayment.length > 0){
      var sortedAwait = sortByApprovalDateDesc(rec.awaitingPayment);
      var shownAwait = sortedAwait.slice(0, REPORT_TOP_N);
      lines.push('Approved but not yet paid (top ' + shownAwait.length + ' of ' + rec.awaitingPayment.length + '):');
      shownAwait.forEach(function(e){
        var d = e.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
        lines.push('  ⏳ ' + (e.vendor || e.body.substring(0,30)) + ' Rs.' + formatINR(e.amount) + ' (approved ' + d + ')');
      });
      if(rec.awaitingPayment.length > REPORT_TOP_N){
        lines.push('  + ' + (rec.awaitingPayment.length - REPORT_TOP_N) + ' more — reply MORE AWAITING to see all');
      }
      lines.push('');
    }
    if(rec.possibleMatch.length > 0){
      var sortedPoss = sortByApprovalDateDesc(rec.possibleMatch);
      var shownPoss = sortedPoss.slice(0, REPORT_TOP_N);
      lines.push('Possible mismatch — needs review (top ' + shownPoss.length + ' of ' + rec.possibleMatch.length + '):');
      shownPoss.forEach(function(e){
        var m = e.matchResult ? e.matchResult.match : null;
        lines.push('  ⚠ ' + (e.vendor || e.body.substring(0,30)) + ' approved Rs.' + formatINR(e.amount) + ' vs Ledger Rs.' + formatINR(m ? m.amount : 0));
      });
      if(rec.possibleMatch.length > REPORT_TOP_N){
        lines.push('  + ' + (rec.possibleMatch.length - REPORT_TOP_N) + ' more — reply MORE MISMATCH to see all');
      }
      lines.push('');
    }
    // v2.6 NEW: reverse-scan — Ledger entries without WhatsApp approval
    if(rec.ledgerWithoutApproval && rec.ledgerWithoutApproval.length > 0){
      var shownRev = rec.ledgerWithoutApproval.slice(0, REPORT_TOP_N);
      lines.push('⚠ Ledger payments WITHOUT approval (top ' + shownRev.length + ' of ' + rec.ledgerWithoutApproval.length + '):');
      shownRev.forEach(function(le){
        var dt = le.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
        var desc = (le.description||le.entity||'').substring(0,40);
        lines.push('  ❗ ' + desc + ' Rs.' + formatINR(le.amount) + ' (' + (le.bankAC||'-') + ', ' + dt + ')');
      });
      if(rec.ledgerWithoutApproval.length > REPORT_TOP_N){
        lines.push('  + ' + (rec.ledgerWithoutApproval.length - REPORT_TOP_N) + ' more — reply MORE UNMATCHED to see all');
      }
      lines.push('');
    }
    if(rec.ledgerRecurring && rec.ledgerRecurring.length > 0){
      lines.push('ℹ ' + rec.ledgerRecurring.length + ' recurring/auto Ledger items suppressed (EMI, salary, bank charges, etc.) — reply MORE RECURRING to see');
      lines.push('');
    }
    lines.push('Total approved: Rs.' + formatINR(rec.summary.totalApproved));
    lines.push('Total paid: Rs.' + formatINR(rec.summary.totalPaid));
    lines.push('Awaiting payment: Rs.' + formatINR(rec.summary.totalAwaiting));
    if(rec.summary.totalUnmatchedLedger > 0){
      lines.push('Ledger unmatched: Rs.' + formatINR(rec.summary.totalUnmatchedLedger));
    }
    return lines.join('\n');
  } catch(e) {
    console.error('[Reconciliation section]', e.message);
    return '';
  }
}
// ── EOD Daily Report Builder (HTML for the JPEG sent to SILENT_OBSERVER) ─────
function escapeHtml(s) {
  if(s === null || s === undefined) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function buildEODReportHTML(opts) {
  var d = opts.date;
  var audit = opts.audit || { fullyApproved:[], partialApproval:[], noApproval:[], allExpenses:[] };
  var rec = opts.rec || { paid:[], paidWithTolerance:[], possibleMatch:[], awaitingPayment:[], summary:{} };
  var outliers = opts.outliers || [];
  var isFriday = opts.isFriday || false;
  var weekStats = opts.weekStats || null;
  var todayDate = new Date(d);
  var todayStr = todayDate.toLocaleDateString('en-IN',{day:'numeric',month:'long',year:'numeric',timeZone:'Asia/Kolkata'});
  var weekday = todayDate.toLocaleDateString('en-IN',{weekday:'long',timeZone:'Asia/Kolkata'});
  var todayKey = d;
  var todaysApprovals = audit.allExpenses ? audit.allExpenses.filter(function(e){
    return e.date.toISOString().split('T')[0] === todayKey;
  }) : [];
  var todayBoth = todaysApprovals.filter(function(e){ return e.status.mm==='yes' && e.status.sm==='yes'; });
  var todayOne  = todaysApprovals.filter(function(e){ return (e.status.mm==='yes')!==(e.status.sm==='yes'); });
  var todayNone = todaysApprovals.filter(function(e){ return e.status.mm!=='yes' && e.status.sm!=='yes'; });
  var todayBothAmt = todayBoth.reduce(function(s,e){return s+e.amount;},0);
  var todayOneAmt = todayOne.reduce(function(s,e){return s+e.amount;},0);
  var todayNoneAmt = todayNone.reduce(function(s,e){return s+e.amount;},0);
  var todayTotalAmt = todayBothAmt + todayOneAmt + todayNoneAmt;
  function bar(pct){ return Math.max(0, Math.min(100, Math.round(pct))); }
  function pctOf(part, whole){ return whole > 0 ? (part/whole)*100 : 0; }
  // v2.6: also show top-N in the JPEG (mirror text format)
  var paidShown = rec.paid.slice().sort(function(a,b){
    var aD = (a.matchResult && a.matchResult.match) ? a.matchResult.match.date.getTime() : 0;
    var bD = (b.matchResult && b.matchResult.match) ? b.matchResult.match.date.getTime() : 0;
    return bD - aD;
  }).slice(0, REPORT_TOP_N);
  var paidExtra = rec.paid.length - paidShown.length;
  var paidHTML = paidShown.length ? paidShown.map(function(e){
    var m = e.matchResult ? e.matchResult.match : null;
    var subDocs = e.supportingDocs && e.supportingDocs.length ? '<div class="item-doc">📎 ' + escapeHtml(e.supportingDocs.map(function(d){return d.filename;}).join(', ')) + '</div>' : '';
    var dt = m ? m.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'}) : '';
    var stageTag = e.matchResult && e.matchResult.stage ? '<span class="stage-tag stage-'+escapeHtml(e.matchResult.stage)+'">'+escapeHtml(e.matchResult.stage)+'</span>' : '';
    return '<div class="item">'+
      '<div class="item-row"><span class="item-name">'+escapeHtml(e.vendor || (e.body||'').substring(0,40))+' '+stageTag+'</span><span class="item-amount">Rs.'+formatINR(e.amount)+'</span></div>'+
      (m ? '<div class="item-meta">Ledger '+escapeHtml(dt)+' · '+escapeHtml(m.bankAC||'-')+'</div>' : '')+
      subDocs+'</div>';
  }).join('') : '<div class="empty">None today</div>';
  if(paidExtra > 0) paidHTML += '<div class="empty">+ ' + paidExtra + ' more — reply MORE PAID</div>';
  var awaitShown = rec.awaitingPayment.slice().sort(function(a,b){ return b.date.getTime() - a.date.getTime(); }).slice(0, REPORT_TOP_N);
  var awaitExtra = rec.awaitingPayment.length - awaitShown.length;
  var awaitingHTML = awaitShown.length ? awaitShown.map(function(e){
    var d2 = e.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
    var ageDays = Math.floor((Date.now() - e.date.getTime()) / 86400000);
    return '<div class="item amber">'+
      '<div class="item-row"><span class="item-name">'+escapeHtml(e.vendor || (e.body||'').substring(0,40))+'</span><span class="item-amount">Rs.'+formatINR(e.amount)+'</span></div>'+
      '<div class="item-meta">approved '+escapeHtml(d2)+' · '+ageDays+'d ago, no Ledger entry yet</div></div>';
  }).join('') : '<div class="empty">None — all approved expenses paid</div>';
  if(awaitExtra > 0) awaitingHTML += '<div class="empty">+ ' + awaitExtra + ' more — reply MORE AWAITING</div>';
  // v2.6: reverse-scan section in JPEG
  var revShown = (rec.ledgerWithoutApproval || []).slice(0, REPORT_TOP_N);
  var revExtra = (rec.ledgerWithoutApproval || []).length - revShown.length;
  var revHTML = revShown.length ? revShown.map(function(le){
    var dt = le.date.toLocaleDateString('en-IN',{day:'numeric',month:'short',timeZone:'Asia/Kolkata'});
    return '<div class="item red">'+
      '<div class="item-row"><span class="item-name">❗ '+escapeHtml((le.description||le.entity||'').substring(0,40))+'</span><span class="item-amount">Rs.'+formatINR(le.amount)+'</span></div>'+
      '<div class="item-meta">'+escapeHtml(le.bankAC||'-')+' · '+escapeHtml(dt)+' · no approval found</div></div>';
  }).join('') : '';
  if(revExtra > 0) revHTML += '<div class="empty">+ ' + revExtra + ' more — reply MORE UNMATCHED</div>';
  var partialFresh = (audit.partialApproval || []).filter(function(e){
    return e.date.getTime() >= REPORT_START_MS && (Date.now()-e.date.getTime()) <= REMINDER_MAX_AGE_DAYS*86400000;
  });
  var partialHTML = partialFresh.length ? partialFresh.slice(0, REPORT_TOP_N).map(function(e){
    var who = e.status.mm==='yes' ? 'M ✓ · S pending' : (e.status.sm==='yes' ? 'S ✓ · M pending' : 'Both pending');
    var hours = Math.floor((Date.now() - e.date.getTime()) / (60*60*1000));
    var ageStr = hours >= 24 ? Math.floor(hours/24)+'d' : hours+'h';
    var subTag = e.subItems && e.subItems.length>1 ? ' · '+e.subItems.length+' sub-items' : '';
    return '<div class="item gray">'+
      '<div class="item-row"><span class="item-name">'+escapeHtml(e.vendor || (e.body||'').substring(0,40))+'</span><span class="item-amount">Rs.'+formatINR(e.amount)+'</span></div>'+
      '<div class="item-meta">'+escapeHtml(who)+' · '+ageStr+escapeHtml(subTag)+'</div></div>';
  }).join('') : '';
  var partialExtra = (audit.partialApproval || []).length - REPORT_TOP_N;
  if(partialExtra > 0) partialHTML += '<div class="empty">+ ' + partialExtra + ' more — reply MORE STALE</div>';
  var queryHTML = (audit.noApproval || []).filter(function(e){return e.queryAnswer;}).slice(0, REPORT_TOP_N).map(function(e){
    return '<div class="item gray">'+
      '<div class="item-row"><span class="item-name">'+escapeHtml(e.vendor || (e.body||'').substring(0,40))+'</span><span class="item-amount">Rs.'+formatINR(e.amount)+'</span></div>'+
      '<div class="item-meta">Query answered · awaiting M + S</div></div>';
  }).join('');
  var stuckOnMM = (audit.partialApproval || []).filter(function(e){ return e.status.mm==='pending' && e.status.sm==='yes'; }).reduce(function(s,e){return s+e.amount;},0);
  var outliersHTML = outliers.length ? outliers.map(function(o){
    var n = o.id;
    if(o.type === 'amount_mismatch'){
      var sign = o.diff < 0 ? '−' : '+';
      var pct = (Math.abs(o.diff)/o.approved*100).toFixed(1);
      return '<div class="item red">'+
        '<div class="item-row"><span class="item-name"><b>'+n+'.</b> Amount mismatch — '+escapeHtml(o.expense.vendor||'')+'</span></div>'+
        '<div class="item-meta">Approved Rs.'+formatINR(o.approved)+' · Ledger Rs.'+formatINR(o.actualPaid)+'<br>Diff '+sign+'Rs.'+formatINR(Math.abs(o.diff))+' ('+sign+pct+'%)'+(o.likelyTDS?' — likely TDS':'')+'</div>'+
        '<div class="action-box"><span class="cmd">'+n+' ok</span>confirm <span class="cmd">'+n+' flag</span>investigate <span class="cmd">'+n+' ignore</span>skip</div></div>';
    }
    if(o.type === 'stale_awaiting'){
      return '<div class="item red">'+
        '<div class="item-row"><span class="item-name"><b>'+n+'.</b> Stale — '+escapeHtml(o.expense.vendor||'')+'</span></div>'+
        '<div class="item-meta">Rs.'+formatINR(o.expense.amount)+' · approved '+o.ageDays+'d ago, no Ledger entry yet</div>'+
        '<div class="action-box"><span class="cmd">'+n+' chase</span>nudge accountant <span class="cmd">'+n+' paid &lt;date&gt;</span>mark paid</div></div>';
    }
    if(o.type === 'ai_low_confidence'){
      return '<div class="item red">'+
        '<div class="item-row"><span class="item-name"><b>'+n+'.</b> AI match (low conf '+(o.aiConfidence||0).toFixed(2)+') — '+escapeHtml(o.expense.vendor||'')+'</span></div>'+
        '<div class="item-meta">Possible: '+escapeHtml(o.ledger ? o.ledger.description+' Rs.'+formatINR(o.ledger.amount) : '')+'<br><i>'+escapeHtml(o.aiReasoning||'')+'</i></div>'+
        '<div class="action-box"><span class="cmd">'+n+' confirm</span>accept match <span class="cmd">'+n+' reject</span>reject match</div></div>';
    }
    if(o.type === 'possible_match'){
      return '<div class="item red">'+
        '<div class="item-row"><span class="item-name"><b>'+n+'.</b> Possible match — '+escapeHtml(o.expense.vendor||'')+'</span></div>'+
        '<div class="item-meta">Approved Rs.'+formatINR(o.approved)+' · Candidate Rs.'+formatINR(o.actualPaid)+'</div>'+
        '<div class="action-box"><span class="cmd">'+n+' confirm</span>accept <span class="cmd">'+n+' reject</span>reject</div></div>';
    }
    if(o.type === 'date_drift'){
      return '<div class="item red">'+
        '<div class="item-row"><span class="item-name"><b>'+n+'.</b> Date drift — '+escapeHtml(o.expense.vendor||'')+'</span></div>'+
        '<div class="item-meta">Paid '+o.daysAfterApproval+'d after approval — review</div>'+
        '<div class="action-box"><span class="cmd">'+n+' ok</span>confirm <span class="cmd">'+n+' flag</span>investigate</div></div>';
    }
    return '';
  }).join('') : '<div class="empty">No outliers — everything matches</div>';
  var weeklyHTML = '';
  if(isFriday && weekStats){
    var total = weekStats.totalMatches || 1;
    var pctExact = pctOf(weekStats.exact||0, total);
    var pctFuzzy = pctOf(weekStats.fuzzy||0, total);
    var pctAI    = pctOf(weekStats.ai||0, total);
    var pctCached= pctOf(weekStats.cached||0, total);
    weeklyHTML =
      '<div class="section weekly">'+
      '<div class="section-header">📈 MATCHER LEARNING (this week)</div>'+
      '<div class="row"><span class="row-label">Total matches</span><span class="row-value">'+total+'</span></div>'+
      '<div style="margin-top:10px">'+
        '<div class="bar-row"><span class="bar-icon">🎯</span><span class="bar-label">Exact</span><span class="bar-track"><span class="bar-fill green" style="width:'+bar(pctExact)+'%"></span></span><span class="bar-num">'+(weekStats.exact||0)+' · '+pctExact.toFixed(0)+'%</span></div>'+
        '<div class="bar-row"><span class="bar-icon">🔍</span><span class="bar-label">Fuzzy</span><span class="bar-track"><span class="bar-fill amber" style="width:'+bar(pctFuzzy)+'%"></span></span><span class="bar-num">'+(weekStats.fuzzy||0)+' · '+pctFuzzy.toFixed(0)+'%</span></div>'+
        '<div class="bar-row"><span class="bar-icon">🤖</span><span class="bar-label">Haiku AI</span><span class="bar-track"><span class="bar-fill ai" style="width:'+bar(pctAI)+'%"></span></span><span class="bar-num">'+(weekStats.ai||0)+' · '+pctAI.toFixed(0)+'%</span></div>'+
        '<div class="bar-row"><span class="bar-icon">💾</span><span class="bar-label">Cached</span><span class="bar-track"><span class="bar-fill cyan" style="width:'+bar(pctCached)+'%"></span></span><span class="bar-num">'+(weekStats.cached||0)+' · '+pctCached.toFixed(0)+'%</span></div>'+
      '</div>'+
      '<div class="total-row"><span class="label">AI cost this week</span><span class="val">~Rs.'+(weekStats.aiCost||'0').toString()+'</span></div>'+
      '<div class="total-row"><span class="label">Manual confirmations</span><span class="val green">'+(weekStats.manualConfirm||0)+'</span></div>'+
      '<div class="total-row"><span class="label">Manual rejections</span><span class="val amber">'+(weekStats.manualReject||0)+'</span></div>'+
      '</div>';
  }
  var html = '<!DOCTYPE html><html><head><meta charset="UTF-8"><style>'+
    '*{box-sizing:border-box;margin:0;padding:0}'+
    'body{background:#ffffff;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;padding:30px 20px;color:#000000}'+
    '.phone-frame{max-width:420px;margin:0 auto;background:#ffffff;border-radius:24px;overflow:hidden;border:1px solid #000}'+
    '.header{background:#ffffff;padding:14px 16px;display:flex;align-items:center;gap:12px;border-bottom:1px solid #cccccc}'+
    '.avatar{width:40px;height:40px;border-radius:50%;background:linear-gradient(135deg,#000000,#333333);display:flex;align-items:center;justify-content:center;color:white;font-weight:600;font-size:16px}'+
    '.header-name{color:#000000;font-size:16px;font-weight:500}'+
    '.header-status{color:#555555;font-size:12px;margin-top:2px}'+
    '.chat-area{background:#ffffff;padding:16px 12px;}'+
    '.timestamp{text-align:center;color:#555555;font-size:12px;margin:8px 0 16px}'+
    '.message{background:#ffffff;border-radius:8px;padding:14px 16px;margin-bottom:10px;max-width:95%}'+
    '.report-title{font-size:14px;font-weight:600;color:#000000;margin-bottom:4px;letter-spacing:0.3px}'+
    '.report-subtitle{font-size:11px;color:#555555;margin-bottom:12px}'+
    '.section{margin-top:14px;padding-top:12px;border-top:1px solid #cccccc}'+
    '.section:first-of-type{border-top:none;margin-top:8px;padding-top:0}'+
    '.section-header{font-size:11px;font-weight:700;color:#000000;text-transform:uppercase;letter-spacing:0.6px;margin-bottom:8px}'+
    '.row{display:flex;justify-content:space-between;align-items:center;font-size:13px;color:#000000;margin-bottom:5px;line-height:1.4}'+
    '.row-label{color:#555555;font-size:12px}.row-value{font-weight:500}'+
    '.bar-row{display:flex;align-items:center;gap:8px;margin-bottom:6px;font-size:12px}'+
    '.bar-icon{width:18px;text-align:center;font-size:13px}.bar-label{width:72px;color:#222222;font-size:11px}'+
    '.bar-track{flex:1;height:8px;background:#cccccc;border-radius:4px;overflow:hidden}'+
    '.bar-fill{height:100%;border-radius:4px}'+
    '.bar-fill.green{background:linear-gradient(90deg,#000000,#000000)}'+
    '.bar-fill.amber{background:linear-gradient(90deg,#888888,#aaaaaa)}'+
    '.bar-fill.red{background:linear-gradient(90deg,#000000,#444444)}'+
    '.bar-fill.ai{background:linear-gradient(90deg,#777777,#999999)}'+
    '.bar-fill.cyan{background:linear-gradient(90deg,#4abdc4,#7adde2)}'+
    '.bar-num{color:#222222;font-size:11px;min-width:75px;text-align:right}'+
    '.item{background:#111b21;border-left:3px solid #000000;padding:8px 10px;margin-bottom:6px;border-radius:4px}'+
    '.item.amber{border-left-color:#888888}.item.red{border-left-color:#000000}.item.gray{border-left-color:#54656f}'+
    '.item-row{display:flex;justify-content:space-between;font-size:12px}'+
    '.item-name{color:#000000;flex:1}.item-amount{color:#000000;font-weight:600;margin-left:8px;white-space:nowrap}'+
    '.item-meta{color:#555555;font-size:11px;margin-top:3px}'+
    '.item-doc{color:#6db4ff;font-size:11px;margin-top:3px;font-style:italic}'+
    '.total-row{display:flex;justify-content:space-between;margin-top:8px;padding-top:8px;border-top:1px dashed #cccccc;font-size:12px}'+
    '.total-row .label{color:#555555}.total-row .val{color:#000000;font-weight:600}'+
    '.total-row .val.green{color:#000000}.total-row .val.amber{color:#aaaaaa}'+
    '.action-box{background:#1f2c33;border:1px solid #cccccc;border-radius:6px;padding:8px 10px;margin-top:6px;font-size:11px;color:#555555;line-height:1.5}'+
    '.action-box .cmd{color:#000000;font-family:monospace;background:#ffffff;padding:1px 5px;border-radius:3px;margin-right:4px}'+
    '.empty{color:#555555;font-size:12px;font-style:italic;padding:6px 0}'+
    '.footer-note{margin-top:12px;padding-top:10px;border-top:1px solid #cccccc;color:#555555;font-size:11px;text-align:center;font-style:italic}'+
    '.stage-tag{display:inline-block;background:#cccccc;color:#555555;font-size:9px;padding:1px 5px;border-radius:3px;margin-left:4px;font-weight:400;text-transform:uppercase;letter-spacing:0.3px}'+
    '.stage-tag.stage-ai{background:#3a2a55;color:#999999}'+
    '.stage-tag.stage-cached{background:#2a3a4a;color:#7adde2}'+
    '.stage-tag.stage-fuzzy{background:#3a3525;color:#aaaaaa}'+
    '.delivered{color:#53bdeb;font-size:11px;margin-left:4px}'+
  '</style></head><body>'+
    '<div class="phone-frame">'+
      '<div class="header"><div class="avatar">F</div><div class="header-info"><div class="header-name">Fidato MIS Bot</div><div class="header-status">+91 98701 11582 · online</div></div></div>'+
      '<div class="chat-area">'+
        '<div class="timestamp">Today, 7:00 PM</div>'+
        '<div class="message">'+
          '<div class="report-title">FIDATO MIS — DAILY REPORT'+(opts.part===2?' · Part 2':(opts.part===1?' · Part 1':''))+'</div>'+
          '<div class="report-subtitle">'+escapeHtml(todayStr)+' · '+escapeHtml(weekday)+'</div>'+
          (opts.part===2 ? '' :
          '<div class="section">'+
            '<div class="section-header">Today\'s Activity</div>'+
            '<div class="row"><span class="row-label">Requests posted</span><span class="row-value">'+todaysApprovals.length+'</span></div>'+
            '<div class="row"><span class="row-label">Total requested</span><span class="row-value">Rs.'+formatINR(todayTotalAmt)+'</span></div>'+
            '<div style="margin-top:10px">'+
              '<div class="bar-row"><span class="bar-icon">✓</span><span class="bar-label">Both ✓</span><span class="bar-track"><span class="bar-fill green" style="width:'+bar(pctOf(todayBoth.length,todaysApprovals.length||1))+'%"></span></span><span class="bar-num">'+todayBoth.length+' · Rs.'+formatINR(todayBothAmt)+'</span></div>'+
              '<div class="bar-row"><span class="bar-icon">◐</span><span class="bar-label">One only</span><span class="bar-track"><span class="bar-fill amber" style="width:'+bar(pctOf(todayOne.length,todaysApprovals.length||1))+'%"></span></span><span class="bar-num">'+todayOne.length+' · Rs.'+formatINR(todayOneAmt)+'</span></div>'+
              '<div class="bar-row"><span class="bar-icon">○</span><span class="bar-label">Neither</span><span class="bar-track"><span class="bar-fill red" style="width:'+bar(pctOf(todayNone.length,todaysApprovals.length||1))+'%"></span></span><span class="bar-num">'+todayNone.length+' · Rs.'+formatINR(todayNoneAmt)+'</span></div>'+
            '</div>'+
          '</div>'+
          '<div class="section">'+
            '<div class="section-header">✓ Recent Paid (top '+REPORT_TOP_N+')</div>'+
            paidHTML+
          '</div>'+
          '<div class="section">'+
            '<div class="section-header">⏳ Approved, Awaiting Payment</div>'+
            awaitingHTML+
            '<div class="total-row"><span class="label">Total paid</span><span class="val green">Rs.'+formatINR(rec.summary.totalPaid||0)+'</span></div>'+
            '<div class="total-row"><span class="label">Awaiting payment</span><span class="val amber">Rs.'+formatINR(rec.summary.totalAwaiting||0)+'</span></div>'+
          '</div>')+
          (opts.part===1 ? '' :
          (revHTML ?
            '<div class="section">'+
              '<div class="section-header">⚠ Ledger Payments WITHOUT Approval</div>'+
              revHTML+
              (rec.summary.totalUnmatchedLedger ? '<div class="total-row"><span class="label">Total unmatched</span><span class="val amber">Rs.'+formatINR(rec.summary.totalUnmatchedLedger)+'</span></div>' : '')+
            '</div>' : ''
          )+
          (partialHTML || queryHTML ?
            '<div class="section">'+
              '<div class="section-header">◐ Partial — Needs M/S (top '+REPORT_TOP_N+')</div>'+
              partialHTML + queryHTML +
              (stuckOnMM > 0 ? '<div class="total-row"><span class="label">Stuck on M</span><span class="val amber">Rs.'+formatINR(stuckOnMM)+'</span></div>' : '')+
            '</div>' : ''
          )+
          '<div class="section">'+
            '<div class="section-header">⚠ Manual Intervention</div>'+
            outliersHTML+
          '</div>'+
          weeklyHTML+
          '<div class="footer-note">'+
            'Reply: MORE STALE | PAID | AWAITING | MISMATCH | UNMATCHED · ' +
            (isFriday ? 'Weekly model report included above.' : 'Next weekly — Friday 7 PM') +
          '</div>')+
        '</div>'+
      '</div>'+
    '</div>'+
  '</body></html>';
  return html;
}
function computeWeeklyMatcherStats(cache) {
  if(!cache) cache = loadMatchCache();
  var oneWeekAgo = Date.now() - 7*86400000;
  var stats = { exact:0, exact_tolerance:0, fuzzy:0, ai:0, cached:0, manual:0, totalMatches:0, manualConfirm:0, manualReject:0, aiCost:0 };
  Object.keys(cache.matches).forEach(function(k){
    var m = cache.matches[k];
    var ts = m.ts ? new Date(m.ts).getTime() : 0;
    if(ts < oneWeekAgo) return;
    stats.totalMatches++;
    if(m.stage === 'exact') stats.exact++;
    else if(m.stage === 'exact_tolerance') stats.exact_tolerance++;
    else if(m.stage === 'fuzzy') stats.fuzzy++;
    else if(m.stage === 'ai') stats.ai++;
    if(m.manuallyConfirmed) stats.manualConfirm++;
    if(m.manuallyRejected) stats.manualReject++;
  });
  stats.cached = Object.keys(cache.matches).length;
  stats.aiCost = (stats.ai * 0.07).toFixed(2);
  return stats;
}
async function sendEODReport(dateStr) {
  if(!waReady){ console.log('[EOD] WA not ready'); return; }
  try {
    var d = dateStr || new Date().toISOString().split('T')[0];
    var audit = await buildApprovalAudit(30);
    var rec = await buildReconciliation(30);
    var outliers = await buildOutliers(rec);
    var dayOfWeek = new Date(d).getDay();
    var isFriday = dayOfWeek === 5;
    var weekStats = isFriday ? computeWeeklyMatcherStats() : null;
    // v2.8 Module 8: black-on-white, posted in two parts.
    var html1 = buildEODReportHTML({ date: d, audit: audit, rec: rec, outliers: outliers, isFriday: isFriday, weekStats: weekStats, part: 1 });
    var html2 = buildEODReportHTML({ date: d, audit: audit, rec: rec, outliers: outliers, isFriday: isFriday, weekStats: weekStats, part: 2 });
    var img1 = await htmlToImage(html1, 460, 1200);
    var img2 = await htmlToImage(html2, 460, 1400);
    var buf = Buffer.isBuffer(img1) ? img1 : Buffer.from(img1);
    var buf2 = Buffer.isBuffer(img2) ? img2 : Buffer.from(img2);
    var captionLines = ['📊 Daily Report — '+ new Date(d).toLocaleDateString('en-IN',{day:'numeric',month:'short',year:'numeric',timeZone:'Asia/Kolkata'})];
    var todaysCount = (audit.allExpenses || []).filter(function(e){ return e.date.toISOString().split('T')[0] === d; }).length;
    captionLines.push(todaysCount+' requests today · '+(rec.paid.length)+' paid · Rs.'+formatINR(rec.summary.totalAwaiting||0)+' awaiting');
    if(rec.ledgerWithoutApproval && rec.ledgerWithoutApproval.length > 0){
      captionLines.push('⚠ '+rec.ledgerWithoutApproval.length+' Ledger payment(s) without approval — reply MORE UNMATCHED');
    }
    if(outliers.length > 0) captionLines.push(outliers.length+' outlier(s) need your input — reply with command shown in image');
    if(isFriday) captionLines.push('📈 Weekly matcher learning included.');
    var jid = getSilentObserverJid();
    await waClient.sendMessage(jid, new MessageMedia('image/png', buf.toString('base64'), 'EOD_'+d+'_part1.png'), { caption: captionLines.join('\n') });
    await waClient.sendMessage(jid, new MessageMedia('image/png', buf2.toString('base64'), 'EOD_'+d+'_part2.png'), { caption: 'Part 2 — pending, held and manual intervention' });
    console.log('[EOD] sent 2 parts to', jid);
    saveDMState(Object.assign(loadDMState(), { lastOutliers: { date: d, items: outliers.map(function(o){
      return { id: o.id, type: o.type, expenseId: o.expense.id, ledgerHash: o.ledger ? ledgerEntryHash(o.ledger) : null };
    })}}));
    return { success: true, outlierCount: outliers.length, unmatchedLedgerCount: (rec.ledgerWithoutApproval||[]).length, rec: rec };
  } catch(e) {
    console.error('[EOD] failed:', e.message);
    return { error: e.message };
  }
}
// ── Report HTML (legacy daily MIS report) ─────────────────────────────────────
async function generateDailyReport(dateStr){
  var entries=await getLedgerData(dateStr);var fp=await getFundPosition();
  var tIn=0,tOut=0,inflows=[],outflows=[];
  entries.forEach(function(e){if(e.inOut==='IN'){tIn+=e.amount;inflows.push(e);}if(e.inOut==='OUT'){tOut+=e.amount;outflows.push(e);}});
  var byTag={};outflows.forEach(function(e){var t=e.tag||'Other';if(!byTag[t])byTag[t]={total:0,items:[]};byTag[t].total+=e.amount;byTag[t].items.push(e);});
  return{date:dateStr,totalIn:tIn,totalOut:tOut,net:tIn-tOut,inflows:inflows,outflows:outflows,byTag:byTag,fundPosition:fp,entryCount:entries.length};
}
function buildReportHTML(data){
  var h='<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{font-family:Arial,sans-serif;background:#fff;padding:20px;max-width:800px;margin:0 auto;color:#222}.hdr{text-align:center;border-bottom:2px solid #333;padding-bottom:10px;margin-bottom:15px}.hdr h1{font-size:22px;margin:0}.hdr p{color:#666;margin:4px 0 0}.metrics{display:flex;gap:10px;margin:15px 0}.mc{flex:1;background:#f5f5f5;border-radius:8px;padding:12px;text-align:center}.mc .lbl{font-size:11px;color:#888}.mc .val{font-size:20px;font-weight:bold;margin:4px 0 0}.gn{color:#0a7}.rd{color:#c33}.bl{color:#36a}.sec{font-size:14px;font-weight:bold;color:#555;border-bottom:1px solid #ddd;padding:8px 0 4px;margin:15px 0 8px}table{width:100%;border-collapse:collapse;font-size:12px}th{text-align:left;padding:5px;background:#f0f0f0;font-size:11px;color:#666}td{padding:5px;border-top:1px solid #eee}.amt{text-align:right;font-family:monospace}</style></head><body>';
  h+='<div class="hdr"><h1>Fidato Group - Daily MIS Report</h1><p>'+data.date+' | '+data.entryCount+' transactions</p></div>';
  h+='<div class="metrics"><div class="mc"><div class="lbl">Total Inflows</div><div class="val gn">'+formatINR(data.totalIn)+'</div></div><div class="mc"><div class="lbl">Total Outflows</div><div class="val rd">'+formatINR(data.totalOut)+'</div></div><div class="mc"><div class="lbl">Net</div><div class="val '+(data.net>=0?'bl':'rd')+'">'+formatINR(data.net)+'</div></div></div>';
  if(data.inflows.length>0){h+='<div class="sec">INFLOWS</div><table><tr><th>Description</th><th>Entity</th><th>Tag</th><th>Bank A/C</th><th style="text-align:right">Amount</th></tr>';data.inflows.forEach(function(e){h+='<tr><td>'+e.description+'</td><td>'+e.entity+'</td><td>'+e.tag+'</td><td>'+e.bankAC+'</td><td class="amt gn">'+formatINR(e.amount)+'</td></tr>';});h+='</table>';}
  h+='<div class="sec">OUTFLOWS BY CATEGORY</div><table><tr><th>Category</th><th>Items</th><th style="text-align:right">Amount</th></tr>';
  Object.keys(data.byTag).sort(function(a,b){return data.byTag[b].total-data.byTag[a].total;}).forEach(function(t){h+='<tr><td>'+t+'</td><td>'+data.byTag[t].items.length+'</td><td class="amt rd">'+formatINR(data.byTag[t].total)+'</td></tr>';});h+='</table>';
  h+='<div class="sec">FUND POSITION</div><table><tr><th>Account</th><th style="text-align:right">Opening</th><th style="text-align:right">IN</th><th style="text-align:right">OUT</th><th style="text-align:right">Closing</th><th style="text-align:right">Cheques</th><th style="text-align:right">Net</th></tr>';
  data.fundPosition.forEach(function(a){h+='<tr><td>'+a.bankAC+'</td><td class="amt">'+formatINR(a.opening)+'</td><td class="amt gn">'+formatINR(a.todayIn)+'</td><td class="amt rd">'+formatINR(a.todayOut)+'</td><td class="amt">'+formatINR(a.closing)+'</td><td class="amt rd">'+formatINR(a.cheques)+'</td><td class="amt '+(a.netBal<0?'rd':'')+'">'+formatINR(a.netBal)+'</td></tr>';});h+='</table></body></html>';
  return h;
}
// ── Endpoints ─────────────────────────────────────────────────────────────────
app.get('/health',function(req,res){res.json({status:'ok',version:'2.8.19',whatsapp:waReady?'connected':'disconnected',sheets:sheetsApi?'initialized':'not configured',botEnabled:CONFIG.BOT_ENABLED,visionEnabled:CONFIG.CLAUDE_API_KEY?true:false,visionCacheSize:visionCache.size,reverseScanWindowDays:REVERSE_SCAN_WINDOW_DAYS,reverseScanMinAmount:REVERSE_SCAN_MIN_AMOUNT,panelUserSet:!!(process.env.PANEL_USER&&process.env.PANEL_USER.length>0),panelUserLen:(process.env.PANEL_USER||'').length,panelPasswordSet:!!(process.env.PANEL_PASSWORD&&process.env.PANEL_PASSWORD.length>0),panelPasswordLen:(process.env.PANEL_PASSWORD||'').length,lockArmed:!!((process.env.PANEL_USER||'').length>0&&(process.env.PANEL_PASSWORD||'').length>0)});});
// ── v2.8.18 endpoint lock: Basic Auth on all /api/* (/health stays open) ──────
var _crypto = require('crypto');
var PANEL_USER = process.env.PANEL_USER || '';
var PANEL_PASSWORD = process.env.PANEL_PASSWORD || '';
if (!PANEL_USER || !PANEL_PASSWORD) {
  console.warn('[AUTH] WARNING: PANEL_USER/PANEL_PASSWORD not set — all /api/* endpoints are LOCKED (fail-closed). Set them in Railway env vars.');
}
// per-IP failed-attempt tracker (in-memory; resets on restart)
var _authFails = {};
var AUTH_MAX_FAILS = 5;
var AUTH_LOCK_MS = 5 * 60 * 1000; // 5 min lockout after MAX_FAILS
function _safeEq(a, b) {
  var ba = Buffer.from(String(a)); var bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  try { return _crypto.timingSafeEqual(ba, bb); } catch (e) { return false; }
}
function authGate(req, res, next) {
  var ip = (req.headers['x-forwarded-for'] || req.ip || req.connection.remoteAddress || 'unknown').split(',')[0].trim();
  var now = Date.now();
  var rec = _authFails[ip];
  if (rec && rec.lockUntil && now < rec.lockUntil) {
    var secs = Math.ceil((rec.lockUntil - now) / 1000);
    res.set('Retry-After', String(secs));
    return res.status(429).json({ error: 'Too many failed attempts. Locked for ' + secs + 's.' });
  }
  // read live from process.env at request time (matches /health which works)
  var EU = process.env.PANEL_USER || '';
  var EP = process.env.PANEL_PASSWORD || '';
  // fail closed if not configured
  if (!EU || !EP) {
    res.set('WWW-Authenticate', 'Basic realm="Fidato MIS", charset="UTF-8"');
    return res.status(503).json({ error: 'Auth not configured on server.', _seen: { userLen: EU.length, passLen: EP.length } });
  }
  var hdr = req.headers.authorization || '';
  var m = /^Basic\s+(.+)$/i.exec(hdr);
  if (m) {
    var decoded = '';
    try { decoded = Buffer.from(m[1], 'base64').toString('utf8'); } catch (e) { decoded = ''; }
    var idx = decoded.indexOf(':');
    var u = idx >= 0 ? decoded.slice(0, idx) : '';
    var p = idx >= 0 ? decoded.slice(idx + 1) : '';
    var okU = _safeEq(u, EU);
    var okP = _safeEq(p, EP);
    if (okU && okP) {
      if (_authFails[ip]) delete _authFails[ip]; // clear on success
      return next();
    }
    // failed credentials
    rec = _authFails[ip] || { count: 0, lockUntil: 0 };
    rec.count += 1;
    if (rec.count >= AUTH_MAX_FAILS) { rec.lockUntil = now + AUTH_LOCK_MS; rec.count = 0; }
    _authFails[ip] = rec;
  }
  res.set('WWW-Authenticate', 'Basic realm="Fidato MIS", charset="UTF-8"');
  return res.status(401).json({ error: 'Authentication required.' });
}
// gate everything below this line (all /api/*). /health is registered ABOVE and stays open.
app.use('/api', authGate);

// ── v2.8.19 master control panel (served at /api/panel, behind the lock) ──────
var PANEL_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Fidato MIS — Control Panel</title>
<style>
  :root{
    --navy:#191C3C; --terra:#9F5355; --bg:#f4f5f7; --card:#ffffff;
    --line:#e3e5ea; --ink:#1d2030; --muted:#6b7080; --ok:#1f9d55; --okbg:#e8f6ee;
    --warn:#9a6b00; --warnbg:#fbf3e0; --danger:#b3261e; --dangerbg:#fbeae9; --info:#2456b3; --infobg:#e9f0fb;
  }
  *{box-sizing:border-box}
  body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;background:var(--bg);color:var(--ink);}
  header{background:var(--navy);color:#fff;padding:14px 20px;display:flex;align-items:center;justify-content:space-between;}
  header .l{display:flex;align-items:center;gap:10px;}
  header .dot{width:9px;height:9px;border-radius:50%;background:#7fdca4;display:inline-block;}
  header h1{font-size:15px;font-weight:600;margin:0;}
  header .sub{font-size:12px;opacity:.7;margin:0;}
  .wrap{max-width:880px;margin:0 auto;padding:18px;}
  .tabs{display:flex;gap:6px;flex-wrap:wrap;border-bottom:1px solid var(--line);padding-bottom:12px;margin-bottom:16px;}
  .tab{font-size:13px;padding:7px 13px;border-radius:8px;cursor:pointer;color:var(--muted);border:1px solid transparent;user-select:none;background:none;}
  .tab:hover{background:#eceef2;}
  .tab.active{background:var(--infobg);color:var(--info);font-weight:600;}
  .tab.dz.active{background:var(--dangerbg);color:var(--danger);}
  .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px;}
  .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px;}
  .card .k{font-size:12px;color:var(--muted);margin:0 0 5px;}
  .card .v{font-size:20px;font-weight:600;margin:0;}
  .btn{display:flex;align-items:center;gap:10px;width:100%;text-align:left;padding:12px 14px;font-size:14px;background:var(--card);border:1px solid var(--line);border-radius:9px;cursor:pointer;color:var(--ink);margin-bottom:9px;transition:background .12s;}
  .btn:hover{background:#f0f2f6;}
  .btn .ico{font-size:16px;width:20px;text-align:center;}
  .btn.dz{color:var(--danger);border-color:#ecc7c5;}
  .btn.act{color:var(--info);}
  .note{font-size:12px;color:var(--muted);margin:8px 2px 18px;line-height:1.6;}
  .out{background:#0f1117;color:#d7dbe6;border-radius:9px;padding:14px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;white-space:pre-wrap;word-break:break-word;max-height:340px;overflow:auto;margin-top:6px;display:none;}
  .out.show{display:block;}
  .out .lbl{color:#7fa7ff;display:block;margin-bottom:6px;}
  .modal{position:fixed;inset:0;background:rgba(10,12,20,.45);display:none;align-items:center;justify-content:center;padding:20px;z-index:50;}
  .modal.show{display:flex;}
  .modal .box{background:#fff;border-radius:12px;max-width:380px;width:100%;padding:20px;}
  .modal h3{margin:0 0 8px;font-size:16px;}
  .modal p{margin:0 0 16px;font-size:13px;color:var(--muted);line-height:1.6;}
  .modal .row{display:flex;gap:8px;justify-content:flex-end;}
  .modal button{font-size:13px;padding:8px 16px;border-radius:8px;border:1px solid var(--line);background:#fff;cursor:pointer;}
  .modal button.go{background:var(--danger);color:#fff;border-color:var(--danger);}
  .modal button.goact{background:var(--info);color:#fff;border-color:var(--info);}
  .spin{display:inline-block;width:13px;height:13px;border:2px solid #ccd;border-top-color:var(--info);border-radius:50%;animation:sp .7s linear infinite;vertical-align:middle;}
  @keyframes sp{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<header>
  <div class="l">
    <span class="dot" id="hdot"></span>
    <div><h1>Fidato MIS — Control Panel</h1><p class="sub" id="hsub">loading…</p></div>
  </div>
  <div style="font-size:12px;opacity:.8;">v<span id="hver">—</span></div>
</header>
<div class="wrap">
  <div class="tabs" id="tabs"></div>
  <div class="cards" id="cards"></div>
  <div id="buttons"></div>
  <div class="out" id="out"></div>
</div>

<div class="modal" id="modal">
  <div class="box">
    <h3 id="mTitle">Are you sure?</h3>
    <p id="mBody"></p>
    <div class="row">
      <button onclick="closeModal()">Cancel</button>
      <button id="mGo" class="goact" onclick="confirmModal()">Confirm</button>
    </div>
  </div>
</div>

<script>
// ---- endpoint catalogue ----
var TABS = [
  {id:'approvals', label:'Approvals', items:[
    {ep:'/api/approval-audit?days=3', ico:'\\uD83D\\uDC41', label:'View pending & recent approvals'},
    {ep:'/api/reminder-digest-preview', ico:'\\uD83D\\uDD14', label:'Preview the digest (no send)'},
    {ep:'/api/reminder-digest-send', ico:'\\uD83D\\uDCE4', label:'Send digest to approval group', act:true, confirm:'This posts the approval digest to your WhatsApp approval group.'},
    {ep:'/api/send-reminders', ico:'\\u23F0', label:'Send reminders now', act:true, confirm:'This sends reminder messages on WhatsApp.'}
  ]},
  {id:'reports', label:'Reports', items:[
    {ep:'/api/eod-preview', ico:'\\uD83D\\uDCC4', label:'Preview tonight\\u2019s EOD report'},
    {ep:'/api/eod-image', ico:'\\uD83D\\uDDBC', label:'EOD as an image'},
    {ep:'/api/eod-send', ico:'\\uD83D\\uDCE4', label:'Send EOD report now', act:true, confirm:'This sends the EOD report on WhatsApp.'},
    {ep:'/api/daily-report', ico:'\\uD83D\\uDCCA', label:'Daily report', act:true, confirm:'This may post the daily report to WhatsApp.'},
    {ep:'/api/report-status', ico:'\\u2139', label:'Report status'}
  ]},
  {id:'recon', label:'Reconciliation', items:[
    {ep:'/api/reconciliation', ico:'\\u2696', label:'Run reconciliation'},
    {ep:'/api/fund-position', ico:'\\uD83D\\uDCB0', label:'Fund position'},
    {ep:'/api/ledger', ico:'\\uD83D\\uDCD2', label:'Ledger view'},
    {ep:'/api/outliers', ico:'\\u26A0', label:'Outliers'},
    {ep:'/api/matcher-stats', ico:'\\uD83D\\uDCC8', label:'Matcher stats'}
  ]},
  {id:'controls', label:'Controls', items:[
    {ep:'/api/bot/on', ico:'\\u2705', label:'Turn bot ON', act:true, confirm:'Enable the bot?'},
    {ep:'/api/bot/off', ico:'\\u23F8', label:'Turn bot OFF', act:true, confirm:'Disable the bot? It will stop posting and processing.'},
    {ep:'/api/silent-status', ico:'\\u2139', label:'Silent mode status'},
    {ep:'/api/silent-on', ico:'\\uD83D\\uDD07', label:'Silent mode ON', act:true, confirm:'Turn on silent mode (bot stops scanning/posting)?'},
    {ep:'/api/silent-off', ico:'\\uD83D\\uDD0A', label:'Silent mode OFF', act:true, confirm:'Turn off silent mode?'},
    {ep:'/api/unapproved-alert-toggle', ico:'\\uD83D\\uDD14', label:'Toggle unapproved alert', act:true, confirm:'Toggle the unapproved-alert setting?'}
  ]},
  {id:'debug', label:'Debug', items:[
    {ep:'/api/wa-status', ico:'\\uD83D\\uDCF1', label:'WhatsApp status'},
    {ep:'/api/groups', ico:'\\uD83D\\uDC65', label:'Groups & JIDs'},
    {ep:'/api/debug-messages', ico:'\\uD83D\\uDCAC', label:'Recent messages'},
    {ep:'/api/debug-replies', ico:'\\u21A9', label:'Recent replies'},
    {ep:'/api/debug-verdict', ico:'\\u2696', label:'Verdict store'},
    {ep:'/api/dm-state', ico:'\\u2709', label:'DM state'},
    {ep:'/api/whoami', ico:'\\uD83D\\uDC64', label:'Who am I'},
    {ep:'/api/auth-list', ico:'\\uD83D\\uDD11', label:'Auth list'},
    {ep:'/api/stale-state', ico:'\\uD83D\\uDD52', label:'Stale state'},
    {ep:'/api/match-cache', ico:'\\uD83D\\uDDC2', label:'Match cache'}
  ]},
  {id:'dz', label:'Danger zone', dz:true, items:[
    {ep:'/api/wa-reset', ico:'\\uD83D\\uDD04', label:'Reset WhatsApp session (re-pair)', dz:true, confirm:'This LOGS THE BOT OUT of WhatsApp. You will have to re-scan the QR to reconnect. This cannot be undone.'},
    {ep:'/api/dm-clear', ico:'\\uD83D\\uDDD1', label:'Clear DM state', dz:true, confirm:'This wipes the bot\\u2019s DM state. This cannot be undone.'},
    {ep:'/api/match-cache-clear', ico:'\\uD83E\\uDDF9', label:'Clear match cache', dz:true, confirm:'This clears the reconciliation match cache. This cannot be undone.'},
    {ep:'/api/stale-reset', ico:'\\u267B', label:'Reset stale scan', dz:true, confirm:'This resets the stale-scan state. This cannot be undone.'}
  ]}
];

var active = 'approvals';
var pendingEp = null, pendingDz = false;

function el(id){return document.getElementById(id);}
function renderTabs(){
  var h='';
  TABS.forEach(function(t){
    h += '<button class="tab'+(t.dz?' dz':'')+(t.id===active?' active':'')+'" onclick="switchTab(\\''+t.id+'\\')">'+t.label+'</button>';
  });
  el('tabs').innerHTML=h;
}
function switchTab(id){ active=id; el('out').classList.remove('show'); renderTabs(); renderButtons(); }
function renderButtons(){
  var t = TABS.filter(function(x){return x.id===active;})[0];
  var h='';
  t.items.forEach(function(it,i){
    var cls='btn'+(it.dz?' dz':(it.act?' act':''));
    h += '<button class="'+cls+'" onclick="hit(\\''+active+'\\','+i+')"><span class="ico">'+it.ico+'</span>'+it.label+'</button>';
  });
  if(t.dz){ h += '<p class="note">Every action here is destructive and asks you to confirm twice.</p>'; }
  else if(t.items.some(function(x){return x.act;})){ h += '<p class="note">Actions marked in blue post to WhatsApp or change settings — they ask you to confirm first.</p>'; }
  el('buttons').innerHTML=h;
}
var confirmsLeft = 0;
function hit(tabId,i){
  var t = TABS.filter(function(x){return x.id===tabId;})[0];
  var it = t.items[i];
  if(it.confirm){
    pendingEp = it.ep; pendingDz = !!it.dz;
    confirmsLeft = it.dz ? 2 : 1;   // danger zone needs two confirms
    el('mTitle').textContent = it.dz ? 'Confirm — destructive' : 'Are you sure?';
    el('mBody').textContent = it.confirm;
    var go = el('mGo');
    go.className = it.dz ? 'go' : 'goact';
    go.textContent = it.dz ? 'Yes, do it' : 'Confirm';
    el('modal').classList.add('show');
  } else {
    run(it.ep);
  }
}
function closeModal(){ el('modal').classList.remove('show'); pendingEp=null; confirmsLeft=0; }
function confirmModal(){
  confirmsLeft--;
  if(confirmsLeft > 0){
    // show the final confirmation step (danger zone)
    el('mTitle').textContent = 'Final confirmation';
    el('mBody').textContent = 'Last chance — are you absolutely sure? This cannot be undone.';
    var go = el('mGo'); go.className='go'; go.textContent='Yes, I am sure';
    return; // modal stays open for the second click
  }
  var ep = pendingEp;
  el('modal').classList.remove('show');
  pendingEp=null;
  if(ep) run(ep);
}

function run(ep){
  var out = el('out');
  out.classList.add('show');
  out.innerHTML = '<span class="lbl">'+ep+'</span><span class="spin"></span> running…';
  fetch(ep, {credentials:'same-origin'})
    .then(function(r){ return r.text().then(function(tx){ return {ok:r.ok, status:r.status, tx:tx}; }); })
    .then(function(res){
      var body = res.tx;
      try { body = JSON.stringify(JSON.parse(res.tx), null, 2); } catch(e){}
      out.innerHTML = '<span class="lbl">'+ep+'  \\u2192  HTTP '+res.status+'</span>'+escapeHtml(body);
    })
    .catch(function(err){
      out.innerHTML = '<span class="lbl">'+ep+'</span>Error: '+escapeHtml(String(err));
    });
}
function escapeHtml(s){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

// load header status + stat cards from /health (open) and approval-audit
function loadStatus(){
  fetch('/health',{credentials:'same-origin'}).then(function(r){return r.json();}).then(function(h){
    el('hver').textContent = h.version || '—';
    el('hsub').textContent = 'WhatsApp '+(h.whatsapp||'?')+' · sheets '+(h.sheets||'?')+' · bot '+(h.botEnabled?'on':'off');
    el('hdot').style.background = (h.whatsapp==='connected') ? '#7fdca4' : '#e0a96d';
  }).catch(function(){});
  fetch('/api/approval-audit?days=3',{credentials:'same-origin'}).then(function(r){return r.ok?r.json():null;}).then(function(a){
    if(!a) return;
    var pending = a.noApproval!=null ? a.noApproval : (a.pending||0);
    var approved = a.fullyApproved!=null ? a.fullyApproved : (a.approved||0);
    var total = a.totalExpenseRequests!=null ? a.totalExpenseRequests : (a.total||0);
    el('cards').innerHTML =
      card('Pending approval', pending) +
      card('Fully approved', approved) +
      card('Total requests', total);
  }).catch(function(){});
}
function card(k,v){ return '<div class="card"><p class="k">'+k+'</p><p class="v">'+v+'</p></div>'; }

renderTabs(); renderButtons(); loadStatus();
</script>
</body>
</html>
`;
app.get('/api/panel', function(req, res){ res.type('html').send(PANEL_HTML); });
app.get('/api/pair',function(req,res){
  if(waReady)return res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><h1 style="color:#0f0">WhatsApp Connected</h1></body></html>');
  if(!latestQRDataUrl)return res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><h1 style="color:white">Waiting for QR...</h1></body></html>');
  res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><div style="text-align:center"><h1 style="color:white">Scan QR with WhatsApp</h1><img src="'+latestQRDataUrl+'" style="width:300px"/></div></body></html>');
});
app.get('/api/wa-status',function(req,res){res.json({connected:waReady});});
app.get('/api/groups',async function(req,res){if(!waReady)return res.json({error:'Not connected'});try{var chats=await waClient.getChats();res.json({groups:chats.filter(function(c){return c.isGroup;}).map(function(c){return{name:c.name,jid:c.id._serialized};})});}catch(e){res.json({error:e.message});}});
app.get('/api/bot/on',function(req,res){CONFIG.BOT_ENABLED=true;res.json({botEnabled:true});});
app.get('/api/bot/off',function(req,res){CONFIG.BOT_ENABLED=false;res.json({botEnabled:false});});
app.get('/api/ledger',async function(req,res){try{var date=req.query.date||new Date().toISOString().split('T')[0];var entries=await getLedgerData(date);var tIn=0,tOut=0;entries.forEach(function(e){if(e.inOut==='IN')tIn+=e.amount;if(e.inOut==='OUT')tOut+=e.amount;});res.json({date:date,entries:entries,totalIn:tIn,totalOut:tOut,net:tIn-tOut,count:entries.length});}catch(e){res.json({error:e.message});}});
app.get('/api/fund-position',async function(req,res){try{res.json({accounts:await getFundPosition()});}catch(e){res.json({error:e.message});}});
app.get('/api/approval-audit',async function(req,res){
  try{
    var days=parseInt(req.query.days)||15;
    var audit=await buildApprovalAudit(days);
    var fmt=function(e){return{date:e.date.toISOString().split('T')[0],time:e.date.toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata',hour:'2-digit',minute:'2-digit'}),message:e.body.substring(0,300),sender:e.sender,vendor:e.vendor,amount:e.amount,amountFormatted:e.amount>0?formatINR(e.amount):'',purpose:e.purpose||'',subItems:e.subItems||null,hasMedia:e.hasMedia,visionParsed:e.visionParsed||false,mm:e.status.mm,sm:e.status.sm,mmReply:e.mmApproval?e.mmApproval.raw:null,smReply:e.smApproval?e.smApproval.raw:null,mmName:e.mmApproval?e.mmApproval.name:null,smName:e.smApproval?e.smApproval.name:null,queryAnswer:e.queryAnswer||null,supportingDocs:e.supportingDocs||null};};
    res.json({summary:{period:days+' days',totalMessages:audit.totalMessages,totalExpenseRequests:audit.totalExpenses,fullyApproved:audit.fullyApproved.length,partialApproval:audit.partialApproval.length,noApproval:audit.noApproval.length,onHold:audit.onHold.length,rejected:audit.rejected.length,dedupedCount:audit.dedupedCount||0,visionCacheSize:audit.visionCacheSize},fullyApproved:audit.fullyApproved.map(fmt),partialApproval:audit.partialApproval.map(fmt),noApproval:audit.noApproval.map(fmt),onHold:audit.onHold.map(fmt),rejected:audit.rejected.map(fmt)});
  }catch(e){res.json({error:e.message});}
});
app.get('/api/send-reminders',async function(req,res){try{if(!waReady)return res.json({error:'WhatsApp not connected'});var count=await sendPendingReminders();res.json({success:true,remindersSent:count});}catch(e){res.json({error:e.message});}});
app.get('/api/unapproved-alert-toggle',function(req,res){var st=(req.query.state||'').toLowerCase();if(st==='on'||st==='off'){saveUnapprovedAlert(st==='on');}res.json({enabled:loadUnapprovedAlert(),note:'Day Book group alert; findings always appear in private summary.'});});
app.get('/api/reminder-digest-send',async function(req,res){try{if(!waReady)return res.json({error:'WhatsApp not connected'});var count=await sendApprovalReminderDigest();res.json({success:true,pendingPosted:count});}catch(e){res.json({error:e.message});}});
app.get('/api/reminder-digest-preview',async function(req,res){try{var d=await buildApprovalReminderDigest();res.json(d||{empty:true,message:'No pending approvals in the '+REMINDER_MAX_AGE_DAYS+'-day window'});}catch(e){res.json({error:e.message});}});
// v2.8.14 diagnostic: inspect the saved digest map, the verdict store, and how a test
// reply would parse — to debug why a verdict isn't registering.
app.get('/api/debug-verdict',async function(req,res){try{
  var map = loadDigestMap();
  var store = loadVerdicts();
  var testBody = req.query.body || '1 Yes';
  var parsed = map && map.items ? parseVerdictMessage(testBody, map.items.length) : null;
  res.json({
    digestMap: map ? { at: map.at, msgId: map.msgId, items: map.items } : null,
    verdictStoreKeys: Object.keys(store),
    verdictStore: store,
    testBody: testBody,
    parsedVerdicts: parsed
  });
}catch(e){res.json({error:e.message, stack:(e.stack||'').substring(0,400)});}});
app.get('/api/send-reminder-test',async function(req,res){
  try{
    if(!waReady)return res.json({error:'WhatsApp not connected'});
    var to=req.query.to; if(!to)return res.json({error:'pass ?to=917838537000'});
    var jid=to.replace(/[^0-9]/g,'')+'@c.us';
    var delay=function(ms){return new Promise(function(r){setTimeout(r,ms);});};
    var samples=[
      {id:'s1',date:new Date(),body:'SM Drawing',sender:'sushant',vendor:'SM Drawing',amount:400000,subItems:null,status:{mm:'pending',sm:'yes'},mmApproval:null,smApproval:{raw:'Ok',date:new Date(),name:'sumit'}},
      {id:'s2',date:new Date(),body:'Kackar + Innocept RMC',sender:'umesh katyal',vendor:'Kackar + Innocept RMC',amount:300000,subItems:[{vendor:'Kackar',amount:300000},{vendor:'Innocept RMC',amount:300000}],status:{mm:'pending',sm:'yes'},mmApproval:null,smApproval:{raw:'Ok',date:new Date(),name:'sumit'}},
      {id:'s3',date:new Date(),body:'Trinity Gurugram advance',sender:'sushant',vendor:'Trinity Gurugram advance',amount:100000,subItems:null,status:{mm:'question',sm:'pending'},mmApproval:{raw:'Advance to whom??',date:new Date(),name:'madhur mittal'},smApproval:null}
    ];
    var count=0;
    for(var i=0;i<samples.length;i++){var text=buildReminderText(samples[i]);if(!text)continue;await waClient.sendMessage(jid,text);await delay(1500);count++;}
    res.json({success:true,sentTo:jid,messages:count});
  }catch(e){res.json({error:e.message});}
});
app.get('/api/debug-messages',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});var chat=await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);var msgs=await chat.fetchMessages({limit:50});var result=[];for(var i=0;i<msgs.length;i++){var m=msgs[i];var info=await identifySender(m.author||m.from||'');result.push({rawSender:m.author||m.from||'',contactName:info.contactName,role:info.role,isReply:m.hasQuotedMsg,hasMedia:m.hasMedia,body:(m.body||'').substring(0,100),time:new Date(m.timestamp*1000).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'})});}res.json({totalMessages:result.length,mmNames:CONFIG.MM_NAMES,smNames:CONFIG.SM_NAMES,messages:result});}catch(e){res.json({error:e.message});}});
// v2.8.5 diagnostic: show M/S replies WITH the body of the message each one quoted,
// so a past swipe-"ok" can be matched to the expense it was actually replying to.
app.get('/api/debug-replies',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});
  var limit=parseInt(req.query.limit)||80;
  var chat=await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);
  var msgs=await chat.fetchMessages({limit:limit});
  var out=[];
  for(var i=0;i<msgs.length;i++){
    var m=msgs[i];
    var info=await identifySender(m.author||m.from||'');
    if(info.role!=='mm'&&info.role!=='sm') continue;
    var rec={role:info.role,who:info.contactName,body:(m.body||'').trim(),msgId:(m.id&&(m.id._serialized||m.id.id))||null,isReply:!!m.hasQuotedMsg,time:new Date(m.timestamp*1000).toLocaleString('en-IN',{timeZone:'Asia/Kolkata'}),quoted:null};
    if(m.hasQuotedMsg){try{var q=await m.getQuotedMessage();rec.quoted={msgId:(q.id&&(q.id._serialized||q.id.id))||null,body:(q.body||'').substring(0,160),fromBot:/PENDING APPROVALS|\[BOT REMINDER\]|EXPENSE REQUEST|🔔/i.test(q.body||'')};}catch(e){rec.quoted={error:e.message};}}
    out.push(rec);
  }
  res.json({count:out.length,replies:out});
}catch(e){res.json({error:e.message});}});
app.get('/api/preview',async function(req,res){try{res.send(buildReportHTML(await generateDailyReport(req.query.date||new Date().toISOString().split('T')[0])));}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/preview-image',async function(req,res){try{var img=await htmlToImage(buildReportHTML(await generateDailyReport(req.query.date||new Date().toISOString().split('T')[0])),800,1200);var buf=Buffer.isBuffer(img)?img:Buffer.from(img);res.set('Content-Type','image/png');res.set('Content-Length',String(buf.length));res.set('Cache-Control','no-store');res.end(buf);}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/daily-report',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});if(!CONFIG.BOT_ENABLED)return res.json({error:'Bot paused'});var d=req.query.date||new Date().toISOString().split('T')[0];var data=await generateDailyReport(d);var img=await htmlToImage(buildReportHTML(data),800,1200);var buf=Buffer.isBuffer(img)?img:Buffer.from(img);await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,new MessageMedia('image/png',buf.toString('base64'),'MIS_'+d+'.png'),{caption:'MIS Report - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut)+' | NET: '+formatINR(data.net)});res.json({success:true,date:d});}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/test-send',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,'MIS Bot test - '+new Date().toLocaleString('en-IN',{timeZone:'Asia/Kolkata'}));res.json({success:true});}catch(e){res.json({error:e.message});}});
app.get('/api/report-status',function(req,res){res.json({botEnabled:CONFIG.BOT_ENABLED,whatsapp:waReady,version:'2.8.17',visionEnabled:CONFIG.CLAUDE_API_KEY?true:false,reverseScanWindowDays:REVERSE_SCAN_WINDOW_DAYS,reverseScanMinAmount:REVERSE_SCAN_MIN_AMOUNT});});
app.get('/api/vision-test',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});var msgId=req.query.msgId;if(!msgId)return res.json({error:'pass ?msgId=...'});var chat=await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);var msgs=await chat.fetchMessages({limit:200});var target=null;for(var i=0;i<msgs.length;i++){var sid=msgs[i].id._serialized||msgs[i].id.id;if(sid===msgId){target=msgs[i];break;}}if(!target)return res.json({error:'message not found in last 200'});if(!target.hasMedia)return res.json({error:'no media'});var media=await target.downloadMedia();if(!media)return res.json({error:'failed to download'});visionCache.delete(msgId);var result=await extractFromImage(media,msgId);res.json({msgId:msgId,mimetype:media.mimetype,dataSize:media.data?media.data.length:0,parsed:result});}catch(e){res.json({error:e.message});}});
app.get('/api/whoami',async function(req,res){
  try {
    if(!waReady) return res.json({error:'not connected'});
    var chats = await waClient.getChats();
    var dms = chats.filter(function(c){ return !c.isGroup; }).slice(0, 30);
    var out = [];
    for(var i=0;i<dms.length;i++){
      var c = dms[i];
      var contact = null;
      try { contact = await waClient.getContactById(c.id._serialized); } catch(e){}
      out.push({
        jid: c.id._serialized,
        name: c.name || (contact ? (contact.pushname || contact.name) : ''),
        number: contact ? contact.number : null,
        lastMsg: c.lastMessage ? (c.lastMessage.body || '').substring(0,80) : ''
      });
    }
    res.json({recentDMs: out});
  } catch(e) { res.json({error: e.message}); }
});
app.get('/api/auth-list',function(req,res){res.json({accountants:CONFIG.ACCOUNTANT_PHONES,testNumbers:CONFIG.TEST_PHONES||[],mm:CONFIG.MM_PHONE,sm:CONFIG.SM_PHONE,note:'Only these phone numbers can DM the bot for expense relay. @lid (anonymous) JIDs are always rejected.'});});
app.get('/api/dm-state',function(req,res){try{res.json(loadDMState());}catch(e){res.json({error:e.message});}});
app.get('/api/dm-clear',function(req,res){try{saveDMState({pending:{}});res.json({success:true});}catch(e){res.json({error:e.message});}});
app.get('/api/reconciliation',async function(req,res){
  try {
    var days = parseInt(req.query.days) || 30;
    var rec = await buildReconciliation(days);
    var fmt = function(e){
      var m = e.matchResult ? e.matchResult.match : null;
      return {
        date: e.date.toISOString().split('T')[0],
        vendor: e.vendor,
        amount: e.amount,
        amountFormatted: formatINR(e.amount),
        body: e.body ? e.body.substring(0,200) : '',
        sender: e.sender,
        subItems: e.subItems || null,
        subItemResults: e.subItemResults || null,
        supportingDocs: e.supportingDocs || null,
        matchStatus: e.matchResult ? e.matchResult.status : null,
        matchConfidence: e.matchResult ? e.matchResult.confidence : null,
        ledgerMatch: m ? {
          date: m.date.toISOString().split('T')[0],
          entity: m.entity,
          description: m.description,
          amount: m.amount,
          mode: m.mode,
          bankAC: m.bankAC,
          person: m.person
        } : null,
        dateDiffDays: e.matchResult ? e.matchResult.dateDiffDays : null,
        pctDiff: e.matchResult ? e.matchResult.pctDiff : null
      };
    };
    var fmtLedger = function(le){
      return {
        date: le.date.toISOString().split('T')[0],
        entity: le.entity,
        description: le.description,
        head: le.head,
        tag: le.tag,
        amount: le.amount,
        amountFormatted: formatINR(le.amount),
        bankAC: le.bankAC,
        person: le.person,
        notes: le.notes
      };
    };
    res.json({
      summary: rec.summary,
      paid: rec.paid.map(fmt),
      paidWithTolerance: rec.paidWithTolerance.map(fmt),
      possibleMatch: rec.possibleMatch.map(fmt),
      awaitingPayment: rec.awaitingPayment.map(fmt),
      ledgerWithoutApproval: (rec.ledgerWithoutApproval||[]).map(fmtLedger),
      ledgerRecurring: (rec.ledgerRecurring||[]).map(fmtLedger),
      reverseScanConfig: {
        windowDays: REVERSE_SCAN_WINDOW_DAYS,
        minAmount: REVERSE_SCAN_MIN_AMOUNT
      }
    });
  } catch(e) { res.json({error: e.message}); }
});
app.get('/api/silent-on',function(req,res){try{saveSilentMode(true);res.json({success:true,silentMode:true,message:'Silent mode ON. Bot will stop group reminders and DM the observer ('+SILENT_OBSERVER+') with daily summaries.'});}catch(e){res.json({error:e.message});}});
app.get('/api/silent-off',function(req,res){try{saveSilentMode(false);res.json({success:true,silentMode:false,message:'Silent mode OFF. Bot will resume group reminders and post evening report to Day Book group.'});}catch(e){res.json({error:e.message});}});
app.get('/api/silent-status',function(req,res){try{res.json({silentMode:loadSilentMode(),observer:SILENT_OBSERVER});}catch(e){res.json({error:e.message});}});
app.get('/api/stale-scan',async function(req,res){try{if(!waReady)return res.json({error:'WhatsApp not connected'});var count=await scanStalePendings();res.json({success:true,remindersSent:count});}catch(e){res.json({error:e.message});}});
app.get('/api/stale-state',function(req,res){try{res.json(loadStaleState());}catch(e){res.json({error:e.message});}});
app.get('/api/stale-reset',function(req,res){try{saveStaleState({reminded:{}});res.json({success:true,message:'Stale reminder state cleared - next scan will re-send any 30+ min pending items'});}catch(e){res.json({error:e.message});}});
app.get('/api/eod-send',async function(req,res){try{var r=await sendEODReport(req.query.date);res.json(r);}catch(e){res.json({error:e.message});}});
app.get('/api/eod-preview',async function(req,res){try{var d=req.query.date||new Date().toISOString().split('T')[0];var audit=await buildApprovalAudit(30);var rec=await buildReconciliation(30);var outliers=await buildOutliers(rec);var dayOfWeek=new Date(d).getDay();var isFriday=req.query.friday==='1'||dayOfWeek===5;var weekStats=isFriday?computeWeeklyMatcherStats():null;res.send(buildEODReportHTML({date:d,audit:audit,rec:rec,outliers:outliers,isFriday:isFriday,weekStats:weekStats}));}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/eod-image',async function(req,res){try{var d=req.query.date||new Date().toISOString().split('T')[0];var audit=await buildApprovalAudit(30);var rec=await buildReconciliation(30);var outliers=await buildOutliers(rec);var dayOfWeek=new Date(d).getDay();var isFriday=req.query.friday==='1'||dayOfWeek===5;var weekStats=isFriday?computeWeeklyMatcherStats():null;var html=buildEODReportHTML({date:d,audit:audit,rec:rec,outliers:outliers,isFriday:isFriday,weekStats:weekStats});var img=await htmlToImage(html,460,2000);var buf=Buffer.isBuffer(img)?img:Buffer.from(img);res.set('Content-Type','image/png');res.set('Cache-Control','no-store');res.end(buf);}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/match-cache',function(req,res){try{res.json(loadMatchCache());}catch(e){res.json({error:e.message});}});
app.get('/api/match-cache-clear',function(req,res){try{saveMatchCache({matches:{},rejected:{},manualPaid:{}});res.json({success:true});}catch(e){res.json({error:e.message});}});
app.get('/api/matcher-stats',function(req,res){try{res.json(computeWeeklyMatcherStats());}catch(e){res.json({error:e.message});}});
app.get('/api/outliers',async function(req,res){try{var rec=await buildReconciliation(30);var outliers=await buildOutliers(rec);res.json({count:outliers.length,outliers:outliers});}catch(e){res.json({error:e.message});}});
app.get('/api/wa-reset',function(req,res){
  try {
    console.log('[WA] Manual reset requested via /api/wa-reset');
    waReady = false;
    latestQR = null; latestQRDataUrl = null;
    try { if (waClient) { waClient.destroy().catch(function(e){}); } } catch(e) {}
    try { if (fs.existsSync('./wa_auth')) { fs.rmSync('./wa_auth', { recursive: true, force: true }); console.log('[WA] wa_auth folder cleared'); } } catch(e) { console.error('[WA] Clear error:', e.message); }
    setTimeout(function() { createWhatsAppClient(); }, 3000);
    res.json({ok:true, message:'WhatsApp reset triggered. Wait 30-60s, then check /api/pair for new QR.'});
  } catch(e) { res.json({error:e.message}); }
});
// ── Crons ─────────────────────────────────────────────────────────────────────
// 7 PM IST — evening report (with v2.6 top-N + reverse-scan sections appended)
cron.schedule('30 13 * * *',async function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  var d=new Date().toISOString().split('T')[0];
  var silent = loadSilentMode();
  var targetJid = silent ? getSilentObserverJid() : CONFIG.WHATSAPP_GROUP_JID;
  var prefix = silent ? '[SILENT MODE] ' : '';
  try {
    var data = await generateDailyReport(d);
    var staleSection = await buildStalePendingSection();
    var recSection = await buildReconciliationSection();
    if(data.entryCount > 0){
      var img = await htmlToImage(buildReportHTML(data),800,1200);
      var buf = Buffer.isBuffer(img)?img:Buffer.from(img);
      var caption = prefix + 'Evening Report - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut) + staleSection + recSection;
      await waClient.sendMessage(targetJid, new MessageMedia('image/png',buf.toString('base64'),'MIS.png'), {caption:caption});
    } else if(staleSection || recSection) {
      await waClient.sendMessage(targetJid, prefix + 'Evening Report - '+d+'\nNo Ledger entries today.'+staleSection+recSection);
    } else if(silent) {
      await waClient.sendMessage(targetJid, prefix + 'Evening Report - '+d+'\nNo Ledger entries and no stale approvals.');
    }
  } catch(e) { console.error('Cron evening:',e.message); }
},{timezone:'Asia/Kolkata'});
// 9 AM IST — morning summary + pending reminders
cron.schedule('30 3 * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  var y=new Date();y.setDate(y.getDate()-1);var d=y.toISOString().split('T')[0];
  var silent = loadSilentMode();
  var targetJid = silent ? getSilentObserverJid() : CONFIG.WHATSAPP_GROUP_JID;
  var prefix = silent ? '[SILENT MODE] ' : '';
  generateDailyReport(d).then(function(data){
    if(data.entryCount>0){htmlToImage(buildReportHTML(data),800,1200).then(function(img){var buf=Buffer.isBuffer(img)?img:Buffer.from(img);waClient.sendMessage(targetJid,new MessageMedia('image/png',buf.toString('base64'),'MIS.png'),{caption:prefix+'Morning Summary - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut)});});}
  }).catch(function(e){console.error('Cron morning:',e.message);});
  // v2.7.2: pending-approval reminders moved to dedicated 10 AM + 7 PM digests (below).
},{timezone:'Asia/Kolkata'});
// v2.7.2: Approval reminder digests — 10:00 AM and 7:00 PM IST.
// Consolidated single message to the approval group, 14-day window, bypasses silent mode.
cron.schedule('0 10 * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  sendApprovalReminderDigest().catch(function(e){console.error('[Digest 10AM]',e.message);});
},{timezone:'Asia/Kolkata'});
cron.schedule('0 19 * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  sendApprovalReminderDigest().catch(function(e){console.error('[Digest 7PM]',e.message);});
},{timezone:'Asia/Kolkata'});
// Every 10 minutes — scan for pending expenses >= 30 min old
cron.schedule('*/10 * * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  scanStalePendings().catch(function(e){console.error('[Cron stale]',e.message);});
},{timezone:'Asia/Kolkata'});
// 7 PM IST daily — EOD JPEG report privately to SILENT_OBSERVER (with v2.6 reverse-scan section)
cron.schedule('0 19 * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  sendEODReport().then(function(r){
    if(r && r.rec) return postUnapprovedAlertIfEnabled(r.rec);
    return buildReconciliation(REVERSE_SCAN_WINDOW_DAYS+14).then(postUnapprovedAlertIfEnabled);
  }).catch(function(e){console.error('[EOD cron]',e.message);});
},{timezone:'Asia/Kolkata'});
// ── Startup ──────────────────────────────────────────────────────────────────
initGoogleSheets();
createWhatsAppClient();
app.listen(CONFIG.PORT,function(){
  console.log('\nFidato MIS Server v2.8.19 | Port:',CONFIG.PORT,'| Vision:',CONFIG.CLAUDE_API_KEY?'enabled':'disabled');
  console.log('  ReverseScan: window='+REVERSE_SCAN_WINDOW_DAYS+'d, floor=Rs.'+REVERSE_SCAN_MIN_AMOUNT);
  console.log('  Report top-N: stale='+STALE_TOP_N+' (recent='+STALE_RECENT_HOURS+'h), reconciliation='+REPORT_TOP_N);
  console.log('  Smart DM parsing: enabled (free-form vendor/amount/company/account extraction)');
  console.log('  Endpoint lock: '+((PANEL_USER&&PANEL_PASSWORD)?'ON (Basic Auth on /api/*)':'FAIL-CLOSED (PANEL_USER/PANEL_PASSWORD unset)'));
});
