// ============================================================
// FIDATO MIS DAILY REPORT + APPROVAL AUDIT SERVER v2.1
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

// ============================================================
// CONFIGURATION
// ============================================================
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
  ACCOUNTANT_PHONES: [
    '919873574112',
    '919873574180',
    '919873574192',
    '919873574103'
    '919773592304'
  ],
};

// ============================================================
// GOOGLE SHEETS API
// ============================================================
var sheetsApi = null;

function initGoogleSheets() {
  if (!CONFIG.GOOGLE_CREDENTIALS) {
    console.log('No GOOGLE_CREDENTIALS set. Sheet reading disabled.');
    return;
  }
  try {
    var creds = JSON.parse(CONFIG.GOOGLE_CREDENTIALS);
    var auth = new google.auth.GoogleAuth({
      credentials: creds,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });
    sheetsApi = google.sheets({ version: 'v4', auth: auth });
    console.log('Google Sheets API initialized.');
  } catch (e) {
    console.error('Failed to init Google Sheets:', e.message);
  }
}

async function readSheet(range) {
  if (!sheetsApi) throw new Error('Google Sheets not initialized');
  var result = await sheetsApi.spreadsheets.values.get({
    spreadsheetId: CONFIG.SHEET_ID,
    range: range,
  });
  return result.data.values || [];
}

// ============================================================
// WHATSAPP-WEB.JS CONNECTION
// ============================================================
var waClient = null;
var waReady = false;
var latestQR = null;
var latestQRDataUrl = null;

function createWhatsAppClient() {
  waClient = new Client({
    authStrategy: new LocalAuth({ dataPath: './wa_auth' }),
    puppeteer: {
      headless: true,
      args: [
        '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas', '--no-first-run', '--no-zygote',
        '--single-process', '--disable-gpu', '--disable-extensions',
      ],
    },
  });

  waClient.on('qr', function(qr) {
    latestQR = qr;
    qrcode.toDataURL(qr, function(err, url) {
      if (!err) latestQRDataUrl = url;
    });
    console.log('New QR code generated. Visit /api/pair to scan.');
  });

  waClient.on('ready', function() {
    waReady = true;
    latestQR = null;
    latestQRDataUrl = null;
    console.log('WhatsApp Web client is ready!');
  });

  waClient.on('authenticated', function() {
    console.log('WhatsApp authenticated.');
  });

  waClient.on('auth_failure', function(msg) {
    console.error('WhatsApp auth failure:', msg);
    waReady = false;
  });

  waClient.on('disconnected', function(reason) {
    console.log('WhatsApp disconnected:', reason);
    waReady = false;
    setTimeout(function() {
      console.log('Reconnecting WhatsApp...');
      waClient.initialize().catch(function(e) { console.error('Reconnect failed:', e.message); });
    }, 10000);
  });

  waClient.initialize().catch(function(e) {
    console.error('WhatsApp init failed:', e.message);
  });
}

// ============================================================
// HTML TO IMAGE (Puppeteer)
// ============================================================
var browserInstance = null;

async function htmlToImage(html, width, height) {
  if (!browserInstance) {
    browserInstance = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
    });
  }
  var page = await browserInstance.newPage();
  await page.setViewport({ width: width || 800, height: height || 600 });
  await page.setContent(html, { waitUntil: 'networkidle0' });
  var bodyHandle = await page.$('body');
  var boundingBox = await bodyHandle.boundingBox();
  var screenshot = await page.screenshot({
    clip: { x: 0, y: 0, width: boundingBox.width, height: boundingBox.height },
    type: 'png',
  });
  await page.close();
  return screenshot;
}

// ============================================================
// HELPER FUNCTIONS
// ============================================================
function parseSheetDate(val) {
  if (!val) return null;
  if (val instanceof Date) return val;
  var str = val.toString().trim();
  var parts = str.split(/[\/\.\-]/);
  if (parts.length === 3) {
    var d = parseInt(parts[0]);
    var m = parseInt(parts[1]);
    var y = parseInt(parts[2]);
    if (y < 100) y += 2000;
    if (d > 0 && d <= 31 && m > 0 && m <= 12) return new Date(y, m - 1, d);
  }
  var parsed = new Date(val);
  if (!isNaN(parsed.getTime())) return parsed;
  return null;
}

function parseAmount(val) {
  if (typeof val === 'number') return val;
  if (!val) return 0;
  var num = parseFloat(String(val).replace(/,/g, '').replace(/[^0-9.\-]/g, ''));
  return isNaN(num) ? 0 : num;
}

function formatINR(num) {
  if (!num) return '0';
  var isNeg = num < 0;
  num = Math.abs(Math.round(num));
  var str = num.toString();
  var lastThree = str.substring(str.length - 3);
  var otherNumbers = str.substring(0, str.length - 3);
  if (otherNumbers !== '') lastThree = ',' + lastThree;
  var formatted = otherNumbers.replace(/\B(?=(\d{2})+(?!\d))/g, ',') + lastThree;
  return (isNeg ? '-' : '') + formatted;
}

function isAccountant(phone) {
  return CONFIG.ACCOUNTANT_PHONES.some(function(ph) {
    return phone === ph || phone.endsWith(ph.slice(-10));
  });
}

function isMM(phone) {
  return phone === CONFIG.MM_PHONE || phone.endsWith(CONFIG.MM_PHONE.slice(-10));
}

function isSM(phone) {
  return phone === CONFIG.SM_PHONE || phone.endsWith(CONFIG.SM_PHONE.slice(-10));
}

// ============================================================
// LEDGER DATA
// ============================================================
async function getLedgerData(dateStr) {
  var rows = await readSheet('Ledger!A:L');
  var targetDate = dateStr || new Date().toISOString().split('T')[0];
  var entries = [];

  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    if (!row[0] || !row[5]) continue;
    var cellDate = parseSheetDate(row[0]);
    if (!cellDate) continue;
    var formatted = cellDate.toISOString().split('T')[0];
    if (formatted === targetDate) {
      entries.push({
        date: cellDate,
        entity: row[1] || '',
        head: row[2] || '',
        description: row[3] || '',
        tag: row[4] || '',
        inOut: row[5] || '',
        amount: parseAmount(row[6]),
        mode: row[7] || '',
        person: row[8] || '',
        bankAC: row[9] || '',
        transferTo: row[10] || '',
        notes: row[11] || '',
      });
    }
  }
  return entries;
}

// ============================================================
// FUND POSITION DATA
// ============================================================
async function getFundPosition() {
  var rows = await readSheet('Fund Position!A4:J27');
  var accounts = [];
  for (var i = 1; i < rows.length; i++) {
    var row = rows[i];
    if (!row[1] || row[1] === 'TOTAL') continue;
    accounts.push({
      num: row[0] || '',
      company: row[1] || '',
      bankAC: row[2] || '',
      opening: parseAmount(row[3]),
      todayIn: parseAmount(row[4]),
      todayOut: parseAmount(row[5]),
      closing: parseAmount(row[6]),
      cheques: parseAmount(row[7]),
      netBal: parseAmount(row[8]),
      status: row[9] || 'Usable',
    });
  }
  return accounts;
}

// ============================================================
// APPROVAL AUDIT SYSTEM
// ============================================================
function parseResponse(text) {
  if (!text) return 'pending';
  var lower = text.toLowerCase().trim();

  // Yes patterns (English + Hindi)
  var yesPatterns = ['yes', 'ok', 'okay', 'approved', 'done', 'go ahead', 'proceed',
    'haan', 'ha', 'han', 'theek hai', 'thik hai', 'kar do', 'karo',
    'y', 'yep', 'yea', 'yeah', 'sure', 'fine', 'agreed', 'confirmed'];
  for (var i = 0; i < yesPatterns.length; i++) {
    if (lower === yesPatterns[i]) return 'yes';
  }

  // Emoji yes
  if (lower.indexOf('\u{1F44D}') >= 0 || lower.indexOf('\u2705') >= 0 ||
      lower.indexOf('\u{1F44C}') >= 0) return 'yes';

  // No patterns
  var noPatterns = ['no', 'nahi', 'nah', 'rejected', 'cancel', 'mat karo',
    'n', 'nope', 'deny', 'denied', 'reject', 'nhi'];
  for (var j = 0; j < noPatterns.length; j++) {
    if (lower === noPatterns[j]) return 'no';
  }

  // Emoji no
  if (lower.indexOf('\u274C') >= 0 || lower.indexOf('\u{1F44E}') >= 0) return 'no';

  // Hold patterns
  var holdPatterns = ['hold', 'wait', 'ruko', 'later', 'baad mein', 'not now',
    'pending', 'rukko', 'abhi nahi', 'bad me'];
  for (var k = 0; k < holdPatterns.length; k++) {
    if (lower === holdPatterns[k] || lower.indexOf(holdPatterns[k]) >= 0) return 'hold';
  }

  return 'other';
}

function parseExpenseMessage(body) {
  if (!body) return { vendor: '', amount: 0 };

  var amountMatch = body.match(/(?:rs\.?\s*|inr\s*|amount\s*:?\s*)?(\d[\d,]*\.?\d*)\s*(?:lac|lakh|lacs|l\b|cr|crore)/i);
  var amount = 0;
  if (amountMatch) {
    amount = parseFloat(amountMatch[1].replace(/,/g, ''));
    if (/cr|crore/i.test(amountMatch[0])) amount *= 10000000;
    else if (/lac|lakh|lacs|l\b/i.test(amountMatch[0])) amount *= 100000;
  } else {
    var rsMatch = body.match(/(?:rs\.?\s*|inr\s*|\u20B9\s*)(\d[\d,]*\.?\d*)/i);
    if (rsMatch) amount = parseFloat(rsMatch[1].replace(/,/g, ''));
  }

  var lines = body.split('\n');
  var vendor = lines[0].substring(0, 150);

  return { vendor: vendor, amount: amount };
}

async function fetchApprovalMessages(days) {
  if (!waReady || !waClient) throw new Error('WhatsApp not connected');
  if (!CONFIG.APPROVAL_GROUP_JID) throw new Error('APPROVAL_GROUP_JID not set. Use /api/groups to find it.');

  var chat = await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);

  // Load more history by fetching with increasing limits
  var allMessages = [];
  var limits = [100, 200, 500, 1000];

  for (var i = 0; i < limits.length; i++) {
    try {
      allMessages = await chat.fetchMessages({ limit: limits[i] });
      console.log('Fetched ' + allMessages.length + ' messages with limit ' + limits[i]);
      if (allMessages.length < limits[i]) break;
    } catch (e) {
      console.error('Fetch with limit ' + limits[i] + ' failed:', e.message);
      break;
    }
  }

  var cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - (days || 15));

  var filtered = allMessages.filter(function(msg) {
    return new Date(msg.timestamp * 1000) >= cutoff;
  });

  console.log('Filtered to ' + filtered.length + ' messages in last ' + days + ' days');
  return filtered;
}

async function buildApprovalAudit(days) {
  var messages = await fetchApprovalMessages(days || 15);
  var expenses = [];
  var replyMap = {};

  // First pass: identify all messages and replies
  for (var i = 0; i < messages.length; i++) {
    var msg = messages[i];
    var sender = (msg.author || msg.from || '').replace('@c.us', '').replace('@s.whatsapp.net', '');
    var msgDate = new Date(msg.timestamp * 1000);
    var body = (msg.body || '').trim();
    var hasMedia = msg.hasMedia || false;

    // Check if this is a reply (swipe reply)
    var quotedMsgId = null;
    if (msg.hasQuotedMsg) {
      try {
        var quoted = await msg.getQuotedMessage();
        quotedMsgId = quoted.id._serialized || quoted.id.id;
      } catch (e) {
        // Could not fetch quoted message — skip
      }
    }

    if (quotedMsgId && (isMM(sender) || isSM(sender))) {
      // This is a reply from MM or SM — track it as approval
      if (!replyMap[quotedMsgId]) replyMap[quotedMsgId] = { mm: null, sm: null };
      var response = parseResponse(body);

      if (isMM(sender)) {
        replyMap[quotedMsgId].mm = { response: response, date: msgDate, raw: body };
      }
      if (isSM(sender)) {
        replyMap[quotedMsgId].sm = { response: response, date: msgDate, raw: body };
      }
    } else if (isAccountant(sender)) {
      // This is a message from an accountant — treat as expense request
      var msgId = msg.id._serialized || msg.id.id;
      var parsed = parseExpenseMessage(body);

      expenses.push({
        id: msgId,
        date: msgDate,
        body: body || (hasMedia ? '[Image/Media attached]' : '[Empty message]'),
        sender: sender,
        senderLabel: 'Accountant',
        vendor: parsed.vendor || (hasMedia ? '[See image]' : ''),
        amount: parsed.amount,
        hasMedia: hasMedia,
        mmApproval: null,
        smApproval: null,
        status: { mm: 'pending', sm: 'pending' },
      });
    }
    // All other messages (from other group members) are ignored
  }

  // Second pass: match replies to expenses
  for (var j = 0; j < expenses.length; j++) {
    var replies = replyMap[expenses[j].id];
    if (replies) {
      expenses[j].mmApproval = replies.mm;
      expenses[j].smApproval = replies.sm;
      expenses[j].status.mm = replies.mm ? replies.mm.response : 'pending';
      expenses[j].status.sm = replies.sm ? replies.sm.response : 'pending';
    }
  }

  // Categorize
  var result = {
    fullyApproved: [],
    partialApproval: [],
    noApproval: [],
    onHold: [],
    rejected: [],
    allExpenses: expenses,
    totalExpenses: expenses.length,
    totalMessages: messages.length,
    fetchedDays: days || 15,
  };

  for (var k = 0; k < expenses.length; k++) {
    var e = expenses[k];
    var mm = e.status.mm;
    var sm = e.status.sm;

    if (mm === 'no' || sm === 'no') {
      result.rejected.push(e);
    } else if (mm === 'hold' || sm === 'hold') {
      result.onHold.push(e);
    } else if (mm === 'yes' && sm === 'yes') {
      result.fullyApproved.push(e);
    } else if (mm === 'yes' || sm === 'yes') {
      result.partialApproval.push(e);
    } else {
      result.noApproval.push(e);
    }
  }

  return result;
}

// ============================================================
// API ENDPOINTS
// ============================================================
app.get('/health', function(req, res) {
  res.json({
    status: 'ok',
    version: '2.1',
    whatsapp: waReady ? 'connected' : 'disconnected',
    sheets: sheetsApi ? 'initialized' : 'not configured',
    botEnabled: CONFIG.BOT_ENABLED,
    dayBookGroup: CONFIG.WHATSAPP_GROUP_JID,
    approvalGroup: CONFIG.APPROVAL_GROUP_JID || 'not set',
    accountants: CONFIG.ACCOUNTANT_PHONES.length,
  });
});

app.get('/api/pair', function(req, res) {
  if (waReady) return res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><div style="text-align:center"><h1 style="color:#0f0">WhatsApp Connected</h1><p style="color:#888">Bot is already paired and ready.</p></div></body></html>');
  if (!latestQRDataUrl) return res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><div style="text-align:center"><h1 style="color:white">Waiting for QR Code...</h1><p style="color:#888">Refresh in a few seconds.</p></div></body></html>');
  res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111">' +
    '<div style="text-align:center"><h1 style="color:white">Scan QR Code with WhatsApp</h1>' +
    '<img src="' + latestQRDataUrl + '" style="width:300px;height:300px" />' +
    '<p style="color:#888">Open WhatsApp > Settings > Linked Devices > Link a Device</p></div></body></html>');
});

app.get('/api/wa-status', function(req, res) {
  res.json({ connected: waReady, hasQR: !!latestQR });
});

app.get('/api/groups', async function(req, res) {
  if (!waReady) return res.json({ error: 'WhatsApp not connected' });
  try {
    var chats = await waClient.getChats();
    var groups = chats.filter(function(c) { return c.isGroup; }).map(function(c) {
      return { name: c.name, jid: c.id._serialized, participants: c.participants ? c.participants.length : 0 };
    });
    res.json({ groups: groups });
  } catch (e) {
    res.json({ error: e.message });
  }
});

app.get('/api/bot/on', function(req, res) {
  CONFIG.BOT_ENABLED = true;
  res.json({ botEnabled: true, message: 'Bot resumed' });
});

app.get('/api/bot/off', function(req, res) {
  CONFIG.BOT_ENABLED = false;
  res.json({ botEnabled: false, message: 'Bot paused' });
});

app.get('/api/ledger', async function(req, res) {
  try {
    var date = req.query.date || new Date().toISOString().split('T')[0];
    var entries = await getLedgerData(date);
    var totalIn = 0, totalOut = 0;
    entries.forEach(function(e) {
      if (e.inOut === 'IN') totalIn += e.amount;
      if (e.inOut === 'OUT') totalOut += e.amount;
    });
    res.json({ date: date, entries: entries, totalIn: totalIn, totalOut: totalOut, net: totalIn - totalOut, count: entries.length });
  } catch (e) {
    res.json({ error: e.message });
  }
});

app.get('/api/fund-position', async function(req, res) {
  try {
    var accounts = await getFundPosition();
    res.json({ accounts: accounts });
  } catch (e) {
    res.json({ error: e.message });
  }
});

// ============================================================
// APPROVAL AUDIT ENDPOINT
// ============================================================
app.get('/api/approval-audit', async function(req, res) {
  try {
    var days = parseInt(req.query.days) || 15;
    var audit = await buildApprovalAudit(days);

    var formatExpense = function(e) {
      return {
        date: e.date.toISOString().split('T')[0],
        time: e.date.toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit' }),
        message: e.body.substring(0, 300),
        vendor: e.vendor,
        amount: e.amount,
        amountFormatted: e.amount > 0 ? formatINR(e.amount) : '',
        hasMedia: e.hasMedia,
        mm: e.status.mm,
        sm: e.status.sm,
        mmReply: e.mmApproval ? e.mmApproval.raw : null,
        smReply: e.smApproval ? e.smApproval.raw : null,
        mmDate: e.mmApproval ? e.mmApproval.date.toISOString().split('T')[0] : null,
        smDate: e.smApproval ? e.smApproval.date.toISOString().split('T')[0] : null,
      };
    };

    res.json({
      summary: {
        period: days + ' days',
        totalMessages: audit.totalMessages,
        totalExpenseRequests: audit.totalExpenses,
        fullyApproved: audit.fullyApproved.length,
        partialApproval: audit.partialApproval.length,
        noApproval: audit.noApproval.length,
        onHold: audit.onHold.length,
        rejected: audit.rejected.length,
      },
      fullyApproved: audit.fullyApproved.map(formatExpense),
      partialApproval: audit.partialApproval.map(formatExpense),
      noApproval: audit.noApproval.map(formatExpense),
      onHold: audit.onHold.map(formatExpense),
      rejected: audit.rejected.map(formatExpense),
    });
  } catch (e) {
    res.json({ error: e.message });
  }
});

// ============================================================
// DAILY REPORT GENERATION
// ============================================================
async function generateDailyReport(dateStr) {
  var entries = await getLedgerData(dateStr);
  var fundPosition = await getFundPosition();

  var totalIn = 0, totalOut = 0;
  var inflows = [], outflows = [];

  entries.forEach(function(e) {
    if (e.inOut === 'IN') { totalIn += e.amount; inflows.push(e); }
    if (e.inOut === 'OUT') { totalOut += e.amount; outflows.push(e); }
  });

  var byTag = {};
  outflows.forEach(function(e) {
    var tag = e.tag || 'Other';
    if (!byTag[tag]) byTag[tag] = { total: 0, items: [] };
    byTag[tag].total += e.amount;
    byTag[tag].items.push(e);
  });

  return {
    date: dateStr,
    totalIn: totalIn,
    totalOut: totalOut,
    net: totalIn - totalOut,
    inflows: inflows,
    outflows: outflows,
    byTag: byTag,
    fundPosition: fundPosition,
    entryCount: entries.length,
  };
}

function buildReportHTML(data) {
  var html = '<!DOCTYPE html><html><head><meta charset="utf-8"><style>';
  html += 'body{font-family:Arial,sans-serif;background:#fff;padding:20px;max-width:800px;margin:0 auto;color:#222}';
  html += '.hdr{text-align:center;border-bottom:2px solid #333;padding-bottom:10px;margin-bottom:15px}';
  html += '.hdr h1{font-size:22px;margin:0}';
  html += '.hdr p{color:#666;margin:4px 0 0}';
  html += '.metrics{display:flex;gap:10px;margin:15px 0}';
  html += '.mc{flex:1;background:#f5f5f5;border-radius:8px;padding:12px;text-align:center}';
  html += '.mc .lbl{font-size:11px;color:#888}';
  html += '.mc .val{font-size:20px;font-weight:bold;margin:4px 0 0}';
  html += '.gn{color:#0a7}.rd{color:#c33}.bl{color:#36a}';
  html += '.sec{font-size:14px;font-weight:bold;color:#555;border-bottom:1px solid #ddd;padding:8px 0 4px;margin:15px 0 8px}';
  html += 'table{width:100%;border-collapse:collapse;font-size:12px}';
  html += 'th{text-align:left;padding:5px;background:#f0f0f0;font-size:11px;color:#666}';
  html += 'td{padding:5px;border-top:1px solid #eee}';
  html += '.amt{text-align:right;font-family:monospace}';
  html += '.tot{font-weight:bold;background:#f5f5f5}';
  html += '</style></head><body>';

  html += '<div class="hdr"><h1>Fidato Group - Daily MIS Report</h1>';
  html += '<p>' + data.date + ' | ' + data.entryCount + ' transactions</p></div>';

  html += '<div class="metrics">';
  html += '<div class="mc"><div class="lbl">Total Inflows</div><div class="val gn">' + formatINR(data.totalIn) + '</div></div>';
  html += '<div class="mc"><div class="lbl">Total Outflows</div><div class="val rd">' + formatINR(data.totalOut) + '</div></div>';
  html += '<div class="mc"><div class="lbl">Net Movement</div><div class="val ' + (data.net >= 0 ? 'bl' : 'rd') + '">' + formatINR(data.net) + '</div></div>';
  html += '</div>';

  if (data.inflows.length > 0) {
    html += '<div class="sec">INFLOWS</div><table>';
    html += '<tr><th>Description</th><th>Entity</th><th>Tag</th><th>Bank A/C</th><th style="text-align:right">Amount</th></tr>';
    data.inflows.forEach(function(e) {
      html += '<tr><td>' + e.description + '</td><td>' + e.entity + '</td><td>' + e.tag + '</td><td>' + e.bankAC + '</td><td class="amt gn">' + formatINR(e.amount) + '</td></tr>';
    });
    html += '</table>';
  }

  html += '<div class="sec">OUTFLOWS BY CATEGORY</div><table>';
  html += '<tr><th>Category</th><th>Items</th><th style="text-align:right">Amount</th></tr>';
  var tags = Object.keys(data.byTag).sort(function(a, b) { return data.byTag[b].total - data.byTag[a].total; });
  tags.forEach(function(tag) {
    html += '<tr><td>' + tag + '</td><td>' + data.byTag[tag].items.length + '</td><td class="amt rd">' + formatINR(data.byTag[tag].total) + '</td></tr>';
  });
  html += '</table>';

  html += '<div class="sec">FUND POSITION</div><table>';
  html += '<tr><th>Account</th><th style="text-align:right">Opening</th><th style="text-align:right">IN</th><th style="text-align:right">OUT</th><th style="text-align:right">Closing</th><th style="text-align:right">Cheques</th><th style="text-align:right">Net</th></tr>';
  data.fundPosition.forEach(function(a) {
    html += '<tr><td>' + a.bankAC + '</td>';
    html += '<td class="amt">' + formatINR(a.opening) + '</td>';
    html += '<td class="amt gn">' + formatINR(a.todayIn) + '</td>';
    html += '<td class="amt rd">' + formatINR(a.todayOut) + '</td>';
    html += '<td class="amt">' + formatINR(a.closing) + '</td>';
    html += '<td class="amt rd">' + formatINR(a.cheques) + '</td>';
    html += '<td class="amt ' + (a.netBal < 0 ? 'rd' : '') + '">' + formatINR(a.netBal) + '</td></tr>';
  });
  html += '</table>';

  html += '</body></html>';
  return html;
}

app.get('/api/preview', async function(req, res) {
  try {
    var date = req.query.date || new Date().toISOString().split('T')[0];
    var data = await generateDailyReport(date);
    var html = buildReportHTML(data);
    res.send(html);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/preview-image', async function(req, res) {
  try {
    var date = req.query.date || new Date().toISOString().split('T')[0];
    var data = await generateDailyReport(date);
    var html = buildReportHTML(data);
    var imgBuffer = await htmlToImage(html, 800, 1200);
    res.set('Content-Type', 'image/png');
    res.send(imgBuffer);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/daily-report', async function(req, res) {
  try {
    if (!waReady) return res.json({ error: 'WhatsApp not connected' });
    if (!CONFIG.BOT_ENABLED) return res.json({ error: 'Bot is paused' });

    var date = req.query.date || new Date().toISOString().split('T')[0];
    var data = await generateDailyReport(date);
    var html = buildReportHTML(data);
    var imgBuffer = await htmlToImage(html, 800, 1200);

    var media = new MessageMedia('image/png', imgBuffer.toString('base64'), 'MIS_Report_' + date + '.png');
    await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID, media, {
      caption: 'Fidato Group MIS Report - ' + date + '\nIN: ' + formatINR(data.totalIn) + ' | OUT: ' + formatINR(data.totalOut) + ' | NET: ' + formatINR(data.net),
    });

    res.json({ success: true, date: date, sentTo: CONFIG.WHATSAPP_GROUP_JID });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/test-send', async function(req, res) {
  try {
    if (!waReady) return res.json({ error: 'WhatsApp not connected' });
    await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID, 'MIS Bot test - ' + new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }));
    res.json({ success: true });
  } catch (e) {
    res.json({ error: e.message });
  }
});

app.get('/api/report-status', function(req, res) {
  res.json({ botEnabled: CONFIG.BOT_ENABLED, whatsapp: waReady, version: '2.1' });
});

// ============================================================
// CRON SCHEDULE
// ============================================================
// 7PM IST daily report
cron.schedule('30 13 * * *', function() {
  if (!CONFIG.BOT_ENABLED || !waReady) return;
  var today = new Date().toISOString().split('T')[0];
  generateDailyReport(today).then(function(data) {
    if (data.entryCount > 0) {
      var html = buildReportHTML(data);
      htmlToImage(html, 800, 1200).then(function(img) {
        var media = new MessageMedia('image/png', img.toString('base64'), 'MIS_Report.png');
        waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID, media, {
          caption: 'Evening MIS Report - ' + today + '\nIN: ' + formatINR(data.totalIn) + ' | OUT: ' + formatINR(data.totalOut),
        });
      });
    }
  }).catch(function(e) { console.error('Cron report error:', e.message); });
}, { timezone: 'Asia/Kolkata' });

// 9AM IST morning summary for yesterday
cron.schedule('30 3 * * *', function() {
  if (!CONFIG.BOT_ENABLED || !waReady) return;
  var yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  var dateStr = yesterday.toISOString().split('T')[0];
  generateDailyReport(dateStr).then(function(data) {
    if (data.entryCount > 0) {
      var html = buildReportHTML(data);
      htmlToImage(html, 800, 1200).then(function(img) {
        var media = new MessageMedia('image/png', img.toString('base64'), 'MIS_Report.png');
        waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID, media, {
          caption: 'Morning Summary (Yesterday) - ' + dateStr + '\nIN: ' + formatINR(data.totalIn) + ' | OUT: ' + formatINR(data.totalOut),
        });
      });
    }
  }).catch(function(e) { console.error('Cron morning error:', e.message); });
}, { timezone: 'Asia/Kolkata' });

// ============================================================
// START SERVER
// ============================================================
initGoogleSheets();
createWhatsAppClient();

app.listen(CONFIG.PORT, function() {
  console.log('\n========================================');
  console.log('Fidato MIS Report Server v2.1');
  console.log('========================================');
  console.log('Port: ' + CONFIG.PORT);
  console.log('Sheet: ' + CONFIG.SHEET_ID);
  console.log('Day Book Group: ' + CONFIG.WHATSAPP_GROUP_JID);
  console.log('Approval Group: ' + CONFIG.APPROVAL_GROUP_JID);
  console.log('MM: ' + CONFIG.MM_PHONE);
  console.log('SM: ' + CONFIG.SM_PHONE);
  console.log('Accountants: ' + CONFIG.ACCOUNTANT_PHONES.join(', '));
  console.log('\nEndpoints:');
  console.log('  /health');
  console.log('  /api/pair');
  console.log('  /api/groups');
  console.log('  /api/wa-status');
  console.log('  /api/bot/on | /api/bot/off');
  console.log('  /api/ledger?date=YYYY-MM-DD');
  console.log('  /api/fund-position');
  console.log('  /api/preview?date=YYYY-MM-DD');
  console.log('  /api/preview-image?date=YYYY-MM-DD');
  console.log('  /api/daily-report?date=YYYY-MM-DD');
  console.log('  /api/approval-audit?days=15');
  console.log('  /api/test-send');
  console.log('\nSchedule: 7PM report, 9AM morning summary');
  console.log('========================================\n');
});
