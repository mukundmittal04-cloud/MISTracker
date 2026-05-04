// ============================================================
// FIDATO MIS DAILY REPORT + APPROVAL AUDIT SERVER v2.0
// Reads Google Sheet, generates reports, sends via WhatsApp
// Reads approval group chat, tracks MM/SM responses
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
  APPROVAL_GROUP_JID: process.env.APPROVAL_GROUP_JID || '',
  BOT_ENABLED: process.env.BOT_ENABLED !== 'false',
  CLAUDE_API_KEY: process.env.CLAUDE_API_KEY,
  PORT: process.env.PORT || 3000,
  MM_PHONE: '919873095398',
  SM_PHONE: '919873429794',
};

// ============================================================
// GOOGLE SHEETS API
// ============================================================
let sheetsApi = null;

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
let waClient = null;
let waReady = false;
let latestQR = null;
let latestQRDataUrl = null;

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
let browser = null;

async function htmlToImage(html, width, height) {
  if (!browser) {
    browser = await puppeteer.launch({
      headless: 'new',
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
    });
  }
  var page = await browser.newPage();
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
// LEDGER DATA PROCESSING
// ============================================================
async function getLedgerData(dateStr) {
  var rows = await readSheet('Ledger!A:L');
  var targetDate = dateStr || new Date().toISOString().split('T')[0];
  var entries = [];

  for (var i = 0; i < rows.length; i++) {
    var row = rows[i];
    if (!row[0] || !row[5]) continue; // skip empty rows
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

function parseSheetDate(val) {
  if (!val) return null;
  if (val instanceof Date) return val;
  var str = val.toString().trim();
  // Handle DD/MM/YYYY or DD.MM.YY
  var parts = str.split(/[\/\.\-]/);
  if (parts.length === 3) {
    var d = parseInt(parts[0]);
    var m = parseInt(parts[1]);
    var y = parseInt(parts[2]);
    if (y < 100) y += 2000;
    if (d > 0 && d <= 31 && m > 0 && m <= 12) return new Date(y, m - 1, d);
  }
  // Try native parse
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

  // Yes patterns
  if (lower === 'yes' || lower === 'ok' || lower === 'approved' || lower === 'done' ||
      lower === 'go ahead' || lower === 'proceed' || lower === 'haan' || lower === 'ha' ||
      lower === 'theek hai' || lower === 'thik hai' || lower === 'kar do' ||
      lower === 'y' || lower === 'han' || lower === 'okay') return 'yes';

  // Emoji patterns
  if (lower.includes('\u{1F44D}') || lower.includes('\u2705')) return 'yes';
  if (lower.includes('\u274C}') || lower.includes('\u{1F44E}')) return 'no';

  // No patterns
  if (lower === 'no' || lower === 'nahi' || lower === 'nah' || lower === 'rejected' ||
      lower === 'cancel' || lower === 'mat karo' || lower === 'n' || lower === 'nope') return 'no';

  // Hold patterns
  if (lower === 'hold' || lower === 'wait' || lower === 'ruko' || lower === 'later' ||
      lower === 'baad mein' || lower === 'not now' || lower === 'pending' ||
      lower.includes('hold') || lower.includes('wait') || lower.includes('ruk')) return 'hold';

  return 'other';
}

function parseExpenseMessage(body) {
  if (!body) return { vendor: '', amount: 0 };

  // Try to extract amount (look for numbers with L/Lac/Lakh/Cr/Rs patterns)
  var amountMatch = body.match(/(?:rs\.?\s*|inr\s*|amount\s*:?\s*)?(\d[\d,]*\.?\d*)\s*(?:lac|lakh|lacs|l\b|cr|crore)/i);
  var amount = 0;
  if (amountMatch) {
    amount = parseFloat(amountMatch[1].replace(/,/g, ''));
    if (/cr|crore/i.test(amountMatch[0])) amount *= 10000000;
    else if (/lac|lakh|lacs|l\b/i.test(amountMatch[0])) amount *= 100000;
  } else {
    // Try plain number with Rs
    var rsMatch = body.match(/(?:rs\.?\s*|inr\s*)(\d[\d,]*\.?\d*)/i);
    if (rsMatch) amount = parseFloat(rsMatch[1].replace(/,/g, ''));
  }

  // Vendor is harder — use the first line or the whole body
  var lines = body.split('\n');
  var vendor = lines[0].substring(0, 100);

  return { vendor: vendor, amount: amount };
}

async function fetchApprovalMessages(days) {
  if (!waReady || !waClient) throw new Error('WhatsApp not connected');
  if (!CONFIG.APPROVAL_GROUP_JID) throw new Error('APPROVAL_GROUP_JID not set. Use /api/groups to find it.');

  var chat = await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);
  var messages = await chat.fetchMessages({ limit: 500 });

  var cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - (days || 15));

  return messages.filter(function(msg) {
    return new Date(msg.timestamp * 1000) >= cutoff;
  });
}

async function buildApprovalAudit(days) {
  var messages = await fetchApprovalMessages(days || 15);
  var expenses = [];
  var replyMap = {};

  for (var i = 0; i < messages.length; i++) {
    var msg = messages[i];
    var sender = (msg.author || msg.from || '').replace('@c.us', '').replace('@s.whatsapp.net', '');
    var msgDate = new Date(msg.timestamp * 1000);
    var body = (msg.body || '').trim();

    var quotedMsgId = null;
    if (msg.hasQuotedMsg) {
      try {
        var quoted = await msg.getQuotedMessage();
        quotedMsgId = quoted.id._serialized || quoted.id.id;
      } catch (e) { /* ignore */ }
    }

    if (quotedMsgId) {
      if (!replyMap[quotedMsgId]) replyMap[quotedMsgId] = { mm: null, sm: null };
      var response = parseResponse(body);

      if (sender === CONFIG.MM_PHONE || sender.endsWith(CONFIG.MM_PHONE.slice(-10))) {
        replyMap[quotedMsgId].mm = { response: response, date: msgDate, raw: body };
      }
      if (sender === CONFIG.SM_PHONE || sender.endsWith(CONFIG.SM_PHONE.slice(-10))) {
        replyMap[quotedMsgId].sm = { response: response, date: msgDate, raw: body };
      }
    } else {
      var msgId = msg.id._serialized || msg.id.id;
      var parsed = parseExpenseMessage(body);
      expenses.push({
        id: msgId,
        date: msgDate,
        body: body,
        sender: sender,
        vendor: parsed.vendor,
        amount: parsed.amount,
        mmApproval: null,
        smApproval: null,
        status: { mm: 'pending', sm: 'pending' },
      });
    }
  }

  // Match replies to expenses
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
    whatsapp: waReady ? 'connected' : 'disconnected',
    sheets: sheetsApi ? 'initialized' : 'not configured',
    botEnabled: CONFIG.BOT_ENABLED,
    approvalGroup: CONFIG.APPROVAL_GROUP_JID ? 'configured' : 'not set',
  });
});

app.get('/api/pair', function(req, res) {
  if (waReady) return res.send('<h1>Already connected to WhatsApp</h1>');
  if (!latestQRDataUrl) return res.send('<h1>No QR code yet. Wait a moment and refresh.</h1>');
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
  res.json({ botEnabled: true });
});

app.get('/api/bot/off', function(req, res) {
  CONFIG.BOT_ENABLED = false;
  res.json({ botEnabled: false });
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
    res.json({ date: date, entries: entries, totalIn: totalIn, totalOut: totalOut, net: totalIn - totalOut });
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

    var summary = {
      period: days + ' days',
      totalExpenses: audit.totalExpenses,
      fullyApproved: audit.fullyApproved.length,
      partialApproval: audit.partialApproval.length,
      noApproval: audit.noApproval.length,
      onHold: audit.onHold.length,
      rejected: audit.rejected.length,
    };

    var formatExpense = function(e) {
      return {
        date: e.date.toISOString().split('T')[0],
        message: e.body.substring(0, 200),
        vendor: e.vendor,
        amount: e.amount,
        mm: e.status.mm,
        sm: e.status.sm,
        mmRaw: e.mmApproval ? e.mmApproval.raw : null,
        smRaw: e.smApproval ? e.smApproval.raw : null,
      };
    };

    res.json({
      summary: summary,
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

  // Group outflows by tag
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
  html += '.gn{color:#0a7}';
  html += '.rd{color:#c33}';
  html += '.bl{color:#36a}';
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

  // Inflows table
  if (data.inflows.length > 0) {
    html += '<div class="sec">INFLOWS</div><table>';
    html += '<tr><th>Description</th><th>Entity</th><th>Tag</th><th>Bank A/C</th><th style="text-align:right">Amount</th></tr>';
    data.inflows.forEach(function(e) {
      html += '<tr><td>' + e.description + '</td><td>' + e.entity + '</td><td>' + e.tag + '</td><td>' + e.bankAC + '</td><td class="amt gn">' + formatINR(e.amount) + '</td></tr>';
    });
    html += '</table>';
  }

  // Outflows by tag
  html += '<div class="sec">OUTFLOWS BY CATEGORY</div><table>';
  html += '<tr><th>Category</th><th>Items</th><th style="text-align:right">Amount</th></tr>';
  var tags = Object.keys(data.byTag).sort(function(a, b) { return data.byTag[b].total - data.byTag[a].total; });
  tags.forEach(function(tag) {
    html += '<tr><td>' + tag + '</td><td>' + data.byTag[tag].items.length + '</td><td class="amt rd">' + formatINR(data.byTag[tag].total) + '</td></tr>';
  });
  html += '</table>';

  // Fund Position
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

    // Send to WhatsApp group
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
  res.json({ botEnabled: CONFIG.BOT_ENABLED, whatsapp: waReady });
});

// ============================================================
// CRON SCHEDULE (IST)
// ============================================================
// 7PM IST = 1:30 PM UTC
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

// 9AM IST = 3:30 AM UTC (morning report for yesterday)
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
  console.log('Fidato MIS Report Server v2.0');
  console.log('========================================');
  console.log('Port: ' + CONFIG.PORT);
  console.log('Sheet: ' + CONFIG.SHEET_ID);
  console.log('Day Book Group: ' + CONFIG.WHATSAPP_GROUP_JID);
  console.log('Approval Group: ' + (CONFIG.APPROVAL_GROUP_JID || 'NOT SET'));
  console.log('MM Phone: ' + CONFIG.MM_PHONE);
  console.log('SM Phone: ' + CONFIG.SM_PHONE);
  console.log('\nEndpoints:');
  console.log('  GET /health');
  console.log('  GET /api/pair');
  console.log('  GET /api/groups');
  console.log('  GET /api/wa-status');
  console.log('  GET /api/bot/on | /api/bot/off');
  console.log('  GET /api/ledger?date=2026-04-30');
  console.log('  GET /api/fund-position');
  console.log('  GET /api/preview?date=2026-04-30');
  console.log('  GET /api/preview-image?date=2026-04-30');
  console.log('  GET /api/daily-report?date=2026-04-30');
  console.log('  GET /api/approval-audit?days=15');
  console.log('  GET /api/test-send');
  console.log('\nSchedule (IST): 7PM daily report, 9AM morning summary');
  console.log('========================================\n');
});
