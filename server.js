// ============================================================
// FIDATO MIS DAILY REPORT SERVER
// Reads Google Sheet → Generates visual report → Sends via WhatsApp
// ============================================================

const express = require('express');
const { google } = require('googleapis');
const fetch = require('node-fetch');
const cron = require('node-cron');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const fs = require('fs');
const app = express();
app.use(express.json());

// ============================================================
// CONFIGURATION (set these as Railway environment variables)
// ============================================================
const CONFIG = {
  // Google Sheets
  SHEET_ID: process.env.SHEET_ID || '1h_62f7kQB1i8_YOWTHKjcbnz4piMr5tYoKJJpKKqwLM',
  GOOGLE_CREDENTIALS: process.env.GOOGLE_CREDENTIALS, // JSON string of service account
  
  // WhatsApp via whatsapp-web.js
  WHATSAPP_GROUP_JID: process.env.WHATSAPP_GROUP_JID, // e.g. '120363xxxxx@g.us'
  
  // Bot on/off toggle — set to 'false' in Railway to pause all sending
  BOT_ENABLED: process.env.BOT_ENABLED !== 'false', // defaults to true
  
  // HCTI (HTML to Image)
  HCTI_USER_ID: process.env.HCTI_USER_ID,
  HCTI_API_KEY: process.env.HCTI_API_KEY,
  
  // Anthropic (for Claude-generated summaries if needed)
  CLAUDE_API_KEY: process.env.CLAUDE_API_KEY,
  
  PORT: process.env.PORT || 3000,
};

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
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
        '--no-zygote',
        '--single-process',
        '--disable-gpu',
        '--disable-extensions',
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
    
    if (!CONFIG.WHATSAPP_GROUP_JID) {
      console.log('No WHATSAPP_GROUP_JID set. Use /api/groups to find your group JID.');
    }
  });

  waClient.on('authenticated', function() {
    console.log('WhatsApp authenticated successfully.');
  });

  waClient.on('auth_failure', function(msg) {
    console.error('WhatsApp auth failure:', msg);
    waReady = false;
  });

  waClient.on('disconnected', function(reason) {
    console.log('WhatsApp disconnected:', reason);
    waReady = false;
    // Auto-reconnect after 10 seconds
    setTimeout(function() {
      console.log('Attempting to reconnect WhatsApp...');
      waClient.initialize().catch(function(err) {
        console.error('Reconnect failed:', err.message);
      });
    }, 10000);
  });

  return waClient;
}

// Do NOT auto-connect on boot — wait for user to visit /api/pair
console.log('WhatsApp: Ready to pair. Visit /api/pair when ready.');

// ============================================================
// GOOGLE SHEETS AUTH
// ============================================================
let sheetsClient = null;

async function getSheets() {
  if (sheetsClient) return sheetsClient;
  
  if (!CONFIG.GOOGLE_CREDENTIALS) {
    throw new Error('GOOGLE_CREDENTIALS not set. Add service account JSON as env variable.');
  }
  
  const credentials = JSON.parse(CONFIG.GOOGLE_CREDENTIALS);
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });
  
  sheetsClient = google.sheets({ version: 'v4', auth });
  return sheetsClient;
}

// ============================================================
// READ LEDGER DATA FOR A SPECIFIC DATE
// ============================================================
async function getLedgerData(dateStr) {
  const sheets = await getSheets();
  
  // Read all Ledger data (A5:K500)
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: CONFIG.SHEET_ID,
    range: 'Ledger!A5:K500',
    valueRenderOption: 'UNFORMATTED_VALUE',
    dateTimeRenderOption: 'SERIAL_NUMBER',
  });
  
  const rows = res.data.values || [];
  
  // Convert target date to serial number for comparison
  // Google Sheets serial: Jan 1, 1900 = 1 (with the Lotus bug: Feb 29 1900 exists)
  const targetDate = new Date(dateStr);
  const targetSerial = dateToSerial(targetDate);
  
  // Filter transactions for the target date
  const transactions = [];
  for (const row of rows) {
    if (!row[0] || typeof row[0] !== 'number') continue; // Skip non-date rows (separators, summaries)
    
    const rowSerial = Math.floor(row[0]);
    if (rowSerial !== targetSerial) continue;
    
    transactions.push({
      date: serialToDate(rowSerial),
      entity: row[1] || '',
      head: row[2] || '',
      description: row[3] || '',
      tag: row[4] || '',
      inOut: row[5] || '',
      amount: row[6] || 0,
      mode: row[7] || '',
      person: row[8] || '',
      bank: row[9] || '',
      notes: row[10] || '',
    });
  }
  
  return transactions;
}

// Date ↔ Serial conversion (Google Sheets epoch)
function dateToSerial(date) {
  const epoch = new Date(1899, 11, 30); // Dec 30, 1899 (Lotus bug offset)
  const diff = date.getTime() - epoch.getTime();
  return Math.floor(diff / 86400000);
}

function serialToDate(serial) {
  const epoch = new Date(1899, 11, 30);
  return new Date(epoch.getTime() + serial * 86400000);
}

// ============================================================
// READ FUND POSITION DATA
// ============================================================
async function getFundPositionData() {
  const sheets = await getSheets();
  
  // Read main table (rows 5-22, cols A-J)
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: CONFIG.SHEET_ID,
    range: "'Fund Position'!A5:J22",
    valueRenderOption: 'FORMATTED_VALUE',
  });
  
  const rows = res.data.values || [];
  const accounts = [];
  
  for (const row of rows) {
    accounts.push({
      company: row[1] || '',
      bankAC: row[2] || '',
      opening: row[3] || '₹0',
      todayIn: row[4] || '₹0',
      todayOut: row[5] || '₹0',
      closing: row[6] || '₹0',
      chqIssued: row[7] || '₹0',
      netBal: row[8] || '₹0',
      status: row[9] || '',
    });
  }
  
  // Read totals
  const totRes = await sheets.spreadsheets.values.get({
    spreadsheetId: CONFIG.SHEET_ID,
    range: "'Fund Position'!D23:I25",
    valueRenderOption: 'FORMATTED_VALUE',
  });
  
  return { accounts, totals: totRes.data.values || [] };
}

// ============================================================
// COMPUTE DAILY SUMMARY FROM TRANSACTIONS
// ============================================================
function computeSummary(transactions) {
  let totalIn = 0, totalOut = 0;
  const inflowByEntity = {};
  const inflowByHead = {};
  const outflowByHead = {};
  const outflowByEntity = {};
  const promoterDraws = { MM: 0, SM: 0 };
  
  for (const t of transactions) {
    const amt = Number(t.amount) || 0;
    
    if (t.inOut === 'IN') {
      totalIn += amt;
      inflowByEntity[t.entity] = (inflowByEntity[t.entity] || 0) + amt;
      inflowByHead[t.head] = (inflowByHead[t.head] || 0) + amt;
    } else if (t.inOut === 'OUT') {
      totalOut += amt;
      outflowByHead[t.head] = (outflowByHead[t.head] || 0) + amt;
      outflowByEntity[t.entity] = (outflowByEntity[t.entity] || 0) + amt;
      
      if (t.person === 'MM') promoterDraws.MM += amt;
      if (t.person === 'SM') promoterDraws.SM += amt;
    }
  }
  
  return {
    totalIn, totalOut,
    netCash: totalIn - totalOut,
    txnCount: transactions.length,
    inflowByEntity, inflowByHead,
    outflowByHead, outflowByEntity,
    promoterDraws,
  };
}

// ============================================================
// FORMAT AMOUNT IN LAKHS
// ============================================================
function toLakhs(amt) {
  const lakhs = Math.abs(amt) / 100000;
  const sign = amt < 0 ? '-' : '';
  if (lakhs >= 100) return sign + '₹' + lakhs.toFixed(1) + 'L';
  if (lakhs >= 10) return sign + '₹' + lakhs.toFixed(1) + 'L';
  return sign + '₹' + lakhs.toFixed(2) + 'L';
}

function toINR(amt) {
  return '₹' + Math.abs(amt).toLocaleString('en-IN');
}

// ============================================================
// GENERATE HTML REPORT (Card 1: Daily MIS)
// ============================================================
function generateDailyReportHTML(transactions, summary, dateStr) {
  const dateObj = new Date(dateStr);
  const dateFormatted = dateObj.toLocaleDateString('en-GB', { 
    day: '2-digit', month: 'long', year: 'numeric', weekday: 'long' 
  });
  
  // Build bar rows for inflow
  const inflowBars = Object.entries(summary.inflowByHead)
    .sort((a, b) => b[1] - a[1])
    .map(([head, amt]) => {
      const pct = Math.round((amt / summary.totalIn) * 100);
      return `<div class="br"><span class="nm">${head}</span><span class="tk"><span class="fl g" style="width:${pct}%"></span></span><span class="am">${toLakhs(amt)}</span></div>`;
    }).join('');
  
  // Build bar rows for outflow
  const outflowBars = Object.entries(summary.outflowByHead)
    .sort((a, b) => b[1] - a[1])
    .map(([head, amt]) => {
      const pct = Math.round((amt / summary.totalOut) * 100);
      return `<div class="br"><span class="nm">${head}</span><span class="tk"><span class="fl r" style="width:${pct}%"></span></span><span class="am">${toLakhs(amt)}</span></div>`;
    }).join('');
  
  // Build transaction rows
  const txnRows = transactions.map((t, i) => {
    const isIN = t.inOut === 'IN';
    const color = isIN ? 'gn' : 'rd';
    const details = [t.tag, t.mode, t.person !== '—' ? t.person : '', t.bank].filter(x => x && x !== '—').join(' · ');
    return `<div class="txr">
      <span class="en">${t.entity}</span><span class="hd">${t.head}</span>
      <span class="ds" style="grid-column:span 2">${t.description}${details ? '<br><span class="txd">' + details + '</span>' : ''}</span>
      <span class="io ${color}">${t.inOut}</span><span class="av ${color}">${toLakhs(t.amount)}</span>
    </div>`;
  }).join('');
  
  // Split IN and OUT transactions
  const inTxns = transactions.filter(t => t.inOut === 'IN');
  const outTxns = transactions.filter(t => t.inOut === 'OUT');
  
  const inRows = inTxns.map(t => {
    const details = [t.tag, t.mode, t.person !== '—' ? t.person : '', t.bank].filter(x => x && x !== '—').join(' · ');
    return `<div class="txr"><span class="en">${t.entity}</span><span class="hd">${t.head}</span>
      <span class="ds" style="grid-column:span 2">${t.description}${details ? '<br><span class="txd">' + details + '</span>' : ''}</span>
      <span class="io gn">IN</span><span class="av gn">${toLakhs(t.amount)}</span></div>`;
  }).join('');
  
  const outRows = outTxns.map(t => {
    const details = [t.tag, t.mode, t.person !== '—' ? t.person : '', t.bank].filter(x => x && x !== '—').join(' · ');
    return `<div class="txr"><span class="en">${t.entity}</span><span class="hd">${t.head}</span>
      <span class="ds" style="grid-column:span 2">${t.description}${details ? '<br><span class="txd">' + details + '</span>' : ''}</span>
      <span class="io rd">OUT</span><span class="av rd">${toLakhs(t.amount)}</span></div>`;
  }).join('');
  
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f4}
.rpt{max-width:600px;margin:0 auto;background:#fff;overflow:hidden}
.hdr{background:#1C1917;color:#fff;padding:20px 24px 16px}
.hdr h1{font-size:14px;font-weight:500;letter-spacing:0.5px;opacity:0.6;margin-bottom:2px}
.hdr .date{font-size:20px;font-weight:500}
.hdr .sub{font-size:11px;opacity:0.4;margin-top:3px}
.pill{display:inline-block;background:rgba(255,255,255,0.12);padding:3px 10px;border-radius:20px;font-size:11px;margin-top:8px;color:rgba(255,255,255,0.75)}
.sum3{display:grid;grid-template-columns:1fr 1fr 1fr;border-bottom:1px solid #e5e5e5}
.sum3 .c{padding:14px 16px;text-align:center}
.sum3 .c:not(:last-child){border-right:1px solid #e5e5e5}
.sum3 .lb{font-size:10px;color:#888;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px}
.sum3 .vl{font-size:20px;font-weight:600}
.gn{color:#16A34A}.rd{color:#DC2626}
.sec{padding:14px 20px;border-bottom:1px solid #e5e5e5}
.st{font-size:10px;font-weight:600;color:#888;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:10px}
.br{display:flex;align-items:center;gap:6px;margin-bottom:6px}
.br .nm{font-size:12px;color:#333;width:100px;flex-shrink:0}
.br .tk{flex:1;height:16px;background:#f0f0f0;border-radius:3px;overflow:hidden}
.br .fl{height:100%;border-radius:3px}
.br .fl.g{background:#16A34A}.br .fl.r{background:#DC2626}
.br .am{font-size:11px;font-weight:600;color:#666;width:70px;text-align:right;flex-shrink:0}
.pm{display:grid;grid-template-columns:1fr 1fr;gap:6px}
.pc{padding:8px 12px;border-radius:6px;background:#f5f5f4}
.pc .w{font-size:11px;font-weight:500;color:#888;margin-bottom:2px}
.pc .pv{font-size:16px;font-weight:600;color:#DC2626}
.txhdr{background:#292524;padding:8px 20px;display:grid;grid-template-columns:72px 80px 90px 1fr 56px 70px;gap:4px;font-size:10px;font-weight:600;color:#A8A29E;text-transform:uppercase;letter-spacing:0.3px}
.txr{padding:7px 20px;display:grid;grid-template-columns:72px 80px 90px 1fr 56px 70px;gap:4px;font-size:12px;align-items:center;border-bottom:1px solid #f0f0f0}
.txr:nth-child(even){background:#fafafa}
.txr .en{color:#888;font-size:11px}.txr .hd{font-weight:600;color:#333;font-size:11px}
.txr .ds{color:#666;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.txr .io{font-weight:600;font-size:11px}.txr .av{font-weight:600;text-align:right;font-size:12px}
.txd{font-size:9px;color:#aaa;margin-top:1px}
.ft{background:#f5f5f4;padding:10px 20px;text-align:center;font-size:10px;color:#aaa}
</style></head><body>
<div class="rpt">
  <div class="hdr">
    <h1>FIDATO GROUP — DAILY MIS</h1>
    <div class="date">${dateFormatted}</div>
    <div class="sub">Auto-generated from Ledger at 7:00 PM IST</div>
    <div class="pill">${summary.txnCount} transactions</div>
  </div>
  <div class="sum3">
    <div class="c"><div class="lb">Inflow</div><div class="vl gn">${toLakhs(summary.totalIn)}</div></div>
    <div class="c"><div class="lb">Outflow</div><div class="vl rd">${toLakhs(summary.totalOut)}</div></div>
    <div class="c"><div class="lb">Net cash</div><div class="vl">${summary.netCash >= 0 ? '+' : ''}${toLakhs(summary.netCash)}</div></div>
  </div>
  <div class="sec"><div class="st">Inflow breakdown</div>${inflowBars}</div>
  <div class="sec"><div class="st">Outflow by head</div>${outflowBars}</div>
  <div class="sec"><div class="st">Promoter draws today</div>
    <div class="pm">
      <div class="pc"><div class="w">MM (Mukund)</div><div class="pv">${toLakhs(summary.promoterDraws.MM)}</div></div>
      <div class="pc"><div class="w">SM</div><div class="pv">${toLakhs(summary.promoterDraws.SM)}</div></div>
    </div>
  </div>
  <div style="padding:14px 20px 6px;border-bottom:1px solid #e5e5e5"><div class="st" style="margin-bottom:0">All ${summary.txnCount} transactions</div></div>
  <div class="txhdr"><span>Entity</span><span>Head</span><span>Description</span><span></span><span>IN/OUT</span><span style="text-align:right">Amount</span></div>
  ${inRows}
  <div style="background:#292524;padding:4px 20px;font-size:10px;color:#4ADE80;font-weight:600;text-align:right">TOTAL INFLOW: ${toINR(summary.totalIn)}</div>
  ${outRows}
  <div style="background:#292524;padding:4px 20px;font-size:10px;color:#F87171;font-weight:600;text-align:right">TOTAL OUTFLOW: ${toINR(summary.totalOut)}</div>
  <div style="background:#1C1917;padding:10px 20px;display:flex;justify-content:space-between;font-size:13px;font-weight:600;color:#fff">
    <span>NET CASH FLOW</span><span style="color:#4ADE80">${summary.netCash >= 0 ? '+' : ''} ${toINR(summary.netCash)}</span>
  </div>
  <div class="ft">Fidato Group — MIS Tracker · Generated by Claude AI · ${dateFormatted}, 7:00 PM IST</div>
</div>
</body></html>`;
}

// ============================================================
// RENDER HTML → IMAGE (using local Puppeteer from whatsapp-web.js)
// Since we already have Chromium running, we reuse it for screenshots
// ============================================================
async function renderToJPEG(html, width = 600) {
  try {
    var puppeteer = require('puppeteer');
    var browser = await puppeteer.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--single-process',
      ],
    });
    
    var page = await browser.newPage();
    await page.setViewport({ width: width, height: 800, deviceScaleFactor: 2 });
    await page.setContent(html, { waitUntil: 'networkidle0', timeout: 15000 });
    
    // Wait a bit for fonts to load
    await new Promise(function(r) { setTimeout(r, 500); });
    
    // Get the full page height
    var bodyHeight = await page.evaluate(function() {
      return document.body.scrollHeight;
    });
    
    await page.setViewport({ width: width, height: bodyHeight, deviceScaleFactor: 2 });
    
    var buffer = await page.screenshot({ 
      type: 'jpeg', 
      quality: 90,
      fullPage: true,
    });
    
    await page.close();
    await browser.close();
    
    console.log('Image rendered via local Puppeteer: ' + (buffer.length / 1024).toFixed(1) + ' KB');
    return buffer;
    
  } catch (err) {
    console.error('Puppeteer render failed:', err.message);
    
    // Store the HTML for preview fallback
    global._lastReportHTML = html;
    global._lastReportDate = new Date().toISOString();
    
    return null;
  }
}

// ============================================================
// SEND IMAGE VIA WHATSAPP-WEB.JS
// ============================================================
async function sendWhatsAppImage(imageBuffer, caption, destination) {
  if (!waReady || !waClient) {
    console.log('WhatsApp not connected. Skipping send.');
    return { success: false, error: 'WhatsApp not connected' };
  }
  
  var groupJid = destination || CONFIG.WHATSAPP_GROUP_JID;
  if (!groupJid) {
    console.log('No group JID set. Use /api/groups to find it.');
    return { success: false, error: 'No group JID configured' };
  }
  
  // If no image buffer (HCTI not configured), send the preview link as text
  if (!imageBuffer) {
    var previewUrl = 'https://mistracker-production.up.railway.app/api/preview';
    var msg = caption + '\n\nView full report: ' + previewUrl;
    await sendTextMessage(msg);
    return { success: true, method: 'text_link' };
  }
  
  try {
    var media = new MessageMedia('image/jpeg', imageBuffer.toString('base64'), 'daily-report.jpg');
    await waClient.sendMessage(groupJid, media, { caption: caption || 'Fidato Group — Daily MIS Report' });
    console.log('Image sent to group:', groupJid);
    return { success: true, method: 'wwjs_image' };
  } catch (err) {
    console.error('Failed to send image:', err.message);
    // Fallback to text
    try {
      var previewUrl2 = 'https://mistracker-production.up.railway.app/api/preview';
      var msg2 = caption + '\n\nView full report: ' + previewUrl2;
      await waClient.sendMessage(groupJid, msg2);
      return { success: true, method: 'text_fallback' };
    } catch (err2) {
      return { success: false, error: err.message };
    }
  }
}

// ============================================================
// MAIN: GENERATE AND SEND DAILY REPORT
// ============================================================
async function generateAndSendDailyReport(dateStr) {
  const startTime = Date.now();
  console.log(`\n=== Generating Daily Report for ${dateStr} ===`);
  
  try {
    // 1. Read Ledger data
    const transactions = await getLedgerData(dateStr);
    console.log(`Found ${transactions.length} transactions`);
    
    if (transactions.length === 0) {
      console.log('No transactions found for this date. Skipping report.');
      return { success: false, reason: 'No transactions' };
    }
    
    // 2. Compute summary
    const summary = computeSummary(transactions);
    console.log(`IN: ${toLakhs(summary.totalIn)}, OUT: ${toLakhs(summary.totalOut)}, NET: ${toLakhs(summary.netCash)}`);
    
    // 3. Generate HTML
    const html = generateDailyReportHTML(transactions, summary, dateStr);
    
    // 4. Render to JPEG
    const imageBuffer = await renderToJPEG(html);
    if (imageBuffer) {
      console.log('Image rendered: ' + (imageBuffer.length / 1024).toFixed(1) + ' KB');
    } else {
      console.log('Image rendering skipped — will send as text link');
    }
    
    // 5. Send via WhatsApp
    const caption = `📊 Fidato MIS — ${new Date(dateStr).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' })}\n` +
      `Inflow: ${toLakhs(summary.totalIn)} | Outflow: ${toLakhs(summary.totalOut)} | Net: ${toLakhs(summary.netCash)}\n` +
      `${summary.txnCount} transactions`;
    
    const result = await sendWhatsAppImage(imageBuffer, caption);
    
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`Report generated and sent in ${elapsed}s`);
    
    return { 
      success: true, 
      summary: { in: summary.totalIn, out: summary.totalOut, net: summary.netCash, txns: summary.txnCount },
      elapsed: elapsed + 's',
    };
    
  } catch (err) {
    console.error('Report generation failed:', err.message);
    return { success: false, error: err.message };
  }
}

// ============================================================
// API ENDPOINTS
// ============================================================

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'Fidato MIS Daily Report',
    sheetsConnected: !!CONFIG.GOOGLE_CREDENTIALS,
    whatsappConnected: waReady,
    whatsappGroupJid: CONFIG.WHATSAPP_GROUP_JID || 'NOT SET — use /api/groups',
    hctiConfigured: !!(CONFIG.HCTI_USER_ID && CONFIG.HCTI_API_KEY),
    sheetId: CONFIG.SHEET_ID,
  });
});

// Pairing endpoint — shows QR code to scan
app.get('/api/pair', async (req, res) => {
  if (waReady) {
    return res.send('<html><body style="font-family:sans-serif;text-align:center;padding:40px"><h1 style="color:green">WhatsApp Connected!</h1><p>The bot is already linked and ready to send messages.</p><p><a href="/api/groups">View Groups</a> | <a href="/health">Health Check</a></p></body></html>');
  }
  
  try {
    // Initialize client if not started
    if (!waClient) {
      createWhatsAppClient();
      waClient.initialize().catch(function(err) {
        console.error('WhatsApp init error:', err.message);
      });
    }
    
    // Wait for QR to generate
    var attempts = 0;
    while (!latestQRDataUrl && !waReady && attempts < 20) {
      await new Promise(function(r) { setTimeout(r, 1000); });
      attempts++;
    }
    
    if (waReady) {
      return res.send('<html><body style="font-family:sans-serif;text-align:center;padding:40px"><h1 style="color:green">WhatsApp Connected!</h1><p>Successfully linked.</p><p><a href="/api/groups">View Groups</a></p></body></html>');
    }
    
    if (!latestQRDataUrl) {
      return res.send('<html><body style="font-family:sans-serif;text-align:center;padding:40px"><h1>Waiting for QR...</h1><p>Refresh this page in 5 seconds.</p><script>setTimeout(function(){location.reload()},5000)</script></body></html>');
    }
    
    res.send('<html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Fidato MIS - WhatsApp Pairing</title></head><body style="font-family:sans-serif;text-align:center;padding:20px;background:#f5f5f4">' +
      '<div style="max-width:400px;margin:0 auto;background:white;border-radius:12px;padding:30px;box-shadow:0 2px 10px rgba(0,0,0,0.1)">' +
      '<h2 style="margin:0 0 5px;color:#1C1917">Fidato MIS Bot</h2>' +
      '<p style="color:#888;margin:0 0 20px">Scan this QR code with WhatsApp</p>' +
      '<img src="' + latestQRDataUrl + '" style="width:260px;height:260px;border:4px solid #e5e5e5;border-radius:8px" />' +
      '<p style="margin:20px 0 5px;font-size:13px;color:#666"><b>Steps:</b></p>' +
      '<p style="font-size:12px;color:#888;line-height:1.6">1. Open WhatsApp on your phone<br>2. Go to Settings → Linked Devices<br>3. Tap "Link a Device"<br>4. Point your camera at this QR code</p>' +
      '<p style="margin-top:20px;font-size:11px;color:#aaa">QR expires in ~60 seconds. <a href="/api/pair" style="color:#16A34A">Refresh for new code</a></p>' +
      '</div>' +
      '<script>setInterval(function(){fetch("/api/wa-status").then(function(r){return r.json()}).then(function(d){if(d.connected){document.body.innerHTML="<div style=\\"text-align:center;padding:60px\\"><h1 style=\\"color:green\\">Connected!</h1><p>WhatsApp linked successfully. <a href=/api/groups>Find your group JID</a></p></div>"}})},3000)</script>' +
      '</body></html>');
      
  } catch (err) {
    console.error('Pairing error:', err.message);
    res.send('<html><body style="font-family:sans-serif;text-align:center;padding:40px"><h1 style="color:red">Error</h1><p>' + err.message + '</p><p><a href="/api/pair">Try Again</a></p></body></html>');
  }
});

// List groups
app.get('/api/groups', async (req, res) => {
  if (!waReady || !waClient) {
    return res.json({ error: 'WhatsApp not connected. Visit /api/pair first.' });
  }
  
  try {
    var chats = await waClient.getChats();
    var groups = chats.filter(function(c) { return c.isGroup; }).map(function(g) {
      return {
        jid: g.id._serialized,
        name: g.name,
        participants: g.participants ? g.participants.length : 0,
      };
    });
    res.json({ groups: groups, hint: 'Set the JID of your MIS group as WHATSAPP_GROUP_JID in Railway variables' });
  } catch (err) {
    res.json({ error: err.message });
  }
});

// WhatsApp status
app.get('/api/wa-status', (req, res) => {
  res.json({
    connected: waReady,
    groupJid: CONFIG.WHATSAPP_GROUP_JID || 'NOT SET',
    authExists: fs.existsSync('./wa_auth'),
  });
});

// Test send — simple text to verify WhatsApp works
app.get('/api/test-send', async (req, res) => {
  if (!waReady || !waClient) {
    return res.json({ error: 'WhatsApp not connected' });
  }
  var jid = CONFIG.WHATSAPP_GROUP_JID;
  if (!jid) {
    return res.json({ error: 'No group JID set' });
  }
  try {
    var msg = req.query.msg || 'MIS Bot test — this message will be deleted';
    var sent = await waClient.sendMessage(jid, msg);
    res.json({ success: true, messageId: sent.id._serialized, jid: jid, hint: 'Delete from WhatsApp: long press > delete for everyone' });
  } catch (err) {
    res.json({ success: false, error: err.message, stack: err.stack ? err.stack.substring(0, 500) : '' });
  }
});

// Manually trigger daily report for a specific date
app.get('/api/daily-report', async (req, res) => {
  const dateStr = req.query.date || new Date().toISOString().split('T')[0];
  const result = await generateAndSendDailyReport(dateStr);
  res.json(result);
});

// Preview report HTML (for testing without sending)
app.get('/api/preview', async (req, res) => {
  const dateStr = req.query.date || new Date().toISOString().split('T')[0];
  
  try {
    const transactions = await getLedgerData(dateStr);
    if (transactions.length === 0) {
      return res.send('<h1>No transactions found for ' + dateStr + '</h1>');
    }
    const summary = computeSummary(transactions);
    const html = generateDailyReportHTML(transactions, summary, dateStr);
    res.setHeader('Content-Type', 'text/html');
    res.send(html);
  } catch (err) {
    res.status(500).send('Error: ' + err.message);
  }
});

// Preview as JPEG image
app.get('/api/preview-image', async (req, res) => {
  const dateStr = req.query.date || new Date().toISOString().split('T')[0];
  
  try {
    const transactions = await getLedgerData(dateStr);
    if (transactions.length === 0) {
      return res.status(404).json({ error: 'No transactions for ' + dateStr });
    }
    const summary = computeSummary(transactions);
    const html = generateDailyReportHTML(transactions, summary, dateStr);
    const imageBuffer = await renderToJPEG(html);
    
    if (!imageBuffer) {
      // No HCTI configured - redirect to HTML preview
      res.setHeader('Content-Type', 'text/html');
      return res.send(html);
    }
    
    res.setHeader('Content-Type', 'image/jpeg');
    res.setHeader('Content-Disposition', `inline; filename="fidato-mis-${dateStr}.jpg"`);
    res.send(imageBuffer);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Read raw Ledger data for a date
app.get('/api/ledger', async (req, res) => {
  const dateStr = req.query.date || new Date().toISOString().split('T')[0];
  try {
    const transactions = await getLedgerData(dateStr);
    const summary = computeSummary(transactions);
    res.json({ date: dateStr, transactions, summary });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Fund Position endpoint
app.get('/api/fund-position', async (req, res) => {
  try {
    const data = await getFundPositionData();
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// (Gupshup webhook removed — now using Baileys for WhatsApp)

// ============================================================
// SMART REMINDER SYSTEM
// Tracks whether today's MIS has been posted.
// Schedule: 7 PM → 8 PM → 9 PM → 12 AM → 9 AM next day
// ============================================================
const reportStatus = {};

function getTodayIST() {
  const d = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
  return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
}

function getYesterdayIST() {
  const d = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
  d.setDate(d.getDate() - 1);
  return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
}

function formatDateNice(dateStr) {
  const d = new Date(dateStr + 'T00:00:00');
  return d.toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
}

async function sendTextMessage(message) {
  if (!waReady || !waClient || !CONFIG.WHATSAPP_GROUP_JID) {
    console.log('[No WhatsApp] Reminder:', message);
    return;
  }
  try {
    await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID, message);
    console.log('Reminder sent:', message.substring(0, 80));
  } catch (err) {
    console.error('Reminder failed:', err.message);
  }
}

async function checkAndReport(dateStr, checkType) {
  // Check if bot is enabled
  if (!CONFIG.BOT_ENABLED) {
    console.log('Bot is PAUSED (BOT_ENABLED=false). Skipping ' + checkType + ' check.');
    return;
  }
  
  console.log('\n--- ' + checkType + ' check for ' + dateStr + ' ---');
  
  if (reportStatus[dateStr] && reportStatus[dateStr].sent) {
    console.log('Report already sent for ' + dateStr + ' at ' + reportStatus[dateStr].sentAt + '. Skipping.');
    return;
  }
  
  try {
    const transactions = await getLedgerData(dateStr);
    
    if (transactions.length > 0) {
      console.log('Found ' + transactions.length + ' transactions. Generating report...');
      const result = await generateAndSendDailyReport(dateStr);
      if (result.success) {
        reportStatus[dateStr] = {
          sent: true,
          txnCount: transactions.length,
          sentAt: new Date().toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit' }),
        };
      }
    } else {
      var dateNice = formatDateNice(dateStr);
      var msgs = {
        '7PM': 'MIS Reminder\n\nNo entries found in the Ledger for ' + dateNice + '.\n\nPlease update today\'s transactions in the MIS Tracker sheet.\nNext check: 8:00 PM',
        '8PM': 'MIS Still Pending\n\nThe Ledger for ' + dateNice + ' has not been updated yet.\n\nPlease complete the entries. Next check: 9:00 PM',
        '9PM': 'MIS Overdue\n\nToday\'s MIS (' + dateNice + ') is still not updated.\n\nPlease update urgently. Final check at midnight.',
        'MIDNIGHT': 'MIS Not Updated\n\nThe MIS for ' + dateNice + ' was NOT completed today.\n\nPlease update first thing tomorrow morning.',
        '9AM': 'Good Morning — Yesterday\'s MIS Missing\n\nThe MIS for ' + dateNice + ' was never completed.\n\nPlease update yesterday\'s entries before starting today\'s work.',
      };
      await sendTextMessage(msgs[checkType] || 'MIS for ' + dateNice + ' is pending.');
      console.log('No data for ' + dateStr + '. ' + checkType + ' reminder sent.');
    }
  } catch (err) {
    console.error(checkType + ' check failed:', err.message);
  }
}

// 7:00 PM IST — First check: send report if data exists, else first reminder
cron.schedule('0 19 * * *', function() { checkAndReport(getTodayIST(), '7PM'); }, { timezone: 'Asia/Kolkata' });

// 8:00 PM IST — Second check
cron.schedule('0 20 * * *', function() { checkAndReport(getTodayIST(), '8PM'); }, { timezone: 'Asia/Kolkata' });

// 9:00 PM IST — Third check
cron.schedule('0 21 * * *', function() { checkAndReport(getTodayIST(), '9PM'); }, { timezone: 'Asia/Kolkata' });

// 12:00 AM IST (midnight) — Final check for the day that just ended
cron.schedule('0 0 * * *', function() { checkAndReport(getYesterdayIST(), 'MIDNIGHT'); }, { timezone: 'Asia/Kolkata' });

// 9:00 AM IST — Morning check: if yesterday was never completed
cron.schedule('0 9 * * *', function() {
  var yesterday = getYesterdayIST();
  if (!reportStatus[yesterday] || !reportStatus[yesterday].sent) {
    checkAndReport(yesterday, '9AM');
  } else {
    console.log('9AM check: Yesterday (' + yesterday + ') was already reported.');
  }
}, { timezone: 'Asia/Kolkata' });

// Status endpoint
app.get('/api/report-status', function(req, res) {
  res.json({
    botEnabled: CONFIG.BOT_ENABLED,
    schedule: {
      '7:00 PM': 'First check — send report or first reminder',
      '8:00 PM': 'Second check — send report or gentle nudge',
      '9:00 PM': 'Third check — send report or firm reminder',
      '12:00 AM': 'Midnight — final check for the day',
      '9:00 AM': 'Morning — remind about yesterday if never completed',
    },
    reportHistory: reportStatus,
  });
});

// Toggle bot on/off — visit /api/bot/off to pause, /api/bot/on to resume
app.get('/api/bot/off', function(req, res) {
  CONFIG.BOT_ENABLED = false;
  console.log('Bot PAUSED by user');
  res.json({ botEnabled: false, message: 'Bot paused. No reports or reminders will be sent. Visit /api/bot/on to resume.' });
});

app.get('/api/bot/on', function(req, res) {
  CONFIG.BOT_ENABLED = true;
  console.log('Bot RESUMED by user');
  res.json({ botEnabled: true, message: 'Bot is now active. Reports and reminders will be sent on schedule.' });
});

// ============================================================
// START SERVER
// ============================================================
app.listen(CONFIG.PORT, function() {
  console.log('\nFidato MIS Report Server running on port ' + CONFIG.PORT);
  console.log('Sheet: ' + CONFIG.SHEET_ID);
  console.log('WhatsApp: ' + (waReady ? 'Connected' : 'Not paired — visit /api/pair'));
  console.log('HCTI: ' + (CONFIG.HCTI_USER_ID ? 'Configured' : 'Not configured'));
  console.log('\nDaily Schedule (IST):');
  console.log('   7:00 PM — First check + report or reminder');
  console.log('   8:00 PM — Second check + nudge if pending');
  console.log('   9:00 PM — Third check + firm reminder');
  console.log('  12:00 AM — Midnight final check');
  console.log('   9:00 AM — Morning check for yesterday');
  console.log('\nEndpoints:');
  console.log('  GET /health');
  console.log('  GET /api/pair?phone=919870111582 — One-time WhatsApp pairing');
  console.log('  GET /api/groups — List WhatsApp groups to find JID');
  console.log('  GET /api/wa-status — WhatsApp connection status');
  console.log('  GET /api/daily-report?date=2026-04-05');
  console.log('  GET /api/preview?date=2026-04-05');
  console.log('  GET /api/preview-image?date=2026-04-05');
  console.log('  GET /api/ledger?date=2026-04-05');
  console.log('  GET /api/fund-position');
  console.log('  GET /api/report-status');
});
