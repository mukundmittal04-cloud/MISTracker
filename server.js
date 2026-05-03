// =====================================================================
// FIDATO GROUP MIS TRACKER — server.js (consolidated single-file build)
// =====================================================================
// Daily WhatsApp reports for Fidato Group, generated from Google Sheet
// ledger data and rendered as PNG images via Puppeteer.
//
// Sections (search for the dividers to jump):
//   §1  Imports & config
//   §2  Sheet helpers (auth, parsing, ranges)
//   §3  Data builders (buildFundPosition, buildExpenditure, buildAnalysis)
//   §4  Outlier detection
//   §5  Formatting helpers (₹ Indian format, dates, pills)
//   §6  HTML templates (renderFundPosition, renderExpenditure, renderAnalysis)
//   §7  Puppeteer renderer (HTML → PNG)
//   §8  WhatsApp client + sender
//   §9  Express endpoints + cron
//
// Endpoints:
//   GET  /health
//   GET  /api/wa-status            connection state
//   GET  /api/pair                 QR for first-time pairing
//   POST /api/bot/on               enable scheduled sends
//   POST /api/bot/off              disable scheduled sends
//   GET  /api/preview?date=&report=fund|expenditure|analysis    HTML preview
//   GET  /api/preview-image?date=&report=...                    PNG preview
//   GET  /api/daily-report?date=   manually trigger send
//   POST /api/test-send            send today's reports right now
//
// Daily cron: 06:30 IST (= 01:00 UTC) sends *yesterday's* reports.
// =====================================================================

// §1 ===================================================================
//    IMPORTS & CONFIG
// =====================================================================

const express = require('express');
const puppeteer = require('puppeteer');
const { Client, LocalAuth, MessageMedia } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const cron = require('node-cron');
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const SHEET_ID = process.env.SHEET_ID;
const GROUP_JID = process.env.WHATSAPP_GROUP_JID || '120363425432126351@g.us';

// --- Process-level safety nets: never crash the server on a single bad request ---
process.on('uncaughtException', (err) => {
  console.error('[FATAL-CAUGHT] uncaughtException:', err && err.stack ? err.stack : err);
});
process.on('unhandledRejection', (reason) => {
  console.error('[FATAL-CAUGHT] unhandledRejection:', reason && reason.stack ? reason.stack : reason);
});

// --- Validate env at startup, but don't crash. Surface via /health instead. ---
const startupIssues = [];
if (!SHEET_ID) startupIssues.push('SHEET_ID not set');
if (!process.env.GOOGLE_CREDENTIALS) {
  startupIssues.push('GOOGLE_CREDENTIALS not set');
} else {
  try { JSON.parse(process.env.GOOGLE_CREDENTIALS); }
  catch (e) { startupIssues.push('GOOGLE_CREDENTIALS is not valid JSON: ' + e.message); }
}
if (!GROUP_JID) startupIssues.push('WHATSAPP_GROUP_JID not set (using default)');
if (startupIssues.length) {
  console.warn('[Startup] Issues detected:');
  for (const s of startupIssues) console.warn('  -', s);
}

// --- Pick a writable auth path. /data on Railway needs a volume; fall back to local. ---
function pickAuthPath() {
  const candidates = ['/data/wa-auth', path.join(process.cwd(), '.wa-auth')];
  for (const p of candidates) {
    try {
      fs.mkdirSync(p, { recursive: true });
      // Test write
      const testFile = path.join(p, '.write-test');
      fs.writeFileSync(testFile, 'ok');
      fs.unlinkSync(testFile);
      console.log('[Auth] Using path:', p);
      return p;
    } catch (e) {
      console.warn('[Auth] Cannot use', p, '—', e.code || e.message);
    }
  }
  console.error('[Auth] No writable path found — WhatsApp auth will not persist');
  return null;
}
const AUTH_PATH = pickAuthPath();

// §2 ===================================================================
//    SHEET HELPERS
// =====================================================================

function getSheetsClient() {
  if (!process.env.GOOGLE_CREDENTIALS) {
    throw new Error('GOOGLE_CREDENTIALS env var not set');
  }
  let credentials;
  try {
    credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS);
  } catch (e) {
    throw new Error('GOOGLE_CREDENTIALS is not valid JSON: ' + e.message);
  }
  if (!credentials.client_email || !credentials.private_key) {
    throw new Error('GOOGLE_CREDENTIALS missing client_email or private_key');
  }
  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

async function getRange(range) {
  const sheets = getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range,
    valueRenderOption: 'UNFORMATTED_VALUE',
    dateTimeRenderOption: 'FORMATTED_STRING',
  });
  return res.data.values || [];
}

function parseDate(v) {
  if (!v) return null;
  if (v instanceof Date) return v;
  if (typeof v === 'number') {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    return new Date(epoch.getTime() + v * 86400000);
  }
  const s = String(v).trim();
  const m = s.match(/^(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})/);
  if (m) {
    const day = parseInt(m[1], 10);
    const month = parseInt(m[2], 10) - 1;
    let year = parseInt(m[3], 10);
    if (year < 100) year += 2000;
    return new Date(year, month, day);
  }
  const d = new Date(s);
  return isNaN(d) ? null : d;
}

function sameDate(a, b) {
  if (!a || !b) return false;
  return a.getFullYear() === b.getFullYear() &&
         a.getMonth() === b.getMonth() &&
         a.getDate() === b.getDate();
}

function num(v) {
  if (v === null || v === undefined || v === '') return 0;
  if (typeof v === 'number') return v;
  const n = parseFloat(String(v).replace(/[,₹\s]/g, ''));
  return isNaN(n) ? 0 : n;
}

function startOfDay(d) {
  const x = new Date(d);
  x.setHours(0, 0, 0, 0);
  return x;
}

async function getLedgerRows() {
  const rows = await getRange('Ledger!A7:L1000');
  return rows.map(r => ({
    date: parseDate(r[0]),
    entity: r[1] || '',
    head: r[2] || '',
    description: r[3] || '',
    tag: r[4] || '',
    inOut: r[5] || '',
    amount: num(r[6]),
    mode: r[7] || '',
    person: r[8] || '',
    bankAc: r[9] || '',
    transferTo: r[10] || '',
    notes: r[11] || ''
  })).filter(r => r.date && r.amount !== 0);
}

// Maps Bank A/C (column J in Ledger) to display name + bank label
const BANK_AC_MAP = {
  'Fidatocity-70%':       { company: 'Fidatocity-70%', bank: 'JKB', notUsable: true },
  'Fidatocity-30%':       { company: 'Fidatocity-30%', bank: 'JKB' },
  'Fidato City Homes':    { company: 'Fidato City Homes — Normal', bank: 'JKB' },
  'Trinity JKB':          { company: 'Trinity Landspace Pvt Ltd', bank: 'JKB' },
  'Hansaflon JKB':        { company: 'Hansaflon Buildcon Pvt Ltd', bank: 'JKB' },
  'Hansaflon AXIS':       { company: 'Hansaflon Buildcon Pvt Ltd', bank: 'AXIS' },
  'Hansaflon HDFC':       { company: 'Hansaflon Buildcon Pvt Ltd', bank: 'HDFC' },
  'Hansaflon Buildwell':  { company: 'Hansaflon Buildwell Pvt Ltd', bank: 'ICICI' },
  'Dholpur JKB':          { company: 'Dholpur Developers Pvt Ltd', bank: 'JKB' },
  'Trinity Tulsivan':     { company: 'Trinity Tulsivan Reality — 1089', bank: 'JKB' },
  'Beatific HDFC':        { company: 'Beatific Hospitality', bank: 'HDFC' },
  'Chahat JKB':           { company: 'Chahat Garments', bank: 'JKB' },
  'Fidato Buildcon':      { company: 'Fidato Buildcon', bank: 'JKB' },
  'Fidato Maintenance':   { company: 'Fidato Maintenance — 980', bank: 'JKB' },
  'Maximal JKB':          { company: 'Maximal Infrastructure', bank: 'JKB' },
  'MM PDC':               { company: 'MM PDC', bank: 'PDC' },
  'SM PDC':               { company: 'SM PDC', bank: 'PDC' },
  'PDC':                  { company: 'PDC General', bank: 'PDC' }
};

async function getFundPosition() {
  const rows = await getRange('Fund Position!B5:I23');
  const out = [];
  for (const r of rows) {
    const bankAc = r[1];
    if (!bankAc) continue;
    const meta = BANK_AC_MAP[bankAc];
    if (!meta) continue;
    const balBank = num(r[5]);
    const lessChq = num(r[6]);
    const balUs = num(r[7]);
    if (balBank === 0 && lessChq === 0 && balUs === 0) continue;
    out.push({ ...meta, bankAcKey: bankAc, balBank, lessChq, balUs });
  }
  return out;
}

// §3 ===================================================================
//    DATA BUILDERS
// =====================================================================

async function buildFundPosition(asOfDate) {
  const rows = await getFundPosition(asOfDate);
  const totals = rows.reduce((acc, r) => ({
    balBank: acc.balBank + r.balBank,
    lessChq: acc.lessChq + r.lessChq,
    balUs:   acc.balUs + r.balUs
  }), { balBank: 0, lessChq: 0, balUs: 0 });

  const notUseable = rows.filter(r => r.notUsable).reduce((s, r) => s + r.balUs, 0);
  const mmPdc = rows.filter(r => r.bankAcKey === 'MM PDC').reduce((s, r) => s + r.balUs, 0);

  return {
    asOfDate,
    rows,
    totals,
    deductions: [
      { label: 'Less : Funds not useable (Fidatocity-70%)', amount: notUseable },
      { label: 'Less : MM PDC', amount: mmPdc }
    ],
    netUseable: totals.balUs - notUseable - mmPdc
  };
}

async function buildExpenditure(date) {
  const all = await getLedgerRows();
  const dayRows = all.filter(r => sameDate(r.date, date));

  const isPdc = r => /PDC/i.test(r.bankAc) || /PDC/i.test(r.mode);

  const pdcRows = dayRows.filter(isPdc);
  const bankRows = dayRows.filter(r => !isPdc(r));

  const beforeDay = all.filter(r => r.date < startOfDay(date));
  const pdcOpening = beforeDay.filter(isPdc).reduce((s, r) => {
    if (r.inOut === 'IN') return s + r.amount;
    if (r.inOut === 'OUT') return s - r.amount;
    return s;
  }, 0);

  const cashWithdrawal = pdcRows
    .filter(r => r.inOut === 'IN' && /cash/i.test(r.description))
    .reduce((s, r) => s + r.amount, 0);

  const pdcExpenseItems = pdcRows
    .filter(r => r.inOut === 'OUT')
    .map(r => ({ description: r.description, head: r.head, amount: r.amount }));

  const pdcTotal = pdcExpenseItems.reduce((s, it) => s + it.amount, 0);
  const pdcRunningTotal = pdcOpening + cashWithdrawal - pdcTotal;

  const fp = await getFundPosition(date);
  const smRow = fp.find(r => r.bankAcKey === 'SM PDC');
  const closingWithSM = smRow ? smRow.balUs : 0;
  const closingWithOffice = pdcRunningTotal - closingWithSM;

  const bankItems = bankRows.map(r => ({
    description: r.description,
    head: r.head,
    inAmount: r.inOut === 'IN' ? r.amount : 0,
    outAmount: r.inOut === 'OUT' ? r.amount : 0
  }));

  const bankTotalIn = bankItems.reduce((s, it) => s + it.inAmount, 0);
  const bankTotalOut = bankItems.reduce((s, it) => s + it.outAmount, 0);

  const bankOpening = beforeDay.filter(r => !isPdc(r)).reduce((s, r) => {
    if (r.inOut === 'IN') return s + r.amount;
    if (r.inOut === 'OUT') return s - r.amount;
    return s;
  }, 0);

  const bankClosing = bankOpening + bankTotalIn - bankTotalOut;

  return {
    date,
    pdc: {
      openingBalance: pdcOpening,
      cashWithdrawal,
      items: pdcExpenseItems,
      total: pdcTotal,
      runningTotal: pdcRunningTotal,
      closingWithOffice,
      closingWithSM
    },
    bank: {
      openingBalance: bankOpening,
      items: bankItems,
      totalIn: bankTotalIn,
      totalOut: bankTotalOut,
      closingBalance: bankClosing
    }
  };
}

async function buildAnalysis(weekEnding) {
  const all = await getLedgerRows();
  const end = startOfDay(weekEnding);
  const sevenStart = new Date(end); sevenStart.setDate(end.getDate() - 6);
  const fourteenStart = new Date(end); fourteenStart.setDate(end.getDate() - 13);

  const inWindow = (d, start, endIncl) => d >= start && d <= new Date(endIncl.getTime() + 86400000 - 1);

  const thisWeek = all.filter(r => inWindow(r.date, sevenStart, end));
  const prevWeek = all.filter(r => inWindow(r.date, fourteenStart, new Date(sevenStart.getTime() - 1)));

  const sumIn  = rows => rows.filter(r => r.inOut === 'IN').reduce((s, r) => s + r.amount, 0);
  const sumOut = rows => rows.filter(r => r.inOut === 'OUT').reduce((s, r) => s + r.amount, 0);

  const thisIn = sumIn(thisWeek), thisOut = sumOut(thisWeek);
  const prevIn = sumIn(prevWeek), prevOut = sumOut(prevWeek);

  const dailyData = [];
  for (let i = 0; i < 7; i++) {
    const d = new Date(sevenStart); d.setDate(sevenStart.getDate() + i);
    const dayRows = thisWeek.filter(r => sameDate(r.date, d));
    dailyData.push({ date: d, in: sumIn(dayRows), out: sumOut(dayRows) });
  }

  // CCM 60/40 rule
  const ccmReceivableTags = /^FBD-(Plot|Floor)-/i;
  const ccmConstructionTags = /^FBD-CCM-/i;
  const collected = thisWeek
    .filter(r => r.inOut === 'IN' && ccmReceivableTags.test(r.tag))
    .reduce((s, r) => s + r.amount, 0);
  const deployedConstruction = thisWeek
    .filter(r => r.inOut === 'OUT' && ccmConstructionTags.test(r.tag))
    .reduce((s, r) => s + r.amount, 0);
  const required = Math.round(collected * 0.6);
  const office = Math.max(0, collected - deployedConstruction);
  const officePct = collected > 0 ? Math.round((office / collected) * 100) : 0;
  const shortfall = Math.max(0, required - deployedConstruction);

  const liquidity = await buildLiquidity(weekEnding);
  const outliers = detectOutliers(all, thisWeek);

  return {
    weekEnding,
    trend: {
      thisIn, thisOut, thisNet: thisIn - thisOut,
      prevIn, prevOut, prevNet: prevIn - prevOut,
      dailyBurn: Math.round(thisOut / 7),
      dailyData
    },
    ccm: { collected, required, deployed: deployedConstruction, office, shortfall, officePct },
    liquidity,
    outliers
  };
}

async function buildLiquidity(asOfDate) {
  const chqRows = await getRange('Cheque Register!A5:J500');
  const fp = await getFundPosition(asOfDate);
  const fifteenAhead = new Date(asOfDate.getTime() + 15 * 86400000);

  const dueByAccount = {};
  for (const r of chqRows) {
    const status = r[7];
    const chqDate = parseDate(r[6]);
    const bankAc = r[2];
    const amt = num(r[5]);
    if (status !== 'Issued') continue;
    if (!chqDate || chqDate > fifteenAhead) continue;
    dueByAccount[bankAc] = (dueByAccount[bankAc] || 0) + amt;
  }

  const watch = [];
  for (const [bankAc, due] of Object.entries(dueByAccount)) {
    const fpRow = fp.find(r => r.bankAcKey === bankAc);
    if (!fpRow) continue;
    const balance = fpRow.balBank;
    const gap = balance - due;
    let status = 'ok';
    if (gap < 0) status = 'critical';
    else if (gap < due * 0.5) status = 'tight';
    if (status === 'ok') continue;
    watch.push({
      account: fpRow.company.replace(/ Pvt Ltd$/i, ''),
      balance, chequesDue: due, gap, status
    });
  }
  watch.sort((a, b) => {
    if (a.status === b.status) return a.gap - b.gap;
    return a.status === 'critical' ? -1 : 1;
  });
  return watch;
}

// §4 ===================================================================
//    OUTLIER DETECTION
// =====================================================================

function detectOutliers(allRows, thisWeekRows) {
  const outliers = [];

  // 1. Same-day reversals
  const grouped = {};
  for (const r of thisWeekRows) {
    const key = `${r.date.toDateString()}|${r.amount}|${r.head}`;
    if (!grouped[key]) grouped[key] = [];
    grouped[key].push(r);
  }
  for (const [, rows] of Object.entries(grouped)) {
    if (rows.length >= 2) {
      const hasIn = rows.some(r => r.inOut === 'IN');
      const hasOut = rows.some(r => r.inOut === 'OUT');
      if (hasIn && hasOut) {
        for (const r of rows) {
          outliers.push({
            date: r.date, description: r.description,
            amount: r.amount, reason: 'Same-day reversal'
          });
        }
      }
    }
  }

  // 2. Single transactions ≥ 3× the head's prior 30-day average
  const thirtyAgo = new Date(thisWeekRows[0]?.date || new Date());
  thirtyAgo.setDate(thirtyAgo.getDate() - 37);
  const recentByHead = {};
  for (const r of allRows.filter(r => r.date >= thirtyAgo && !thisWeekRows.includes(r))) {
    if (!recentByHead[r.head]) recentByHead[r.head] = [];
    recentByHead[r.head].push(r.amount);
  }
  for (const r of thisWeekRows) {
    const past = recentByHead[r.head];
    if (!past || past.length < 3) continue;
    const avg = past.reduce((s, x) => s + x, 0) / past.length;
    if (r.amount >= avg * 3 && r.amount >= 100000) {
      const already = outliers.find(o => o.date === r.date && o.description === r.description);
      if (!already) {
        outliers.push({
          date: r.date, description: r.description, amount: r.amount,
          reason: `${(r.amount / avg).toFixed(1)}× average for ${r.head}`
        });
      }
    }
  }

  // 3. Large drawing entries (>5L)
  for (const r of thisWeekRows) {
    if (/director|drawing/i.test(r.head) && r.amount >= 500000) {
      const already = outliers.find(o => o.date === r.date && o.description === r.description);
      if (!already) {
        outliers.push({
          date: r.date, description: r.description, amount: r.amount,
          reason: 'Single largest drawing entry'
        });
      }
    }
  }

  outliers.sort((a, b) => b.amount - a.amount);
  return outliers.slice(0, 6);
}

// §5 ===================================================================
//    FORMATTING HELPERS
// =====================================================================

function fmtINR(n, opts = {}) {
  if (n === null || n === undefined || n === '' || isNaN(n)) {
    return opts.dash ? '—' : (opts.prefix || '') + '0';
  }
  const numVal = Math.round(Number(n));
  if (numVal === 0 && opts.dash) return '—';
  const negative = numVal < 0;
  const abs = Math.abs(numVal).toString();
  const lastThree = abs.slice(-3);
  const rest = abs.slice(0, -3);
  const formatted = rest
    ? rest.replace(/\B(?=(\d{2})+(?!\d))/g, ',') + ',' + lastThree
    : lastThree;
  const prefix = opts.prefix !== undefined ? opts.prefix : '';
  if (negative) return '– ' + prefix.replace('– ', '') + formatted;
  return prefix + formatted;
}

function fmtDate(d) {
  const dt = (d instanceof Date) ? d : new Date(d);
  if (isNaN(dt)) return '';
  const dd = String(dt.getDate()).padStart(2, '0');
  const mm = String(dt.getMonth() + 1).padStart(2, '0');
  const yy = String(dt.getFullYear()).slice(-2);
  return `${dd}.${mm}.${yy}`;
}

function fmtDateLong(d) {
  const dt = (d instanceof Date) ? d : new Date(d);
  if (isNaN(dt)) return '';
  const months = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  return `${dt.getDate()} ${months[dt.getMonth()]} ${dt.getFullYear()}`;
}

function fmtTimestamp(d) {
  const dt = (d instanceof Date) ? d : new Date(d);
  const dd = String(dt.getDate()).padStart(2, '0');
  const mm = String(dt.getMonth() + 1).padStart(2, '0');
  const yyyy = dt.getFullYear();
  const hh = String(dt.getHours()).padStart(2, '0');
  const min = String(dt.getMinutes()).padStart(2, '0');
  return `${dd}.${mm}.${yyyy} · ${hh}:${min} IST`;
}

function bankPill(bank) {
  if (!bank) return '';
  const b = bank.toUpperCase();
  let bg = '#f4f1e8', fg = '#5a4a1a';
  if (b.includes('AXIS')) { bg = '#f0e8f2'; fg = '#5a2a5e'; }
  else if (b.includes('HDFC')) { bg = '#e8eef8'; fg = '#1a3a6e'; }
  else if (b.includes('ICICI')) { bg = '#fdebe6'; fg = '#7a2a1a'; }
  else if (b.includes('PDC')) { bg = '#eee'; fg = '#555'; }
  return `<span style="display:inline-block;padding:2px 9px;background:${bg};color:${fg};border-radius:3px;font-size:12px;font-weight:600;letter-spacing:0.3px;">${bank}</span>`;
}

function headPill(head) {
  if (!head) return '';
  const h = head.toLowerCase();
  let bg = '#f0eef6', fg = '#3a2d5a';
  if (h.includes('capital site') || h.includes('ccm')) { bg = '#e6f3ec'; fg = '#1a5a3a'; }
  else if (h.includes('director') || h.includes('drawing')) { bg = '#fdebe6'; fg = '#7a2a1a'; }
  else if (h.includes('vrindavan') || h.includes('vrn')) { bg = '#e0f2ee'; fg = '#0d4d3e'; }
  else if (h.includes('sec 70') || h.includes('land')) { bg = '#fef4e6'; fg = '#7a4a0a'; }
  else if (h.includes('legal') || h.includes('misc') || h.includes('adv')) { bg = '#f4f1e8'; fg = '#5a4a1a'; }
  else if (h.includes('noida')) { bg = '#e8eef8'; fg = '#1a3a6e'; }
  else if (h.includes('fidato 88')) { bg = '#eee'; fg = '#555'; }
  return `<span style="display:inline-block;padding:2px 9px;background:${bg};color:${fg};border-radius:3px;font-size:12px;font-weight:600;">${head}</span>`;
}

const baseStyles = `
  *{box-sizing:border-box;margin:0;padding:0;}
  body{font-family:'Calibri','Segoe UI','Helvetica Neue',Arial,sans-serif;color:#1a1a1a;background:#fff;font-size:15px;line-height:1.5;-webkit-font-smoothing:antialiased;}
  .page{padding:32px 28px;background:#fff;}
  .header{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:18px;padding-bottom:12px;border-bottom:2px solid #1a1a1a;}
  .eyebrow{font-size:11px;letter-spacing:2px;color:#666;text-transform:uppercase;margin-bottom:4px;}
  .title{font-size:20px;font-weight:700;letter-spacing:-0.2px;}
  .date-label{font-size:11px;letter-spacing:1.5px;color:#666;text-transform:uppercase;}
  .date-value{font-size:18px;font-weight:600;}
  .section-head{display:flex;align-items:center;gap:10px;margin:18px 0 12px 0;}
  .section-bar{width:6px;height:22px;background:#1a1a1a;}
  .section-title{font-size:16px;font-weight:700;letter-spacing:0.3px;}
  .section-sub{font-size:13px;color:#888;margin-left:6px;}
  table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums;}
  th{font-weight:600;font-size:12px;letter-spacing:0.8px;text-transform:uppercase;color:#555;}
  .num{text-align:right;}
  .muted{color:#aaa;}
  .footer{margin-top:18px;padding-top:12px;border-top:1px solid #e0ddd2;display:flex;justify-content:space-between;font-size:11px;color:#888;letter-spacing:0.5px;}
  .hero{padding:18px 22px;background:linear-gradient(135deg,#1a4480,#2a5fa8);border-radius:4px;display:flex;justify-content:space-between;align-items:center;margin-top:14px;}
  .hero-label{font-size:11px;letter-spacing:2px;color:rgba(255,255,255,0.7);text-transform:uppercase;margin-bottom:2px;}
  .hero-sub{font-size:14px;color:rgba(255,255,255,0.9);font-weight:500;}
  .hero-amt{font-size:26px;font-weight:700;color:#fff;font-variant-numeric:tabular-nums;letter-spacing:-0.5px;}
`;

function wrapPage(bodyContent, generatedAt) {
  return `<!doctype html><html><head><meta charset="utf-8"><style>${baseStyles}</style></head><body>
<div class="page">${bodyContent}
<div class="footer"><div>Generated ${fmtTimestamp(generatedAt || new Date())}</div><div>All amounts in ₹</div></div>
</div></body></html>`;
}

// §6 ===================================================================
//    HTML TEMPLATES
// =====================================================================

function renderFundPosition({ asOfDate, rows, totals, deductions, netUseable }) {
  const trs = rows.map((r, i) => {
    const zebra = i % 2 === 1 ? 'background:#fbfaf6;' : '';
    const isLast = i === rows.length - 1;
    const borderBottom = isLast ? 'border-bottom:1.5px solid #1a1a1a;' : 'border-bottom:1px solid #f0eee7;';
    const companyLabel = r.notUsable
      ? `${r.company} <span style="color:#999;font-size:12px;margin-left:4px;">(Not Usable)</span>`
      : r.company;
    const lessChqCell = r.lessChq > 0
      ? `<td class="num" style="padding:11px 12px;color:#a02828;">${fmtINR(r.lessChq)}</td>`
      : `<td class="num muted" style="padding:11px 12px;">—</td>`;
    return `<tr style="${zebra}${borderBottom}">
      <td style="padding:11px 12px 11px 4px;">${companyLabel}</td>
      <td style="padding:11px 12px;">${bankPill(r.bank)}</td>
      <td class="num" style="padding:11px 12px;">${fmtINR(r.balBank, {dash:true})}</td>
      ${lessChqCell}
      <td class="num" style="padding:11px 4px 11px 12px;font-weight:500;">${fmtINR(r.balUs, {dash:true})}</td>
    </tr>`;
  }).join('');

  const dedRows = (deductions || []).map(d => `<tr>
    <td style="padding:5px 0;color:#555;">${d.label}</td>
    <td class="num" style="padding:5px 0;font-weight:500;">${fmtINR(d.amount)}</td>
  </tr>`).join('');

  const body = `
<div class="header">
  <div><div class="eyebrow">Fidato Group</div><div class="title">Fund Position</div></div>
  <div style="text-align:right;"><div class="date-label">As on</div><div class="date-value">${fmtDateLong(asOfDate)}</div></div>
</div>
<table style="font-size:14.5px;">
<thead><tr style="border-bottom:1.5px solid #1a1a1a;">
  <th style="padding:12px 12px 10px 4px;text-align:left;width:36%;">Company</th>
  <th style="padding:12px;text-align:left;width:10%;">Bank</th>
  <th class="num" style="padding:12px;width:18%;">Bal as per Bank</th>
  <th class="num" style="padding:12px;width:18%;">Less : Chq Issued</th>
  <th class="num" style="padding:12px 4px 10px 12px;width:18%;">Balance as per Us</th>
</tr></thead>
<tbody>
${trs}
<tr style="background:#fbfaf6;">
  <td style="padding:14px 12px 14px 4px;font-weight:700;font-size:15px;letter-spacing:1px;">TOTAL</td>
  <td></td>
  <td class="num" style="padding:14px 12px;font-weight:700;font-size:16px;">${fmtINR(totals.balBank)}</td>
  <td class="num" style="padding:14px 12px;font-weight:700;font-size:16px;color:#a02828;">${fmtINR(totals.lessChq)}</td>
  <td class="num" style="padding:14px 4px 14px 12px;font-weight:700;font-size:16px;">${fmtINR(totals.balUs)}</td>
</tr>
</tbody>
</table>
<div style="margin-top:20px;padding:16px 18px;background:#fbfaf6;border-radius:4px;">
<table style="font-size:14.5px;">${dedRows}</table>
</div>
<div class="hero">
  <div><div class="hero-label">Net Bank Balance</div><div class="hero-sub">Useable</div></div>
  <div class="hero-amt">${fmtINR(netUseable, {prefix:'₹ '})}</div>
</div>`;
  return wrapPage(body);
}

function renderExpenditure({ date, pdc, bank }) {
  const pdcOpening = `<tr style="background:#fbfaf6;border-bottom:1px solid #f0eee7;">
    <td style="padding:11px 12px 11px 4px;font-weight:500;">Opening Balance <span style="color:#888;font-size:12.5px;">(Incl. Dasti with SM &amp; Locker)</span></td>
    <td></td>
    <td class="num muted" style="padding:11px 12px;">—</td>
    <td class="num" style="padding:11px 4px 11px 12px;font-weight:600;">${fmtINR(pdc.openingBalance)}</td>
  </tr>`;

  const pdcCashWd = (pdc.cashWithdrawal && pdc.cashWithdrawal > 0)
    ? `<tr style="background:#fbfaf6;border-bottom:1px solid #f0eee7;">
        <td style="padding:11px 12px 11px 4px;">Cash Withdrawal for Exp.</td>
        <td></td>
        <td class="num muted" style="padding:11px 12px;">—</td>
        <td class="num" style="padding:11px 4px 11px 12px;font-weight:500;color:#1a7a4a;">+ ${fmtINR(pdc.cashWithdrawal)}</td>
      </tr>` : '';

  const pdcItemRows = (pdc.items || []).map((it, i) => {
    const isLast = i === pdc.items.length - 1;
    const borderBottom = isLast ? 'border-bottom:1.5px solid #1a1a1a;' : 'border-bottom:1px solid #f0eee7;';
    return `<tr style="${borderBottom}">
      <td style="padding:11px 12px 11px 4px;">${it.description}</td>
      <td style="padding:11px 12px;">${headPill(it.head)}</td>
      <td class="num" style="padding:11px 12px;">${fmtINR(it.amount)}</td>
      <td class="num muted" style="padding:11px 4px 11px 12px;">—</td>
    </tr>`;
  }).join('');

  const pdcTotalRow = `<tr style="background:#fff8b8;">
    <td style="padding:14px 12px 14px 4px;font-weight:700;font-size:15px;letter-spacing:1px;">TOTAL</td>
    <td></td>
    <td class="num" style="padding:14px 12px;font-weight:700;font-size:16px;">${fmtINR(pdc.total)}</td>
    <td class="num" style="padding:14px 4px 14px 12px;font-weight:700;font-size:16px;">${fmtINR(pdc.runningTotal)}</td>
  </tr>`;

  const pdcClose1 = `<tr style="background:#fdebe6;">
    <td style="padding:12px;color:#7a2a1a;font-weight:600;">Closing balance PDC### (with office)</td>
    <td></td>
    <td class="num" style="padding:12px;color:#7a2a1a;font-weight:700;font-size:15px;">${fmtINR(pdc.closingWithOffice)}</td>
    <td></td>
  </tr>`;
  const pdcClose2 = `<tr style="background:#fdebe6;">
    <td style="padding:12px;color:#7a2a1a;font-weight:600;">Closing balance PDC### (with SM)</td>
    <td></td>
    <td class="num" style="padding:12px;color:#7a2a1a;font-weight:700;font-size:15px;">${fmtINR(pdc.closingWithSM)}</td>
    <td></td>
  </tr>`;

  const bankOpening = `<tr style="background:#fbfaf6;border-bottom:1px solid #f0eee7;">
    <td style="padding:11px 12px 11px 4px;font-weight:500;">Opening Balance</td>
    <td></td>
    <td class="num" style="padding:11px 12px;font-weight:600;">${fmtINR(bank.openingBalance)}</td>
    <td class="num muted" style="padding:11px 4px 11px 12px;">—</td>
  </tr>`;

  const bankItemRows = (bank.items || []).map(it => {
    const inCell = (it.inAmount && it.inAmount > 0)
      ? `<td class="num" style="padding:11px 12px;color:#1a7a4a;font-weight:500;">${fmtINR(it.inAmount)}</td>`
      : `<td class="num muted" style="padding:11px 12px;">—</td>`;
    const outCell = (it.outAmount && it.outAmount > 0)
      ? `<td class="num" style="padding:11px 4px 11px 12px;color:#a02828;">${fmtINR(it.outAmount)}</td>`
      : `<td class="num muted" style="padding:11px 4px 11px 12px;">—</td>`;
    return `<tr style="border-bottom:1px solid #f0eee7;">
      <td style="padding:11px 12px 11px 4px;">${it.description}</td>
      <td style="padding:11px 12px;">${headPill(it.head)}</td>
      ${inCell}${outCell}
    </tr>`;
  }).join('');

  const bankTotalRow = `<tr style="border-bottom:1.5px solid #1a1a1a;background:#fbfaf6;">
    <td style="padding:14px 12px 14px 4px;font-weight:700;font-size:15px;letter-spacing:1px;">TOTAL</td>
    <td></td>
    <td class="num" style="padding:14px 12px;font-weight:700;font-size:16px;color:#1a7a4a;">${fmtINR(bank.totalIn)}</td>
    <td class="num" style="padding:14px 4px 14px 12px;font-weight:700;font-size:16px;color:#a02828;">${fmtINR(bank.totalOut)}</td>
  </tr>`;

  const body = `
<div class="header">
  <div><div class="eyebrow">Fidato Group</div><div class="title">Daily Expenditure</div></div>
  <div style="text-align:right;"><div class="date-label">Dated</div><div class="date-value">${fmtDateLong(date)}</div></div>
</div>
<div class="section-head" style="margin-top:8px;"><div class="section-bar"></div><div class="section-title">PDC <span style="color:#999;">###</span></div></div>
<table style="font-size:14.5px;margin-bottom:18px;">
<thead><tr style="border-bottom:1.5px solid #1a1a1a;">
  <th style="padding:10px 12px 10px 4px;text-align:left;width:50%;">Particulars</th>
  <th style="padding:10px 12px;text-align:left;width:22%;">Head</th>
  <th class="num" style="padding:10px 12px;width:14%;">Amount</th>
  <th class="num" style="padding:10px 4px 10px 12px;width:14%;">Balance</th>
</tr></thead>
<tbody>${pdcOpening}${pdcCashWd}${pdcItemRows}${pdcTotalRow}${pdcClose1}${pdcClose2}</tbody>
</table>
<div class="section-head"><div class="section-bar"></div><div class="section-title">Bank Movement</div></div>
<table style="font-size:14.5px;">
<thead><tr style="border-bottom:1.5px solid #1a1a1a;">
  <th style="padding:10px 12px 10px 4px;text-align:left;width:50%;">Particulars</th>
  <th style="padding:10px 12px;text-align:left;width:22%;">Head</th>
  <th class="num" style="padding:10px 12px;width:14%;">In</th>
  <th class="num" style="padding:10px 4px 10px 12px;width:14%;">Out</th>
</tr></thead>
<tbody>${bankOpening}${bankItemRows}${bankTotalRow}</tbody>
</table>
<div class="hero">
  <div><div class="hero-label">Closing Balance</div><div class="hero-sub">After today's bank movement</div></div>
  <div class="hero-amt">${fmtINR(bank.closingBalance, {prefix:'₹ '})}</div>
</div>`;
  return wrapPage(body);
}

function renderAnalysis({ weekEnding, trend, ccm, liquidity, outliers }) {
  const inDeltaPct = trend.prevIn > 0 ? Math.round(((trend.thisIn - trend.prevIn) / trend.prevIn) * 100) : 0;
  const outDeltaPct = trend.prevOut > 0 ? Math.round(((trend.thisOut - trend.prevOut) / trend.prevOut) * 100) : 0;
  const netDelta = trend.thisNet - trend.prevNet;
  const inArrow = trend.thisIn >= trend.prevIn ? '▲' : '▼';
  const outArrow = trend.thisOut >= trend.prevOut ? '▲' : '▼';
  const netArrow = netDelta >= 0 ? '▲' : '▼';
  const netCardBg = trend.thisNet < 0 ? '#fdebe6' : '#fbfaf6';
  const netCardColor = trend.thisNet < 0 ? '#7a2a1a' : '#1a1a1a';
  const netBorderColor = trend.thisNet < 0 ? '#7a2a1a' : '#1a7a4a';

  const maxDay = Math.max(1, ...(trend.dailyData || []).map(d => Math.max(d.in || 0, d.out || 0)));
  const barChart = (trend.dailyData || []).slice(0, 7).map((d, i) => {
    const x = 20 + i * 100;
    const inHeight = Math.round(((d.in || 0) / maxDay) * 60);
    const outHeight = Math.round(((d.out || 0) / maxDay) * 60);
    const dt = new Date(d.date);
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const label = `${dt.getDate()} ${months[dt.getMonth()]}`;
    return `<g>
      <rect x="${x}" y="${70 - inHeight}" width="40" height="${inHeight}" fill="#1a7a4a"/>
      <rect x="${x}" y="70" width="40" height="${outHeight}" fill="#a02828"/>
      <text x="${x + 20}" y="125" text-anchor="middle" font-size="11" fill="#666">${label}</text>
    </g>`;
  }).join('');

  const fillPct = ccm.collected > 0 ? Math.round((ccm.deployed / ccm.collected) * 100) : 0;

  const ccmShortfall = ccm.shortfall && ccm.shortfall > 0 ? `
<div style="padding:14px 18px;background:#fdebe6;border-left:4px solid #7a2a1a;border-radius:3px;margin-bottom:32px;">
  <div style="font-size:13px;color:#7a2a1a;font-weight:600;margin-bottom:2px;">Shortfall</div>
  <div style="font-size:13px;color:#7a2a1a;">Need <span style="font-weight:700;">${fmtINR(ccm.shortfall, {prefix:'₹ '})}</span> more in CCM construction this week to meet the 60% rule.${ccm.officePct > 40 ? ` Office spending is at ${ccm.officePct}% of collections (${fmtINR(ccm.office, {prefix:'₹ '})}) — over the 40% ceiling.` : ''}</div>
</div>` : `
<div style="padding:14px 18px;background:#e6f3ec;border-left:4px solid #1a5a3a;border-radius:3px;margin-bottom:32px;">
  <div style="font-size:13px;color:#1a5a3a;font-weight:600;">On track</div>
  <div style="font-size:13px;color:#1a5a3a;">CCM 60/40 rule satisfied this week.</div>
</div>`;

  const liquidityRows = (liquidity || []).map(l => {
    let bg, borderColor, statusColor, statusLabel, gapColor;
    if (l.status === 'critical') {
      bg = '#fdebe6'; borderColor = '#a02828'; statusColor = '#7a2a1a'; statusLabel = 'Critical'; gapColor = '#7a2a1a';
    } else if (l.status === 'tight') {
      bg = '#fff8b8'; borderColor = '#b08810'; statusColor = '#5a4410'; statusLabel = 'Tight'; gapColor = '#5a4410';
    } else {
      bg = '#e6f3ec'; borderColor = '#1a5a3a'; statusColor = '#1a5a3a'; statusLabel = 'OK'; gapColor = '#1a5a3a';
    }
    const gapPrefix = l.gap >= 0 ? '+ ₹ ' : '– ₹ ';
    const gapValue = fmtINR(Math.abs(l.gap));
    return `<div style="padding:16px 20px;background:${bg};border-left:4px solid ${borderColor};border-radius:3px;margin-bottom:8px;">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <div style="font-size:14px;font-weight:600;color:#1a1a1a;margin-bottom:2px;">${l.account}</div>
          <div style="font-size:12px;color:#666;">Balance ${fmtINR(l.balance, {prefix:'₹ '})} · Cheques due ${fmtINR(l.chequesDue, {prefix:'₹ '})}</div>
        </div>
        <div style="text-align:right;">
          <div style="font-size:11px;letter-spacing:1.5px;color:${statusColor};text-transform:uppercase;font-weight:600;">${statusLabel}</div>
          <div style="font-size:18px;font-weight:700;color:${gapColor};font-variant-numeric:tabular-nums;">${gapPrefix}${gapValue}</div>
        </div>
      </div>
    </div>`;
  }).join('');

  const outlierRows = (outliers && outliers.length > 0) ? outliers.map((o, i) => {
    const zebra = i % 2 === 1 ? 'background:#fbfaf6;' : '';
    return `<tr style="${zebra}border-bottom:1px solid #f0eee7;">
      <td style="padding:11px 12px 11px 4px;">${fmtDate(o.date)}</td>
      <td style="padding:11px 12px;">${o.description}</td>
      <td class="num" style="padding:11px 12px;font-weight:500;">${fmtINR(o.amount, {prefix:'₹ '})}</td>
      <td style="padding:11px 4px 11px 12px;color:#666;">${o.reason}</td>
    </tr>`;
  }).join('') : `<tr><td colspan="4" style="padding:18px;text-align:center;color:#888;font-style:italic;">No significant outliers this week</td></tr>`;

  const body = `
<div class="header">
  <div><div class="eyebrow">Fidato Group</div><div class="title">Weekly Analysis</div></div>
  <div style="text-align:right;"><div class="date-label">Week ending</div><div class="date-value">${fmtDateLong(weekEnding)}</div></div>
</div>
<div class="section-head" style="margin-top:24px;"><div class="section-bar"></div><div class="section-title">7-Day Movement</div><div class="section-sub">vs prior 7 days</div></div>
<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:14px;">
  <div style="padding:18px 20px;background:#fbfaf6;border-radius:4px;border-left:4px solid #1a7a4a;">
    <div style="font-size:11px;letter-spacing:1.5px;color:#666;text-transform:uppercase;margin-bottom:6px;">Inflows</div>
    <div style="font-size:24px;font-weight:700;letter-spacing:-0.3px;font-variant-numeric:tabular-nums;margin-bottom:4px;">${fmtINR(trend.thisIn, {prefix:'₹ '})}</div>
    <div style="font-size:13px;color:#1a7a4a;font-weight:600;">${inArrow} ${Math.abs(inDeltaPct)}% vs ${fmtINR(trend.prevIn, {prefix:'₹ '})}</div>
  </div>
  <div style="padding:18px 20px;background:#fbfaf6;border-radius:4px;border-left:4px solid #a02828;">
    <div style="font-size:11px;letter-spacing:1.5px;color:#666;text-transform:uppercase;margin-bottom:6px;">Outflows</div>
    <div style="font-size:24px;font-weight:700;letter-spacing:-0.3px;font-variant-numeric:tabular-nums;margin-bottom:4px;">${fmtINR(trend.thisOut, {prefix:'₹ '})}</div>
    <div style="font-size:13px;color:#a02828;font-weight:600;">${outArrow} ${Math.abs(outDeltaPct)}% vs ${fmtINR(trend.prevOut, {prefix:'₹ '})}</div>
  </div>
  <div style="padding:18px 20px;background:${netCardBg};border-radius:4px;border-left:4px solid ${netBorderColor};">
    <div style="font-size:11px;letter-spacing:1.5px;color:${netCardColor};text-transform:uppercase;margin-bottom:6px;">Net Movement</div>
    <div style="font-size:24px;font-weight:700;letter-spacing:-0.3px;font-variant-numeric:tabular-nums;color:${netCardColor};margin-bottom:4px;">${fmtINR(trend.thisNet, {prefix:'₹ '})}</div>
    <div style="font-size:13px;color:${netCardColor};font-weight:600;">${netArrow} ${fmtINR(Math.abs(netDelta), {prefix:'₹ '})} vs prev</div>
  </div>
</div>
<div style="margin-bottom:32px;padding:16px 20px;background:#fbfaf6;border-radius:4px;">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
    <div style="font-size:13px;color:#555;font-weight:500;">Daily flow this week</div>
    <div style="font-size:11px;color:#888;">Burn rate: ${fmtINR(trend.dailyBurn, {prefix:'₹ '})} / day</div>
  </div>
  <svg viewBox="0 0 720 140" style="width:100%;height:auto;">
    <line x1="0" y1="70" x2="720" y2="70" stroke="#d0ccbc" stroke-width="0.5" stroke-dasharray="3 3"/>
    ${barChart}
    <text x="0" y="35" font-size="10" fill="#888">In</text>
    <text x="0" y="105" font-size="10" fill="#888">Out</text>
  </svg>
</div>
<div class="section-head"><div class="section-bar"></div><div class="section-title">Capital Central Market — 60 / 40 Rule</div></div>
<div style="font-size:13px;color:#666;margin-bottom:14px;padding-left:16px;">Of every ₹100 collected from CCM customers, ₹60 must be deployed to FBD-CCM construction within the same week.</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px;">
  <div style="padding:16px 18px;background:#fbfaf6;border-radius:4px;">
    <div style="font-size:11px;letter-spacing:1.5px;color:#666;text-transform:uppercase;margin-bottom:6px;">Collected from CCM (7d)</div>
    <div style="font-size:22px;font-weight:700;font-variant-numeric:tabular-nums;">${fmtINR(ccm.collected, {prefix:'₹ '})}</div>
  </div>
  <div style="padding:16px 18px;background:#fbfaf6;border-radius:4px;">
    <div style="font-size:11px;letter-spacing:1.5px;color:#666;text-transform:uppercase;margin-bottom:6px;">Required (60%)</div>
    <div style="font-size:22px;font-weight:700;font-variant-numeric:tabular-nums;">${fmtINR(ccm.required, {prefix:'₹ '})}</div>
  </div>
</div>
<div style="margin-bottom:14px;">
  <div style="display:flex;justify-content:space-between;font-size:12px;color:#666;margin-bottom:18px;">
    <div>Deployed to construction</div>
    <div><span style="color:#1a1a1a;font-weight:600;">${fmtINR(ccm.deployed, {prefix:'₹ '})}</span> <span style="color:#888;">/ ${fmtINR(ccm.required, {prefix:'₹ '})} needed</span></div>
  </div>
  <div style="height:14px;background:#f0eee7;border-radius:3px;overflow:hidden;position:relative;">
    <div style="height:100%;width:${Math.min(100, fillPct)}%;background:${fillPct >= 60 ? '#1a7a4a' : '#a02828'};"></div>
    <div style="position:absolute;left:60%;top:-2px;bottom:-2px;width:1px;background:#1a1a1a;"></div>
    <div style="position:absolute;left:60%;top:-16px;font-size:10px;font-weight:600;color:#1a1a1a;transform:translateX(-50%);">60%</div>
  </div>
</div>
${ccmShortfall}
<div class="section-head"><div class="section-bar"></div><div class="section-title">Liquidity Watch</div><div class="section-sub">Cheques due in next 15 days</div></div>
<div style="margin-bottom:32px;">
${liquidityRows || '<div style="padding:18px;text-align:center;color:#888;font-style:italic;background:#fbfaf6;border-radius:3px;">All accounts comfortable</div>'}
</div>
<div class="section-head"><div class="section-bar"></div><div class="section-title">Outliers</div><div class="section-sub">last 7 days</div></div>
<table style="font-size:14px;">
<thead><tr style="border-bottom:1.5px solid #1a1a1a;">
  <th style="padding:10px 12px 10px 4px;text-align:left;font-size:11px;width:13%;">Date</th>
  <th style="padding:10px 12px;text-align:left;font-size:11px;width:38%;">Particulars</th>
  <th style="padding:10px 12px;text-align:right;font-size:11px;width:18%;">Amount</th>
  <th style="padding:10px 4px 10px 12px;text-align:left;font-size:11px;width:31%;">Why flagged</th>
</tr></thead>
<tbody>${outlierRows}</tbody>
</table>`;
  return wrapPage(body);
}

// §7 ===================================================================
//    PUPPETEER RENDERER
// =====================================================================

let browserInstance = null;
let browserLaunching = null;

async function getBrowser() {
  if (browserInstance && browserInstance.isConnected()) return browserInstance;
  if (browserLaunching) return browserLaunching;

  browserLaunching = (async () => {
    const launchOpts = {
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-gpu',
        '--no-first-run',
        '--no-zygote',
        '--font-render-hinting=none'
      ]
    };
    // Honor explicit env path if provided (e.g., Railway with custom Dockerfile)
    if (process.env.PUPPETEER_EXECUTABLE_PATH) {
      launchOpts.executablePath = process.env.PUPPETEER_EXECUTABLE_PATH;
    }
    try {
      const b = await puppeteer.launch(launchOpts);
      b.on('disconnected', () => {
        console.warn('[Puppeteer] Browser disconnected');
        browserInstance = null;
      });
      browserInstance = b;
      return b;
    } catch (e) {
      console.error('[Puppeteer] Launch failed:', e.message);
      throw new Error('Puppeteer launch failed — Chromium may not be installed: ' + e.message);
    } finally {
      browserLaunching = null;
    }
  })();
  return browserLaunching;
}

async function htmlToPng(html, viewportWidth = 900) {
  const browser = await getBrowser();
  const page = await browser.newPage();
  try {
    await page.setViewport({ width: viewportWidth, height: 800, deviceScaleFactor: 2 });
    await page.setContent(html, { waitUntil: 'networkidle0', timeout: 30000 });
    try { await page.evaluate(() => document.fonts.ready); } catch {}
    const buffer = await page.screenshot({ fullPage: true, type: 'png' });
    return buffer;
  } finally {
    try { await page.close(); } catch {}
  }
}

// §8 ===================================================================
//    WHATSAPP CLIENT + SENDER
// =====================================================================

let waClient = null;
let waReady = false;
let lastQR = null;
let botEnabled = true;
let waInitAttempts = 0;

function initWhatsApp() {
  waInitAttempts += 1;
  try {
    const authStrategy = AUTH_PATH
      ? new LocalAuth({ dataPath: AUTH_PATH })
      : new LocalAuth();  // fallback to default in-memory-ish

    waClient = new Client({
      authStrategy,
      puppeteer: {
        headless: true,
        executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
        args: [
          '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
          '--disable-accelerated-2d-canvas', '--no-first-run', '--no-zygote', '--disable-gpu'
        ]
      }
    });

    waClient.on('qr', (qr) => {
      lastQR = qr;
      console.log('[WA] QR generated. Visit /api/pair to view.');
      try { qrcode.generate(qr, { small: true }); } catch {}
    });
    waClient.on('ready', () => {
      waReady = true;
      lastQR = null;
      console.log('[WA] Client ready');
    });
    waClient.on('disconnected', (reason) => {
      waReady = false;
      console.log('[WA] Disconnected:', reason);
      // Auto-retry init after 30s, but cap attempts
      if (waInitAttempts < 5) {
        setTimeout(() => {
          console.log('[WA] Retrying init (attempt', waInitAttempts + 1, ')');
          initWhatsApp();
        }, 30000);
      }
    });
    waClient.on('auth_failure', (msg) => {
      console.error('[WA] Auth failed:', msg);
      waReady = false;
    });

    waClient.initialize().catch(err => {
      console.error('[WA] initialize() rejected:', err && err.message);
    });
  } catch (e) {
    console.error('[WA] init threw:', e && e.message);
    // Don't crash — leave waReady=false so endpoints can report it
  }
}

initWhatsApp();

function dateSlug(d) {
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

async function generateThreeReports(date) {
  const targetDate = date instanceof Date ? date : new Date(date);

  const [fundData, expData, anaData] = await Promise.all([
    buildFundPosition(targetDate),
    buildExpenditure(targetDate),
    buildAnalysis(targetDate)
  ]);

  const fundHtml = renderFundPosition(fundData);
  const expHtml  = renderExpenditure(expData);
  const anaHtml  = renderAnalysis(anaData);

  const [fundPng, expPng, anaPng] = await Promise.all([
    htmlToPng(fundHtml),
    htmlToPng(expHtml),
    htmlToPng(anaHtml)
  ]);

  return {
    fundPosition: { html: fundHtml, png: fundPng, filename: `fund-position-${dateSlug(targetDate)}.png` },
    expenditure:  { html: expHtml,  png: expPng,  filename: `expenditure-${dateSlug(targetDate)}.png` },
    analysis:     { html: anaHtml,  png: anaPng,  filename: `analysis-${dateSlug(targetDate)}.png` }
  };
}

async function sendReportsToGroup(date) {
  if (!waReady) throw new Error('WhatsApp client not ready');
  if (!botEnabled) {
    console.log('[Report] Bot disabled, skipping send');
    return { skipped: true };
  }

  const r = await generateThreeReports(date);

  const sequence = [
    { name: 'Fund Position', report: r.fundPosition },
    { name: 'Daily Expenditure', report: r.expenditure },
    { name: 'Weekly Analysis', report: r.analysis }
  ];

  for (const { name, report } of sequence) {
    const media = new MessageMedia('image/png', report.png.toString('base64'), report.filename);
    await waClient.sendMessage(GROUP_JID, media, { caption: name });
    await new Promise(res => setTimeout(res, 1500));
  }

  return { sent: sequence.map(s => s.report.filename) };
}

// §9 ===================================================================
//    EXPRESS ENDPOINTS + CRON
// =====================================================================

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => res.json({
  status: 'ok',
  waReady,
  hasQR: !!lastQR,
  botEnabled,
  authPath: AUTH_PATH,
  startupIssues,
  env: {
    hasSheetId: !!SHEET_ID,
    hasGoogleCreds: !!process.env.GOOGLE_CREDENTIALS,
    groupJid: GROUP_JID,
    puppeteerExecPath: process.env.PUPPETEER_EXECUTABLE_PATH || null
  }
}));

app.get('/api/wa-status', (_req, res) => {
  res.json({ ready: waReady, hasQR: !!lastQR, botEnabled });
});

app.get('/api/pair', (_req, res) => {
  if (!lastQR) return res.send('<h2>No QR — already paired or initializing.</h2>');
  res.send(`<h2>Scan with WhatsApp → Linked Devices</h2>
<img src="https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(lastQR)}"/>
<p><small style="word-break:break-all;">${lastQR}</small></p>`);
});

app.post('/api/bot/on',  (_req, res) => { botEnabled = true;  res.json({ botEnabled }); });
app.post('/api/bot/off', (_req, res) => { botEnabled = false; res.json({ botEnabled }); });

app.get('/api/preview', async (req, res) => {
  try {
    const date = req.query.date ? new Date(req.query.date) : new Date();
    const which = req.query.report || 'fund';
    const all = await generateThreeReports(date);
    const map = { fund: 'fundPosition', expenditure: 'expenditure', analysis: 'analysis' };
    const key = map[which] || 'fundPosition';
    res.set('Content-Type', 'text/html');
    res.send(all[key].html);
  } catch (e) {
    res.status(500).send(`<pre>${e.stack}</pre>`);
  }
});

app.get('/api/preview-image', async (req, res) => {
  try {
    const date = req.query.date ? new Date(req.query.date) : new Date();
    const which = req.query.report || 'fund';
    const all = await generateThreeReports(date);
    const map = { fund: 'fundPosition', expenditure: 'expenditure', analysis: 'analysis' };
    const key = map[which] || 'fundPosition';
    res.set('Content-Type', 'image/png');
    res.send(all[key].png);
  } catch (e) {
    res.status(500).send(`<pre>${e.stack}</pre>`);
  }
});

app.get('/api/daily-report', async (req, res) => {
  try {
    const date = req.query.date ? new Date(req.query.date) : new Date();
    const result = await sendReportsToGroup(date);
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/test-send', async (_req, res) => {
  try {
    const result = await sendReportsToGroup(new Date());
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Daily at 06:30 IST → sends *yesterday's* reports
try {
  cron.schedule('30 6 * * *', async () => {
    try {
      console.log('[Cron] Sending daily reports');
      const yesterday = new Date(); yesterday.setDate(yesterday.getDate() - 1);
      await sendReportsToGroup(yesterday);
    } catch (e) {
      console.error('[Cron] Failed:', e && e.message);
    }
  }, { timezone: 'Asia/Kolkata' });
  console.log('[Cron] Scheduled: 06:30 IST daily');
} catch (e) {
  console.error('[Cron] Schedule failed:', e && e.message, '— server will still run, but no auto-send');
}

// Bind to 0.0.0.0 so Railway can reach the port
app.listen(PORT, '0.0.0.0', () => console.log(`[Server] Listening on ${PORT}`));
