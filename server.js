const express = require('express');
const https = require('https');
const http = require('http');
const app = express();
app.use(express.json());

const VERIFY_TOKEN = process.env.VERIFY_TOKEN || 'fidato_mis_2026';
const PORT = process.env.PORT || 3000;
const GROUP_ID = process.env.GROUP_ID || "";
const CLAUDE_API_KEY = process.env.CLAUDE_API_KEY || "";
const GUPSHUP_API_KEY = process.env.GUPSHUP_API_KEY || "";

const messages = [];
const debugLog = [];

const EXPECTED_REPORTS = [
  { type: 'daily_mis', label: 'Daily Cash Sheet', icon: '📅', keywords: ['collection','expenditure','closing balance','opening balance','daily mis'] },
  { type: 'fund_position', label: 'Fund Position', icon: '🏦', keywords: ['fund position','net usable','bank balance','bal as per bank','useable'] },
  { type: 'pdc_bankbook', label: 'PDC & Bank Book', icon: '📄', keywords: ['pdc','bank book','vipin kackar','closing balance pdc'] },
  { type: 'site_update', label: 'Site & Receivables', icon: '🏗️', keywords: ['expenditure at site','contractor','steel','rmc','floor (normal)','receivable','plot const'] },
  { type: 'projections', label: 'Projected Liabilities', icon: '📊', keywords: ['projected','provision','edc','liability','balance floor','balance plot','unsold'] },
];

// ═══ IMAGE DOWNLOAD — with Gupshup auth ═══
function downloadImage(url) {
  return new Promise((resolve, reject) => {
    // Add apikey to Gupshup URLs
    let fetchUrl = url;
    if (url.includes('filemanager.gupshup.io') || url.includes('gupshup')) {
      const sep = url.includes('?') ? '&' : '?';
      fetchUrl = url.replace('download=false', 'download=true');
      if (!fetchUrl.includes('download=true')) fetchUrl += sep + 'download=true';
    }
    
    const urlObj = new URL(fetchUrl);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: 'GET',
      headers: {
        'Accept': 'image/jpeg,image/*,*/*',
        'User-Agent': 'MISTracker/1.0',
      }
    };
    
    // Add Gupshup API key for their file manager
    if (GUPSHUP_API_KEY && (url.includes('gupshup') || url.includes('filemanager'))) {
      options.headers['apikey'] = GUPSHUP_API_KEY;
      options.headers['token'] = GUPSHUP_API_KEY;
      options.headers['Authorization'] = 'Bearer ' + GUPSHUP_API_KEY;
    }
    
    console.log(`  Fetching: ${fetchUrl.substring(0, 120)}...`);
    
    const client = fetchUrl.startsWith('https') ? https : http;
    const req = client.request(options, (res) => {
      console.log(`  Download status: ${res.statusCode} ${res.statusMessage}`);
      if (res.statusCode === 301 || res.statusCode === 302 || res.statusCode === 307) {
        console.log(`  Redirect to: ${res.headers.location}`);
        return downloadImage(res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode === 401 || res.statusCode === 403) {
        console.log(`  Auth failed! Headers sent: apikey=${GUPSHUP_API_KEY ? 'SET' : 'MISSING'}`);
        return reject(new Error(`Auth failed: ${res.statusCode}`));
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`));
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        console.log(`  Downloaded: ${buf.length} bytes, content-type: ${res.headers['content-type']}`);
        resolve(buf);
      });
      res.on('error', reject);
    });
    req.on('error', (err) => {
      console.log(`  Download error: ${err.message}`);
      reject(err);
    });
    req.setTimeout(15000, () => {
      console.log('  Download timeout after 15s');
      req.destroy(new Error('Timeout'));
    });
    req.end();
  });
}

// ═══ CLAUDE OCR ═══
async function extractTextFromImage(imageBuffer, mediaType) {
  if (!CLAUDE_API_KEY) { console.log('  No CLAUDE_API_KEY'); return null; }
  const base64 = imageBuffer.toString('base64');
  const body = JSON.stringify({
    model: "claude-sonnet-4-20250514",
    max_tokens: 4000,
    messages: [{
      role: "user",
      content: [
        { type: "image", source: { type: "base64", media_type: mediaType || 'image/jpeg', data: base64 } },
        { type: "text", text: `This is an MIS report from a real estate group. Extract ALL financial data. Output: 1) Report type 2) Date 3) ALL amounts with labels 4) Totals and balances 5) Key figures. Use ₹ symbol. Be exhaustive.` }
      ]
    }]
  });
  console.log(`  Calling Claude API (${(Buffer.byteLength(body)/1024).toFixed(0)}KB payload)...`);
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': CLAUDE_API_KEY, 'anthropic-version': '2023-06-01', 'Content-Length': Buffer.byteLength(body) }
    }, (res) => {
      console.log(`  Claude API status: ${res.statusCode}`);
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const raw = Buffer.concat(chunks).toString();
        try {
          const r = JSON.parse(raw);
          if (r.error) { console.log(`  Claude error: ${r.error.message || JSON.stringify(r.error)}`); resolve(null); return; }
          const text = r.content?.[0]?.text || null;
          if (text) console.log(`  Claude OCR success: ${text.substring(0, 100)}...`);
          else console.log(`  Claude returned no text: ${raw.substring(0, 200)}`);
          resolve(text);
        } catch(e) { console.log(`  Claude parse error: ${e.message}, raw: ${raw.substring(0, 200)}`); reject(e); }
      });
      res.on('error', reject);
    });
    req.on('error', (err) => { console.log(`  Claude request error: ${err.message}`); reject(err); });
    req.setTimeout(60000, () => { console.log('  Claude timeout 60s'); req.destroy(new Error('Claude timeout')); });
    req.write(body);
    req.end();
  });
}

// ═══ MIS PARSER ═══
function parseMIS(text, sender, timestamp, source) {
  const result = { raw: text, sender, source: source || 'text', date: new Date(timestamp * 1000).toISOString().split('T')[0], time: new Date(timestamp * 1000).toISOString().split('T')[1].slice(0, 5), type: 'general', amounts: [], signals: [] };
  const lower = text.toLowerCase();
  for (const m of text.matchAll(/₹?\s*(\d+(?:[.,]\d+)?)\s*(?:cr|crore)/gi)) result.amounts.push({ value: parseFloat(m[1].replace(/,/g, '')), unit: 'Cr', raw: m[0].trim() });
  for (const m of text.matchAll(/₹?\s*(\d+(?:[.,]\d+)?)\s*(?:l|lakh|lac)/gi)) result.amounts.push({ value: parseFloat(m[1].replace(/,/g, '')), unit: 'L', raw: m[0].trim() });
  for (const m of text.matchAll(/₹\s*(\d{1,3}(?:,\d{2,3})*(?:\.\d+)?)/g)) { const val = parseFloat(m[1].replace(/,/g, '')); if (!result.amounts.some(a => m[0].includes(a.raw))) result.amounts.push({ value: val, unit: 'Rs', raw: m[0].trim() }); }
  for (const report of EXPECTED_REPORTS) { if (report.keywords.some(kw => lower.includes(kw))) { result.type = report.type; break; } }
  if (result.type === 'general') {
    if (lower.includes('mm ') || lower.includes('sm ') || lower.includes('drawing')) result.type = 'promoter_draw';
    else if (lower.includes('office') || lower.includes('gk-1') || lower.includes('salary')) result.type = 'office_expense';
    else if (lower.includes('booking') || lower.includes('unsold')) result.type = 'sales_update';
  }
  for (const k of ['pressing','delay','shortage','risk','overrun','urgent','critical','crunch','deficit','negative','cancel','refund']) if (lower.includes(k)) result.signals.push({ type: 'risk', keyword: k });
  for (const k of ['price up','price increase','hike','escalation']) if (lower.includes(k)) result.signals.push({ type: 'cost', keyword: k });
  return result;
}

// ═══ PROCESS IMAGE ═══
async function processImage(body, sender, ts, groupId) {
  try {
    const payload = body.payload || body;
    const mediaUrl = payload.payload?.url || payload.url || payload.mediaUrl || payload.payload?.mediaUrl || body.mediaUrl || '';
    const caption = payload.payload?.caption || payload.caption || body.caption || '';
    const mediaType = payload.payload?.contentType || payload.contentType || 'image/jpeg';
    console.log(`  Image URL: ${mediaUrl.substring(0, 120)}`);
    if (!mediaUrl) { messages.push({ raw: `[Image — no URL] ${caption}`, sender, date: new Date(ts*1000).toISOString().split('T')[0], time: new Date(ts*1000).toISOString().split('T')[1].slice(0,5), type: 'image_no_url', source: 'image', amounts: [], signals: [], groupId }); return; }
    const buf = await downloadImage(mediaUrl);
    if (buf.length < 1000) { console.log(`  Image too small (${buf.length}B) — likely error page`); messages.push({ raw: `[Image — download returned ${buf.length}B, likely error]`, sender, date: new Date(ts*1000).toISOString().split('T')[0], time: new Date(ts*1000).toISOString().split('T')[1].slice(0,5), type: 'image_download_error', source: 'image', amounts: [], signals: [], groupId }); return; }
    const ocrText = await extractTextFromImage(buf, mediaType);
    if (ocrText) {
      const parsed = parseMIS(ocrText, sender, ts, 'image_ocr');
      parsed.groupId = groupId; parsed.caption = caption; parsed.ocrFull = ocrText;
      messages.push(parsed);
      if (messages.length > 2000) messages.shift();
      console.log(`  STORED (OCR): ${parsed.type} | ${parsed.amounts.length} amounts`);
    } else {
      messages.push({ raw: `[Image — OCR failed] ${caption}`, sender, date: new Date(ts*1000).toISOString().split('T')[0], time: new Date(ts*1000).toISOString().split('T')[1].slice(0,5), type: 'image_ocr_failed', source: 'image', amounts: [], signals: [], groupId });
    }
  } catch (err) { console.error(`  Image error: ${err.message}`); messages.push({ raw: `[Image error: ${err.message}]`, sender, date: new Date(ts*1000).toISOString().split('T')[0], time: new Date(ts*1000).toISOString().split('T')[1].slice(0,5), type: 'image_error', source: 'image', amounts: [], signals: [] }); }
}

// ═══ WEBHOOK ═══
app.post('/webhook', async (req, res) => {
  res.status(200).send('OK');
  try {
    const body = req.body;
    debugLog.push({ timestamp: new Date().toISOString(), payload: JSON.stringify(body).substring(0, 3000) }); if (debugLog.length > 50) debugLog.shift();
    console.log('\n--- INCOMING ---');
    console.log(JSON.stringify(body).substring(0, 400));
    if (body.type || body.payload || body.app) {
      const payload = body.payload || body;
      const sender = payload.sender?.name || payload.sender?.phone || payload.source || body.senderName || 'Unknown';
      const ts = body.timestamp ? Math.floor(new Date(body.timestamp).getTime() / 1000) : Math.floor(Date.now() / 1000);
      const msgGroupId = body.waGroupId || body.groupId || payload.waGroupId || payload.groupId || '';
      if (GROUP_ID && msgGroupId && msgGroupId !== GROUP_ID) { console.log('  SKIP group'); return; }
      const payloadType = payload.payload?.type || payload.type || body.type || '';
      if (payloadType === 'image' || payload.payload?.url || (payload.contentType && payload.contentType.startsWith('image'))) {
        console.log('  IMAGE — OCR pipeline');
        await processImage(body, sender, ts, msgGroupId);
      } else {
        const text = payload.payload?.text || payload.text || payload.body || body.text || '';
        if (text) { const parsed = parseMIS(text, sender, ts, 'text'); parsed.groupId = msgGroupId; messages.push(parsed); if (messages.length > 2000) messages.shift(); console.log(`  STORED: ${parsed.type}`); }
      }
    }
    if (body.object === 'whatsapp_business_account') { for (const entry of body.entry || []) { for (const change of entry.changes || []) { if (change.field === 'messages') { for (const msg of change.value?.messages || []) { if (msg.type === 'text') { const contact = (change.value.contacts || []).find(c => c.wa_id === msg.from); const sender = contact?.profile?.name || msg.from; const parsed = parseMIS(msg.text.body, sender, parseInt(msg.timestamp), 'text'); messages.push(parsed); if (messages.length > 2000) messages.shift(); } } } } } }
  } catch (err) { console.error('Webhook error:', err.message); }
});
app.get('/webhook', (req, res) => { if (req.query['hub.mode'] === 'subscribe' && req.query['hub.verify_token'] === VERIFY_TOKEN) return res.status(200).send(req.query['hub.challenge']); res.sendStatus(403); });

// ═══ API ═══
app.get('/api/messages', (req, res) => { const limit = parseInt(req.query.limit) || 50; const type = req.query.type; const source = req.query.source; const date = req.query.date; let f = messages; if (type) f = f.filter(m => m.type === type); if (source) f = f.filter(m => m.source === source); if (date) f = f.filter(m => m.date === date); res.json({ count: f.length, messages: f.slice(-limit).reverse() }); });
app.get('/api/signals', (req, res) => { res.json({ count: messages.filter(m => m.signals.length > 0).length, signals: messages.filter(m => m.signals.length > 0).slice(-20).reverse() }); });
app.get('/api/ocr', (req, res) => { const o = messages.filter(m => m.source === 'image_ocr'); res.json({ count: o.length, messages: o.slice(-10).reverse().map(m => ({ date: m.date, sender: m.sender, type: m.type, amounts: m.amounts, ocrText: m.ocrFull || m.raw })) }); });
app.get('/api/daily-status', (req, res) => { const today = new Date().toISOString().split('T')[0]; const date = req.query.date || today; const dayMsgs = messages.filter(m => m.date === date); const reports = EXPECTED_REPORTS.map(r => { const received = dayMsgs.filter(m => m.type === r.type); const latest = received.length > 0 ? received[received.length - 1] : null; return { type: r.type, label: r.label, icon: r.icon, status: received.length > 0 ? 'received' : 'missing', count: received.length, lastReceived: latest ? { time: latest.time, sender: latest.sender, source: latest.source, amounts: latest.amounts.length } : null }; }); const rc = reports.filter(r => r.status === 'received').length; const mc = reports.filter(r => r.status === 'missing').length; const hour = new Date().getHours(); let urgency = 'normal'; if (hour >= 18 && mc > 0) urgency = 'critical'; else if (hour >= 14 && mc > 2) urgency = 'warning'; res.json({ date, totalExpected: 5, received: rc, missing: mc, completionPct: Math.round((rc / 5) * 100), urgency, reports, allMessages: dayMsgs.length }); });
app.get('/api/debug', (req, res) => { res.json({ currentFilter: GROUP_ID || "NONE", claudeOCR: CLAUDE_API_KEY ? "ENABLED" : "DISABLED", gupshupKey: GUPSHUP_API_KEY ? "SET" : "MISSING", totalStored: messages.length, recentPayloads: debugLog.slice(-10).reverse() }); });
app.get('/', (req, res) => { res.json({ status: 'running', app: 'Fidato MIS Bot', messages: messages.length, groupFilter: GROUP_ID || 'all', ocrEnabled: !!CLAUDE_API_KEY, gupshupAuth: !!GUPSHUP_API_KEY, uptime: Math.floor(process.uptime()) + 's' }); });

// ═══ LIVE DASHBOARD ═══
app.get('/dashboard', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Fidato Group — MIS Tracker</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'DM Sans',system-ui,sans-serif;background:#F7F5F2;color:#1C1917;min-height:100vh}
.hdr{background:#1C1917;color:#fff;padding:20px 24px 14px}
.hdr h1{font-size:22px;font-weight:800}
.hdr .sub{font-size:10px;color:#A8A29E;margin-top:3px}
.health{background:rgba(220,38,38,.2);border:1px solid rgba(220,38,38,.4);border-radius:8px;padding:6px 14px;text-align:center;font-size:14px;font-weight:800;color:#FCA5A5}
.tabs{display:flex;gap:3px;margin-top:12px;overflow-x:auto}
.tabs button{background:transparent;color:#78716C;border:none;border-radius:6px;padding:7px 12px;font-size:11px;font-weight:600;cursor:pointer;white-space:nowrap}
.tabs button.active{background:rgba(255,255,255,.12);color:#fff}
.main{padding:14px;max-width:1100px;margin:0 auto}
.grid{display:grid;gap:10px;margin-bottom:14px}
.g4{grid-template-columns:repeat(auto-fit,minmax(170px,1fr))}
.g2{grid-template-columns:1fr 1fr}
.card{background:#fff;border-radius:14px;border:1px solid #E5E0D8;padding:18px 20px;position:relative;overflow:hidden}
.card .bar{position:absolute;top:0;left:0;right:0;height:3px}
.card .lbl{font-size:10px;color:#8C857D;letter-spacing:.1em;text-transform:uppercase;font-family:'JetBrains Mono',monospace;margin-bottom:6px}
.card .val{font-size:24px;font-weight:800;line-height:1.1}
.card .sub{font-size:11px;color:#8C857D;margin-top:5px}
.warn{color:#DC2626!important}
.alert{border-radius:10px;padding:12px 14px;margin-bottom:8px;border-left:4px solid}
.alert.critical{background:#FEF2F2;border-color:#DC2626}.alert.critical .t{color:#DC2626}
.alert.warning{background:#FFFBEB;border-color:#D97706}.alert.warning .t{color:#D97706}
.alert.info{background:#EFF6FF;border-color:#2563EB}.alert.info .t{color:#2563EB}
.alert .t{font-weight:700;font-size:12px;margin-bottom:3px}
.alert .d{font-size:11px;color:#374151;line-height:1.5}
.rpt-grid{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px}
.rpt{flex:1 1 110px;border-radius:8px;padding:8px 10px;text-align:center}
.rpt.ok{background:#ECFDF5;border:1px solid #A7F3D0}
.rpt.miss{background:#FEF2F2;border:1px solid #FECACA}
.rpt .ic{font-size:14px;margin-bottom:2px}
.rpt .nm{font-size:9px;font-weight:600}
.rpt.ok .nm{color:#065F46}.rpt.miss .nm{color:#991B1B}
.rpt .st{font-size:8px;font-family:'JetBrains Mono',monospace;margin-top:2px}
.rpt.ok .st{color:#2D6A4F}.rpt.miss .st{color:#DC2626}
.wa-bubble{background:#DCF8C6;border-radius:10px 10px 10px 3px;padding:8px 12px;margin-bottom:6px;box-shadow:0 1px 2px rgba(0,0,0,.05)}
.wa-bubble .sn{font-size:10px;font-weight:700;color:#075E54;margin-bottom:3px}
.wa-bubble .msg{font-size:11px;color:#111;line-height:1.5;max-height:120px;overflow:hidden}
.wa-bubble .dt{font-size:9px;color:#888;text-align:right;margin-top:3px}
.sig{border-radius:8px;padding:8px 10px;margin-bottom:5px;font-size:10px}
.sig .sl{font-size:8px;font-weight:700;font-family:'JetBrains Mono',monospace;text-transform:uppercase}
.sig .sm{font-size:10px;color:#1F2937;line-height:1.4;margin-top:2px;max-height:60px;overflow:hidden}
.mono{font-family:'JetBrains Mono',monospace}
.status-bar{display:flex;align-items:center;gap:8px;margin-bottom:10px}
.dot{width:10px;height:10px;border-radius:50%}
.dot.on{background:#2D6A4F;box-shadow:0 0 6px rgba(45,106,79,.5)}
.dot.off{background:#DC2626}
.dot.wait{background:#D4A843}
.refresh-btn{background:#1C1917;color:#fff;border:none;border-radius:6px;padding:6px 12px;font-size:10px;font-weight:600;cursor:pointer}
table{width:100%;border-collapse:collapse;font-size:11px}
th{padding:9px 12px;text-align:left;font-weight:600;font-size:9px;text-transform:uppercase;letter-spacing:.08em;color:#8C857D;font-family:'JetBrains Mono',monospace;background:#F5F3F0}
td{padding:8px 12px;border-top:1px solid #E5E0D8}
.hidden{display:none}
</style></head><body>
<div class="hdr">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px">
    <div>
      <div style="font-size:9px;letter-spacing:.15em;text-transform:uppercase;color:#78716C;font-family:'JetBrains Mono',monospace;margin-bottom:3px">Live MIS Tracker</div>
      <h1>Fidato Group</h1>
      <div class="sub">Fidatocity · Trinity · Hansaflon · Dholpur · Auto-refreshes every 30s</div>
    </div>
    <div class="health" id="health-badge">LOADING</div>
  </div>
  <div class="tabs" id="tabs">
    <button class="active" data-tab="overview">◉ Overview</button>
    <button data-tab="messages">💬 Messages</button>
    <button data-tab="ocr">📸 OCR Data</button>
    <button data-tab="signals">⚠️ Signals</button>
  </div>
</div>

<div class="main">
  <!-- OVERVIEW TAB -->
  <div id="tab-overview">
    <div id="report-status" class="card" style="margin-bottom:14px"></div>
    <div class="grid g4" id="metrics"></div>
    <div id="alerts"></div>
  </div>

  <!-- MESSAGES TAB -->
  <div id="tab-messages" class="hidden">
    <div class="status-bar">
      <div class="dot wait" id="live-dot"></div>
      <div>
        <div style="font-size:11px;font-weight:600" id="live-label">Connecting...</div>
        <div style="font-size:9px;color:#8C857D" class="mono" id="live-detail">Waiting for first sync...</div>
      </div>
      <button class="refresh-btn" onclick="fetchAll()">Refresh Now</button>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
      <div>
        <div style="font-size:9px;font-weight:600;color:#8C857D;font-family:'JetBrains Mono',monospace;text-transform:uppercase;margin-bottom:6px">Messages (<span id="msg-count">0</span>)</div>
        <div id="msg-list" style="background:#E5DDD5;border-radius:10px;padding:8px;max-height:500px;overflow-y:auto"></div>
      </div>
      <div>
        <div style="font-size:9px;font-weight:600;color:#8C857D;font-family:'JetBrains Mono',monospace;text-transform:uppercase;margin-bottom:6px">Signals (<span id="sig-count">0</span>)</div>
        <div id="sig-list"></div>
      </div>
    </div>
  </div>

  <!-- OCR TAB -->
  <div id="tab-ocr" class="hidden">
    <div id="ocr-list"></div>
  </div>

  <!-- SIGNALS TAB -->
  <div id="tab-signals" class="hidden">
    <div id="signals-full"></div>
  </div>
</div>

<script>
const SIG_STYLES={cost_escalation:{bg:'#FEF3C7',brd:'#FDE68A',c:'#92400E',l:'Cost'},risk:{bg:'#FEF2F2',brd:'#FECACA',c:'#991B1B',l:'Risk'},revenue:{bg:'#ECFDF5',brd:'#A7F3D0',c:'#065F46',l:'Revenue'},progress:{bg:'#EFF6FF',brd:'#BFDBFE',c:'#1E40AF',l:'Progress'},compliance:{bg:'#F5F3FF',brd:'#DDD6FE',c:'#5B21B6',l:'Compliance'},cost_info:{bg:'#FFF7ED',brd:'#FED7AA',c:'#9A3412',l:'Cost Info'},cost:{bg:'#FEF3C7',brd:'#FDE68A',c:'#92400E',l:'Cost'}};
let allMessages=[],dailyStatus=null,signals=[];

// Tab switching
document.querySelectorAll('.tabs button').forEach(btn=>{
  btn.addEventListener('click',()=>{
    document.querySelectorAll('.tabs button').forEach(b=>b.classList.remove('active'));
    btn.classList.add('active');
    ['overview','messages','ocr','signals'].forEach(t=>{
      document.getElementById('tab-'+t).classList.toggle('hidden',t!==btn.dataset.tab);
    });
  });
});

function fmt(n){return '₹'+Math.round(n).toLocaleString('en-IN')}

async function fetchAll(){
  try{
    const [msgR,statusR,sigR]=await Promise.all([
      fetch('/api/messages?limit=100'),
      fetch('/api/daily-status'),
      fetch('/api/signals'),
    ]);
    if(msgR.ok){const d=await msgR.json();allMessages=d.messages||[];document.getElementById('msg-count').textContent=d.count;}
    if(statusR.ok){dailyStatus=await statusR.json();}
    if(sigR.ok){const d=await sigR.json();signals=d.signals||[];}
    document.getElementById('live-dot').className='dot on';
    document.getElementById('live-label').textContent='Live — Connected';
    document.getElementById('live-label').style.color='#2D6A4F';
    document.getElementById('live-detail').textContent='Last sync: '+new Date().toLocaleTimeString()+' · '+allMessages.length+' messages';
    render();
  }catch(e){
    document.getElementById('live-dot').className='dot off';
    document.getElementById('live-label').textContent='Disconnected';
    document.getElementById('live-label').style.color='#DC2626';
  }
}

function render(){
  // Health badge
  const hb=document.getElementById('health-badge');
  if(dailyStatus){
    if(dailyStatus.urgency==='critical'){hb.textContent='CRITICAL';hb.style.color='#FCA5A5';}
    else if(dailyStatus.urgency==='warning'){hb.textContent='CAUTION';hb.style.color='#FDE68A';}
    else if(dailyStatus.completionPct===100){hb.textContent='ALL RECEIVED';hb.style.color='#86EFAC';hb.style.background='rgba(45,106,79,.2)';hb.style.borderColor='rgba(45,106,79,.4)';}
    else{hb.textContent=dailyStatus.received+'/5 REPORTS';hb.style.color='#FDE68A';}
  }

  // Report status
  if(dailyStatus){
    let html='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px"><div style="font-size:12px;font-weight:700">Today\\'s MIS — '+dailyStatus.received+'/'+dailyStatus.totalExpected+' received</div>';
    html+='<div style="display:flex;align-items:center;gap:6px"><div style="background:#E5E0D8;border-radius:3px;height:6px;width:120px;overflow:hidden"><div style="background:'+(dailyStatus.completionPct===100?'#2D6A4F':'#D4A843')+';height:100%;width:'+dailyStatus.completionPct+'%;border-radius:3px"></div></div><span class="mono" style="font-size:10px;font-weight:700;color:'+(dailyStatus.completionPct===100?'#2D6A4F':'#D4A843')+'">'+dailyStatus.completionPct+'%</span></div></div>';
    html+='<div class="rpt-grid">';
    dailyStatus.reports.forEach(r=>{
      const ok=r.status==='received';
      html+='<div class="rpt '+(ok?'ok':'miss')+'"><div class="ic">'+r.icon+'</div><div class="nm">'+r.label+'</div><div class="st">'+(ok?'✓ '+r.lastReceived.time+' · '+(r.lastReceived.source==='image_ocr'?'IMG':'TXT'):'✗ NOT RECEIVED')+'</div></div>';
    });
    html+='</div>';
    if(dailyStatus.urgency==='critical'&&dailyStatus.missing>0){
      html+='<div style="background:#FEF2F2;border-radius:6px;padding:6px 10px;font-size:10px;color:#991B1B;border-left:3px solid #DC2626">'+dailyStatus.missing+' report(s) missing: '+dailyStatus.reports.filter(r=>r.status==='missing').map(r=>r.label).join(', ')+'</div>';
    }
    document.getElementById('report-status').innerHTML=html;
  }

  // Metrics
  const ocrMsgs=allMessages.filter(m=>m.source==='image_ocr');
  const totalAmounts=allMessages.reduce((s,m)=>s+(m.amounts||[]).length,0);
  const riskCount=allMessages.filter(m=>(m.signals||[]).length>0).length;
  document.getElementById('metrics').innerHTML=[
    {l:'Total Messages',v:allMessages.length,s:'Today',a:'#4A7FB5'},
    {l:'OCR Processed',v:ocrMsgs.length,s:'Images parsed',a:'#2D6A4F'},
    {l:'Amounts Extracted',v:totalAmounts,s:'Financial data points',a:'#D4A843'},
    {l:'Risk Signals',v:riskCount,s:'Flagged messages',a:'#DC2626',w:riskCount>0},
  ].map(m=>'<div class="card"><div class="bar" style="background:'+m.a+'"></div><div class="lbl">'+m.l+'</div><div class="val'+(m.w?' warn':'')+'">'+m.v+'</div><div class="sub">'+m.s+'</div></div>').join('');

  // Alerts from signals
  let alertHtml='<div style="font-size:12px;font-weight:700;margin-bottom:8px">⚡ Live Signals</div>';
  signals.slice(0,10).forEach(s=>{
    const st=SIG_STYLES[s.signals?.[0]?.type||'risk']||SIG_STYLES.risk;
    alertHtml+='<div class="alert '+(s.signals?.[0]?.type==='risk'?'critical':'warning')+'"><div class="t">'+(s.signals?.map(x=>x.keyword).join(', ')||'signal')+' — '+s.sender+'</div><div class="d">'+((s.raw||s.message||'').substring(0,200))+'</div></div>';
  });
  if(signals.length===0) alertHtml+='<div style="font-size:11px;color:#8C857D">No risk signals detected today.</div>';
  document.getElementById('alerts').innerHTML=alertHtml;

  // Messages list
  let msgHtml='';
  allMessages.forEach(m=>{
    const txt=(m.raw||m.message||'').substring(0,300);
    const src=m.source==='image_ocr'?'<span style="background:#ECFDF5;color:#065F46;font-size:8px;padding:1px 4px;border-radius:3px;margin-left:4px">IMG OCR</span>':'';
    msgHtml+='<div class="wa-bubble"><div class="sn">'+m.sender+src+'</div><div class="msg">'+txt.replace(/\\n/g,'<br>').substring(0,300)+'</div><div class="dt">'+m.date+' '+m.time+' · '+m.type+'</div></div>';
  });
  document.getElementById('msg-list').innerHTML=msgHtml||'<div style="text-align:center;padding:20px;color:#78716C;font-size:11px">No messages yet</div>';

  // Signals list
  let sigHtml='';
  document.getElementById('sig-count').textContent=signals.length;
  signals.forEach(s=>{
    const st=SIG_STYLES[s.signals?.[0]?.type||'risk']||SIG_STYLES.risk;
    sigHtml+='<div class="sig" style="background:'+st.bg+';border:1px solid '+st.brd+'"><div class="sl" style="color:'+st.c+'">'+st.l+' · '+s.sender+'</div><div class="sm">'+((s.raw||'').substring(0,150))+'</div></div>';
  });
  document.getElementById('sig-list').innerHTML=sigHtml||'<div style="font-size:11px;color:#8C857D;padding:10px">No signals</div>';

  // OCR tab
  let ocrHtml='<div style="font-size:14px;font-weight:700;margin-bottom:10px">OCR Extracted Reports ('+ocrMsgs.length+')</div>';
  ocrMsgs.forEach(m=>{
    ocrHtml+='<div class="card" style="margin-bottom:10px"><div style="display:flex;justify-content:space-between;margin-bottom:8px"><span style="font-size:12px;font-weight:700">'+m.type+'</span><span class="mono" style="font-size:10px;color:#8C857D">'+m.date+' '+m.time+' · '+(m.amounts||[]).length+' amounts</span></div>';
    ocrHtml+='<div style="font-size:11px;line-height:1.6;max-height:300px;overflow-y:auto;white-space:pre-wrap">'+((m.raw||'').substring(0,2000).replace(/</g,'&lt;'))+'</div></div>';
  });
  if(ocrMsgs.length===0) ocrHtml+='<div style="font-size:11px;color:#8C857D">No OCR data yet. Send MIS screenshots to the bot.</div>';
  document.getElementById('ocr-list').innerHTML=ocrHtml;

  // Signals full tab
  let sfHtml='<div style="font-size:14px;font-weight:700;margin-bottom:10px">All Risk Signals ('+signals.length+')</div>';
  signals.forEach(s=>{
    const st=SIG_STYLES[s.signals?.[0]?.type||'risk']||SIG_STYLES.risk;
    sfHtml+='<div class="card" style="margin-bottom:8px;border-left:4px solid '+st.c+'"><div style="font-size:11px;font-weight:700;color:'+st.c+';margin-bottom:4px">'+st.l+' — '+(s.signals?.map(x=>x.keyword).join(', ')||'')+'</div><div style="font-size:11px;color:#374151;line-height:1.5">'+((s.raw||'').substring(0,300))+'</div><div style="font-size:9px;color:#8C857D;margin-top:4px">'+s.sender+' · '+s.date+'</div></div>';
  });
  if(signals.length===0) sfHtml+='<div style="font-size:11px;color:#8C857D">No signals detected.</div>';
  document.getElementById('signals-full').innerHTML=sfHtml;
}

// Auto-fetch every 30 seconds
fetchAll();
setInterval(fetchAll,30000);
</script>
</body></html>`);
});

app.get('/health', (req, res) => { res.json({ status: 'ok' }); });
app.listen(PORT, () => { console.log(`Fidato MIS Bot on port ${PORT}`); console.log(`Group: ${GROUP_ID || 'ALL'} | OCR: ${CLAUDE_API_KEY ? 'ON' : 'OFF'} | Gupshup Auth: ${GUPSHUP_API_KEY ? 'ON' : 'OFF'}`); console.log(`Dashboard: http://localhost:${PORT}/dashboard`); });
