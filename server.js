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
const fss = require('fs');
const pathMod = require('path');
app.get('/dashboard', (req, res) => {
  const htmlPath = pathMod.join(__dirname, 'dashboard.html');
  if (fss.existsSync(htmlPath)) {
    res.setHeader('Content-Type', 'text/html');
    res.send(fss.readFileSync(htmlPath, 'utf8'));
  } else {
    res.status(404).send('dashboard.html not found. Upload it to the repo alongside server.js.');
  }
});

app.get('/health', (req, res) => { res.json({ status: 'ok' }); });
app.listen(PORT, () => { console.log(`Fidato MIS Bot on port ${PORT}`); console.log(`Group: ${GROUP_ID || 'ALL'} | OCR: ${CLAUDE_API_KEY ? 'ON' : 'OFF'} | Gupshup Auth: ${GUPSHUP_API_KEY ? 'ON' : 'OFF'}`); console.log(`Dashboard: http://localhost:${PORT}/dashboard`); });
