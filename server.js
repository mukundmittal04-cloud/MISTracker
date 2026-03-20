const express = require('express');
const https = require('https');
const http = require('http');
const app = express();
app.use(express.json());

const VERIFY_TOKEN = process.env.VERIFY_TOKEN || 'fidato_mis_2026';
const PORT = process.env.PORT || 3000;
const GROUP_ID = process.env.GROUP_ID || "";
const CLAUDE_API_KEY = process.env.CLAUDE_API_KEY || "";

const messages = [];
const debugLog = [];

// ═══ THE 5 EXPECTED DAILY REPORTS ═══
const EXPECTED_REPORTS = [
  { type: 'daily_mis', label: 'Daily Cash Sheet', icon: '📅', keywords: ['collection','expenditure','closing balance','opening balance','daily mis'] },
  { type: 'fund_position', label: 'Fund Position', icon: '🏦', keywords: ['fund position','net usable','bank balance','bal as per bank','useable'] },
  { type: 'pdc_bankbook', label: 'PDC & Bank Book', icon: '📄', keywords: ['pdc','bank book','opening balance (incl','vipin kackar','closing balance pdc'] },
  { type: 'site_update', label: 'Site Expenditure & Receivables', icon: '🏗️', keywords: ['expenditure at site','contractor','steel','rmc','floor (normal)','receivable','plot const'] },
  { type: 'projections', label: 'Projected Liabilities', icon: '📊', keywords: ['projected','provision','edc','liability','balance floor','balance plot','unsold'] },
];

// ═══ IMAGE DOWNLOAD ═══
function downloadImage(url) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : http;
    client.get(url, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        return downloadImage(res.headers.location).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    }).on('error', reject);
  });
}

// ═══ CLAUDE OCR ═══
async function extractTextFromImage(imageBuffer, mediaType) {
  if (!CLAUDE_API_KEY) { console.log('  No CLAUDE_API_KEY — skip OCR'); return null; }
  const base64 = imageBuffer.toString('base64');
  const body = JSON.stringify({
    model: "claude-sonnet-4-20250514",
    max_tokens: 4000,
    messages: [{
      role: "user",
      content: [
        { type: "image", source: { type: "base64", media_type: mediaType || 'image/jpeg', data: base64 } },
        { type: "text", text: `This is an MIS (Management Information System) report from a real estate group. Extract ALL financial data from this image. Output:
1. Report type (Daily Cash Sheet / Fund Position / PDC & Bank Book / Site Expenditure & Receivables / Projected Liabilities & Inventory)
2. Date if visible
3. ALL amounts with their heads/labels exactly as shown
4. Totals and balances
5. Key figures: collection, expenditure, closing balance, net usable, outstanding amounts, receivables, provisions
Format as plain text with ₹ symbol. Be exhaustive — capture every number.` }
      ]
    }]
  });
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': CLAUDE_API_KEY, 'anthropic-version': '2023-06-01', 'Content-Length': Buffer.byteLength(body) }
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => { try { const r = JSON.parse(Buffer.concat(chunks).toString()); resolve(r.content?.[0]?.text || null); } catch(e) { reject(e); } });
      res.on('error', reject);
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ═══ MIS PARSER ═══
function parseMIS(text, sender, timestamp, source) {
  const result = {
    raw: text,
    sender, source: source || 'text',
    date: new Date(timestamp * 1000).toISOString().split('T')[0],
    time: new Date(timestamp * 1000).toISOString().split('T')[1].slice(0, 5),
    type: 'general',
    amounts: [], signals: [],
  };
  const lower = text.toLowerCase();

  // Amounts
  for (const m of text.matchAll(/₹?\s*(\d+(?:[.,]\d+)?)\s*(?:cr|crore)/gi))
    result.amounts.push({ value: parseFloat(m[1].replace(/,/g, '')), unit: 'Cr', raw: m[0].trim() });
  for (const m of text.matchAll(/₹?\s*(\d+(?:[.,]\d+)?)\s*(?:l|lakh|lac)/gi))
    result.amounts.push({ value: parseFloat(m[1].replace(/,/g, '')), unit: 'L', raw: m[0].trim() });
  for (const m of text.matchAll(/₹\s*(\d{1,3}(?:,\d{2,3})*(?:\.\d+)?)/g)) {
    const val = parseFloat(m[1].replace(/,/g, ''));
    if (!result.amounts.some(a => m[0].includes(a.raw)))
      result.amounts.push({ value: val, unit: 'Rs', raw: m[0].trim() });
  }
  for (const m of text.matchAll(/(?:collection|expenditure|balance|closing|opening|total|paid|received|outstanding)\s*[:=]?\s*₹?\s*(\d{1,3}(?:,\d{2,3})*(?:\.\d+)?)/gi)) {
    const val = parseFloat(m[1].replace(/,/g, ''));
    if (val > 0 && !result.amounts.some(a => a.value === val))
      result.amounts.push({ value: val, unit: 'Rs', raw: m[0].trim() });
  }

  // Categorize using the expected report keywords
  for (const report of EXPECTED_REPORTS) {
    if (report.keywords.some(kw => lower.includes(kw))) {
      result.type = report.type;
      break;
    }
  }
  // Fallback categories
  if (result.type === 'general') {
    if (lower.includes('mm ') || lower.includes('sm ') || lower.includes('drawing') || lower.includes('directors')) result.type = 'promoter_draw';
    else if (lower.includes('office') || lower.includes('gk-1') || lower.includes('salary')) result.type = 'office_expense';
    else if (lower.includes('rera') || lower.includes('compliance') || lower.includes('noc')) result.type = 'compliance';
    else if (lower.includes('booking') || lower.includes('unsold') || lower.includes('sold')) result.type = 'sales_update';
  }

  // Signals
  for (const k of ['pressing','delay','shortage','risk','overrun','urgent','critical','crunch','deficit','negative','cancel','refund'])
    if (lower.includes(k)) result.signals.push({ type: 'risk', keyword: k });
  for (const k of ['price up','price increase','hike','escalation','costlier'])
    if (lower.includes(k)) result.signals.push({ type: 'cost', keyword: k });

  return result;
}

// ═══ PROCESS IMAGE ═══
async function processImage(body, sender, ts, groupId) {
  try {
    const payload = body.payload || body;
    const mediaUrl = payload.payload?.url || payload.url || payload.mediaUrl || payload.payload?.mediaUrl || body.mediaUrl || '';
    const caption = payload.payload?.caption || payload.caption || body.caption || '';
    const mediaType = payload.payload?.contentType || payload.contentType || 'image/jpeg';
    console.log(`  Image: ${mediaUrl.substring(0, 80)} | Caption: ${caption || 'none'}`);
    if (!mediaUrl) {
      messages.push({ raw: `[Image — no URL] ${caption}`, sender, date: new Date(ts*1000).toISOString().split('T')[0], time: new Date(ts*1000).toISOString().split('T')[1].slice(0,5), type: 'image_no_url', source: 'image', amounts: [], signals: [], groupId });
      return;
    }
    console.log('  Downloading...');
    const buf = await downloadImage(mediaUrl);
    console.log(`  ${buf.length} bytes. Running OCR...`);
    const ocrText = await extractTextFromImage(buf, mediaType);
    if (ocrText) {
      console.log(`  OCR: ${ocrText.substring(0, 120)}...`);
      const parsed = parseMIS(ocrText, sender, ts, 'image_ocr');
      parsed.groupId = groupId;
      parsed.caption = caption;
      parsed.ocrFull = ocrText;
      messages.push(parsed);
      if (messages.length > 2000) messages.shift();
      console.log(`  STORED (OCR): ${parsed.type} | ${parsed.amounts.length} amounts`);
    } else {
      messages.push({ raw: `[Image — OCR failed] ${caption}`, sender, date: new Date(ts*1000).toISOString().split('T')[0], time: new Date(ts*1000).toISOString().split('T')[1].slice(0,5), type: 'image_ocr_failed', source: 'image', amounts: [], signals: [], groupId });
    }
  } catch (err) {
    console.error('  Image error:', err.message);
  }
}

// ═══ WEBHOOK ═══
app.post('/webhook', async (req, res) => {
  res.status(200).send('OK');
  try {
    const body = req.body;
    debugLog.push({ timestamp: new Date().toISOString(), payload: JSON.stringify(body).substring(0, 3000) });
    if (debugLog.length > 50) debugLog.shift();
    console.log('\n--- INCOMING ---');
    console.log(JSON.stringify(body).substring(0, 400));

    if (body.type || body.payload || body.app) {
      const payload = body.payload || body;
      const sender = payload.sender?.name || payload.sender?.phone || payload.source || body.senderName || 'Unknown';
      const ts = body.timestamp ? Math.floor(new Date(body.timestamp).getTime() / 1000) : Math.floor(Date.now() / 1000);
      const msgGroupId = body.waGroupId || body.groupId || payload.waGroupId || payload.groupId || payload.context?.gsId || payload.context?.waGroupId || body.destination || '';

      if (GROUP_ID) {
        if (!msgGroupId || msgGroupId !== GROUP_ID) { console.log('  SKIP — wrong group/direct'); return; }
      }

      const payloadType = payload.payload?.type || payload.type || body.type || '';
      if (payloadType === 'image' || payload.payload?.url || payload.contentType?.startsWith?.('image')) {
        console.log('  IMAGE — OCR pipeline');
        await processImage(body, sender, ts, msgGroupId);
      } else {
        const text = payload.payload?.text || payload.text || payload.body || body.text || '';
        if (text) {
          const parsed = parseMIS(text, sender, ts, 'text');
          parsed.groupId = msgGroupId;
          messages.push(parsed);
          if (messages.length > 2000) messages.shift();
          console.log(`  STORED: ${parsed.type} | ${parsed.amounts.length} amounts`);
        }
      }
    }

    if (body.object === 'whatsapp_business_account') {
      for (const entry of body.entry || []) {
        for (const change of entry.changes || []) {
          if (change.field === 'messages') {
            for (const msg of change.value?.messages || []) {
              const contact = (change.value.contacts || []).find(c => c.wa_id === msg.from);
              const sender = contact?.profile?.name || msg.from;
              if (msg.type === 'text') {
                const parsed = parseMIS(msg.text.body, sender, parseInt(msg.timestamp), 'text');
                messages.push(parsed);
                if (messages.length > 2000) messages.shift();
              }
            }
          }
        }
      }
    }
  } catch (err) { console.error('Webhook error:', err.message); }
});

app.get('/webhook', (req, res) => {
  if (req.query['hub.mode'] === 'subscribe' && req.query['hub.verify_token'] === VERIFY_TOKEN)
    return res.status(200).send(req.query['hub.challenge']);
  res.sendStatus(403);
});

// ═══ API ═══
app.get('/api/messages', (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const type = req.query.type;
  const source = req.query.source;
  const date = req.query.date;
  let f = messages;
  if (type) f = f.filter(m => m.type === type);
  if (source) f = f.filter(m => m.source === source);
  if (date) f = f.filter(m => m.date === date);
  res.json({ count: f.length, messages: f.slice(-limit).reverse() });
});

app.get('/api/signals', (req, res) => {
  const w = messages.filter(m => m.signals.length > 0);
  res.json({ count: w.length, signals: w.slice(-20).reverse() });
});

app.get('/api/ocr', (req, res) => {
  const o = messages.filter(m => m.source === 'image_ocr');
  res.json({ count: o.length, messages: o.slice(-10).reverse().map(m => ({ date: m.date, sender: m.sender, type: m.type, amounts: m.amounts, ocrText: m.ocrFull || m.raw, caption: m.caption })) });
});

// ═══ DAILY REPORT STATUS — the key endpoint ═══
app.get('/api/daily-status', (req, res) => {
  const today = new Date().toISOString().split('T')[0];
  const date = req.query.date || today;
  const dayMsgs = messages.filter(m => m.date === date);

  const reports = EXPECTED_REPORTS.map(r => {
    const received = dayMsgs.filter(m => m.type === r.type);
    const latest = received.length > 0 ? received[received.length - 1] : null;
    return {
      type: r.type,
      label: r.label,
      icon: r.icon,
      status: received.length > 0 ? 'received' : 'missing',
      count: received.length,
      lastReceived: latest ? { time: latest.time, sender: latest.sender, source: latest.source, amounts: latest.amounts.length } : null,
    };
  });

  const receivedCount = reports.filter(r => r.status === 'received').length;
  const missingCount = reports.filter(r => r.status === 'missing').length;

  // Calculate current hour to determine urgency
  const now = new Date();
  const hour = now.getHours();
  let urgency = 'normal';
  if (hour >= 18 && missingCount > 0) urgency = 'critical'; // After 6 PM, missing reports are critical
  else if (hour >= 14 && missingCount > 2) urgency = 'warning'; // After 2 PM, 3+ missing is a warning
  else if (hour >= 11 && missingCount > 3) urgency = 'warning'; // After 11 AM, 4+ missing is a warning

  res.json({
    date,
    totalExpected: EXPECTED_REPORTS.length,
    received: receivedCount,
    missing: missingCount,
    completionPct: Math.round((receivedCount / EXPECTED_REPORTS.length) * 100),
    urgency,
    reports,
    allMessages: dayMsgs.length,
    textMessages: dayMsgs.filter(m => m.source === 'text').length,
    imageOCR: dayMsgs.filter(m => m.source === 'image_ocr').length,
    risks: dayMsgs.filter(m => m.signals.length > 0).length,
  });
});

app.get('/api/summary', (req, res) => {
  const today = new Date().toISOString().split('T')[0];
  const date = req.query.date || today;
  const dayMsgs = messages.filter(m => m.date === date);
  res.json({
    date, total: dayMsgs.length,
    textMessages: dayMsgs.filter(m => m.source === 'text').length,
    imageOCR: dayMsgs.filter(m => m.source === 'image_ocr').length,
    byType: dayMsgs.reduce((a, m) => { a[m.type] = (a[m.type] || 0) + 1; return a; }, {}),
    risks: dayMsgs.filter(m => m.signals.length > 0).length,
  });
});

app.get('/api/debug', (req, res) => {
  res.json({
    instructions: "Find 'waGroupId'/'groupId' in payloads. Set as GROUP_ID in Railway.",
    currentFilter: GROUP_ID || "NONE",
    claudeOCR: CLAUDE_API_KEY ? "ENABLED" : "DISABLED",
    totalStored: messages.length,
    recentGroups: [...new Set(messages.map(m => m.groupId).filter(Boolean))],
    recentPayloads: debugLog.slice(-10).reverse(),
  });
});

app.get('/', (req, res) => {
  res.json({ status: 'running', app: 'Fidato MIS Bot', messages: messages.length, groupFilter: GROUP_ID || 'all', ocrEnabled: !!CLAUDE_API_KEY, uptime: Math.floor(process.uptime()) + 's' });
});

app.get('/health', (req, res) => { res.json({ status: 'ok', messages: messages.length, ocr: !!CLAUDE_API_KEY }); });

app.listen(PORT, () => {
  console.log(`Fidato MIS Bot on port ${PORT}`);
  console.log(`Group: ${GROUP_ID || 'ALL'} | OCR: ${CLAUDE_API_KEY ? 'ON' : 'OFF'}`);
});
