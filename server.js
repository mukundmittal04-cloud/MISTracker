const express = require('express');
const app = express();
app.use(express.json());

const VERIFY_TOKEN = process.env.VERIFY_TOKEN || 'fidato_mis_2026';
const PORT = process.env.PORT || 3000;

// In-memory message store
const messages = [];

// ═══ MIS PARSER ═══
function parseMIS(text, sender, timestamp) {
  const result = {
    raw: text,
    sender: sender,
    date: new Date(timestamp * 1000).toISOString().split('T')[0],
    time: new Date(timestamp * 1000).toISOString().split('T')[1].slice(0,5),
    type: 'general',
    amounts: [],
    signals: [],
  };

  const lower = text.toLowerCase();

  // Extract amounts
  const crMatch = text.matchAll(/₹?\s*(\d+(?:\.\d+)?)\s*(?:cr|crore)/gi);
  for (const m of crMatch) result.amounts.push({ value: parseFloat(m[1]), unit: 'Cr' });
  
  const lMatch = text.matchAll(/₹?\s*(\d+(?:\.\d+)?)\s*(?:l|lakh|lac)/gi);
  for (const m of lMatch) result.amounts.push({ value: parseFloat(m[1]), unit: 'L' });

  const rMatch = text.matchAll(/₹\s*(\d{1,3}(?:,\d{2,3})*(?:\.\d+)?)/g);
  for (const m of rMatch) result.amounts.push({ value: parseFloat(m[1].replace(/,/g, '')), unit: 'Rs' });

  // Categorize
  if (lower.includes('collection') && lower.includes('expenditure') || lower.includes('closing balance') || lower.includes('daily mis')) {
    result.type = 'daily_mis';
  } else if (lower.includes('fund position') || lower.includes('net usable') || lower.includes('bank balance')) {
    result.type = 'fund_position';
  } else if (lower.includes('contractor') || lower.includes('outstanding') || lower.includes('steel') || lower.includes('rmc')) {
    result.type = 'site_update';
  } else if (lower.includes('booking') || lower.includes('unsold') || lower.includes('inventory') || lower.includes('sold')) {
    result.type = 'sales_update';
  } else if (lower.includes('mm ') || lower.includes('sm ') || lower.includes('drawing') || lower.includes('directors')) {
    result.type = 'promoter_draw';
  } else if (lower.includes('office') || lower.includes('gk-1') || lower.includes('salary')) {
    result.type = 'office_expense';
  } else if (lower.includes('rera') || lower.includes('compliance') || lower.includes('edc') || lower.includes('noc')) {
    result.type = 'compliance';
  }

  // Risk signals
  const risks = ['pressing','delay','shortage','risk','overrun','urgent','critical','crunch','deficit','negative','cancel','refund'];
  const costs = ['price up','price increase','hike','escalation','costlier'];
  risks.forEach(k => { if (lower.includes(k)) result.signals.push({ type: 'risk', keyword: k }); });
  costs.forEach(k => { if (lower.includes(k)) result.signals.push({ type: 'cost', keyword: k }); });

  return result;
}

// ═══ WEBHOOK — Gupshup sends messages here ═══
app.post('/webhook', (req, res) => {
  try {
    const body = req.body;
    
    // Gupshup format
    if (body.type === 'message' || body.payload) {
      const payload = body.payload || body;
      const sender = payload.sender?.name || payload.sender?.phone || payload.from || 'Unknown';
      const text = payload.payload?.text || payload.text || payload.body || '';
      const ts = payload.timestamp ? Math.floor(new Date(payload.timestamp).getTime() / 1000) : Math.floor(Date.now() / 1000);

      if (text) {
        const parsed = parseMIS(text, sender, ts);
        messages.push(parsed);
        if (messages.length > 2000) messages.shift();
        console.log(`[${parsed.date} ${parsed.time}] ${sender}: ${text.substring(0, 80)}... | Type: ${parsed.type} | Amounts: ${parsed.amounts.length}`);
      }
    }
    
    // Meta Cloud API format (if you use direct Meta instead of Gupshup)
    if (body.object === 'whatsapp_business_account') {
      for (const entry of body.entry || []) {
        for (const change of entry.changes || []) {
          if (change.field === 'messages') {
            for (const msg of change.value?.messages || []) {
              if (msg.type === 'text') {
                const contact = (change.value.contacts || []).find(c => c.wa_id === msg.from);
                const sender = contact?.profile?.name || msg.from;
                const parsed = parseMIS(msg.text.body, sender, parseInt(msg.timestamp));
                messages.push(parsed);
                if (messages.length > 2000) messages.shift();
                console.log(`[${parsed.date}] ${sender}: ${msg.text.body.substring(0, 80)}... | Type: ${parsed.type}`);
              }
            }
          }
        }
      }
    }

    res.status(200).send('OK');
  } catch (err) {
    console.error('Webhook error:', err.message);
    res.status(200).send('OK');
  }
});

// ═══ WEBHOOK VERIFICATION (Meta requires this) ═══
app.get('/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    console.log('Webhook verified');
    return res.status(200).send(challenge);
  }
  res.sendStatus(403);
});

// ═══ API ENDPOINTS (your tracker reads these) ═══

app.get('/api/messages', (req, res) => {
  const limit = parseInt(req.query.limit) || 50;
  const type = req.query.type;
  let filtered = type ? messages.filter(m => m.type === type) : messages;
  res.json({ count: filtered.length, messages: filtered.slice(-limit).reverse() });
});

app.get('/api/signals', (req, res) => {
  const withSignals = messages.filter(m => m.signals.length > 0);
  res.json({ count: withSignals.length, signals: withSignals.slice(-20).reverse() });
});

app.get('/api/summary', (req, res) => {
  const today = new Date().toISOString().split('T')[0];
  const date = req.query.date || today;
  const dayMsgs = messages.filter(m => m.date === date);
  res.json({
    date,
    total: dayMsgs.length,
    byType: dayMsgs.reduce((a, m) => { a[m.type] = (a[m.type] || 0) + 1; return a; }, {}),
    risks: dayMsgs.filter(m => m.signals.length > 0).length,
  });
});

// Health check — Railway uses this to confirm the app is alive
app.get('/', (req, res) => {
  res.json({
    status: 'running',
    app: 'Fidato MIS WhatsApp Bot',
    messages: messages.length,
    uptime: Math.floor(process.uptime()) + 's',
  });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', messages: messages.length });
});

// ═══ START ═══
app.listen(PORT, () => {
  console.log(`Fidato MIS Bot running on port ${PORT}`);
  console.log(`Webhook: POST /webhook`);
  console.log(`API: GET /api/messages, /api/signals, /api/summary`);
});
