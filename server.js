// ============================================================
// FIDATO MIS SERVER v2.5 — Vision + PDF + Pending Reminders
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
  TEST_PHONES: ['917838537000'], // approved test numbers for DM relay (e.g., your own number for testing)
  MM_NAMES: ['madhur', 'madhur mittal'],
  SM_NAMES: ['sumit', 'sumit mittal'],
};

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
function createWhatsAppClient() {
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
    // Track LOGOUT count over a 5-min window. Only wipe wa_auth after 3 consecutive LOGOUTs
    // (prevents accidental wipe on transient disconnects, but recovers from real session corruption).
    if (!global._waLogoutLog) global._waLogoutLog = [];
    var now = Date.now();
    if (reason === 'LOGOUT') {
      global._waLogoutLog.push(now);
      // Keep only LOGOUTs from last 5 minutes
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
    // Default behavior: just try to reinitialize the existing client (preserves saved session)
    setTimeout(function() {
      try { waClient.initialize().catch(function(e) { console.error('[WA] reinit failed:', e.message); }); }
      catch(e) { console.error('[WA] reinit threw:', e.message); }
    }, 10000);
  });
  // Listen for direct messages from accountants — bot acts as relay to approval group
  waClient.on('message', function(msg) {
    handleAccountantDM(msg).catch(function(e){ console.error('[DM handler]', e.message); });
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
function extractLineAmount(line) {
  // Pattern 1: lac/lakh/cr/crore units
  var am=line.match(/(\d[\d,]*\.?\d*)\s*(?:lac|lakh|lacs|l\b|cr|crore)/i);
  if(am){var a=parseFloat(am[1].replace(/,/g,'')); return /cr|crore/i.test(am[0])?a*10000000:a*100000;}
  // Pattern 2: "k" suffix e.g. 10k, 50k = thousand
  var km=line.match(/(\d[\d,]*\.?\d*)\s*k\b/i);
  if(km) return parseFloat(km[1].replace(/,/g,''))*1000;
  // Pattern 3: numbers ending with /-, /- with spaces, or just / at end
  var pm=line.match(/(\d[\d,]{3,})\s*\/\s*\-?/);
  if(pm) return parseFloat(pm[1].replace(/,/g,''));
  // Pattern 4: Rs/INR/₹ prefix
  var rm=line.match(/(?:rs\.?\s*|inr\s*|\u20B9\s*)(\d[\d,]*\.?\d*)/i);
  if(rm) return parseFloat(rm[1].replace(/,/g,''));
  // Pattern 5: standalone large numbers (>=10000), supports Indian-format with commas like "7,08,708"
  // Match either plain digits 5+ long, OR digits with comma-groupings
  var lm=line.match(/\b(\d{1,3}(?:,\d{2,3}){1,3}|\d{5,})\b/);
  if(lm){var v=parseFloat(lm[1].replace(/,/g,'')); if(v>=10000&&v<1000000000) return v;}
  return 0;
}
function extractLineVendor(line) {
  return line
    .replace(/(\d[\d,]*\.?\d*)\s*(?:lac|lakh|lacs|l\b|cr|crore)/i, '')
    .replace(/(\d[\d,]*\.?\d*)\s*k\b/i, '')
    .replace(/(\d[\d,]{3,})\s*\/\s*\-?/, '')
    .replace(/(?:rs\.?\s*|inr\s*|\u20B9\s*)(\d[\d,]*\.?\d*)/i, '')
    .replace(/^\s*(please approve|kindly approve|approve|for|to|on account of|on account|payment to|pay to)\s*/i, '')
    .replace(/\s+/g,' ').trim();
}
function parseExpenseMessage(body) {
  if(!body) return [{vendor:'',amount:0}];
  var lines=body.split('\n').map(function(l){return l.trim();}).filter(Boolean);
  var itemLines=[];
  for(var i=0;i<lines.length;i++){
    var a=extractLineAmount(lines[i]);
    if(a>0){var v=extractLineVendor(lines[i]); if(v&&v.length>1)itemLines.push({vendor:v,amount:a});}
  }
  if(itemLines.length>1) return itemLines;
  var total=0; for(var j=0;j<lines.length;j++) total+=extractLineAmount(lines[j]);
  var vendor=extractLineVendor(lines[0])||lines[0].substring(0,150);
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
      ? 'This PDF is attached to an expense approval request. It is likely an invoice, PO, bill, or payment challan. Extract: (1) vendor/payee name, (2) total amount in INR as a number, (3) brief purpose max 10 words. Reply ONLY with JSON on one line: {"vendor":"","amount":0,"purpose":"","imageType":"invoice","confidence":"high"}.'
      : 'This image is attached to an expense approval request. Classify it: imageType = "cheque" (any bank cheque even cancelled), "invoice" (printed bill), "receipt", "screenshot", or "other". For CHEQUES: set vendor to "" and amount to 0 — they are shared as bank reference only, not expense amounts. For printed INVOICES/RECEIPTS: extract vendor, total amount in INR, and purpose. Set confidence to "high" if clearly printed, "low" if handwritten or blurry. Reply ONLY with JSON on one line: {"vendor":"","amount":0,"purpose":"","imageType":"cheque","confidence":"low"}.';
    var resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': CONFIG.CLAUDE_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: 'claude-haiku-4-5-20251001', max_tokens: 300, messages: [{ role: 'user', content: [mediaBlock, { type: 'text', text: prompt }] }] })
    });
    if (!resp.ok) { console.error('[Vision] API error', resp.status); return null; }
    var data = await resp.json();
    var text = '';
    if (data.content) { for (var i=0;i<data.content.length;i++) { if(data.content[i].type==='text'){text=data.content[i].text;break;} } }
    if (!text) return null;
    text = text.replace(/```json|```/g, '').trim();
    var parsed; try { parsed = JSON.parse(text); } catch(e) { var m=text.match(/\{[^}]*\}/); if(m){try{parsed=JSON.parse(m[0]);}catch(e2){return null;}} else return null; }
    var result = { vendor:(parsed.vendor||'').toString().substring(0,150), amount:parseAmount(parsed.amount), purpose:(parsed.purpose||'').toString().substring(0,200), imageType:parsed.imageType||'other', confidence:parsed.confidence||'low' };
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
  // questionMessages: msgId -> { expenseId, role, question, date, name }
  // tracks MM/SM messages that were classified as questions, so accountant
  // replies to them can be linked back to the original expense
  var questionMessages = {};
  // answerMap: expenseId -> { role: 'mm'|'sm', question, answer, answerDate, answerBy }
  var answerMap = {};

  for(var i=0;i<messages.length;i++){
    var msg=messages[i];
    var rawSender=msg.author||msg.from||'';
    var msgDate=new Date(msg.timestamp*1000);
    var body=(msg.body||'').trim();
    var hasMedia=msg.hasMedia||false;
    var senderInfo = await identifySender(rawSender);
    var thisMsgId = msg.id._serialized||msg.id.id;
    var quotedMsgId=null;
    if(msg.hasQuotedMsg){try{var q=await msg.getQuotedMessage();quotedMsgId=q.id._serialized||q.id.id;}catch(e){}}

    if(quotedMsgId){
      var resp=parseResponse(body);

      // Case A: this is a swipe-reply to an MM/SM question message → it's an ANSWER
      // Accept answers from: accountants, OR the OTHER promoter (e.g. SM clarifies MM's question)
      if(questionMessages[quotedMsgId]){
        var qInfo = questionMessages[quotedMsgId];
        var answerFromOtherPromoter = (qInfo.role==='mm' && senderInfo.role==='sm') || (qInfo.role==='sm' && senderInfo.role==='mm');
        var answerFromAccountant = senderInfo.role!=='mm' && senderInfo.role!=='sm';
        if(answerFromOtherPromoter || answerFromAccountant){
          // Don't treat short Yes/No replies from the other promoter as the answer — those are votes
          var promoterReplyShort = answerFromOtherPromoter && (parseResponse(body)==='yes' || parseResponse(body)==='no' || parseResponse(body)==='hold');
          if(!promoterReplyShort){
            answerMap[qInfo.expenseId] = {
              role: qInfo.role,
              question: qInfo.question,
              questionDate: qInfo.date,
              answer: body,
              answerDate: msgDate,
              answerBy: senderInfo.contactName || rawSender,
              answerByRole: answerFromOtherPromoter ? (senderInfo.role==='mm' ? 'MM' : 'SM') : 'accountant'
            };
            continue; // don't process as a normal vote
          }
        }
      }

      // Case B: normal MM/SM swipe-reply to an expense
      if(!replyMap[quotedMsgId])replyMap[quotedMsgId]={mm:null,sm:null};
      if(senderInfo.role==='mm'){
        replyMap[quotedMsgId].mm={response:resp,date:msgDate,raw:body,name:senderInfo.contactName,msgId:thisMsgId};
        // If MM raised a question, register this message ID so accountant replies link back
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
      // Skip bot's own reminder messages — not expense requests
      if(body.indexOf('[BOT REMINDER]')===0){continue;}
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
                // Cheques: hard-skip vendor/amount (handwriting unreliable, just bank reference)
                if(!isCheque){
                  // Use vision amount if text had none — even from low-conf, since amount=0 means we have nothing else
                  if(amount===0 && visionResult.amount>0) amount=visionResult.amount;
                  // Vendor: only from high-confidence reads
                  if(!isLow && (!vendor||body.length<15) && visionResult.vendor) vendor=visionResult.vendor;
                  // Purpose: only from high-confidence reads
                  if(!isLow && visionResult.purpose) purpose=visionResult.purpose;
                }
              }
            }
          }catch(e){console.error('[Vision] Failed for',msgId,e.message);}
        }
        // Skip junk: no body, no media, no amount → not a real expense request
        var isJunk = (!body) && !hasMedia && amount === 0;
        // Also skip super-short non-meaningful messages from system/unknown senders
        if(!isJunk && body && body.length < 4 && !hasMedia && amount === 0) isJunk = true;
        if(!isJunk){
          expenses.push({id:msgId,date:msgDate,body:body||(hasMedia?'[Image attached]':'[Empty]'),sender:senderInfo.contactName||rawSender,vendor:vendor||(hasMedia?'[See image]':''),amount:amount,purpose:purpose,subItems:subItems,hasMedia:hasMedia,visionParsed:visionResult?true:false,mmApproval:null,smApproval:null,status:{mm:'pending',sm:'pending'},queryAnswer:null});
        }
      }
    }
  }

  // Wire votes into expenses
  for(var j=0;j<expenses.length;j++){
    var rep=replyMap[expenses[j].id];
    if(rep){expenses[j].mmApproval=rep.mm;expenses[j].smApproval=rep.sm;expenses[j].status.mm=rep.mm?rep.mm.response:'pending';expenses[j].status.sm=rep.sm?rep.sm.response:'pending';}

    // Wire query-answer pair if one exists for this expense
    if(answerMap[expenses[j].id]){
      expenses[j].queryAnswer = answerMap[expenses[j].id];
    }
  }

  // ── Vision-based de-duplication ───────────────────────────────────────────
  // When accountants send a text expense AND a supporting PDF/image as separate messages,
  // the bot would treat them as 2 separate requests. Detect and merge them so the PDF
  // becomes a supportingDoc on the primary text expense.
  //
  // Match criteria (ALL must hold):
  //   1. Same sender
  //   2. Same amount (within Rs 1 tolerance for rounding)
  //   3. Within 10 minutes of each other
  //   4. At least one meaningful word in common between the text body and vision purpose/vendor
  //   5. The media message has NO MM/SM approvals on its own (otherwise it's a separately-tracked item)
  //   6. Both are from non-MM/SM senders (accountants)
  //
  // The text message becomes the "primary"; the media message is attached as supportingDoc.
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

  var dedupedIds = {}; // ids that should be removed from final results
  for(var di=0; di<expenses.length; di++) {
    var mediaExp = expenses[di];
    // Only consider media-only/vision-parsed expenses with a real amount
    if(!mediaExp.hasMedia || !mediaExp.visionParsed || mediaExp.amount <= 0) continue;
    // Skip if this media expense already has its own approvals (someone swipe-replied to it directly)
    if(mediaExp.mmApproval || mediaExp.smApproval) continue;

    // Search for a matching text expense
    for(var dj=0; dj<expenses.length; dj++) {
      if(di === dj) continue;
      var textExp = expenses[dj];
      // Primary must be a text expense (not media-only)
      if(textExp.hasMedia && (!textExp.body || textExp.body.length < 10)) continue;
      // Same sender
      if(textExp.sender !== mediaExp.sender) continue;
      // Same amount (within Rs 1 for rounding)
      if(Math.abs(textExp.amount - mediaExp.amount) > 1) continue;
      // Within 10 minutes
      var timeDelta = Math.abs(textExp.date.getTime() - mediaExp.date.getTime());
      if(timeDelta > 10 * 60 * 1000) continue;
      // Description overlap: any meaningful word in common
      // Look at vision data via the cached result (we need the vendor/purpose from extractFromImage)
      var cached = visionCache.get(mediaExp.id);
      if(cached && !descriptionsRelate(cached, textExp.body)) continue;
      // If no cached vision data (shouldn't happen for visionParsed=true), skip overlap check

      // MATCH — attach as supportingDoc on the text expense
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
      break; // attached to one primary, done
    }
  }

  // Filter out the de-duplicated media expenses
  var consolidatedExpenses = expenses.filter(function(e){ return !dedupedIds[e.id]; });

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
  if((queryMM||querySM) && queryAnswered) header='[BOT REMINDER] - Query answered - awaiting MM+SM approval';
  else if(queryMM||querySM) header='[BOT REMINDER] - Query unanswered - '+getDaysPending(expense.date)+' day(s) pending';
  else if(bothPending) header='[BOT REMINDER] - Approval needed';
  else if(mmOnly) header='[BOT REMINDER] - MM approval needed';
  else if(smOnly) header='[BOT REMINDER] - SM approval needed';
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
  // Mention supporting documents if any
  if(expense.supportingDocs && expense.supportingDocs.length > 0){
    var docNames = expense.supportingDocs.map(function(d){ return d.filename; }).join(', ');
    lines.push('Supporting docs: '+docNames);
  }
  lines.push('');
  var mmLabel=mm==='yes'?'MM: Ok':mm==='question'?'MM: query raised':'MM: pending';
  var smLabel=sm==='yes'?'SM: Ok':sm==='question'?'SM: query raised':'SM: pending';
  lines.push(mmLabel+' | '+smLabel);
  if((queryMM||querySM) && queryAnswered){
    var ans = expense.queryAnswer;
    var who = ans.role==='mm'?'MM':'SM';
    var answerLabel = ans.answerByRole && ans.answerByRole !== 'accountant' ? (ans.answerByRole + ' (' + ans.answerBy + ')') : ans.answerBy;
    lines.push('');
    lines.push(who+' asked:');
    lines.push('"'+ans.question+'"');
    lines.push('');
    lines.push(answerLabel+' answered:');
    lines.push('"'+ans.answer+'"');
    lines.push('');
    lines.push(who==='MM'?'Madhur sir, please confirm to approve':'Sumit sir, please confirm to approve');
  }
  else if(queryMM&&expense.mmApproval){lines.push('');lines.push('MM asked:');lines.push('"'+expense.mmApproval.raw+'"');lines.push('');lines.push('Please answer MM\'s query to proceed');}
  else if(querySM&&expense.smApproval){lines.push('');lines.push('SM asked:');lines.push('"'+expense.smApproval.raw+'"');lines.push('');lines.push('Please answer SM\'s query to proceed');}
  else if(mmOnly){lines.push('');lines.push('Madhur sir, please swipe-reply Ok to approve');}
  else if(smOnly){lines.push('');lines.push('Sumit sir, please swipe-reply Ok to approve');}
  else{lines.push('');lines.push('Please swipe-reply Ok to approve');}
  return lines.join('\n');
}

async function sendPendingReminders() {
  if(!waReady){console.log('[Reminders] WA not connected');return 0;}
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




// ── DM Relay: accountants DM bot, bot validates and posts to group ───────────
var DM_STATE_FILE = './wa_auth/dm_state.json';

function loadDMState() {
  try {
    if(fs.existsSync(DM_STATE_FILE)){
      return JSON.parse(fs.readFileSync(DM_STATE_FILE, 'utf8'));
    }
  } catch(e) { console.error('[DM] state load failed:', e.message); }
  return { pending: {} }; // jid -> { amount, vendor, reason, mediaIds: [], lastUpdate: ISO, askedFor: 'amount'|'vendor'|null }
}

function saveDMState(state) {
  try {
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth', { recursive: true });
    fs.writeFileSync(DM_STATE_FILE, JSON.stringify(state, null, 2));
  } catch(e) { console.error('[DM] state save failed:', e.message); }
}

// Pending state expires after 30 minutes — accountant has to start fresh
function pruneStaleDMState(state) {
  var now = Date.now();
  Object.keys(state.pending).forEach(function(jid){
    var entry = state.pending[jid];
    if(now - new Date(entry.lastUpdate).getTime() > 30*60*1000){
      delete state.pending[jid];
    }
  });
}

// Determine whether a sender is allowed to use the DM relay.
// Phone-based whitelist ONLY — names are unreliable (anyone can rename a contact "Madhur Mittal").
// Allowed: ACCOUNTANT_PHONES, MM_PHONE, SM_PHONE, TEST_PHONES.
// JIDs ending in @lid are opaque WhatsApp internal IDs (not phone numbers) and are REJECTED
// unless we have a verified mapping. This prevents impersonation via LID-only contacts.
function isAuthorisedAccountant(rawJid, contactName) {
  if(!rawJid) return false;
  // Reject @lid JIDs — these are anonymous WhatsApp internal IDs, no phone verification possible
  if(rawJid.indexOf('@lid') >= 0) {
    console.log('[Auth] reject @lid sender:', rawJid, '(name:', contactName, ')');
    return false;
  }
  // Standard phone JID: phone@c.us
  if(rawJid.indexOf('@c.us') < 0) return false;
  var phoneOnly = rawJid.split('@')[0].replace(/[^0-9]/g, '');
  if(!phoneOnly) return false;
  // Strict whitelist — phone number must be in one of the approved lists
  var whitelist = CONFIG.ACCOUNTANT_PHONES.concat([CONFIG.MM_PHONE, CONFIG.SM_PHONE]).concat(CONFIG.TEST_PHONES || []);
  if(whitelist.indexOf(phoneOnly) >= 0) {
    console.log('[Auth] allow:', phoneOnly, '(', contactName, ')');
    return true;
  }
  console.log('[Auth] reject:', phoneOnly, '(', contactName, ') - not in whitelist');
  return false;
}

// Build the structured group post text from a complete pending entry
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
  lines.push('MM/SM please review.');
  return lines.join('\n');
}

// Handle an incoming DM. Returns true if handled, false if not relevant.
async function handleAccountantDM(msg) {
  if(!msg || !waReady) return false;
  var rawFrom = msg.from || '';
  // Only direct chats, not groups
  if(rawFrom.indexOf('@g.us') >= 0) return false;
  if(rawFrom.indexOf('@c.us') < 0 && rawFrom.indexOf('@lid') < 0) return false;
  // Don't process bot's own messages
  if(msg.fromMe) return false;

  var senderInfo = await identifySender(rawFrom);
  if(!isAuthorisedAccountant(rawFrom, senderInfo.contactName)){
    console.log('[DM] unauthorised sender:', rawFrom, senderInfo.contactName);
    // Send a polite reject message ONLY ONCE per sender per day to avoid spam loops
    if(!global._unauthorisedNotified) global._unauthorisedNotified = {};
    var dayKey = rawFrom + '_' + new Date().toISOString().split('T')[0];
    if(!global._unauthorisedNotified[dayKey]){
      global._unauthorisedNotified[dayKey] = true;
      try {
        await waClient.sendMessage(rawFrom, 'This is the Fidato MIS Bot. Your number is not authorised to use the expense approval relay. Please contact Mukund or Madhur if you need access.');
      } catch(e) { /* ignore */ }
    }
    return false;
  }

  var body = (msg.body || '').trim();
  var hasMedia = msg.hasMedia || false;
  var thisMsgId = msg.id._serialized || msg.id.id;
  console.log('[DM] from', senderInfo.contactName || rawFrom, ':', body.substring(0,80), hasMedia?'[+media]':'');

  var state = loadDMState();
  pruneStaleDMState(state);

  // Special commands
  if(/^\s*(cancel|reset|clear)\s*$/i.test(body)){
    delete state.pending[rawFrom];
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'Pending request cleared. Send a new expense to start over.');
    return true;
  }
  if(/^\s*help\s*$/i.test(body)){
    await waClient.sendMessage(rawFrom, [
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
      'Or just write naturally and I will ask for whatever is missing.',
      'Attachments (PDF/image) are optional — vision will read them.',
      '',
      'Reply "cancel" to clear a pending request.'
    ].join('\n'));
    return true;
  }

  // Get or create pending entry for this sender
  if(!state.pending[rawFrom]) state.pending[rawFrom] = { details: '', amount: 0, company: '', fromAC: '', mediaIds: [], lastUpdate: new Date().toISOString(), askedFor: null, posterName: senderInfo.contactName || rawFrom, subItems: null };
  var entry = state.pending[rawFrom];
  entry.lastUpdate = new Date().toISOString();
  entry.posterName = senderInfo.contactName || entry.posterName;

  // Try to parse a structured single-message format (4 fields with prefixes)
  // Example:
  //   Details: TMT bar payment for slab 110-118
  //   Amount: 7,08,708
  //   Company: Hansaflon Buildcon
  //   From: Hansaflon JKB
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

  // Apply structured fields if any are found in current message
  var structured = parseStructuredFields(body);
  if(structured.details && !entry.details) entry.details = structured.details;
  if(structured.amount && entry.amount === 0) entry.amount = structured.amount;
  if(structured.company && !entry.company) entry.company = structured.company;
  if(structured.fromAC && !entry.fromAC) entry.fromAC = structured.fromAC;
  var hadAnyStructured = structured.details || structured.amount || structured.company || structured.fromAC;

  // If accountant is responding to a follow-up question, fill that field
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
        await waClient.sendMessage(rawFrom, 'Could not detect an amount. Please reply with the amount (e.g. "3 lac" or "300000" or "10k").');
        saveDMState(state);
        return true;
      }
    } else if(entry.askedFor === 'company'){
      entry.company = body;
      entry.askedFor = null;
    } else if(entry.askedFor === 'fromAC'){
      entry.fromAC = body;
      entry.askedFor = null;
    }
  } else if(body && !hadAnyStructured && !entry.askedFor){
    // Free-form first message — try to extract amount + details using existing parser
    var parsed = parseExpenseMessage(body);
    if(parsed.length > 1){
      entry.subItems = parsed;
      if(entry.amount === 0) entry.amount = parsed.reduce(function(s,p){return s+p.amount;},0);
      if(!entry.details) entry.details = parsed.map(function(p){return p.vendor+' '+formatINR(p.amount);}).join(', ');
    } else {
      var p = parsed[0];
      if(p.amount > 0 && entry.amount === 0) entry.amount = p.amount;
      if(!entry.details && body.length > 0){
        // The whole body becomes details (we'll still ask for the missing fields)
        entry.details = body.substring(0, 250);
      }
    }
  }

  // Handle attached media (PDF/image)
  if(hasMedia){
    try {
      var media = await msg.downloadMedia();
      if(media && media.data){
        var visionResult = await extractFromImage(media, thisMsgId);
        if(visionResult){
          entry.mediaIds.push(thisMsgId);
          // If text didn't supply amount/details, take from vision
          if(visionResult.imageType !== 'cheque'){
            if(entry.amount === 0 && visionResult.amount > 0) entry.amount = visionResult.amount;
            if(!entry.details && visionResult.confidence !== 'low'){
              var dParts = [];
              if(visionResult.vendor) dParts.push(visionResult.vendor);
              if(visionResult.purpose) dParts.push(visionResult.purpose);
              if(dParts.length > 0) entry.details = dParts.join(' - ').substring(0,250);
            }
          }
          // Cache the media filename so we can attach in the group
          if(!entry.mediaFiles) entry.mediaFiles = [];
          entry.mediaFiles.push({ msgId: thisMsgId, filename: body || ('attachment_'+entry.mediaFiles.length+'.'+(media.mimetype||'').split('/')[1]), mimetype: media.mimetype, dataB64: media.data });
        }
      }
    } catch(e) { console.error('[DM] media download failed:', e.message); }
  }

  // Validate required fields in order: details, amount, company, fromAC
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
    saveDMState(state);
    // Helpful: suggest valid companies from Fund Position
    var companies = '';
    try {
      var fp = await getFundPosition();
      var uniqueCompanies = [];
      fp.forEach(function(a){ if(a.company && uniqueCompanies.indexOf(a.company) < 0) uniqueCompanies.push(a.company); });
      if(uniqueCompanies.length > 0) companies = '\n\nValid options:\n' + uniqueCompanies.map(function(c,i){return (i+1)+'. '+c;}).join('\n');
    } catch(e) {}
    await waClient.sendMessage(rawFrom, 'Which company is this expense for?' + companies);
    return true;
  }
  if(!entry.fromAC){
    entry.askedFor = 'fromAC';
    saveDMState(state);
    // Helpful: suggest bank A/Cs of that company from Fund Position
    var accounts = '';
    try {
      var fp2 = await getFundPosition();
      var matchingAccounts = fp2.filter(function(a){ return a.company && entry.company && a.company.toLowerCase().indexOf(entry.company.toLowerCase()) >= 0; }).map(function(a){return a.bankAC;});
      if(matchingAccounts.length === 0) matchingAccounts = fp2.map(function(a){return a.bankAC;}).filter(function(b){return b;});
      if(matchingAccounts.length > 0) accounts = '\n\nValid options:\n' + matchingAccounts.slice(0,15).map(function(c,i){return (i+1)+'. '+c;}).join('\n');
    } catch(e) {}
    await waClient.sendMessage(rawFrom, 'Which bank A/C should we pay from?' + accounts);
    return true;
  }

  // All required fields present — confirm and post
  var preview = buildGroupPostFromDM(entry, entry.posterName);
  var confirmText = 'Ready to post:\n\n' + preview + '\n\nReply "yes" to post, "edit reason: <text>" to add reason, or "cancel" to clear.';
  if(body.toLowerCase() === 'yes' || body.toLowerCase() === 'post' || body.toLowerCase() === 'send'){
    // Post to group
    try {
      var groupText = buildGroupPostFromDM(entry, entry.posterName);
      var postedMsg;
      if(entry.mediaFiles && entry.mediaFiles.length > 0){
        // Send first media with caption, then any additional media without caption
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
      await waClient.sendMessage(rawFrom, 'Posted to approval group. MM/SM will review and respond.');
      console.log('[DM] posted to group from', entry.posterName);
    } catch(e) {
      console.error('[DM] post failed:', e.message);
      await waClient.sendMessage(rawFrom, 'Failed to post: ' + e.message + '. Reply "yes" to retry or "cancel" to clear.');
    }
    return true;
  }
  // edit field: <new value>
  var editMatch = body.match(/^\s*edit\s+(details|amount|company|from|account)\s*:\s*(.+)$/i);
  if(editMatch){
    var fld = editMatch[1].toLowerCase();
    var val = editMatch[2].trim();
    if(fld === 'details') entry.details = val;
    else if(fld === 'amount'){
      var pe = parseExpenseMessage(val);
      if(pe[0].amount > 0) entry.amount = pe[0].amount;
    }
    else if(fld === 'company') entry.company = val;
    else if(fld === 'from' || fld === 'account') entry.fromAC = val;
    saveDMState(state);
    await waClient.sendMessage(rawFrom, 'Updated. ' + buildGroupPostFromDM(entry, entry.posterName) + '\n\nReply "yes" to post.');
    return true;
  }

  // Show preview and wait for confirmation
  saveDMState(state);
  await waClient.sendMessage(rawFrom, confirmText);
  return true;
}

// ── Stale-pending scanner (Option 4: tag once after 30 min, no repeat) ───────
// State persisted on disk (under ./wa_auth so it survives redeploys via the Volume).
var STALE_STATE_FILE = './wa_auth/reminder_state.json';

function loadStaleState() {
  try {
    if(fs.existsSync(STALE_STATE_FILE)){
      return JSON.parse(fs.readFileSync(STALE_STATE_FILE, 'utf8'));
    }
  } catch(e) { console.error('[Stale] state load failed:', e.message); }
  return { reminded: {} }; // expenseId -> { sentAt: ISO timestamp, missing: ['mm','sm'] }
}

function saveStaleState(state) {
  try {
    if(!fs.existsSync('./wa_auth')) fs.mkdirSync('./wa_auth', { recursive: true });
    fs.writeFileSync(STALE_STATE_FILE, JSON.stringify(state, null, 2));
  } catch(e) { console.error('[Stale] state save failed:', e.message); }
}

// Build a reminder message that @mentions whoever still hasn't replied.
// Returns { text, mentionJids } so caller can pass mentions to WhatsApp.
function buildStaleReminderText(expense, now) {
  var mm = expense.status.mm, sm = expense.status.sm;
  var bothPending = mm === 'pending' && sm === 'pending';
  var mmOnly = mm === 'pending' && sm === 'yes';
  var smOnly = sm === 'pending' && mm === 'yes';
  var queryMM = mm === 'question', querySM = sm === 'question';
  var queryAnswered = expense.queryAnswer ? true : false;

  // Determine who needs to be tagged
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
  // If query unanswered, tag the accountants instead (the askers)
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
  else if(queryMM || querySM) header = '[BOT REMINDER] Query answered - awaiting MM/SM - pending ' + minutesPending + ' min';
  else if(bothPending) header = '[BOT REMINDER] Approval needed - pending ' + minutesPending + ' min';
  else if(mmOnly) header = '[BOT REMINDER] MM approval needed - pending ' + minutesPending + ' min';
  else if(smOnly) header = '[BOT REMINDER] SM approval needed - pending ' + minutesPending + ' min';
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
  var mmLabel = mm==='yes'?'MM: Ok':mm==='question'?'MM: query raised':'MM: pending';
  var smLabel = sm==='yes'?'SM: Ok':sm==='question'?'SM: query raised':'SM: pending';
  lines.push(mmLabel + ' | ' + smLabel);

  if((queryMM||querySM) && queryAnswered){
    var ans = expense.queryAnswer;
    var who = ans.role === 'mm' ? 'MM' : 'SM';
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

// Run every 10 minutes — find expenses posted >= 30 min ago that we haven't yet reminded about.
async function scanStalePendings() {
  if(!waReady || !CONFIG.BOT_ENABLED) return 0;
  try {
    var state = loadStaleState();
    var audit = await buildApprovalAudit(2); // last 2 days is enough for stale check
    var now = Date.now();
    var THIRTY_MIN = 30 * 60 * 1000;

    // Quiet hours: 9 PM - 9 AM IST. Convert to local time check.
    var nowIST = new Date(now);
    var hourIST = parseInt(nowIST.toLocaleString('en-IN', { hour: '2-digit', hour12: false, timeZone: 'Asia/Kolkata' }));
    if(hourIST >= 21 || hourIST < 9) {
      console.log('[Stale] quiet hours (IST '+hourIST+'h), skipping scan');
      return 0;
    }

    // Anything still needing attention = partial OR (noApproval with an actual amount)
    var candidates = audit.partialApproval.concat(
      audit.noApproval.filter(function(e){ return e.amount > 0 || (e.subItems && e.subItems.length > 0); })
    );

    var sentCount = 0;
    var delay = function(ms){ return new Promise(function(r){ setTimeout(r, ms); }); };

    for(var i=0; i<candidates.length; i++) {
      var expense = candidates[i];
      var expenseAge = now - expense.date.getTime();
      if(expenseAge < THIRTY_MIN) continue; // too fresh
      if(state.reminded[expense.id]) continue; // already reminded once — Option 4 says don't repeat

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

// Build the "Stale Pending" section that gets appended to evening report
async function buildStalePendingSection() {
  try {
    var audit = await buildApprovalAudit(7);
    var stillPending = audit.partialApproval.concat(
      audit.noApproval.filter(function(e){ return e.amount > 0; })
    );
    if(stillPending.length === 0) return '';

    var lines = [''];
    lines.push('--- STALE PENDING APPROVALS ---');
    lines.push('');
    var totalStale = 0;
    stillPending.forEach(function(e){
      var status = e.status.mm === 'yes' ? 'SM pending' : e.status.sm === 'yes' ? 'MM pending' : e.status.mm === 'question' ? 'Query open' : e.status.sm === 'question' ? 'Query open' : 'Both pending';
      var age = Math.floor((Date.now() - e.date.getTime()) / (60*60*1000));
      lines.push('- ' + (e.vendor || e.body.substring(0,40)) + ' Rs.' + formatINR(e.amount) + ' [' + status + ', ' + age + 'h]');
      totalStale += e.amount;
    });
    lines.push('');
    lines.push('Total stale: Rs.' + formatINR(totalStale));
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

// ── Report HTML ───────────────────────────────────────────────────────────────
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
app.get('/health',function(req,res){res.json({status:'ok',version:'2.5',whatsapp:waReady?'connected':'disconnected',sheets:sheetsApi?'initialized':'not configured',botEnabled:CONFIG.BOT_ENABLED,visionEnabled:CONFIG.CLAUDE_API_KEY?true:false,visionCacheSize:visionCache.size});});
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
app.get('/api/preview',async function(req,res){try{res.send(buildReportHTML(await generateDailyReport(req.query.date||new Date().toISOString().split('T')[0])));}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/preview-image',async function(req,res){try{var img=await htmlToImage(buildReportHTML(await generateDailyReport(req.query.date||new Date().toISOString().split('T')[0])),800,1200);var buf=Buffer.isBuffer(img)?img:Buffer.from(img);res.set('Content-Type','image/png');res.set('Content-Length',String(buf.length));res.set('Cache-Control','no-store');res.end(buf);}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/daily-report',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});if(!CONFIG.BOT_ENABLED)return res.json({error:'Bot paused'});var d=req.query.date||new Date().toISOString().split('T')[0];var data=await generateDailyReport(d);var img=await htmlToImage(buildReportHTML(data),800,1200);var buf=Buffer.isBuffer(img)?img:Buffer.from(img);await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,new MessageMedia('image/png',buf.toString('base64'),'MIS_'+d+'.png'),{caption:'MIS Report - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut)+' | NET: '+formatINR(data.net)});res.json({success:true,date:d});}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/test-send',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,'MIS Bot test - '+new Date().toLocaleString('en-IN',{timeZone:'Asia/Kolkata'}));res.json({success:true});}catch(e){res.json({error:e.message});}});
app.get('/api/report-status',function(req,res){res.json({botEnabled:CONFIG.BOT_ENABLED,whatsapp:waReady,version:'2.5',visionEnabled:CONFIG.CLAUDE_API_KEY?true:false});});
app.get('/api/vision-test',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});var msgId=req.query.msgId;if(!msgId)return res.json({error:'pass ?msgId=...'});var chat=await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);var msgs=await chat.fetchMessages({limit:200});var target=null;for(var i=0;i<msgs.length;i++){var sid=msgs[i].id._serialized||msgs[i].id.id;if(sid===msgId){target=msgs[i];break;}}if(!target)return res.json({error:'message not found in last 200'});if(!target.hasMedia)return res.json({error:'no media'});var media=await target.downloadMedia();if(!media)return res.json({error:'failed to download'});visionCache.delete(msgId);var result=await extractFromImage(media,msgId);res.json({msgId:msgId,mimetype:media.mimetype,dataSize:media.data?media.data.length:0,parsed:result});}catch(e){res.json({error:e.message});}});

// ── Crons ─────────────────────────────────────────────────────────────────────
// 7 PM IST — evening report
cron.schedule('30 13 * * *',async function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  var d=new Date().toISOString().split('T')[0];
  try {
    var data=await generateDailyReport(d);
    var staleSection = await buildStalePendingSection();
    if(data.entryCount>0){
      var img = await htmlToImage(buildReportHTML(data),800,1200);
      var buf = Buffer.isBuffer(img)?img:Buffer.from(img);
      var caption = 'Evening Report - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut) + staleSection;
      await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,new MessageMedia('image/png',buf.toString('base64'),'MIS.png'),{caption:caption});
    } else if(staleSection) {
      await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,'Evening Report - '+d+'\nNo Ledger entries today.'+staleSection);
    }
  } catch(e) { console.error('Cron evening:',e.message); }
},{timezone:'Asia/Kolkata'});

// 9 AM IST — morning summary + pending reminders
cron.schedule('30 3 * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  var y=new Date();y.setDate(y.getDate()-1);var d=y.toISOString().split('T')[0];
  generateDailyReport(d).then(function(data){
    if(data.entryCount>0){htmlToImage(buildReportHTML(data),800,1200).then(function(img){var buf=Buffer.isBuffer(img)?img:Buffer.from(img);waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,new MessageMedia('image/png',buf.toString('base64'),'MIS.png'),{caption:'Morning Summary - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut)});});}
  }).catch(function(e){console.error('Cron morning:',e.message);});
  // 30s after morning summary, send pending approval reminders to approval group
  setTimeout(function(){sendPendingReminders().catch(function(e){console.error('[Reminders cron]',e.message);});},30000);
},{timezone:'Asia/Kolkata'});

// Every 10 minutes — scan for pending expenses >= 30 min old, send one reminder each (no repeat)
cron.schedule('*/10 * * * *',function(){
  if(!CONFIG.BOT_ENABLED||!waReady)return;
  scanStalePendings().catch(function(e){console.error('[Cron stale]',e.message);});
},{timezone:'Asia/Kolkata'});

app.get('/api/auth-list',function(req,res){res.json({accountants:CONFIG.ACCOUNTANT_PHONES,testNumbers:CONFIG.TEST_PHONES||[],mm:CONFIG.MM_PHONE,sm:CONFIG.SM_PHONE,note:'Only these phone numbers can DM the bot for expense relay. @lid (anonymous) JIDs are always rejected.'});});
app.get('/api/dm-state',function(req,res){try{res.json(loadDMState());}catch(e){res.json({error:e.message});}});
app.get('/api/dm-clear',function(req,res){try{saveDMState({pending:{}});res.json({success:true});}catch(e){res.json({error:e.message});}});
app.get('/api/stale-scan',async function(req,res){try{if(!waReady)return res.json({error:'WhatsApp not connected'});var count=await scanStalePendings();res.json({success:true,remindersSent:count});}catch(e){res.json({error:e.message});}});
app.get('/api/stale-state',function(req,res){try{res.json(loadStaleState());}catch(e){res.json({error:e.message});}});
app.get('/api/stale-reset',function(req,res){try{saveStaleState({reminded:{}});res.json({success:true,message:'Stale reminder state cleared - next scan will re-send any 30+ min pending items'});}catch(e){res.json({error:e.message});}});
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

initGoogleSheets();
createWhatsAppClient();
app.listen(CONFIG.PORT,function(){console.log('\nFidato MIS Server v2.5 | Port:',CONFIG.PORT,'| Vision:',CONFIG.CLAUDE_API_KEY?'enabled':'disabled');});
