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
    if (reason === 'LOGOUT') {
      console.log('[WA] LOGOUT — clearing wa_auth in 5s then restarting');
      setTimeout(function() {
        try { if (fs.existsSync('./wa_auth')) { fs.rmSync('./wa_auth', { recursive: true, force: true }); console.log('[WA] wa_auth cleared'); } } catch(e) { console.error('[WA] Clear failed:', e.message); }
        createWhatsAppClient();
      }, 5000);
    } else {
      setTimeout(function() { waClient.initialize().catch(function(e) { console.error('[WA] reinit failed:', e.message); }); }, 10000);
    }
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
  var am=line.match(/(\d[\d,]*\.?\d*)\s*(?:lac|lakh|lacs|l\b|cr|crore)/i);
  if(am){var a=parseFloat(am[1].replace(/,/g,'')); return /cr|crore/i.test(am[0])?a*10000000:a*100000;}
  var pm=line.match(/(\d[\d,]{4,})\/?-/);
  if(pm) return parseFloat(pm[1].replace(/,/g,''));
  var rm=line.match(/(?:rs\.?\s*|inr\s*|\u20B9\s*)(\d[\d,]*\.?\d*)/i);
  if(rm) return parseFloat(rm[1].replace(/,/g,''));
  return 0;
}
function extractLineVendor(line) {
  return line
    .replace(/(\d[\d,]*\.?\d*)\s*(?:lac|lakh|lacs|l\b|cr|crore)/i, '')
    .replace(/(\d[\d,]{4,})\/?-/, '')
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
      if(questionMessages[quotedMsgId] && senderInfo.role!=='mm' && senderInfo.role!=='sm'){
        var qInfo = questionMessages[quotedMsgId];
        answerMap[qInfo.expenseId] = {
          role: qInfo.role,
          question: qInfo.question,
          questionDate: qInfo.date,
          answer: body,
          answerDate: msgDate,
          answerBy: senderInfo.contactName || rawSender
        };
        continue; // don't process as a normal vote
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
                if(!isCheque&&!isLow){if(amount===0&&visionResult.amount>0)amount=visionResult.amount; if((!vendor||body.length<15)&&visionResult.vendor)vendor=visionResult.vendor;}
                if(!isCheque&&!isLow&&visionResult.purpose)purpose=visionResult.purpose;
              }
            }
          }catch(e){console.error('[Vision] Failed for',msgId,e.message);}
        }
        expenses.push({id:msgId,date:msgDate,body:body||(hasMedia?'[Image attached]':'[Empty]'),sender:senderInfo.contactName||rawSender,vendor:vendor||(hasMedia?'[See image]':''),amount:amount,purpose:purpose,subItems:subItems,hasMedia:hasMedia,visionParsed:visionResult?true:false,mmApproval:null,smApproval:null,status:{mm:'pending',sm:'pending'},queryAnswer:null});
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
  var result={fullyApproved:[],partialApproval:[],noApproval:[],onHold:[],rejected:[],allExpenses:expenses,totalExpenses:expenses.length,totalMessages:messages.length,fetchedDays:days||15,visionCacheSize:visionCache.size};
  for(var k=0;k<expenses.length;k++){
    var e=expenses[k],mm=e.status.mm,sm=e.status.sm;
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
  lines.push('');
  var mmLabel=mm==='yes'?'MM: Ok':mm==='question'?'MM: query raised':'MM: pending';
  var smLabel=sm==='yes'?'SM: Ok':sm==='question'?'SM: query raised':'SM: pending';
  lines.push(mmLabel+' | '+smLabel);
  if((queryMM||querySM) && queryAnswered){
    var ans = expense.queryAnswer;
    var who = ans.role==='mm'?'MM':'SM';
    lines.push('');
    lines.push(who+' asked:');
    lines.push('"'+ans.question+'"');
    lines.push('');
    lines.push(ans.answerBy+' answered:');
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
    var fmt=function(e){return{date:e.date.toISOString().split('T')[0],time:e.date.toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata',hour:'2-digit',minute:'2-digit'}),message:e.body.substring(0,300),sender:e.sender,vendor:e.vendor,amount:e.amount,amountFormatted:e.amount>0?formatINR(e.amount):'',purpose:e.purpose||'',subItems:e.subItems||null,hasMedia:e.hasMedia,visionParsed:e.visionParsed||false,mm:e.status.mm,sm:e.status.sm,mmReply:e.mmApproval?e.mmApproval.raw:null,smReply:e.smApproval?e.smApproval.raw:null,mmName:e.mmApproval?e.mmApproval.name:null,smName:e.smApproval?e.smApproval.name:null,queryAnswer:e.queryAnswer||null};};
    res.json({summary:{period:days+' days',totalMessages:audit.totalMessages,totalExpenseRequests:audit.totalExpenses,fullyApproved:audit.fullyApproved.length,partialApproval:audit.partialApproval.length,noApproval:audit.noApproval.length,onHold:audit.onHold.length,rejected:audit.rejected.length,visionCacheSize:audit.visionCacheSize},fullyApproved:audit.fullyApproved.map(fmt),partialApproval:audit.partialApproval.map(fmt),noApproval:audit.noApproval.map(fmt),onHold:audit.onHold.map(fmt),rejected:audit.rejected.map(fmt)});
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
cron.schedule('30 13 * * *',function(){if(!CONFIG.BOT_ENABLED||!waReady)return;var d=new Date().toISOString().split('T')[0];generateDailyReport(d).then(function(data){if(data.entryCount>0){htmlToImage(buildReportHTML(data),800,1200).then(function(img){var buf=Buffer.isBuffer(img)?img:Buffer.from(img);waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,new MessageMedia('image/png',buf.toString('base64'),'MIS.png'),{caption:'Evening Report - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut)});});}}).catch(function(e){console.error('Cron evening:',e.message);});},{timezone:'Asia/Kolkata'});

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
