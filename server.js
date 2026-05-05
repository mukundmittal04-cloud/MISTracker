// ============================================================
// FIDATO MIS DAILY REPORT + APPROVAL AUDIT SERVER v2.2
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
  ACCOUNTANT_PHONES: [
    '919873574112',
    '919873574180',
    '919873574192',
    '919873574103',
    '919773592304'
  ],
};

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

var waClient = null, waReady = false, latestQR = null, latestQRDataUrl = null;
function createWhatsAppClient() {
  waClient = new Client({
    authStrategy: new LocalAuth({ dataPath: './wa_auth' }),
    puppeteer: { headless: true, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-accelerated-2d-canvas','--no-first-run','--no-zygote','--single-process','--disable-gpu','--disable-extensions'] },
  });
  waClient.on('qr', function(qr) { latestQR = qr; qrcode.toDataURL(qr, function(err, url) { if (!err) latestQRDataUrl = url; }); console.log('QR generated. Visit /api/pair'); });
  waClient.on('ready', function() { waReady = true; latestQR = null; latestQRDataUrl = null; console.log('WhatsApp ready!'); });
  waClient.on('authenticated', function() { console.log('WhatsApp authenticated.'); });
  waClient.on('auth_failure', function(msg) { console.error('Auth failure:', msg); waReady = false; });
  waClient.on('disconnected', function(reason) { console.log('Disconnected:', reason); waReady = false; setTimeout(function() { waClient.initialize().catch(function(e) { console.error('Reconnect failed:', e.message); }); }, 10000); });
  waClient.initialize().catch(function(e) { console.error('WA init failed:', e.message); });
}

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

function parseSheetDate(val) {
  if (!val) return null;
  if (val instanceof Date) return val;
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
function isAccountant(ph) { return CONFIG.ACCOUNTANT_PHONES.some(function(p){return ph===p||ph.endsWith(p.slice(-10));}); }
function isMM(ph) { return ph===CONFIG.MM_PHONE||ph.endsWith(CONFIG.MM_PHONE.slice(-10)); }
function isSM(ph) { return ph===CONFIG.SM_PHONE||ph.endsWith(CONFIG.SM_PHONE.slice(-10)); }

async function getLedgerData(dateStr) {
  var rows = await readSheet('Ledger!A:L');
  var target = dateStr || new Date().toISOString().split('T')[0], entries = [];
  for (var i=0;i<rows.length;i++) {
    var row=rows[i]; if(!row[0]||!row[5])continue;
    var cd=parseSheetDate(row[0]); if(!cd)continue;
    if(cd.toISOString().split('T')[0]===target) {
      entries.push({date:cd,entity:row[1]||'',head:row[2]||'',description:row[3]||'',tag:row[4]||'',inOut:row[5]||'',amount:parseAmount(row[6]),mode:row[7]||'',person:row[8]||'',bankAC:row[9]||'',transferTo:row[10]||'',notes:row[11]||''});
    }
  }
  return entries;
}

async function getFundPosition() {
  var rows = await readSheet('Fund Position!A4:J27'), accounts = [];
  for (var i=1;i<rows.length;i++) {
    var r=rows[i]; if(!r[1]||r[1]==='TOTAL')continue;
    accounts.push({num:r[0]||'',company:r[1]||'',bankAC:r[2]||'',opening:parseAmount(r[3]),todayIn:parseAmount(r[4]),todayOut:parseAmount(r[5]),closing:parseAmount(r[6]),cheques:parseAmount(r[7]),netBal:parseAmount(r[8]),status:r[9]||'Usable'});
  }
  return accounts;
}

function parseResponse(text) {
  if(!text)return 'pending'; var l=text.toLowerCase().trim();
  var yes=['yes','ok','okay','approved','done','go ahead','proceed','haan','ha','han','theek hai','thik hai','kar do','karo','y','yep','yea','yeah','sure','fine','agreed','confirmed'];
  for(var i=0;i<yes.length;i++){if(l===yes[i])return 'yes';}
  if(l.indexOf('\u{1F44D}')>=0||l.indexOf('\u2705')>=0||l.indexOf('\u{1F44C}')>=0)return 'yes';
  var no=['no','nahi','nah','rejected','cancel','mat karo','n','nope','deny','denied','reject','nhi'];
  for(var j=0;j<no.length;j++){if(l===no[j])return 'no';}
  if(l.indexOf('\u274C')>=0||l.indexOf('\u{1F44E}')>=0)return 'no';
  var hold=['hold','wait','ruko','later','baad mein','not now','pending','rukko','abhi nahi','bad me'];
  for(var k=0;k<hold.length;k++){if(l===hold[k]||l.indexOf(hold[k])>=0)return 'hold';}
  return 'other';
}

function parseExpenseMessage(body) {
  if(!body)return {vendor:'',amount:0};
  var am=body.match(/(?:rs\.?\s*|inr\s*|amount\s*:?\s*)?(\d[\d,]*\.?\d*)\s*(?:lac|lakh|lacs|l\b|cr|crore)/i), amount=0;
  if(am){amount=parseFloat(am[1].replace(/,/g,'')); if(/cr|crore/i.test(am[0]))amount*=10000000; else if(/lac|lakh|lacs|l\b/i.test(am[0]))amount*=100000;}
  else{var rm=body.match(/(?:rs\.?\s*|inr\s*|\u20B9\s*)(\d[\d,]*\.?\d*)/i); if(rm)amount=parseFloat(rm[1].replace(/,/g,''));}
  return {vendor:body.split('\n')[0].substring(0,150),amount:amount};
}

async function fetchApprovalMessages(days) {
  if(!waReady||!waClient)throw new Error('WhatsApp not connected');
  if(!CONFIG.APPROVAL_GROUP_JID)throw new Error('APPROVAL_GROUP_JID not set.');
  var chat = await waClient.getChatById(CONFIG.APPROVAL_GROUP_JID);
  var allMessages = [];
  var limits = [100,200,500,1000];
  for(var i=0;i<limits.length;i++){
    try{allMessages=await chat.fetchMessages({limit:limits[i]}); console.log('Fetched '+allMessages.length+' msgs (limit '+limits[i]+')'); if(allMessages.length<limits[i])break;}
    catch(e){console.error('Fetch limit '+limits[i]+' failed:',e.message);break;}
  }
  var cutoff=new Date(); cutoff.setDate(cutoff.getDate()-(days||15));
  var filtered=allMessages.filter(function(m){return new Date(m.timestamp*1000)>=cutoff;});
  console.log('Filtered to '+filtered.length+' msgs in last '+days+' days');
  return filtered;
}

async function buildApprovalAudit(days) {
  var messages = await fetchApprovalMessages(days||15);
  var expenses = [], replyMap = {};

  for(var i=0;i<messages.length;i++){
    var msg=messages[i];
    var sender=(msg.author||msg.from||'').replace('@c.us','').replace('@s.whatsapp.net','');
    var msgDate=new Date(msg.timestamp*1000);
    var body=(msg.body||'').trim();
    var hasMedia=msg.hasMedia||false;

    var quotedMsgId=null;
    if(msg.hasQuotedMsg){try{var q=await msg.getQuotedMessage();quotedMsgId=q.id._serialized||q.id.id;}catch(e){}}

    if(quotedMsgId&&(isMM(sender)||isSM(sender))){
      if(!replyMap[quotedMsgId])replyMap[quotedMsgId]={mm:null,sm:null};
      var resp=parseResponse(body);
      if(isMM(sender))replyMap[quotedMsgId].mm={response:resp,date:msgDate,raw:body};
      if(isSM(sender))replyMap[quotedMsgId].sm={response:resp,date:msgDate,raw:body};
    } else if(isAccountant(sender)){
      var msgId=msg.id._serialized||msg.id.id;
      var parsed=parseExpenseMessage(body);
      expenses.push({id:msgId,date:msgDate,body:body||(hasMedia?'[Image/Media]':'[Empty]'),sender:sender,vendor:parsed.vendor||(hasMedia?'[See image]':''),amount:parsed.amount,hasMedia:hasMedia,mmApproval:null,smApproval:null,status:{mm:'pending',sm:'pending'}});
    }
  }

  for(var j=0;j<expenses.length;j++){
    var rep=replyMap[expenses[j].id];
    if(rep){expenses[j].mmApproval=rep.mm;expenses[j].smApproval=rep.sm;expenses[j].status.mm=rep.mm?rep.mm.response:'pending';expenses[j].status.sm=rep.sm?rep.sm.response:'pending';}
  }

  var result={fullyApproved:[],partialApproval:[],noApproval:[],onHold:[],rejected:[],allExpenses:expenses,totalExpenses:expenses.length,totalMessages:messages.length,fetchedDays:days||15};
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

// ============================================================
// ENDPOINTS
// ============================================================
app.get('/health',function(req,res){res.json({status:'ok',version:'2.2',whatsapp:waReady?'connected':'disconnected',sheets:sheetsApi?'initialized':'not configured',botEnabled:CONFIG.BOT_ENABLED,approvalGroup:CONFIG.APPROVAL_GROUP_JID||'not set',accountants:CONFIG.ACCOUNTANT_PHONES.length});});

app.get('/api/pair',function(req,res){
  if(waReady)return res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><h1 style="color:#0f0">WhatsApp Connected</h1></body></html>');
  if(!latestQRDataUrl)return res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><h1 style="color:white">Waiting for QR... refresh in a few seconds</h1></body></html>');
  res.send('<html><body style="display:flex;justify-content:center;align-items:center;min-height:100vh;background:#111"><div style="text-align:center"><h1 style="color:white">Scan QR with WhatsApp</h1><img src="'+latestQRDataUrl+'" style="width:300px"/><p style="color:#888">WhatsApp > Settings > Linked Devices > Link</p></div></body></html>');
});

app.get('/api/wa-status',function(req,res){res.json({connected:waReady,hasQR:!!latestQR});});

app.get('/api/groups',async function(req,res){
  if(!waReady)return res.json({error:'WhatsApp not connected'});
  try{var chats=await waClient.getChats();var groups=chats.filter(function(c){return c.isGroup;}).map(function(c){return{name:c.name,jid:c.id._serialized,participants:c.participants?c.participants.length:0};});res.json({groups:groups});}
  catch(e){res.json({error:e.message});}
});

app.get('/api/bot/on',function(req,res){CONFIG.BOT_ENABLED=true;res.json({botEnabled:true});});
app.get('/api/bot/off',function(req,res){CONFIG.BOT_ENABLED=false;res.json({botEnabled:false});});

app.get('/api/ledger',async function(req,res){
  try{var date=req.query.date||new Date().toISOString().split('T')[0];var entries=await getLedgerData(date);var tIn=0,tOut=0;entries.forEach(function(e){if(e.inOut==='IN')tIn+=e.amount;if(e.inOut==='OUT')tOut+=e.amount;});res.json({date:date,entries:entries,totalIn:tIn,totalOut:tOut,net:tIn-tOut,count:entries.length});}
  catch(e){res.json({error:e.message});}
});

app.get('/api/fund-position',async function(req,res){try{res.json({accounts:await getFundPosition()});}catch(e){res.json({error:e.message});}});

app.get('/api/approval-audit',async function(req,res){
  try{
    var days=parseInt(req.query.days)||15;
    var audit=await buildApprovalAudit(days);
    var fmt=function(e){return{date:e.date.toISOString().split('T')[0],time:e.date.toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata',hour:'2-digit',minute:'2-digit'}),message:e.body.substring(0,300),vendor:e.vendor,amount:e.amount,amountFormatted:e.amount>0?formatINR(e.amount):'',hasMedia:e.hasMedia,mm:e.status.mm,sm:e.status.sm,mmReply:e.mmApproval?e.mmApproval.raw:null,smReply:e.smApproval?e.smApproval.raw:null,mmDate:e.mmApproval?e.mmApproval.date.toISOString().split('T')[0]:null,smDate:e.smApproval?e.smApproval.date.toISOString().split('T')[0]:null};};
    res.json({summary:{period:days+' days',totalMessages:audit.totalMessages,totalExpenseRequests:audit.totalExpenses,fullyApproved:audit.fullyApproved.length,partialApproval:audit.partialApproval.length,noApproval:audit.noApproval.length,onHold:audit.onHold.length,rejected:audit.rejected.length},fullyApproved:audit.fullyApproved.map(fmt),partialApproval:audit.partialApproval.map(fmt),noApproval:audit.noApproval.map(fmt),onHold:audit.onHold.map(fmt),rejected:audit.rejected.map(fmt)});
  }catch(e){res.json({error:e.message});}
});

async function generateDailyReport(dateStr){
  var entries=await getLedgerData(dateStr);var fp=await getFundPosition();
  var tIn=0,tOut=0,inflows=[],outflows=[];
  entries.forEach(function(e){if(e.inOut==='IN'){tIn+=e.amount;inflows.push(e);}if(e.inOut==='OUT'){tOut+=e.amount;outflows.push(e);}});
  var byTag={};outflows.forEach(function(e){var t=e.tag||'Other';if(!byTag[t])byTag[t]={total:0,items:[]};byTag[t].total+=e.amount;byTag[t].items.push(e);});
  return{date:dateStr,totalIn:tIn,totalOut:tOut,net:tIn-tOut,inflows:inflows,outflows:outflows,byTag:byTag,fundPosition:fp,entryCount:entries.length};
}

function buildReportHTML(data){
  var h='<!DOCTYPE html><html><head><meta charset="utf-8"><style>';
  h+='body{font-family:Arial,sans-serif;background:#fff;padding:20px;max-width:800px;margin:0 auto;color:#222}';
  h+='.hdr{text-align:center;border-bottom:2px solid #333;padding-bottom:10px;margin-bottom:15px}.hdr h1{font-size:22px;margin:0}.hdr p{color:#666;margin:4px 0 0}';
  h+='.metrics{display:flex;gap:10px;margin:15px 0}.mc{flex:1;background:#f5f5f5;border-radius:8px;padding:12px;text-align:center}.mc .lbl{font-size:11px;color:#888}.mc .val{font-size:20px;font-weight:bold;margin:4px 0 0}';
  h+='.gn{color:#0a7}.rd{color:#c33}.bl{color:#36a}.sec{font-size:14px;font-weight:bold;color:#555;border-bottom:1px solid #ddd;padding:8px 0 4px;margin:15px 0 8px}';
  h+='table{width:100%;border-collapse:collapse;font-size:12px}th{text-align:left;padding:5px;background:#f0f0f0;font-size:11px;color:#666}td{padding:5px;border-top:1px solid #eee}.amt{text-align:right;font-family:monospace}';
  h+='</style></head><body>';
  h+='<div class="hdr"><h1>Fidato Group - Daily MIS Report</h1><p>'+data.date+' | '+data.entryCount+' transactions</p></div>';
  h+='<div class="metrics"><div class="mc"><div class="lbl">Total Inflows</div><div class="val gn">'+formatINR(data.totalIn)+'</div></div>';
  h+='<div class="mc"><div class="lbl">Total Outflows</div><div class="val rd">'+formatINR(data.totalOut)+'</div></div>';
  h+='<div class="mc"><div class="lbl">Net</div><div class="val '+(data.net>=0?'bl':'rd')+'">'+formatINR(data.net)+'</div></div></div>';
  if(data.inflows.length>0){h+='<div class="sec">INFLOWS</div><table><tr><th>Description</th><th>Entity</th><th>Tag</th><th>Bank A/C</th><th style="text-align:right">Amount</th></tr>';data.inflows.forEach(function(e){h+='<tr><td>'+e.description+'</td><td>'+e.entity+'</td><td>'+e.tag+'</td><td>'+e.bankAC+'</td><td class="amt gn">'+formatINR(e.amount)+'</td></tr>';});h+='</table>';}
  h+='<div class="sec">OUTFLOWS BY CATEGORY</div><table><tr><th>Category</th><th>Items</th><th style="text-align:right">Amount</th></tr>';
  Object.keys(data.byTag).sort(function(a,b){return data.byTag[b].total-data.byTag[a].total;}).forEach(function(t){h+='<tr><td>'+t+'</td><td>'+data.byTag[t].items.length+'</td><td class="amt rd">'+formatINR(data.byTag[t].total)+'</td></tr>';});h+='</table>';
  h+='<div class="sec">FUND POSITION</div><table><tr><th>Account</th><th style="text-align:right">Opening</th><th style="text-align:right">IN</th><th style="text-align:right">OUT</th><th style="text-align:right">Closing</th><th style="text-align:right">Cheques</th><th style="text-align:right">Net</th></tr>';
  data.fundPosition.forEach(function(a){h+='<tr><td>'+a.bankAC+'</td><td class="amt">'+formatINR(a.opening)+'</td><td class="amt gn">'+formatINR(a.todayIn)+'</td><td class="amt rd">'+formatINR(a.todayOut)+'</td><td class="amt">'+formatINR(a.closing)+'</td><td class="amt rd">'+formatINR(a.cheques)+'</td><td class="amt '+(a.netBal<0?'rd':'')+'">'+formatINR(a.netBal)+'</td></tr>';});h+='</table>';
  h+='</body></html>';return h;
}

app.get('/api/preview',async function(req,res){try{var d=req.query.date||new Date().toISOString().split('T')[0];res.send(buildReportHTML(await generateDailyReport(d)));}catch(e){res.status(500).json({error:e.message});}});
app.get('/api/preview-image',async function(req,res){try{var d=req.query.date||new Date().toISOString().split('T')[0];var img=await htmlToImage(buildReportHTML(await generateDailyReport(d)),800,1200);res.set('Content-Type','image/png');res.send(img);}catch(e){res.status(500).json({error:e.message});}});

app.get('/api/daily-report',async function(req,res){
  try{if(!waReady)return res.json({error:'WhatsApp not connected'});if(!CONFIG.BOT_ENABLED)return res.json({error:'Bot paused'});
  var d=req.query.date||new Date().toISOString().split('T')[0];var data=await generateDailyReport(d);var img=await htmlToImage(buildReportHTML(data),800,1200);
  var media=new MessageMedia('image/png',img.toString('base64'),'MIS_'+d+'.png');
  await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,media,{caption:'MIS Report - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut)+' | NET: '+formatINR(data.net)});
  res.json({success:true,date:d});}catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/test-send',async function(req,res){try{if(!waReady)return res.json({error:'Not connected'});await waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,'MIS Bot test - '+new Date().toLocaleString('en-IN',{timeZone:'Asia/Kolkata'}));res.json({success:true});}catch(e){res.json({error:e.message});}});
app.get('/api/report-status',function(req,res){res.json({botEnabled:CONFIG.BOT_ENABLED,whatsapp:waReady,version:'2.2'});});

// 7PM IST daily report
cron.schedule('30 13 * * *',function(){if(!CONFIG.BOT_ENABLED||!waReady)return;var d=new Date().toISOString().split('T')[0];generateDailyReport(d).then(function(data){if(data.entryCount>0){htmlToImage(buildReportHTML(data),800,1200).then(function(img){var m=new MessageMedia('image/png',img.toString('base64'),'MIS.png');waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,m,{caption:'Evening Report - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut)});});}}).catch(function(e){console.error('Cron error:',e.message);});},{timezone:'Asia/Kolkata'});

// 9AM IST morning summary
cron.schedule('30 3 * * *',function(){if(!CONFIG.BOT_ENABLED||!waReady)return;var y=new Date();y.setDate(y.getDate()-1);var d=y.toISOString().split('T')[0];generateDailyReport(d).then(function(data){if(data.entryCount>0){htmlToImage(buildReportHTML(data),800,1200).then(function(img){var m=new MessageMedia('image/png',img.toString('base64'),'MIS.png');waClient.sendMessage(CONFIG.WHATSAPP_GROUP_JID,m,{caption:'Morning Summary - '+d+'\nIN: '+formatINR(data.totalIn)+' | OUT: '+formatINR(data.totalOut)});});}}).catch(function(e){console.error('Cron error:',e.message);});},{timezone:'Asia/Kolkata'});

initGoogleSheets();
createWhatsAppClient();
app.listen(CONFIG.PORT,function(){
  console.log('\n========================================');
  console.log('Fidato MIS Server v2.2');
  console.log('========================================');
  console.log('Port:',CONFIG.PORT);
  console.log('Sheet:',CONFIG.SHEET_ID);
  console.log('Day Book:',CONFIG.WHATSAPP_GROUP_JID);
  console.log('Approval:',CONFIG.APPROVAL_GROUP_JID);
  console.log('Accountants:',CONFIG.ACCOUNTANT_PHONES.join(', '));
  console.log('MM:',CONFIG.MM_PHONE,'SM:',CONFIG.SM_PHONE);
  console.log('\n/health /api/pair /api/groups /api/ledger /api/fund-position');
  console.log('/api/preview /api/preview-image /api/daily-report');
  console.log('/api/approval-audit?days=15 /api/test-send /api/bot/on /api/bot/off');
  console.log('========================================\n');
});
