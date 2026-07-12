// ============================================================
// FIDATO SALES MODULE v1.0.0-b19 (b18 + FEATURE FLAGS: deps.salesFeatures() from the dashboard gates each command. SHIP DEFAULT = booking ONLY; cancel / brokerage_adjust / allocate / ai_planner all OFF until switched on from the control panel. Disabled commands return false (silent, fall through) so the team only sees booking. Booking is never gated - it is the shipped feature) (b17 + ESCAPE HATCH: a stuck/half-open session from an earlier stalled run was swallowing every new message (line-414 session intercept) with no way out - now "reset"/"abort"/"clear"/"stop" clears any session+plan for the sender, AND a fresh high-level command (book/cancel/brokerage/allocate or an NL cancel-transfer) breaks OUT of a stale session instead of being eaten, EXCEPT at decisive money steps (confirm/disposition/transfertarget/brokerage) where we do not silently abandon an approval-ready flow) (b16 + STATUS FIX: the live inventory status is "Allotted" (per the sheet dropdown Unbooked/Allotted/Cancelled/Adjusted), NOT "Sold" - every cancel/brokerage/allocate/planner gate was checking !==Sold and so silently rejected or STALLED on real booked units (the 115-GF plan hang). Now isLiveBooked() accepts Allotted/Sold/Booked and isCancelledStatus() accepts Cancelled/Adjusted; a plan step that cannot open (already cancelled / not live) now advances the plan instead of hanging) (b15 + TRANSFER TARGET: cancel-transfer now asks "which unit NOW or hold"; the tracker writes the CLEAN target unit id in the MONEY OUT To cell so the target pool picks the credit up (was writing a descriptive label that pooled nowhere). Planner auto-links the target from the following allocate step. Tracker rebuilds the Allocations hub after money-out writes and before pool reads (pools were stale: 115-GF showed inflow 0 / used 1cr). apiCancel idempotent on already-cancelled) (b14 + FIXES from live: (1) plan answers are STEP-KEYED not positional - a parsed disposition can no longer be swallowed by the brokerage question (the 114-GF bug where "transfer" was consumed as "forgo" and wrongly routed to M+S); drain runs after reasonconfirm AND after a human brokerage answer. (2) the sales skip-approval toggle now also silences the M+S capital-group routing on cancel (refund/forgo commit directly with approvedBy=skip-toggle) so testing stays out of the capital group) (b13 + AI NL LAYER: free-form messages in the sales group are parsed by aiParseIntent into an ordered plan (book/cancel/brokerage_adjust/allocate), each step validated against the live sheet, the WHOLE resolved plan echoed for a yes/no, then executed by driving the existing structured flows with synthetic pre-filled answers - so gates, previews and approval routing are the SAME tested code. Plans pause at approvals and resume on commit; human still confirms the AI reason-classification; blockers (missing suffix, not-sold, over-pending) halt before anything runs) (b12 + CANCEL REASON AI-CLASSIFY + RECOVER/FORGO + M+S ROUTING + ALLOCATE. cancel now: free-text reason -> aiClassifyReason company_fault|normal (agent can override) -> brokerage recover|forgo -> disposition refund|transfer -> ROUTING: refund OR forgo => M+S in capital approval group, else senior in sales group. New allocate <unit>: apply pooled credit at rate level or to a tranche ("latest" = last with due balance). Verdict handler split so booking(M+S)/cancel(senior or M+S)/brokadjust(senior)/allocate(senior) each resolve correctly across sales + capital groups) (b11 + BROKERAGE ADJUST: brokerage adjust <unit> redirects a broker pending commission on a live unit into a target pool; MONEY OUT Brokerage row, non-GST by sheet rule; senior direct / junior needs senior yes; Mukund DM. Cancel still owns refund + adjust-out) (b10 + LID RESOLUTION FIX for changes: resolvePhone now uses an injected server resolveLidPhone(jid) [same logic as the auth layer: waClient.getContactById(jid).number] plus an explicit LID_PHONE_MAP fallback, because identifySender returns {role,contactName} with NO phone field - so cancel from a linked-device @lid now resolves to the real phone and seniorityOf works) (b9 + CANCELLATION: "cancel <unit>" in the sales group -> shows paid-to-date -> disposition refund/hold-for-transfer. Senior (Umesh/accountant) acts directly; junior (Gautam) posts for a senior yes in-group. On commit the tracker archives+hides the cover as "<unit> - Cancelled - N", moves paid-to-date to the Refund Register, flips inventory to Cancelled. Mukund DM notified every time) (b8 + ECONOMICS BLOCK: after brokerage the bot asks yes/skip to add on-form discount (shares broker commission) / DP discount / gift / NPV / marketing / other, each as % or amount; written to the cover economics cells by the tracker, which returns balance-payable + net-realization. Preview + approval post show the adjustment lines) (b7 + SKIP-APPROVAL TOGGLE: deps.skipApproval() live panel switch; when ON, preview "yes" bypasses the M+S approval post and goes straight to agent re-confirm -> commit. Default OFF (M+S required). Edits honour the toggle too) (b6 + MANUAL TSV: if a unit has no price list filled, the bot asks for the sale value directly instead of dead-ending; commit sends tsv so the API writes it. Lets bookings proceed before Price Lists are populated) (b5 + SALES GROUP ROUTING: primary channel is the dedicated sales group JID (deps.SALES_GROUP_JID); any member may raise a booking there - stable @g.us routing, no @lid/@c.us guessing. Agent DMs still accepted as a fallback. Origin chat for group bookings is the sales group, so re-confirm pings land there) (b4 + GATE FIX: WhatsApp delivers DMs from linked-device users as @lid, not @c.us; the book gate now accepts ANY non-group jid as a DM and rejects only OTHER groups. isAgent resolves @lid via identifySender and re-checks the RESOLVED phone against the agent lists, so 86960253214761@lid -> 917838537000 is recognized) (b3 + FAIL-LOUD: missing TRACKER env vars or a thrown tracker call now CLAIM the message with a clear error instead of silently falling through to the expense flow; commit wrapped; outer catch logs stack head; SALES_AGENT_LIDS whitelist for group @lid authors) (b2 + diagnostic logging on the book path: prints trigger/gate/agent/API-URL/lookup so Railway logs show exactly why a booking is or is not claimed) (b1 + fixes: edit-from-preview no longer crashes; brokerage unit-suffix "3.78L" reads as absolute lakh not %; isAgent resolves group @lid authors via identifySender) - UNIT BOOKING over WhatsApp.
// Separate module; server.js wires it with 3 lines (see WIRING at bottom).
// Flow: accountant/agent says "book <unit> <customer>" (expense group or DM)
//   -> bot LOOKUPs the tracker API, shows price menu (current list = standard,
//      alternatives with delta vs standard) -> broker -> brokerage (% OR amount,
//      both always displayed) -> advance amount/mode/account (LEDGER_ACCOUNTS)
//   -> PREVIEW to agent (incl delta) -> agent confirms -> posted to APPROVAL
//      group with full economics -> M+S both-yes (swipe replies, same rail as
//      expenses) -> agent notified, must RE-CONFIRM -> only then POST booking
//      to the tracker API (inventory flips Sold, cover sheet created/seeded).
//   Any EDIT at re-confirm loops back through approval (verdicts reset).
// Persistence: wa_auth/sales_pending.json (survives restarts on the volume).
// Offline test: node sales.js --test   (no WhatsApp / no network; fetch mocked)
// ============================================================
'use strict';

module.exports = function initSales(deps){
  var CONFIG   = deps.CONFIG;
  var getClient= deps.getClient;                 // () => waClient (lazy; client boots async)
  var identifySender = deps.identifySender;      // server's promoter/contact resolver
  var ACCOUNTS = deps.LEDGER_ACCOUNTS || [];
  var fetchImpl= deps.fetch;
  var fs       = deps.fs;
  var AUTH_DIR = deps.authDir || './wa_auth';
  var API_URL  = deps.TRACKER_API_URL  || process.env.TRACKER_API_URL  || '';
  var API_SECRET=deps.TRACKER_API_SECRET|| process.env.TRACKER_API_SECRET|| '';
  var SALES_GROUP_JID = deps.SALES_GROUP_JID || CONFIG.SALES_GROUP_JID || '';  // dedicated bookings/collections group
  var skipApprovalFn = deps.skipApproval || function(){ return false; };  // live toggle: bypass M+S when true
  var SALES_SENIOR_PHONES = deps.SALES_SENIOR_PHONES || [];  // can approve juniors' changes; own changes execute
  var SALES_JUNIOR_PHONES = deps.SALES_JUNIOR_PHONES || [];  // changes need a senior's approval
  var NOTIFY_DM_PHONE      = deps.NOTIFY_DM_PHONE || '';       // Mukund: DM'd on every non-booking change
  var resolveLidPhone      = deps.resolveLidPhone || null;    // async (jid)->phone, server's lid resolver
  var LID_PHONE_MAP        = deps.LID_PHONE_MAP || {};        // explicit @lid -> phone fallback
  var aiClassifyReason     = deps.aiClassifyReason || null;   // async (text)->{classification,confidence,reasoning}
  var aiParseIntent        = deps.aiParseIntent || null;      // async (text)->{steps:[...]} ordered plan
  var salesFeatures        = deps.salesFeatures || function(){ return { booking:true, cancel:false, brokerage_adjust:false, allocate:false, ai_planner:false }; };
  function featureOn(name){ try{ var f=salesFeatures()||{}; return f[name]===true; }catch(e){ return name==='booking'; } }
  function isLiveBooked(status){ var s=String(status||'').toLowerCase(); return s==='allotted'||s==='sold'||s==='booked'; }
  function isCancelledStatus(status){ var s=String(status||'').toLowerCase(); return s==='cancelled'||s==='adjusted'; }
  var CAPITAL_APPROVAL_JID = deps.CAPITAL_APPROVAL_JID || CONFIG.APPROVAL_GROUP_JID || '';  // M+S refund/forgo channel
  var SALES_AGENT_PHONES = deps.SALES_AGENT_PHONES || [];  // extra numbers allowed to raise bookings (beyond accountants)
  var SALES_AGENT_LIDS = deps.SALES_AGENT_LIDS || [];    // group @lid authors allowed to raise bookings (e.g. Umesh in a group)

  var PENDING_FILE = AUTH_DIR + '/sales_pending.json';

  // ---------- persistence ----------
  function loadPending(){
    try{ return JSON.parse(fs.readFileSync(PENDING_FILE,'utf8')); }catch(e){ return {items:{}}; }
  }
  function savePending(p){
    try{ fs.writeFileSync(PENDING_FILE, JSON.stringify(p,null,2)); }catch(e){ console.error('[sales] save failed', e.message); }
  }

  // in-memory Q&A sessions, keyed by sender jid (short-lived; ok to lose on restart)
  var sessions = {};

  // ---------- formatting ----------
  function inr(n){
    n = Math.round(Number(n)||0);
    var neg = n<0; if(neg) n=-n;
    var s;
    if(n>=1e7)      s=(n/1e7).toFixed(2).replace(/\.00$/,'')+' Cr';
    else if(n>=1e5) s=(n/1e5).toFixed(2).replace(/\.00$/,'')+' L';
    else            s=n.toLocaleString('en-IN');
    return (neg?'-':'')+'\u20b9'+s;
  }
  function inrFull(n){
    n=Math.round(Number(n)||0); var neg=n<0; if(neg)n=-n;
    return (neg?'-':'')+'\u20b9'+n.toLocaleString('en-IN');
  }
  function pct(x){ return (Math.round(x*10000)/100)+'%'; }

  // ---------- parsing ----------
  var UNIT_RE = /\b(\d{2,3}[A-Z]?)\s*-\s*(GF|FF|SF|TF|PLOT)\b/i;
  function parseOpening(text){
    // "book 214-GF Rajesh Kumar" (customer optional at this stage)
    var t=String(text||'').trim();
    if(!/^book\b/i.test(t)) return null;
    var m=t.match(UNIT_RE);
    var unit = m ? (m[1].toUpperCase()+'-'+m[2].toUpperCase()) : null;
    var rest = t.replace(/^book\b/i,'');
    if(m) rest = rest.replace(m[0],'');
    var customer = rest.replace(/[,;]+/g,' ').replace(/\s+/g,' ').trim();
    return { unit: unit, customer: customer || null };
  }
  function parseBrokerage(text, tsv){
    // "2%" / "2" (<=15) / "0.02" => percentage.  "3.78L" / "1 cr" / "378000" => absolute.
    // Always returns BOTH: {pct (fraction), amt}.
    var t=String(text||'').trim().replace(/,/g,'');
    var m=t.match(/^([\d.]+)\s*%$/);
    if(m){ var p=parseFloat(m[1])/100; return {pct:p, amt:Math.round(p*tsv)}; }
    // unit suffix => unambiguous absolute amount
    if(/^[\d.]+\s*(cr|crore|l|lac|lakh|lk|k)$/i.test(t)){
      var abs=parseAmount(t);
      if(abs===null||abs<0) return null;
      return {pct: tsv>0? abs/tsv : 0, amt: abs};
    }
    var n=parseFloat(t);
    if(isNaN(n)||n<0) return null;
    // RULE: a bare number <=15 is always a PERCENT ("2" => 2%, "0.5" => 0.5%);
    // anything larger, or unit-suffixed above, is an absolute amount.
    if(n>0 && n<=15){ var p2=n/100; return {pct:p2, amt:Math.round(p2*tsv)}; }
    return {pct: tsv>0? n/tsv : 0, amt: Math.round(n)};
  }
  function parseEconLine(text, tsv){
    // returns {skip:true} | {pct} | {amt} | null(invalid)
    var t=String(text||'').trim().toLowerCase();
    if(t==='' || t==='0' || t==='skip' || t==='none' || t==='no' || t==='-') return {skip:true};
    var m=t.match(/^([\d.]+)\s*%$/);
    if(m){ return {pct: parseFloat(m[1]) }; }                 // "5%" -> 5 (percent)
    // unit-suffixed => absolute amount
    if(/^[\d.]+\s*(cr|crore|l|lac|lakh|lk|k)$/i.test(t)){ var a=parseAmount(t); return a?{amt:a}:null; }
    var n=parseFloat(t.replace(/,/g,''));
    if(isNaN(n)||n<0) return null;
    if(n>0 && n<=100) return {pct:n};                          // bare <=100 -> percent
    return {amt: Math.round(n) };                              // large -> amount
  }
  function parseAmount(text){
    var t=String(text||'').toLowerCase().replace(/,/g,'').trim();
    var m=t.match(/^([\d.]+)\s*(cr|crore|l|lac|lakh|lk|k)?$/);
    if(!m) return null;
    var n=parseFloat(m[1]); if(isNaN(n)||n<0) return null;
    var u=m[2]||'';
    if(u==='cr'||u==='crore') n*=1e7;
    else if(u==='l'||u==='lac'||u==='lakh'||u==='lk') n*=1e5;
    else if(u==='k') n*=1e3;
    return Math.round(n);
  }

  // ---------- tracker API ----------
  function trackerPost(payload){
    payload.secret = API_SECRET;
    return fetchImpl(API_URL, {
      method:'POST',
      headers:{'Content-Type':'text/plain'},
      body: JSON.stringify(payload),
      redirect:'follow'
    }).then(function(r){ return r.json(); });
  }
  function lookupUnit(unit){ return trackerPost({action:'lookup', unit:unit}); }
  function commitCancel(f){ return trackerPost({action:'cancel', unit:f.unit, disposition:f.disposition, transferTarget:f.transferTarget||'', brokerageTreatment:f.brokerageTreatment||'none', agent:f.agentName||'', approvedBy:f.approvedBy||''}); }
  function brokerageInfo(unit){ return trackerPost({action:'brokerageInfo', unit:unit}); }
  function poolInfo(unit){ return trackerPost({action:'poolInfo', unit:unit}); }
  function commitAllocate(f){ return trackerPost({action:'allocate', unit:f.unit, amount:f.amount, method:f.method, tranche:f.tranche||0, side:f.side||'nongst'}); }
  function commitBrokerageAdjust(f){ return trackerPost({action:'brokerageAdjust', unit:f.unit, target:f.target, amount:f.amount, mode:f.mode, approvedBy:f.approvedBy||'', agent:f.agentName||''}); }
  function commitBooking(f){
    var payload={
      action:'booking', unit:f.unit, customer:f.customer,
      broker: f.broker||'', brokeragePct: f.bkPct||0,
      advance: { amount:f.advAmt||0, mode:f.advMode||'Cheque', account:f.advAcct||'' },
      agent: f.agentName||'', date: new Date().toISOString().slice(0,10)
    };
    if(f.manualTsv || !f.listIndex){ payload.tsv=f.tsv; }        // manual value -> send TSV directly
    else { payload.priceList=f.listIndex; }                       // else let the list drive it
    if(f.economics && Object.keys(f.economics).length) payload.economics=f.economics;
    return trackerPost(payload);
  }

  // ---------- message builders ----------
  function priceMenu(lk){
    var lines=['Unit '+lk.unit+' ('+lk.configKey+')'];
    var cur=lk.prices.filter(function(p){return p.isCurrent;})[0];
    if(cur) lines.push('Standard ('+cur.name+', current): '+inrFull(cur.price));
    var alts=lk.prices.filter(function(p){return !p.isCurrent;});
    if(alts.length){
      lines.push('Other lists:');
      alts.forEach(function(p){
        lines.push('  '+p.list+') '+p.name+': '+inrFull(p.price)+'  ('+(p.delta>=0?'+':'')+inr(p.delta)+' / '+(p.deltaPct>=0?'+':'')+p.deltaPct+'% vs standard)');
      });
    }
    lines.push('');
    lines.push('Reply "ok" for standard, or the list number.');
    return lines.join('\n');
  }
  function acctMenu(){
    var out=['Advance received in which account?'];
    for(var i=0;i<ACCOUNTS.length;i++) out.push('  '+(i+1)+') '+ACCOUNTS[i]);
    out.push('Reply the number or exact name.');
    return out.join('\n');
  }
  function deltaLine(f){
    if(f.manualTsv) return 'Price: '+inrFull(f.tsv)+' (manual TSV)';
    if(!f.stdPrice || f.tsv===f.stdPrice) return 'Price: '+inrFull(f.tsv)+' ('+f.listName+' = standard)';
    var d=f.tsv-f.stdPrice, dp=f.stdPrice? Math.round(d/f.stdPrice*10000)/100 : 0;
    return 'Price: '+inrFull(f.tsv)+' ('+f.listName+') \u2014 '+inr(Math.abs(d))+' '+(d<0?'BELOW':'ABOVE')+' standard ('+(d<0?'':'+')+dp+'%)';
  }
  function econLines(f){
    var e=f.economics||{}, out=[];
    ECON_LINES.forEach(function(L){
      if(e[L.key+'Pct']!==undefined) out.push('  '+L.label+': '+e[L.key+'Pct']+'%');
      else if(e[L.key+'Amt']!==undefined) out.push('  '+L.label+': '+inrFull(e[L.key+'Amt']));
    });
    return out;
  }
  function previewText(f){
    var lines=[
      'BOOKING PREVIEW \u2014 confirm before it goes for approval',
      '\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500',
      'Unit:      '+f.unit+' ('+f.configKey+')',
      'Customer:  '+f.customer,
      deltaLine(f),
      'Broker:    '+(f.broker||'\u2014'),
      'Brokerage: '+inrFull(f.bkAmt)+' ('+pct(f.bkPct)+' of TSV)'
    ];
    var el=econLines(f);
    if(el.length){ lines.push('Adjustments:'); lines=lines.concat(el); }
    lines.push('Advance:   '+inrFull(f.advAmt)+' via '+f.advMode+(f.advAcct?(' \u2192 '+f.advAcct):''));
    lines.push('Balance:   '+inrFull(f.tsv-(f.advAmt||0)));
    lines.push('');
    lines.push('Reply "yes" to send for M+S approval, "edit" to change, "cancel" to drop.');
    return lines.join('\n');
  }
  function approvalText(f){
    return [
      '\ud83c\udfe0 BOOKING for approval',
      '\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500',
      'Unit:      '+f.unit+' ('+f.configKey+')',
      'Customer:  '+f.customer,
      deltaLine(f),
      'Broker:    '+(f.broker||'\u2014'),
      'Brokerage: '+inrFull(f.bkAmt)+' ('+pct(f.bkPct)+' of TSV)'
    ].concat(econLines(f).length?['Adjustments:'].concat(econLines(f)):[]).concat([
      'Advance:   '+inrFull(f.advAmt)+' via '+f.advMode+(f.advAcct?(' \u2192 '+f.advAcct):''),
      'Balance:   '+inrFull(f.tsv-(f.advAmt||0)),
      'Raised by: '+(f.agentName||'agent'),
      '',
      'Reply to THIS message: yes / no'
    ]).join('\n');
  }
  var ECON_LINES=[
    {key:'disc',  label:'On-form discount (comes out of broker commission)'},
    {key:'dp',    label:'Down-payment discount'},
    {key:'gift',  label:'Gift'},
    {key:'npv',   label:'NPV adjustment'},
    {key:'mktg',  label:'Marketing / staff'},
    {key:'other', label:'Other'}
  ];
  var EDIT_FIELDS=['customer','price list','broker','brokerage','advance amount','advance mode','advance account'];
  function editMenu(){
    var out=['Which field? Reply the number:'];
    EDIT_FIELDS.forEach(function(x,i){ out.push('  '+(i+1)+') '+x); });
    return out.join('\n');
  }

  // ---------- session helpers ----------
  function jidOf(msg){ return msg.author || msg.from; }
  async function resolvePhone(msg){
    var j=String(jidOf(msg)||''); var num=j.replace(/@.*$/,'');
    if(/@lid$/.test(j)){
      // 1) use the server-provided resolver (same logic the auth layer uses)
      if(resolveLidPhone){
        try{ var rp=await resolveLidPhone(j); rp=String(rp||'').replace(/[^0-9]/g,''); if(rp && rp.length>=10) return rp; }catch(e){ console.log('[sales] resolveLidPhone err: '+e.message); }
      }
      // 2) explicit lid->phone map fallback (e.g. Mukund's known lid)
      if(LID_PHONE_MAP && LID_PHONE_MAP[j]) return LID_PHONE_MAP[j];
      // 3) if this lid is the known M linked-device, map to Mukund's notify number as a last resort
    }
    return num;
  }
  function seniorityOf(phone){
    if(SALES_SENIOR_PHONES.indexOf(phone)>=0) return 'senior';
    if(SALES_JUNIOR_PHONES.indexOf(phone)>=0) return 'junior';
    if((CONFIG.ACCOUNTANT_PHONES||[]).indexOf(phone)>=0) return 'senior'; // default accountants act as senior for changes
    return 'none';
  }
  async function senderName(msg){
    if(!identifySender) return '';
    try{ var i=await identifySender(jidOf(msg)); return (i&&i.contactName)||''; }catch(e){ return ''; }
  }
  async function notifyMukund(text){
    if(!NOTIFY_DM_PHONE) return;
    try{ await getClient().sendMessage(NOTIFY_DM_PHONE+'@c.us', text); }catch(e){ console.log('[sales] mukund DM failed: '+e.message); }
  }
  function newItemId(){ return 'bk-'+Date.now().toString(36)+Math.random().toString(36).slice(2,6); }
  async function isAgent(msg){
    var j=String(jidOf(msg)||'');
    var num=j.replace(/@.*$/,'');
    // 1. direct phone match
    if((CONFIG.ACCOUNTANT_PHONES||[]).indexOf(num)>=0) return true;
    if((SALES_AGENT_PHONES||[]).indexOf(num)>=0) return true;
    // 2. explicit lid whitelist
    if((SALES_AGENT_LIDS||[]).indexOf(j)>=0) return true;
    // 3. resolve @lid (or anything) via server's identifySender, then re-check the RESOLVED phone
    if(identifySender){
      try{
        var info=await identifySender(j);
        var rphone=info && (info.phone||info.resolvedPhone||'');
        rphone=String(rphone||'').replace(/@.*$/,'');
        if(rphone){
          if((CONFIG.ACCOUNTANT_PHONES||[]).indexOf(rphone)>=0) return true;
          if((SALES_AGENT_PHONES||[]).indexOf(rphone)>=0) return true;
        }
        var role=String((info&&info.role)||'');
        if(/account/i.test(role)) return true;
        // MM/SM resolving as a promoter can also raise (raiser != approver is enforced elsewhere)
        if(/^(m|s|mm|sm)$/i.test(role.trim())) return true;
      }catch(e){ console.log('[sales] isAgent identifySender error: '+e.message); }
    }
    return false;
  }
  function promoterOf(msg){
    // resolve M / S using the server's identifySender when available
    return Promise.resolve().then(function(){
      if(identifySender) return identifySender(jidOf(msg)||'');
      return null;
    }).then(function(info){
      var role=(info&&info.role)||'';
      var name=(info&&info.contactName)||'';
      if(/mm|madhur/i.test(role)||/madhur/i.test(name)) return 'M';
      if(/sm|sumit/i.test(role)||/sumit/i.test(name))  return 'S';
      var num=String(jidOf(msg)||'').replace(/@.*$/,'');
      if(num===CONFIG.MM_PHONE) return 'M';
      if(num===CONFIG.SM_PHONE) return 'S';
      if((CONFIG.LID_WHITELIST||[]).indexOf(String(jidOf(msg)||''))>=0) return 'M'; // known M linked device
      return null;
    });
  }

  // ---------- the main handler ----------
  // Returns true if this module consumed the message.
  async function handleSalesMessage(msg){
    try{
      var client=getClient();
      if(!client) return false;
      var body=String(msg.body||'').trim();
      var from=msg.from;
      var senderJid=jidOf(msg);

      // ===== 1. verdict swipes in the APPROVAL group on OUR posts =====
      if(from===CONFIG.APPROVAL_GROUP_JID && msg.hasQuotedMsg){
        var p=loadPending();
        var quoted=await msg.getQuotedMessage().catch(function(){return null;});
        var qid=quoted && quoted.id && quoted.id._serialized;
        var itemId=null;
        if(qid){ Object.keys(p.items).forEach(function(k){ if(p.items[k].msgId===qid && (!p.items[k].kind || p.items[k].kind==='booking')) itemId=k; }); }
        if(itemId){
          var it=p.items[itemId];
          if(it.state!=='await_approval') { return true; } // stale swipe on decided item
          var who=await promoterOf(msg);
          if(!who) return true;
          var low=body.toLowerCase();
          var v = /^(yes|ok|okay|approved|approve|y|\ud83d\udc4d)\b/.test(low) ? 'yes'
                : /^(no|reject|rejected|n)\b/.test(low) ? 'no' : null;
          if(!v) return true;
          it.verdicts=it.verdicts||{};
          it.verdicts[who]=v;
          if(v==='no'){
            it.state='rejected';
            savePending(p);
            await client.sendMessage(CONFIG.APPROVAL_GROUP_JID,'\u274c Booking '+it.fields.unit+' rejected by '+who+'.');
            await client.sendMessage(it.originChat,'\u274c Booking '+it.fields.unit+' was rejected by '+who+'. Not committed.');
            return true;
          }
          if(it.verdicts.M==='yes' && it.verdicts.S==='yes'){
            it.state='await_reconfirm';
            savePending(p);
            await client.sendMessage(CONFIG.APPROVAL_GROUP_JID,'\u2705 Booking '+it.fields.unit+' approved (M+S). Awaiting agent re-confirmation.');
            await client.sendMessage(it.originChat,
              '\u2705 APPROVED (M+S): booking '+it.fields.unit+' for '+it.fields.customer+'.\n'+
              'Re-confirm to commit to inventory: reply "confirm '+it.fields.unit+'"\n'+
              'Or "edit '+it.fields.unit+'" to change (changes go back for approval).');
          } else {
            savePending(p); // one yes recorded, waiting for the other (silent, like s6.4)
          }
          return true;
        }
        // no BOOKING item matched this quoted msg. If a non-booking sales item (cancel/brokadjust/
        // allocate) matches, fall through to those verdict handlers below; else defer to server.
        var otherSales=false;
        if(qid){ Object.keys(p.items).forEach(function(k){ if(p.items[k].msgId===qid && p.items[k].kind && p.items[k].kind!=='booking') otherSales=true; }); }
        if(!otherSales) return false;
        // else: do not return - let execution continue to the cancel/ba/allocate verdict block
      }

      // ===== 2. re-confirm / edit after approval (origin chat) =====
      var mConfirm=body.match(/^confirm\s+(\S+)/i);
      var mEdit=body.match(/^edit\s+(\S+)/i);
      if(mConfirm||mEdit){
        var unitRef=(mConfirm?mConfirm[1]:mEdit[1]).toUpperCase();
        var p2=loadPending(); var itemId2=null;
        Object.keys(p2.items).forEach(function(k){
          var it=p2.items[k];
          if(it.fields.unit===unitRef && (it.state==='await_reconfirm'||it.state==='await_approval')) itemId2=k;
        });
        if(!itemId2) return false;
        var it2=p2.items[itemId2];
        if(mConfirm){
          if(it2.state!=='await_reconfirm'){
            await getClient().sendMessage(from,'Booking '+unitRef+' is not approved yet \u2014 waiting on M+S.');
            return true;
          }
          var res;
          try{ res=await commitBooking(it2.fields); }
          catch(ce){ console.log('[sales] commit THREW: '+ce.message);
            await getClient().sendMessage(from,'\u26a0\ufe0f Tracker unreachable while committing '+unitRef+' ('+ce.message+'). Booking NOT written - try "confirm '+unitRef+'" again or tell M.');
            return true; }
          if(res && res.ok){
            it2.state='committed'; savePending(p2);
            await maybePlanAdvance_(it2.fields);
            await getClient().sendMessage(from,'\u2705 '+unitRef+' BOOKED & inventory updated.\nCustomer: '+it2.fields.customer+'\nTSV: '+inrFull(res.tsv)+' ('+res.priceList+')\nBalance: '+inrFull(res.balance));
            var wasApproved = it2.verdicts && it2.verdicts.M==='yes' && it2.verdicts.S==='yes';
            if(wasApproved) await getClient().sendMessage(CONFIG.APPROVAL_GROUP_JID,'\ud83d\udcd7 '+unitRef+' committed to inventory ('+it2.fields.customer+').');
          } else {
            await getClient().sendMessage(from,'\u26a0\ufe0f Tracker error for '+unitRef+': '+((res&&res.error)||'no response')+'. Not committed \u2014 try "confirm '+unitRef+'" again or tell M.');
          }
          return true;
        }
        // edit -> open an edit session; on completion re-post to approval (verdicts reset)
        sessions[senderJid]={ mode:'edit', itemId:itemId2, step:'pickfield', fields:p2.items[itemId2].fields, originChat:from };
        await getClient().sendMessage(from, editMenu());
        return true;
      }

      // ===== 2.9 escape hatch: clear any stuck session/plan =====
      if(/^(reset|abort|start over|cancel plan|clear|stop)$/i.test(String(body).trim().toLowerCase())){
        var hadSes=!!sessions[senderJid];
        delete sessions[senderJid];
        var pr=loadPending(); var hadPlan=pr.plans&&pr.plans[senderJid];
        if(pr.plans) delete pr.plans[senderJid]; savePending(pr);
        if(hadSes||hadPlan){ await getClient().sendMessage(from,'\u21ba Cleared. Any in-progress flow was reset \u2014 you can start fresh.'); }
        else { await getClient().sendMessage(from,'Nothing in progress to reset.'); }
        return true;
      }

      // ===== 3. active Q&A session =====
      if(sessions[senderJid]){
        // a fresh high-level command breaks out of a stale session instead of being swallowed
        var freshCmd=/^(book|cancel|brokerage\s+adjust|allocate)\b/i.test(String(body).trim()) ||
                     (aiParseIntent && /\d{2,3}\s*-?\s*(GF|FF|SF|TF|PLOT)/i.test(String(body)) && /(cancel|transfer|refund|book|allocate|brokerage|milestone)/i.test(String(body)) && String(body).length>25);
        var curSes=sessions[senderJid];
        var curStep=curSes&&curSes.step;
        // only break out if the session is NOT at a decisive money step (don't abandon an approval-ready flow silently)
        if(freshCmd && !msg._synthetic && curStep!=='confirm' && curStep!=='disposition' && curStep!=='transfertarget' && curStep!=='brokerage'){
          delete sessions[senderJid];
          var pr2=loadPending(); if(pr2.plans) delete pr2.plans[senderJid]; savePending(pr2);
          // fall through to normal command handling below (do NOT return)
        } else {
          var handledSes=await advanceSession(msg, sessions[senderJid]);
          if(handledSes && !msg._synthetic){
            var sesNow=sessions[senderJid];
            if(sesNow && sesNow.fields && sesNow.fields.planOwner) await drainPlanAnswers_(senderJid, sesNow.originChat);
          }
          return handledSes;
        }
      }

      // ===== 3.3 allocate: apply a unit's pooled credit at rate level or to a tranche =====
      var mAlloc=body.match(/^allocate\s+(\d{2,3}[A-Z]?-(?:GF|FF|SF|TF|PLOT))\b/i);
      if(mAlloc){
        if(!featureOn('allocate')){ return false; }   // feature disabled from the dashboard
        var aunit=mAlloc[1].toUpperCase();
        var inSGa = SALES_GROUP_JID && from===SALES_GROUP_JID;
        if(!inSGa) return false;
        var aphone2=await resolvePhone(msg);
        var arole=seniorityOf(aphone2);
        if(arole==='none') return false;
        var pinfo=await poolInfo(aunit);
        if(!pinfo||!pinfo.ok){ await client.sendMessage(from,'\u26a0\ufe0f '+((pinfo&&pinfo.error)||'lookup failed')+'.'); return true; }
        if(!(pinfo.available>0)){ await client.sendMessage(from,'\u26a0\ufe0f No pooled credit available on '+aunit+' to allocate.'); return true; }
        var ainfo=identifySender?await identifySender(senderJid):null;
        sessions[senderJid]={ mode:'allocate', step:'amount', originChat:from,
          fields:{ unit:aunit, available:pinfo.available, availGst:pinfo.availableGst, availNon:pinfo.availableNonGst, originChat:from,
                   planOwner:(planStore().plans[senderJid]?senderJid:null),
                   tranches:pinfo.tranches||[], raiserRole:arole, raiserName:(ainfo&&ainfo.contactName)||'', raiserPhone:aphone2 } };
        await client.sendMessage(from,'ALLOCATE credit \u2014 '+aunit+'\nAvailable pool credit: '+inrFull(pinfo.available)+'\n\nHow much to allocate? (up to '+inrFull(pinfo.available)+')');
        return true;
      }

      // ===== 3.4 brokerage adjust: redirect broker pending commission to a target unit =====
      var mBrok=body.match(/^brokerage\s+adjust(?:\s+(\d{2,3}[A-Z]?-(?:GF|FF|SF|TF|PLOT)))?/i);
      if(mBrok){
        if(!featureOn('brokerage_adjust')){ return false; }   // feature disabled from the dashboard
        var inSGb = SALES_GROUP_JID && from===SALES_GROUP_JID;
        if(!inSGb) return false;
        var bphone=await resolvePhone(msg);
        var brole=seniorityOf(bphone);
        if(brole==='none') return false;
        var bunit=mBrok[1]?mBrok[1].toUpperCase():null;
        if(!bunit){ await client.sendMessage(from,'Brokerage adjust \u2014 which unit is the commission on? e.g. "brokerage adjust 214-GF"'); return true; }
        var bi=await brokerageInfo(bunit);
        if(!bi||!bi.ok){ await client.sendMessage(from,'\u26a0\ufe0f '+((bi&&bi.error)||'lookup failed')+'.'); return true; }
        if(!isLiveBooked(bi.status)){ await client.sendMessage(from,'\u26a0\ufe0f '+bunit+' is not a live booked unit (status '+bi.status+'). Brokerage set-off needs a live unit.'); return true; }
        if(!(bi.pendingCommission>0)){ await client.sendMessage(from,'\u26a0\ufe0f No pending broker commission on '+bunit+' ('+(bi.broker||'no broker')+') to set off.'); return true; }
        var binfo=identifySender?await identifySender(senderJid):null;
        sessions[senderJid]={ mode:'brokadjust', step:'amount', originChat:from,
          fields:{ unit:bunit, broker:bi.broker, pending:bi.pendingCommission, originChat:from,
                   planOwner:(planStore().plans[senderJid]?senderJid:null),
                   raiserRole:brole, raiserName:(binfo&&binfo.contactName)||'', raiserPhone:bphone } };
        await client.sendMessage(from,
          'BROKERAGE ADJUST \u2014 '+bunit+'\nBroker: '+(bi.broker||'(none)')+'\nPending commission: '+inrFull(bi.pendingCommission)+
          '\n\nHow much to set off? (amount, up to '+inrFull(bi.pendingCommission)+')');
        return true;
      }

      // ===== 3.5 cancellation: "cancel <unit>" (Gautam->Umesh approve; Umesh acts direct) =====
      var mCancel=body.match(/^cancel\s+(\d{2,3}[A-Z]?-(?:GF|FF|SF|TF|PLOT))\s*$/i);  // exact form only; longer text goes to the AI planner
      if(mCancel){
        if(!featureOn('cancel')){ return false; }   // feature disabled from the dashboard
        var cunit=mCancel[1].toUpperCase();
        var inSG = SALES_GROUP_JID && from===SALES_GROUP_JID;
        if(!inSG) return false;                      // cancellations only in the sales group
        var cphone=await resolvePhone(msg);
        var role=seniorityOf(cphone);
        if(role==='none') return false;              // not a sales actor
        var clk=await lookupUnit(cunit);
        if(!clk||!clk.ok){ await client.sendMessage(from,'\u26a0\ufe0f '+((clk&&clk.error)||'lookup failed')+'.'); return true; }
        if(isCancelledStatus(clk.status)){ await client.sendMessage(from,'\u2139\ufe0f '+cunit+' is already '+clk.status+' \u2014 nothing to cancel.'); if(planStore().plans[senderJid]) await planStepDone_(senderJid, from); return true; }
        if(!isLiveBooked(clk.status)){ await client.sendMessage(from,'\u26a0\ufe0f '+cunit+' is not a live booked unit (status '+clk.status+') \u2014 nothing to cancel.'); if(planStore().plans[senderJid]) await planStepDone_(senderJid, from); return true; }
        var cinfo=identifySender?await identifySender(senderJid):null;
        sessions[senderJid]={ mode:'cancel', step:'reason', originChat:from,
          fields:{ unit:cunit, customer:clk.customer, paid:clk.paidToDate||0, originChat:from,
                   planOwner:(planStore().plans[senderJid]?senderJid:null),
                   raiserRole:role, raiserName:(cinfo&&cinfo.contactName)||'', raiserPhone:cphone } };
        await client.sendMessage(from,
          'CANCEL '+cunit+' \u2014 '+(clk.customer||'(no customer)')+'\nPaid to date: '+inrFull(clk.paidToDate||0)+
          '\n\nWhat is the reason for cancellation? (type it in your own words)');
        return true;
      }
      // verdict on a pending cancel approval (senior replies in the sales group, quoting our post)
      if((from===SALES_GROUP_JID || from===CAPITAL_APPROVAL_JID || from===CONFIG.APPROVAL_GROUP_JID) && msg.hasQuotedMsg){
        var pc=loadPending();
        var qm=await msg.getQuotedMessage().catch(function(){return null;});
        var qmid=qm&&qm.id&&qm.id._serialized;
        var cid=null,baid=null;
        if(qmid){ Object.keys(pc.items).forEach(function(k){ if(pc.items[k].msgId===qmid){ if(pc.items[k].kind==='cancel')cid=k; if(pc.items[k].kind==='brokadjust')baid=k; } }); }
        if(baid){
          var bit=pc.items[baid];
          if(bit.state!=='await_senior') return true;
          var baphone=await resolvePhone(msg);
          if(seniorityOf(baphone)!=='senior'){ await client.sendMessage(from,'Only a senior can approve this.'); return true; }
          if(/^(no|reject|n)\b/i.test(body.toLowerCase())){ bit.state='rejected'; savePending(pc); await client.sendMessage(from,'\u274c Brokerage adjust for '+bit.fields.unit+' rejected.'); return true; }
          if(/^(yes|ok|approve|approved|y)\b/i.test(body.toLowerCase())){
            bit.fields.approvedBy=(await senderName(msg))||'senior';
            var bres=await commitBrokerageAdjust(bit.fields);
            if(bres&&bres.ok){ bit.state='done'; savePending(pc);
              await client.sendMessage(from,'\u2705 Brokerage set off: '+inrFull(bres.amount)+' from '+bres.unit+' \u2192 '+bres.target+'. Broker pending now '+inrFull(bres.pendingAfter)+'.');
              await notifyMukund('\u2139\ufe0f BROKERAGE ADJUST\nUnit: '+bres.unit+'\nBroker: '+(bres.broker||'\u2014')+'\nSet off: '+inrFull(bres.amount)+'\nCredited to: '+bres.target+'\nApproved by: '+bit.fields.approvedBy+'\nRaised by: '+bit.fields.raiserName);
              await maybePlanAdvance_(bit.fields);
            } else { await client.sendMessage(from,'\u26a0\ufe0f Brokerage adjust failed: '+((bres&&bres.error)||'no response')+'.'); }
            return true;
          }
          return true;
        }
        if(cid){
          var cit=pc.items[cid];
          if(cit.state!=='await_senior' && cit.state!=='await_ms'){ return true; }
          var aphone=await resolvePhone(msg);
          var isNo=/^(no|reject|n)\b/i.test(body.toLowerCase());
          var isYes=/^(yes|ok|approve|approved|y)\b/i.test(body.toLowerCase());
          if(!isNo && !isYes) return true;
          if(cit.needsMS){
            // M+S dual approval (capital group). resolve who.
            var who=await cancelPromoter_(msg);
            if(!who){ return true; }   // not M or S
            if(isNo){ cit.state='rejected'; savePending(pc);
              await client.sendMessage(cit.approvalChat||from,'\u274c Cancellation of '+cit.fields.unit+' rejected by '+who+'.');
              await client.sendMessage(cit.originChat,'\u274c '+cit.fields.unit+' cancellation rejected by '+who+'. Not committed.');
              return true; }
            cit.verdicts=cit.verdicts||{}; cit.verdicts[who]='yes';
            if(cit.verdicts.M==='yes' && cit.verdicts.S==='yes'){
              cit.fields.approvedBy='M+S';
              var resM=await commitCancel(cit.fields);
              cit.state='done'; savePending(pc);
              await cancelDone_(resM, cit.fields, 'M+S ('+cit.fields.raiserName+' raised)');
            } else { savePending(pc); }   // one yes, wait for the other
            return true;
          }
          // single senior (sales group)
          if(seniorityOf(aphone)!=='senior'){ await client.sendMessage(from,'Only a senior can approve this cancellation.'); return true; }
          if(isNo){ cit.state='rejected'; savePending(pc);
            await client.sendMessage(from,'\u274c Cancellation of '+cit.fields.unit+' rejected.'); return true; }
          cit.fields.approvedBy=(await senderName(msg))||'senior';
          var res=await commitCancel(cit.fields);
          cit.state='done'; savePending(pc);
          await cancelDone_(res, cit.fields, 'senior ('+cit.fields.raiserName+' raised)');
          return true;
        }
      }

      // ===== 4. opening: "book ..." from an agent =====
      var open=parseOpening(body);
      if(!open) return await maybeNL_(msg, from, senderJid, body);
      console.log('[sales] book trigger from='+from+' author='+(msg.author||'-')+' body='+JSON.stringify(body));
      // PRIMARY channel: the dedicated sales group. In-group, any member may raise a booking.
      var inSalesGroup = SALES_GROUP_JID && from===SALES_GROUP_JID;
      if(!inSalesGroup){
        // allow known agents to also book via direct message as a convenience
        var isGroup = /@g\.us$/.test(from);
        if(isGroup){ console.log('[sales] rejected: booking must be in the sales group (from='+from+')'); return false; }
        var agentOK = await isAgent(msg);
        if(!agentOK){ console.log('[sales] rejected: DM sender not a recognized agent (from='+from+')'); return false; }
      }
      console.log('[sales] accepted book for unit='+open.unit+' via '+(inSalesGroup?'SALES GROUP':'agent DM')+' API_URL='+(API_URL?'set':'MISSING'));
      if(!API_URL || !API_SECRET){
        await client.sendMessage(from,'\u26a0\ufe0f Booking system not configured: TRACKER_API_URL / TRACKER_API_SECRET missing on the server (Railway env vars). Tell M.');
        return true;
      }

      if(!open.unit){
        await client.sendMessage(from,'Which unit? e.g. "book 214-GF Rajesh Kumar"');
        return true;
      }
      var lk;
      try{ lk=await lookupUnit(open.unit); }
      catch(fe){ console.log('[sales] lookup THREW: '+fe.message); await client.sendMessage(from,'\u26a0\ufe0f Tracker unreachable ('+fe.message+'). Tell M.'); return true; }
      console.log('[sales] lookup result: '+JSON.stringify(lk).slice(0,200));
      if(!lk || !lk.ok){
        await client.sendMessage(from,'\u26a0\ufe0f '+((lk&&lk.error)||'tracker not reachable')+'.');
        return true;
      }
      if(isLiveBooked(lk.status)){
        await client.sendMessage(from,'\u26a0\ufe0f '+open.unit+' is already SOLD'+(lk.customer?(' to '+lk.customer):'')+'.');
        return true;
      }
      var info=identifySender ? await identifySender(senderJid) : null;
      var hasPrices = lk.prices && lk.prices.length;
      var cur = hasPrices ? (lk.prices.filter(function(x){return x.isCurrent;})[0]||lk.prices[lk.prices.length-1]) : null;
      sessions[senderJid]={
        mode:'new', originChat: from, lk: lk, noPrices: !hasPrices,
        step: open.customer ? (hasPrices?'pricelist':'manualtsv') : 'customer',
        fields:{
          unit: open.unit, configKey: lk.configKey, customer: open.customer,
          listIndex: cur?cur.list:0, listName: cur?cur.name:'Manual', tsv: cur?cur.price:0,
          stdPrice: cur?cur.price:0, mortgaged: lk.mortgaged, manualTsv: !hasPrices,
          originChat: from, planOwner:(planStore().plans[senderJid]?senderJid:null),
          agentName: (info&&info.contactName)||'', agentJid: senderJid
        }
      };
      if(!open.customer){
        await client.sendMessage(from,'Customer name for '+open.unit+'?');
      } else if(hasPrices){
        await client.sendMessage(from, priceMenu(lk));
      } else {
        await client.sendMessage(from,'No price list is filled for '+open.unit+' ('+(lk.configKey||'no config')+' ) yet.\nEnter the total sale value (TSV) manually \u2014 e.g. "1.98cr" or "19800000".');
      }
      return true;

    }catch(e){
      console.error('[sales] handler error:', e.message, (e.stack||'').split('\n')[1]||'');
      return false;
    }
  }

  // ---------- session state machine ----------
  // ============ AI NATURAL-LANGUAGE LAYER (single-op + multi-step planner) ============
  // Parse -> resolve vs sheet -> ECHO full plan -> confirm -> drive the EXISTING flows
  // by synthesizing the structured opening + pre-filled answers. Gates unchanged.
  function planStore(){ var p=loadPending(); p.plans=p.plans||{}; return p; }
  function fmtStep_(i,s,resolved){
    var n=(i+1)+') ';
    if(s.op==='cancel') return n+'CANCEL '+s.unit+(resolved.customer?(' ('+resolved.customer+', paid '+inrFull(resolved.paid||0)+')'):'')+
      (s.disposition?(' \u2014 money: '+s.disposition):' \u2014 money: (will ask)')+(s.brokerage?(' \u2014 brokerage: '+s.brokerage):'');
    if(s.op==='brokerage_adjust') return n+'BROKERAGE SET-OFF '+(s.amount?inrFull(s.amount):'(amount?)')+' from '+s.unit+
      (resolved.broker?(' ('+resolved.broker+', pending '+inrFull(resolved.pending||0)+')'):'')+' \u2192 '+(s.target||'(target?)');
    if(s.op==='allocate') return n+'ALLOCATE '+(s.amount?inrFull(s.amount):'(amount?)')+' on '+s.unit+
      (s.tranche==='latest'?' to the LATEST due milestone':(s.tranche?(' to tranche '+s.tranche):(s.method==='rate'?' at rate level':' (how? will ask)')))+
      (resolved.available!==undefined?(' [pool available '+inrFull(resolved.available)+']'):'');
    if(s.op==='book') return n+'BOOK '+s.unit+' for '+(s.customer||'(customer?)')+(s.broker?(' \u2014 broker '+s.broker):'')+
      (s.brokeragePct?(' @'+s.brokeragePct+'%'):'')+(s.advance?(' \u2014 advance '+inrFull(s.advance)):'');
    return n+JSON.stringify(s);
  }
  function answersFor_(s){
    // STEP-KEYED synthetic answers; a step with no key pauses for the human. Never positional.
    if(s.op==='cancel'){
      return {answers:{
        reason: s.reason||null,                       // reasonconfirm never keyed - human confirms the AI read
        brokerage: s.brokerage ? (s.brokerage==='forgo'?'2':'1') : null,
        disposition: s.disposition ? (s.disposition==='refund'?'1':'2') : null
      }};
    }
    if(s.op==='brokerage_adjust'){
      return {answers:{
        amount: s.amount?String(s.amount):null,
        mode: s.mode ? (/cash/i.test(s.mode)?'2':'1') : (s.amount?'1':null),
        target: s.target||null,
        confirm:'yes'
      }};
    }
    if(s.op==='allocate'){
      return {answers:{
        amount: s.amount?String(s.amount):null,
        method: s.tranche?'2':(s.method==='rate'?'1':null),
        tranche: s.tranche?String(s.tranche):null,
        confirm:'yes'
      }};
    }
    if(s.op==='book'){
      return {answers:{
        customer: s.customer||null,
        pricelist: s.priceList?String(s.priceList):'ok',
        manualtsv: null,
        broker: s.broker||'none',
        brokerage: s.broker ? (s.brokeragePct?String(s.brokeragePct)+'%':(s.brokerageAmt?String(s.brokerageAmt):null)) : null,
        econ_ask:'skip',
        advamt: (s.advance!==undefined&&s.advance!==null)?String(s.advance):'0',
        advmode: s.advance ? (/cash/i.test(s.advanceMode||'')?'2':(/transfer|bank/i.test(s.advanceMode||'')?'3':'1')) : null,
        advacct: s.account||null,
        preview:'yes'
      }};
    }
    return {answers:{}};
  }
  function openingFor_(s){
    if(s.op==='cancel') return 'cancel '+s.unit;
    if(s.op==='brokerage_adjust') return 'brokerage adjust '+s.unit;
    if(s.op==='allocate') return 'allocate '+s.unit;
    if(s.op==='book') return 'book '+s.unit+(s.customer?(' '+s.customer):'');
    return '';
  }
  async function runPlanStep_(ownerJid, originChat){
    var p=planStore(); var pl=p.plans[ownerJid];
    if(!pl || pl.idx>=pl.steps.length){ if(pl){ delete p.plans[ownerJid]; savePending(p);} return; }
    var s=pl.steps[pl.idx];
    var ans=answersFor_(s);
    pl.stepAnswers = ans.answers||{};
    // link: a transfer-cancel followed by an allocate on unit X transfers straight to X
    if(s.op==='cancel' && s.disposition==='transfer'){
      var nxt=pl.steps[pl.idx+1];
      pl.stepAnswers.transfertarget = (nxt && nxt.op==='allocate' && nxt.unit) ? String(nxt.unit).toUpperCase() : 'hold';
    }
    savePending(p);
    var client=getClient();
    await client.sendMessage(originChat,'\u25b6\ufe0f Plan step '+(pl.idx+1)+'/'+pl.steps.length+': '+openingFor_(s));
    await feedSynthetic_(ownerJid, originChat, openingFor_(s));
    await drainPlanAnswers_(ownerJid, originChat);        // keyed: feeds only steps it has answers for
  }
  async function drainPlanAnswers_(ownerJid, chat){
    var guard=0;
    while(guard<6){
      var ses=sessions[ownerJid]; if(!ses) break;
      var p=planStore(); var pl=p.plans[ownerJid]; if(!pl||!pl.stepAnswers) break;
      var val=pl.stepAnswers[ses.step]; if(val===undefined||val===null) break;
      delete pl.stepAnswers[ses.step]; savePending(p);
      await feedSynthetic_(ownerJid, chat, String(val));
      guard++;
    }
  }
  async function feedSynthetic_(ownerJid, chat, text){
    var fake={ from: chat, author: ownerJid, body: String(text), hasQuotedMsg:false, _synthetic:true };
    try{ await handleSalesMessage(fake); }catch(e){ console.log('[plan] synthetic err '+e.message); }
  }
  async function planStepDone_(ownerJid, originChat){
    var p=planStore(); var pl=p.plans[ownerJid];
    if(!pl) return;
    pl.idx++; savePending(p);
    if(pl.idx>=pl.steps.length){
      delete p.plans[ownerJid]; savePending(p);
      await getClient().sendMessage(originChat,'\u2705 Plan complete \u2014 all '+pl.steps.length+' step(s) done.');
      return;
    }
    await runPlanStep_(ownerJid, originChat);
  }
  async function maybePlanAdvance_(f){
    // called after any op commits; f carries planOwner when running under a plan
    if(f && f.planOwner){ await planStepDone_(f.planOwner, f.originChat||SALES_GROUP_JID); }
  }

  async function maybeNL_(msg, from, senderJid, body){
    if(msg._synthetic) return false;
    if(!featureOn('ai_planner')) return false;   // free-form AI planner disabled from the dashboard
    if(!aiParseIntent) return false;
    if(!SALES_GROUP_JID || from!==SALES_GROUP_JID) return false;
    var t=String(body||'');
    if(t.length<10) return false;
    if(!/\d{2,3}\s*-?\s*(GF|FF|SF|TF|PLOT)?/i.test(t)) return false;
    if(!/(book|cancel|refund|transfer|broker|brokerage|allocate|adjust|milestone|credit|set\s*off|move)/i.test(t)) return false;
    var ph=await resolvePhone(msg);
    if(seniorityOf(ph)==='none') return false;
    var parsed=null;
    try{ parsed=await aiParseIntent(t); }catch(e){ console.log('[plan] parse err '+e.message); return false; }
    if(!parsed||!parsed.steps||!parsed.steps.length) return false;
    var client=getClient();
    // validate units + resolve live facts per step
    var lines=[], blockers=[];
    for(var i=0;i<parsed.steps.length;i++){
      var s=parsed.steps[i]; var r={};
      s.unit=String(s.unit||'').toUpperCase().replace(/\s+/g,'');
      if(!/^\d{2,3}[A-Z]?-(GF|FF|SF|TF|PLOT)$/.test(s.unit)){ blockers.push('Step '+(i+1)+': which unit exactly? "'+(s.unit||'?')+'" needs the floor suffix (e.g. 105-GF).'); lines.push(fmtStep_(i,s,r)); continue; }
      try{
        if(s.op==='cancel'){ var lk=await lookupUnit(s.unit); if(lk&&lk.ok){ r.customer=lk.customer; r.paid=lk.paidToDate; if(isCancelledStatus(lk.status)) blockers.push('Step '+(i+1)+': '+s.unit+' is already '+lk.status+'.'); else if(!isLiveBooked(lk.status)) blockers.push('Step '+(i+1)+': '+s.unit+' is not a live booked unit (status '+lk.status+').'); } }
        else if(s.op==='brokerage_adjust'){ var bi=await brokerageInfo(s.unit); if(bi&&bi.ok){ r.broker=bi.broker; r.pending=bi.pendingCommission; if(!(bi.pendingCommission>0)) blockers.push('Step '+(i+1)+': no pending brokerage on '+s.unit+'.'); if(s.amount&&s.amount>bi.pendingCommission+1) blockers.push('Step '+(i+1)+': '+inrFull(s.amount)+' exceeds pending '+inrFull(bi.pendingCommission)+'.'); } }
        else if(s.op==='allocate'){ var pi=await poolInfo(s.unit); if(pi&&pi.ok){ r.available=pi.available; } }
        else if(s.op==='book'){ var lb=await lookupUnit(s.unit); if(lb&&lb.ok&&isLiveBooked(lb.status)) blockers.push('Step '+(i+1)+': '+s.unit+' already booked'+(lb.customer?(' to '+lb.customer):'')+'.'); }
      }catch(e){}
      lines.push(fmtStep_(i,s,r));
    }
    var head='\ud83e\udd16 I read that as '+(parsed.steps.length>1?('a '+parsed.steps.length+'-step plan'):'this operation')+':';
    var msgTxt=head+'\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n'+lines.join('\n');
    if(blockers.length){
      msgTxt+='\n\n\u26a0\ufe0f Cannot run yet:\n'+blockers.join('\n')+'\n\nFix and resend, or use the structured commands.';
      await client.sendMessage(from,msgTxt); return true;
    }
    msgTxt+='\n\nApprovals still apply per step (refund/forgo \u2192 M+S; changes \u2192 senior).\nReply "yes" to run this plan, "no" to drop.';
    var info=identifySender?await identifySender(senderJid):null;
    sessions[senderJid]={ mode:'plan', step:'confirm', originChat:from,
      fields:{ raiserName:(info&&info.contactName)||'', raiserPhone:ph },
      planSteps: parsed.steps };
    await client.sendMessage(from,msgTxt);
    return true;
  }

  async function cancelRoute_(msg, ses, f){
    var from=ses.originChat;
    var send=function(t){ return getClient().sendMessage(from,t); };
    // ROUTING: refund OR forgo(company) => M+S in capital approval group. else senior in sales group.
    var needsMS = (f.disposition==='refund') || (f.brokerageTreatment==='forgo');
    var skipT=false; try{ skipT=!!skipApprovalFn(); }catch(e){}
    if(needsMS && skipT && f.raiserRole==='senior'){
      // TESTING MODE: skip-approval toggle is ON - commit directly, do NOT post to the capital group
      f.needsMS=false; f.approvedBy='(skip-toggle testing)';
      var resT=await commitCancel(f);
      delete sessions[jidOf(msg)];
      await send('\u26a1 (Testing: M+S skipped) ');
      await cancelDone_(resT,f,'senior direct, skip-toggle');
      return true;
    }
    f.needsMS=needsMS;
    if(f.raiserRole==='senior' && !needsMS){
      f.approvedBy=f.raiserName||'senior';
      var res=await commitCancel(f);
      delete sessions[jidOf(msg)];
      await cancelDone_(res,f,'senior direct');
      return true;
    }
    // build the approval post
    var toCapital = needsMS;
    var chan = toCapital ? CAPITAL_APPROVAL_JID : from;
    var gate = toCapital ? 'M + S' : 'a senior';
    var itemId='cx-'+Date.now().toString(36);
    var postText=(toCapital?'\ud83d\uddd1 CANCELLATION for M+S approval':'\ud83d\uddd1 CANCELLATION for senior approval')+
      '\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500'+
      '\nUnit: '+f.unit+'\nCustomer: '+f.customer+'\nPaid: '+inrFull(f.paid)+
      '\nReason: '+(f.companyFault?'company non-delivery':'customer cancellation')+
      '\nMoney: '+f.human+
      '\nBrokerage: '+(f.brokerageTreatment==='recover'?'recover from broker':'FOREGO (write off)')+
      '\nRaised by: '+(f.raiserName||'agent')+
      '\n\nReply to THIS message: yes / no';
    var sent=await getClient().sendMessage(chan, postText);
    var p=loadPending();
    p.items[itemId]={ kind:'cancel', needsMS:needsMS, msgId:sent&&sent.id&&sent.id._serialized, fields:f,
              verdicts:{}, state: needsMS?'await_ms':'await_senior', originChat:from, approvalChat:chan, at:Date.now() };
    savePending(p);
    delete sessions[jidOf(msg)];
    await send('Sent for '+gate+' approval'+(toCapital?' (capital approval group).':'.'));
    return true;
  }
  async function cancelPromoter_(msg){
    // returns 'M' | 'S' | null based on identifySender/phone
    var ph=await resolvePhone(msg);
    if(ph===CONFIG.MM_PHONE) return 'M';
    if(ph===CONFIG.SM_PHONE) return 'S';
    if(identifySender){ try{ var i=await identifySender(jidOf(msg)); var r=String((i&&i.role)||''); if(/^m$|mm|madhur/i.test(r))return 'M'; if(/^s$|sm|sumit/i.test(r))return 'S'; }catch(e){} }
    return null;
  }
  async function cancelDone_(res, f, byWhom){
    var client=getClient();
    if(res&&res.ok&&res.alreadyCancelled){
      await client.sendMessage(f.originChat,'\u2139\ufe0f '+f.unit+' was already cancelled \u2014 nothing re-done.');
      await maybePlanAdvance_(f);
      return;
    }
    if(res&&res.ok){
      var brokLine = (res.brokerageTreatment && res.brokerageTreatment!=='none' && res.broker)
        ? ('\nBrokerage: '+(res.brokerageTreatment==='recover'?'recoverable ':'written off ')+inrFull(res.brokeragePaid||0)+' ('+res.broker+')')
        : '';
      var moneyLine='';
      if(res.customerMoney!==undefined && res.poolCreditExcluded>0){
        moneyLine='\nCustomer money moved: '+inrFull(res.customerMoney)+' (cheque '+inrFull(res.customerCheque||0)+' / cash '+inrFull(res.customerCash||0)+')'+
                  '\nPool credit excluded (broker/other-unit money, not transferred): '+inrFull(res.poolCreditExcluded);
      }
      var headAmt=(res.customerMoney!==undefined)?res.customerMoney:res.paid;
      await client.sendMessage(f.originChat,'\u2705 '+f.unit+' CANCELLED ('+(f.human||'')+'). '+inrFull(headAmt)+' \u2192 '+res.disposition+' ('+res.creditId+'). Archived as "'+res.archived+'".'+moneyLine+brokLine);
      await notifyMukund('\u2139\ufe0f Booking CANCELLED\nUnit: '+f.unit+'\nCustomer: '+f.customer+'\nReason: '+(f.companyFault?'company non-delivery':'customer cancellation')+'\nPaid: '+inrFull(res.paid)+'\nMoney: '+res.disposition+' ('+res.creditId+')'+brokLine+'\nBy: '+byWhom);
    } else {
      await client.sendMessage(f.originChat,'\u26a0\ufe0f Cancel failed: '+((res&&res.error)||'no response')+'.');
    }
    if(res&&res.ok) await maybePlanAdvance_(f);
  }

  async function advanceSession(msg, ses){
    var client=getClient();
    var from=ses.originChat;
    var body=String(msg.body||'').trim();
    var low=body.toLowerCase();
    var f=ses.fields;
    var send=function(t){ return client.sendMessage(from,t); };

    if(low==='cancel'){ delete sessions[jidOf(msg)]; await send('Booking flow cancelled.'); return true; }

    // ----- plan confirm mode -----
    if(ses.mode==='plan'){
      if(/^(no|n|cancel|drop)$/i.test(low)){ delete sessions[jidOf(msg)]; await send('Plan dropped.'); return true; }
      if(!/^(yes|y|ok|run)$/i.test(low)){ await send('Reply "yes" to run the plan or "no" to drop.'); return true; }
      var ownerJid=jidOf(msg);
      var p=planStore();
      p.plans[ownerJid]={ steps: ses.planSteps, idx: 0, originChat: from, raiserName: f.raiserName };
      savePending(p);
      delete sessions[ownerJid];
      await runPlanStep_(ownerJid, from);
      return true;
    }

    // ----- allocate mode -----
    if(ses.mode==='allocate'){
      if(ses.step==='amount'){
        var av=parseAmount(body);
        if(av===null||av<=0){ await send('Enter the amount to allocate (e.g. "3L").'); return true; }
        if(av>f.available+1){ await send('That exceeds available credit ('+inrFull(f.available)+'). Enter a smaller amount.'); return true; }
        f.amount=av; ses.step='method';
        await send('Apply how?\n1) Rate level (reduce overall balance)\n2) Against a specific tranche (milestone)\nReply 1 or 2.'); return true;
      }
      if(ses.step==='method'){
        if(/^1\b|rate/i.test(low)){ f.method='rate'; f.side=(f.availNon>=f.amount?'nongst':'gst');
          ses.step='confirm';
          await send('ALLOCATE preview\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\nUnit: '+f.unit+'\nAllocate: '+inrFull(f.amount)+' at RATE level\n\nReply "yes" to proceed, "cancel" to drop.'); return true; }
        if(/^2\b|tranche|milestone/i.test(low)){ f.method='tranche'; ses.step='tranche';
          var tl=['Which tranche / milestone?'];
          (f.tranches||[]).forEach(function(t){ tl.push('  '+t.idx+') '+(t.label||('Tranche '+t.idx))+(t.due?(' \u2014 due '+inrFull(t.due)):'')); });
          tl.push('Reply the number, or "latest" for the last one with a due balance.');
          await send(tl.join('\n')); return true; }
        await send('Reply 1 (Rate level) or 2 (Tranche).'); return true;
      }
      if(ses.step==='tranche'){
        var ti=null;
        if(/^latest$/i.test(low)){
          var withDue=(f.tranches||[]).filter(function(t){return t.due>t.poolApplied;});
          ti = withDue.length ? withDue[withDue.length-1].idx : ((f.tranches||[]).length||1);
        } else { var n=parseInt(low,10); if(n>=1&&n<=8) ti=n; }
        if(!ti){ await send('Reply a tranche number (1-8) or "latest".'); return true; }
        f.tranche=ti; f.side=(f.availNon>=f.amount?'nongst':'gst');
        var tobj=(f.tranches||[]).filter(function(t){return t.idx===ti;})[0];
        ses.step='confirm';
        await send('ALLOCATE preview\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\nUnit: '+f.unit+'\nAllocate: '+inrFull(f.amount)+' to '+(tobj?(tobj.label||('Tranche '+ti)):('Tranche '+ti))+(tobj&&tobj.due?(' (due '+inrFull(tobj.due)+')'):'')+'\n\nReply "yes" to proceed, "cancel" to drop.'); return true;
      }
      if(ses.step==='confirm'){
        if(!/^(yes|y|ok)$/i.test(low)){ await send('Reply "yes" to proceed or "cancel".'); return true; }
        if(f.raiserRole==='senior'){
          var res=await commitAllocate(f);
          delete sessions[jidOf(msg)];
          if(res&&res.ok){
            var where = res.method==='rate' ? 'at rate level' : ('to '+(res.trancheLabel||('tranche '+res.tranche)));
            await send('\u2705 Allocated '+inrFull(res.amount)+' on '+res.unit+' '+where+'. Balance now '+inrFull(res.balanceAfter)+'.');
            await notifyMukund('\u2139\ufe0f CREDIT ALLOCATED\nUnit: '+res.unit+'\nAmount: '+inrFull(res.amount)+'\nApplied: '+where+'\nBalance after: '+inrFull(res.balanceAfter)+'\nBy: '+f.raiserName+' (senior, direct)');
            await maybePlanAdvance_(f);
          } else { await send('\u26a0\ufe0f Allocate failed: '+((res&&res.error)||'no response')+'.'); }
          return true;
        }
        var itemId='al-'+Date.now().toString(36);
        var postText='\ud83d\udcd2 ALLOCATE for senior approval'+
          '\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500'+
          '\nUnit: '+f.unit+'\nAllocate: '+inrFull(f.amount)+(f.method==='tranche'?(' to tranche '+f.tranche):' at rate level')+
          '\nRaised by: '+(f.raiserName||'junior')+'\n\nReply to THIS message: yes / no';
        var sent=await getClient().sendMessage(from, postText);
        var p=loadPending();
        p.items[itemId]={ kind:'allocate', msgId:sent&&sent.id&&sent.id._serialized, fields:f, state:'await_senior', originChat:from, at:Date.now() };
        savePending(p); delete sessions[jidOf(msg)];
        await send('Sent for senior approval.'); return true;
      }
    }

    // ----- brokerage-adjust mode -----
    if(ses.mode==='brokadjust'){
      if(ses.step==='amount'){
        var a=parseAmount(body);
        if(a===null||a<=0){ await send('Enter the amount to set off (e.g. "5L" or "500000").'); return true; }
        if(a>f.pending+1){ await send('That exceeds the pending commission ('+inrFull(f.pending)+'). Enter a smaller amount.'); return true; }
        f.amount=a; ses.step='mode';
        await send('Set off via? 1) Cheque  2) Cash/PDC  (recorded for the ledger; brokerage is non-GST either way)'); return true;
      }
      if(ses.step==='mode'){
        var bmm={'1':'Cheque','2':'Cash','cheque':'Cheque','cash':'Cash','pdc':'Cash'};
        var bmode=bmm[low]; if(!bmode){ await send('Reply 1 (Cheque) or 2 (Cash/PDC).'); return true; }
        f.mode=bmode; ses.step='target';
        await send('Credit this to which unit / customer? (the target that receives the pool credit, e.g. "310-FF")'); return true;
      }
      if(ses.step==='target'){
        if(!body){ await send('Name the target unit or customer.'); return true; }
        f.target=body.trim(); ses.step='confirm';
        await send('BROKERAGE ADJUST preview\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\nUnit: '+f.unit+'  (broker '+(f.broker||'\u2014')+')\nSet off: '+inrFull(f.amount)+' via '+f.mode+'\nCredit to: '+f.target+'\nBroker pending after: '+inrFull(f.pending-f.amount)+'\n\nReply "yes" to proceed, "cancel" to drop.'); return true;
      }
      if(ses.step==='confirm'){
        if(!/^(yes|y|ok)$/i.test(low)){ await send('Reply "yes" to proceed or "cancel".'); return true; }
        if(f.raiserRole==='senior'){
          f.approvedBy=f.raiserName||'senior';
          var res=await commitBrokerageAdjust(f);
          delete sessions[jidOf(msg)];
          if(res&&res.ok){
            await send('\u2705 Brokerage set off: '+inrFull(res.amount)+' from '+res.unit+' ('+(res.broker||'broker')+') \u2192 '+res.target+'. Broker pending now '+inrFull(res.pendingAfter)+'.');
            await notifyMukund('\u2139\ufe0f BROKERAGE ADJUST\nUnit: '+res.unit+'\nBroker: '+(res.broker||'\u2014')+'\nSet off: '+inrFull(res.amount)+' ('+f.mode+')\nCredited to: '+res.target+'\nBroker pending after: '+inrFull(res.pendingAfter)+'\nBy: '+f.raiserName+' (senior, direct)');
            await maybePlanAdvance_(f);
          } else { await send('\u26a0\ufe0f Brokerage adjust failed: '+((res&&res.error)||'no response')+'.'); }
          return true;
        }
        var itemId='ba-'+Date.now().toString(36);
        var postText='\ud83d\udcb8 BROKERAGE ADJUST for senior approval'+
          '\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500'+
          '\nUnit: '+f.unit+' (broker '+(f.broker||'\u2014')+')\nSet off: '+inrFull(f.amount)+' via '+f.mode+
          '\nCredit to: '+f.target+'\nRaised by: '+(f.raiserName||'junior')+
          '\n\nReply to THIS message: yes / no';
        var sent=await getClient().sendMessage(from, postText);
        var p=loadPending();
        p.items[itemId]={ kind:'brokadjust', msgId:sent&&sent.id&&sent.id._serialized, fields:f,
                          state:'await_senior', originChat:from, at:Date.now() };
        savePending(p); delete sessions[jidOf(msg)];
        await send('Sent for senior approval.'); return true;
      }
    }

    // ----- cancel mode -----
    if(ses.mode==='cancel'){
      // step 1: free-text reason -> AI classifies company-fault vs normal
      if(ses.step==='reason'){
        if(!body){ await send('Please type the reason for cancellation.'); return true; }
        f.reasonText=body;
        var cls={classification:'normal',confidence:0,reasoning:''};
        if(aiClassifyReason){ try{ cls=await aiClassifyReason(body)||cls; }catch(e){ console.log('[sales] aiClassifyReason err: '+e.message); } }
        f.reasonClass=cls.classification;
        var isCompany = cls.classification==='company_fault';
        f.companyFault=isCompany;
        var read = isCompany ? 'COMPANY non-delivery (our fault)' : 'CUSTOMER-side cancellation';
        ses.step='reasonconfirm';
        await send('Read as: '+read+(cls.reasoning?('\n("'+cls.reasoning+'")'):'')+
          '\n\nIs that right?\n1) Yes, '+ (isCompany?'company fault':'customer cancelled') +
          '\n2) No, it is the '+(isCompany?'customer\u2019s cancellation':'company\u2019s non-delivery'));
        return true;
      }
      if(ses.step==='reasonconfirm'){
        if(/^2\b|^no\b/i.test(low)){ f.companyFault=!f.companyFault; }         // agent overrides the AI read
        else if(!/^1\b|^yes\b|^y\b/i.test(low)){ await send('Reply 1 (yes) or 2 (no).'); return true; }
        // brokerage treatment prompt (recover vs forgo), framed by fault
        var brokerLine = 'Past brokerage on this unit:';
        ses.step='brokerage';
        if(f.companyFault){
          await send(brokerLine+'\n(Company fault \u2014 forgoing is standard, and needs M+S special approval.)\n1) Recover anyway\n2) Forego (write off) \u2014 M+S approval');
        } else {
          await send(brokerLine+'\n(Customer cancellation \u2014 brokerage is revoked; already-paid is normally recovered.)\n1) Recover (log as recoverable from broker)\n2) Forego (write off)');
        }
        // plan step: feed the answer for whichever step we are now on (keyed, never positional)
        if(f.planOwner) await drainPlanAnswers_(f.planOwner, ses.originChat);
        return true;
      }
      if(ses.step==='brokerage'){
        var bt = /^1\b|recover/i.test(low) ? 'recover' : /^2\b|forego|forgo|write/i.test(low) ? 'forgo' : null;
        if(!bt){ await send('Reply 1 (Recover) or 2 (Forego).'); return true; }
        f.brokerageTreatment=bt;
        ses.step='disposition';
        await send('What happens to the customer money ('+inrFull(f.paid)+')?\n1) Refund\n2) Hold for transfer to another unit\nReply 1 or 2.');
        if(f.planOwner) await drainPlanAnswers_(f.planOwner, ses.originChat);
        return true;
      }
      if(ses.step==='transfertarget'){
        var tt=String(body||'').trim().toUpperCase();
        if(/^HOLD$/i.test(tt)){ f.transferTarget=''; }
        else if(/^\d{2,3}[A-Z]?-(GF|FF|SF|TF|PLOT)$/.test(tt)){ f.transferTarget=tt; }
        else { await send('Reply a unit id like "114-FF", or "hold".'); return true; }
        f.human = f.transferTarget ? ('Transfer credit to '+f.transferTarget) : 'Hold in pool for later';
        // fall through to the routing logic by simulating the disposition tail
        return await cancelRoute_(msg, ses, f);
      }
      if(ses.step==='disposition'){
        var d = /^1$|refund/i.test(low) ? 'Refund' : /^2$|transfer|hold/i.test(low) ? 'Transfer-pending' : null;
        if(!d){ await send('Reply 1 (Refund) or 2 (Hold for transfer).'); return true; }
        f.disposition = d==='Refund' ? 'refund' : 'transfer';
        f.human = d==='Refund' ? 'Refund into pool' : 'Transfer credit out';
        if(f.disposition==='transfer' && !f.transferTarget){
          ses.step='transfertarget';
          await send('Transfer the credit to which unit NOW? (e.g. "114-FF")\nOr reply "hold" to park it in the pool for later.');
          if(f.planOwner) await drainPlanAnswers_(f.planOwner, ses.originChat);
          return true;
        }
        return await cancelRoute_(msg, ses, f);
      }
    }

    // ----- edit mode -----
    if(ses.mode==='edit'){
      if(ses.step==='pickfield'){
        var n=parseInt(low,10);
        if(!(n>=1&&n<=EDIT_FIELDS.length)){ await send(editMenu()); return true; }
        ses.editField=n; ses.step='newval';
        if(n===2){ await send(priceMenu(ses.lkCache||{unit:f.unit,configKey:f.configKey,prices:pricesFromFields(f)})); }
        else if(n===7){ await send(acctMenu()); }
        else { await send('New value for '+EDIT_FIELDS[n-1]+'?'); }
        return true;
      }
      if(ses.step==='newval'){
        var ok=applyEdit(ses, body);
        if(!ok.done){ await send(ok.msg); return true; }
        if(!ses.itemId){
          // editing BEFORE first send (from the preview stage): back to preview, nothing posted yet
          ses.mode='new'; ses.step='preview';
          await send(previewText(f)); return true;
        }
        // re-post for approval with verdicts reset (or straight to re-confirm if skipping)
        var p=loadPending(); var it=p.items[ses.itemId];
        var skipE=false; try{ skipE=!!skipApprovalFn(); }catch(e){}
        it.fields=f;
        if(skipE){
          it.verdicts={M:'skip',S:'skip'}; it.state='await_reconfirm'; it.msgId=null;
          savePending(p); delete sessions[jidOf(msg)];
          await send('\u26a1 Updated (approval skipped). Re-confirm to commit: reply "confirm '+f.unit+'".');
          return true;
        }
        it.verdicts={}; it.state='await_approval';
        var sent=await client.sendMessage(CONFIG.APPROVAL_GROUP_JID, approvalText(f));
        it.msgId=sent && sent.id && sent.id._serialized;
        savePending(p);
        delete sessions[jidOf(msg)];
        await send('Updated. Re-sent for M+S approval (previous approvals reset).');
        return true;
      }
    }

    // ----- new-booking mode -----
    switch(ses.step){
      case 'customer':
        if(!body){ await send('Customer name?'); return true; }
        f.customer=body;
        if(ses.noPrices){ ses.step='manualtsv'; await send('No price list for '+f.unit+' ('+(f.configKey||'no config')+') yet.\nEnter the total sale value (TSV) manually \u2014 e.g. "1.98cr" or "19800000".'); return true; }
        ses.step='pricelist';
        await send(priceMenu(ses.lk)); return true;

      case 'manualtsv': {
        var mv=parseAmount(body);
        if(mv===null||mv<=0){ await send('Enter a valid amount, e.g. "1.98cr" or "19800000".'); return true; }
        f.tsv=mv; f.stdPrice=mv; f.listName='Manual'; f.listIndex=0; f.manualTsv=true;
        ses.step='broker';
        await send('TSV set to '+inrFull(mv)+' (manual).\nBroker name? (or "none")'); return true;
      }
      case 'pricelist': {
        var pick=null;
        if(low==='ok'){ pick=ses.lk.prices.filter(function(x){return x.isCurrent;})[0]; }
        else { var ln=parseInt(low,10); pick=ses.lk.prices.filter(function(x){return x.list===ln;})[0]; }
        if(!pick){ await send('Reply "ok" for standard or a valid list number.'); return true; }
        f.listIndex=pick.list; f.listName=pick.name; f.tsv=pick.price;
        ses.step='broker';
        await send('Broker name? (or "none")'); return true;
      }
      case 'broker':
        f.broker = /^none$/i.test(body)? '' : body;
        if(!f.broker){ f.bkPct=0; f.bkAmt=0; ses.step='econ_ask'; await send('No broker. Add discounts / adjustments (on-form discount, DP, gift, NPV, marketing, other)? Reply "yes" or "skip".'); return true; }
        ses.step='brokerage';
        await send('Brokerage? Enter % (e.g. "2%") or amount (e.g. "378000" / "3.78L"). I will show both.'); return true;

      case 'brokerage': {
        var bk=parseBrokerage(body, f.tsv);
        if(!bk){ await send('Could not read that. Enter like "2%" or "3.78L".'); return true; }
        f.bkPct=bk.pct; f.bkAmt=bk.amt;
        ses.step='econ_ask';
        await send('Brokerage noted: '+inrFull(bk.amt)+' ('+pct(bk.pct)+').\nAdd discounts / adjustments (on-form discount, DP, gift, NPV, marketing, other)? Reply "yes" to enter them, or "skip" for none.'); return true;
      }
      case 'econ_ask': {
        if(/^(skip|no|n|none)$/i.test(low)){ f.economics={}; ses.step='advamt'; await send('No adjustments. Advance received? (amount \u2014 or 0)'); return true; }
        if(/^(yes|y|ok)$/i.test(low)){ f.economics={}; ses.econIdx=0; ses.step='econ_line';
          await send('For each line, reply a % (e.g. "5%"), an amount (e.g. "2L"), or "skip".\n\n'+ECON_LINES[0].label+'?'); return true; }
        await send('Reply "yes" to enter adjustments or "skip" for none.'); return true;
      }
      case 'econ_line': {
        var line=ECON_LINES[ses.econIdx];
        var r=parseEconLine(body, f.tsv);
        if(!r){ await send('Enter a % (e.g. "5%"), an amount (e.g. "2L"), or "skip".'); return true; }
        if(!r.skip){
          if(r.pct!==undefined) f.economics[line.key+'Pct']=r.pct;
          else f.economics[line.key+'Amt']=r.amt;
        }
        ses.econIdx++;
        if(ses.econIdx<ECON_LINES.length){ await send(ECON_LINES[ses.econIdx].label+'?'); return true; }
        ses.step='advamt';
        await send('Adjustments captured. Advance received? (amount \u2014 or 0)'); return true;
      }
      case 'advamt': {
        var a=parseAmount(body);
        if(a===null){ await send('Enter the advance amount (e.g. "5L", "500000", or 0).'); return true; }
        f.advAmt=a;
        if(a===0){ f.advMode=''; f.advAcct=''; ses.step='preview'; await send(previewText(f)); return true; }
        ses.step='advmode';
        await send('Advance received via? 1) Cheque  2) Cash  3) Bank transfer  (number or word)'); return true;
      }
      case 'advmode': {
        var mmap={'1':'Cheque','2':'Cash','3':'Bank transfer','cheque':'Cheque','cash':'Cash','transfer':'Bank transfer','bank':'Bank transfer','neft':'Bank transfer','rtgs':'Bank transfer','upi':'Bank transfer'};
        var mode=mmap[low]||null;
        if(!mode){ await send('Reply 1 (Cheque), 2 (Cash) or 3 (Bank transfer).'); return true; }
        f.advMode=mode;
        if(mode==='Cash'){ f.advAcct=''; ses.step='preview'; await send(previewText(f)); return true; }
        ses.step='advacct';
        await send(acctMenu()); return true;
      }
      case 'advacct': {
        var acct=null;
        var ai=parseInt(low,10);
        if(ai>=1&&ai<=ACCOUNTS.length) acct=ACCOUNTS[ai-1];
        else { ACCOUNTS.forEach(function(x){ if(x.toLowerCase()===low) acct=x; }); }
        if(!acct){ await send('Pick the account by number (1-'+ACCOUNTS.length+') or exact name.'); return true; }
        f.advAcct=acct; ses.step='preview';
        await send(previewText(f)); return true;
      }
      case 'preview': {
        if(/^(yes|y|ok|send)$/i.test(low)){
          var itemId=newItemId();
          var skip = false; try{ skip = !!skipApprovalFn(); }catch(e){}
          if(skip){
            // approvals bypassed: go straight to agent re-confirm (still one safety tap)
            var p0=loadPending();
            p0.items[itemId]={ msgId:null, fields:f, verdicts:{M:'skip',S:'skip'},
                               state:'await_reconfirm', originChat: from, at: Date.now() };
            savePending(p0);
            delete sessions[jidOf(msg)];
            await send('\u26a1 Approval skipped (testing mode). Re-confirm to commit: reply "confirm '+f.unit+'"  (or "edit '+f.unit+'").');
            return true;
          }
          var sent=await client.sendMessage(CONFIG.APPROVAL_GROUP_JID, approvalText(f));
          var p=loadPending();
          p.items[itemId]={ msgId: sent && sent.id && sent.id._serialized,
                            fields:f, verdicts:{}, state:'await_approval',
                            originChat: from, at: Date.now() };
          savePending(p);
          delete sessions[jidOf(msg)];
          await send('Sent for M+S approval. I will ping you here once both approve.');
          return true;
        }
        if(/^edit$/i.test(low)){
          ses.mode='edit'; ses.step='pickfield'; ses.itemId=null; ses.lkCache=ses.lk;
          await send(editMenu()); return true;
        }
        await send('Reply "yes" to send for approval, "edit" to change, or "cancel".'); return true;
      }
    }
    return true;
  }

  function pricesFromFields(f){
    return [{list:f.listIndex,name:f.listName,price:f.tsv,isCurrent:true,delta:0,deltaPct:0}];
  }
  function applyEdit(ses, body){
    var f=ses.fields, low=body.toLowerCase().trim();
    switch(ses.editField){
      case 1: if(!body) return {done:false,msg:'Customer name?'}; f.customer=body; return {done:true};
      case 2: {
        var src=(ses.lkCache&&ses.lkCache.prices)||pricesFromFields(f);
        var pick=null;
        if(low==='ok') pick=src.filter(function(x){return x.isCurrent;})[0];
        else { var n=parseInt(low,10); pick=src.filter(function(x){return x.list===n;})[0]; }
        if(!pick) return {done:false,msg:'Reply "ok" or a valid list number.'};
        f.listIndex=pick.list; f.listName=pick.name; f.tsv=pick.price;
        if(f.bkPct>0) f.bkAmt=Math.round(f.bkPct*f.tsv);
        return {done:true};
      }
      case 3: f.broker=/^none$/i.test(body)?'':body; if(!f.broker){f.bkPct=0;f.bkAmt=0;} return {done:true};
      case 4: {
        var bk=parseBrokerage(body,f.tsv);
        if(!bk) return {done:false,msg:'Enter like "2%" or "3.78L".'};
        f.bkPct=bk.pct; f.bkAmt=bk.amt; return {done:true};
      }
      case 5: {
        var a=parseAmount(body);
        if(a===null) return {done:false,msg:'Enter the advance amount (or 0).'};
        f.advAmt=a; if(a===0){f.advMode='';f.advAcct='';} return {done:true};
      }
      case 6: {
        var mmap={'1':'Cheque','2':'Cash','3':'Bank transfer','cheque':'Cheque','cash':'Cash','transfer':'Bank transfer'};
        var mode=mmap[low]; if(!mode) return {done:false,msg:'1 Cheque / 2 Cash / 3 Bank transfer'};
        f.advMode=mode; if(mode==='Cash')f.advAcct=''; return {done:true};
      }
      case 7: {
        var acct=null; var ai=parseInt(low,10);
        if(ai>=1&&ai<=ACCOUNTS.length) acct=ACCOUNTS[ai-1];
        else ACCOUNTS.forEach(function(x){ if(x.toLowerCase()===low) acct=x; });
        if(!acct) return {done:false,msg:'Pick the account by number or exact name.'};
        f.advAcct=acct; return {done:true};
      }
    }
    return {done:false,msg:'?'};
  }

  return { handleSalesMessage: handleSalesMessage, _test:{parseOpening:parseOpening,parseBrokerage:parseBrokerage,parseAmount:parseAmount,previewText:previewText,approvalText:approvalText,inr:inr} };
};

// ============================================================
// OFFLINE SELF-TEST:  node sales.js --test
// ============================================================
if(require.main===module && process.argv.indexOf('--test')>=0){
  var assert=require('assert');
  var mod=module.exports({
    CONFIG:{ACCOUNTANT_PHONES:['919873574112'],APPROVAL_GROUP_JID:'ap@g.us',WHATSAPP_GROUP_JID:'wg@g.us',MM_PHONE:'1',SM_PHONE:'2',LID_WHITELIST:[]},
    getClient:function(){return null;}, identifySender:null,
    LEDGER_ACCOUNTS:['HDFC-A','AXIS-B'], fetch:function(){}, fs:require('fs'), authDir:'/tmp',
    TRACKER_API_URL:'http://x', TRACKER_API_SECRET:'s'
  });
  var T=mod._test;
  // opening parse
  var o=T.parseOpening('book 214-GF Rajesh Kumar');
  assert.strictEqual(o.unit,'214-GF'); assert.strictEqual(o.customer,'Rajesh Kumar');
  o=T.parseOpening('Book 112A-PLOT');
  assert.strictEqual(o.unit,'112A-PLOT'); assert.strictEqual(o.customer,null);
  assert.strictEqual(T.parseOpening('booking done thanks'),null);
  assert.strictEqual(T.parseOpening('paid 5 lakh'),null);
  // brokerage both ways on tsv 1.89cr
  var tsv=18900000;
  var b=T.parseBrokerage('2%',tsv); assert.strictEqual(b.amt,378000); assert.ok(Math.abs(b.pct-0.02)<1e-9);
  b=T.parseBrokerage('2',tsv); assert.strictEqual(b.amt,378000);
  b=T.parseBrokerage('378000',tsv); assert.ok(Math.abs(b.pct-0.02)<1e-6); assert.strictEqual(b.amt,378000);
  b=T.parseBrokerage('3.78L',tsv);  // unit suffix => ABSOLUTE 3.78 lakh, never 3.78%
  assert.strictEqual(b.amt,378000,'3.78L is 3.78 lakh absolute'); assert.ok(Math.abs(b.pct-0.02)<1e-6);
  b=T.parseBrokerage('0.5 cr',tsv); assert.strictEqual(b.amt,5000000,'0.5 cr absolute');
  b=T.parseBrokerage('9L',tsv); assert.strictEqual(b.amt,900000,'9L is 9 lakh, not 9%');
  // amounts
  assert.strictEqual(T.parseAmount('5L'),500000);
  assert.strictEqual(T.parseAmount('1.5 cr'),15000000);
  assert.strictEqual(T.parseAmount('500000'),500000);
  assert.strictEqual(T.parseAmount('0'),0);
  assert.strictEqual(T.parseAmount('hello'),null);
  // preview + approval text carry the delta
  var f={unit:'214-GF',configKey:'GF-270',customer:'Rajesh Kumar',listName:'List-3',listIndex:3,
         tsv:18900000,stdPrice:19800000,broker:'Sharma',bkPct:0.02,bkAmt:378000,
         advAmt:500000,advMode:'Cheque',advAcct:'HDFC-A',agentName:'Umesh'};
  var pv=T.previewText(f);
  assert.ok(pv.indexOf('BELOW standard')>=0,'delta in preview');
  assert.ok(pv.indexOf('3,78,000')>=0 && pv.indexOf('2%')>=0,'brokerage both ways');
  var ap=T.approvalText(f);
  assert.ok(ap.indexOf('BELOW standard')>=0,'delta in approval');
  assert.ok(ap.indexOf('Umesh')>=0,'agent named');
  assert.ok(ap.indexOf('Balance')>=0);
  console.log('sales.js self-test: ALL ASSERTIONS PASSED');
}

/* ============================================================
WIRING into server.js (3 changes):

1. Top of file, with the other requires:
     const initSales = require('./sales');

2. After CONFIG + LEDGER_ACCOUNTS + identifySender are defined
   (anywhere after line ~881), add:
     var sales = initSales({
       CONFIG: CONFIG, getClient: function(){ return waClient; },
       identifySender: identifySender, LEDGER_ACCOUNTS: LEDGER_ACCOUNTS,
       fetch: fetch, fs: fs, authDir: './wa_auth',
       TRACKER_API_URL: process.env.TRACKER_API_URL,
       TRACKER_API_SECRET: process.env.TRACKER_API_SECRET
     });

3. In the dispatcher (waClient.on('message'), ~line 158), make sales
   first in the chain:
     waClient.on('message', function(msg) {
       sales.handleSalesMessage(msg).then(function(handledSales){
         if(handledSales) return;
         return handleInflowFlow(msg).then(function(handledInflow){
           ... (existing chain unchanged)
       }).catch(function(e){ console.error('[Msg handler]', e.message); });
     });

4. Railway env vars:
     TRACKER_API_URL   = https://script.google.com/macros/s/AKfycbz.../exec
     TRACKER_API_SECRET= Fid8to-Vrnd7vn-Kx29pLmQ-4847
============================================================ */
