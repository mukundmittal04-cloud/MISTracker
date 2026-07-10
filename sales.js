// ============================================================
// FIDATO SALES MODULE v1.0.0-b11 (b10 + LID RESOLUTION FIX for changes: resolvePhone now uses an injected server resolveLidPhone(jid) [same logic as the auth layer: waClient.getContactById(jid).number] plus an explicit LID_PHONE_MAP fallback, because identifySender returns {role,contactName} with NO phone field - so cancel from a linked-device @lid now resolves to the real phone and seniorityOf works) (b9 + CANCELLATION: "cancel <unit>" in the sales group -> shows paid-to-date -> disposition refund/hold-for-transfer. Senior (Umesh/accountant) acts directly; junior (Gautam) posts for a senior yes in-group. On commit the tracker archives+hides the cover as "<unit> - Cancelled - N", moves paid-to-date to the Refund Register, flips inventory to Cancelled. Mukund DM notified every time) (b8 + ECONOMICS BLOCK: after brokerage the bot asks yes/skip to add on-form discount (shares broker commission) / DP discount / gift / NPV / marketing / other, each as % or amount; written to the cover economics cells by the tracker, which returns balance-payable + net-realization. Preview + approval post show the adjustment lines) (b7 + SKIP-APPROVAL TOGGLE: deps.skipApproval() live panel switch; when ON, preview "yes" bypasses the M+S approval post and goes straight to agent re-confirm -> commit. Default OFF (M+S required). Edits honour the toggle too) (b6 + MANUAL TSV: if a unit has no price list filled, the bot asks for the sale value directly instead of dead-ending; commit sends tsv so the API writes it. Lets bookings proceed before Price Lists are populated) (b5 + SALES GROUP ROUTING: primary channel is the dedicated sales group JID (deps.SALES_GROUP_JID); any member may raise a booking there - stable @g.us routing, no @lid/@c.us guessing. Agent DMs still accepted as a fallback. Origin chat for group bookings is the sales group, so re-confirm pings land there) (b4 + GATE FIX: WhatsApp delivers DMs from linked-device users as @lid, not @c.us; the book gate now accepts ANY non-group jid as a DM and rejects only OTHER groups. isAgent resolves @lid via identifySender and re-checks the RESOLVED phone against the agent lists, so 86960253214761@lid -> 917838537000 is recognized) (b3 + FAIL-LOUD: missing TRACKER env vars or a thrown tracker call now CLAIM the message with a clear error instead of silently falling through to the expense flow; commit wrapped; outer catch logs stack head; SALES_AGENT_LIDS whitelist for group @lid authors) (b2 + diagnostic logging on the book path: prints trigger/gate/agent/API-URL/lookup so Railway logs show exactly why a booking is or is not claimed) (b1 + fixes: edit-from-preview no longer crashes; brokerage unit-suffix "3.78L" reads as absolute lakh not %; isAgent resolves group @lid authors via identifySender) - UNIT BOOKING over WhatsApp.
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
  function commitCancel(f){ return trackerPost({action:'cancel', unit:f.unit, disposition:f.disposition, agent:f.agentName||'', approvedBy:f.approvedBy||''}); }
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
        if(qid){ Object.keys(p.items).forEach(function(k){ if(p.items[k].msgId===qid) itemId=k; }); }
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
        // not ours - fall through to server's own verdict handling
        return false;
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

      // ===== 3. active Q&A session =====
      if(sessions[senderJid]){
        return await advanceSession(msg, sessions[senderJid]);
      }

      // ===== 3.5 cancellation: "cancel <unit>" (Gautam->Umesh approve; Umesh acts direct) =====
      var mCancel=body.match(/^cancel\s+(\d{2,3}[A-Z]?-(?:GF|FF|SF|TF|PLOT))\b/i);
      if(mCancel){
        var cunit=mCancel[1].toUpperCase();
        var inSG = SALES_GROUP_JID && from===SALES_GROUP_JID;
        if(!inSG) return false;                      // cancellations only in the sales group
        var cphone=await resolvePhone(msg);
        var role=seniorityOf(cphone);
        if(role==='none') return false;              // not a sales actor
        var clk=await lookupUnit(cunit);
        if(!clk||!clk.ok){ await client.sendMessage(from,'\u26a0\ufe0f '+((clk&&clk.error)||'lookup failed')+'.'); return true; }
        if(clk.status!=='Sold'){ await client.sendMessage(from,'\u26a0\ufe0f '+cunit+' is not sold (status '+clk.status+') \u2014 nothing to cancel.'); return true; }
        var cinfo=identifySender?await identifySender(senderJid):null;
        sessions[senderJid]={ mode:'cancel', step:'disposition', originChat:from,
          fields:{ unit:cunit, customer:clk.customer, paid:clk.paidToDate||0,
                   raiserRole:role, raiserName:(cinfo&&cinfo.contactName)||'', raiserPhone:cphone } };
        await client.sendMessage(from,
          'CANCEL '+cunit+' \u2014 '+(clk.customer||'(no customer)')+'\nPaid to date: '+inrFull(clk.paidToDate||0)+
          '\n\nWhat happens to the money?\n1) Refund (into refund pool)\n2) Hold for transfer to another unit\nReply 1 or 2.');
        return true;
      }
      // verdict on a pending cancel approval (senior replies in the sales group, quoting our post)
      if(SALES_GROUP_JID && from===SALES_GROUP_JID && msg.hasQuotedMsg){
        var pc=loadPending();
        var qm=await msg.getQuotedMessage().catch(function(){return null;});
        var qmid=qm&&qm.id&&qm.id._serialized;
        var cid=null; if(qmid){ Object.keys(pc.items).forEach(function(k){ if(pc.items[k].msgId===qmid && pc.items[k].kind==='cancel') cid=k; }); }
        if(cid){
          var cit=pc.items[cid];
          if(cit.state!=='await_senior'){ return true; }
          var aphone=await resolvePhone(msg);
          if(seniorityOf(aphone)!=='senior'){ await client.sendMessage(from,'Only a senior can approve this cancellation.'); return true; }
          if(/^(no|reject|n)\b/i.test(body.toLowerCase())){ cit.state='rejected'; savePending(pc);
            await client.sendMessage(from,'\u274c Cancellation of '+cit.fields.unit+' rejected.'); return true; }
          if(/^(yes|ok|approve|approved|y)\b/i.test(body.toLowerCase())){
            cit.fields.approvedBy=(await senderName(msg))||'senior';
            var res=await commitCancel(cit.fields);
            if(res&&res.ok){
              cit.state='done'; savePending(pc);
              await client.sendMessage(from,'\u2705 '+cit.fields.unit+' CANCELLED. '+inrFull(res.paid)+' \u2192 '+res.disposition+' ('+res.creditId+'). Archived as "'+res.archived+'".');
              await notifyMukund('\u2139\ufe0f Booking CANCELLED\nUnit: '+cit.fields.unit+'\nCustomer: '+cit.fields.customer+'\nPaid: '+inrFull(res.paid)+'\nDisposition: '+res.disposition+'\nCredit: '+res.creditId+'\nApproved by: '+cit.fields.approvedBy+'\nRaised by: '+cit.fields.raiserName);
            } else {
              await client.sendMessage(from,'\u26a0\ufe0f Cancel failed: '+((res&&res.error)||'no response')+'.');
            }
            return true;
          }
          return true;
        }
      }

      // ===== 4. opening: "book ..." from an agent =====
      var open=parseOpening(body);
      if(!open) return false;
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
      if(lk.status==='Sold'){
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
  async function advanceSession(msg, ses){
    var client=getClient();
    var from=ses.originChat;
    var body=String(msg.body||'').trim();
    var low=body.toLowerCase();
    var f=ses.fields;
    var send=function(t){ return client.sendMessage(from,t); };

    if(low==='cancel'){ delete sessions[jidOf(msg)]; await send('Booking flow cancelled.'); return true; }

    // ----- cancel mode -----
    if(ses.mode==='cancel'){
      if(ses.step==='disposition'){
        var d = /^1$|refund/i.test(low) ? 'Refund' : /^2$|transfer|hold/i.test(low) ? 'Transfer-pending' : null;
        if(!d){ await send('Reply 1 (Refund) or 2 (Hold for transfer).'); return true; }
        f.disposition = d==='Refund' ? 'refund' : 'transfer';
        var human = d==='Refund' ? 'Refund into pool' : 'Hold for transfer to another unit';
        if(f.raiserRole==='senior'){
          // senior acts directly - commit now
          f.approvedBy=f.raiserName||'senior';
          var res=await commitCancel(f);
          delete sessions[jidOf(msg)];
          if(res&&res.ok){
            await send('\u2705 '+f.unit+' CANCELLED ('+human+'). '+inrFull(res.paid)+' \u2192 '+res.disposition+' ('+res.creditId+'). Archived as "'+res.archived+'".');
            await notifyMukund('\u2139\ufe0f Booking CANCELLED\nUnit: '+f.unit+'\nCustomer: '+f.customer+'\nPaid: '+inrFull(res.paid)+'\nDisposition: '+res.disposition+'\nCredit: '+res.creditId+'\nBy: '+f.raiserName+' (senior, direct)');
          } else { await send('\u26a0\ufe0f Cancel failed: '+((res&&res.error)||'no response')+'.'); }
          return true;
        }
        // junior raised -> post to sales group for a senior to approve
        var itemId='cx-'+Date.now().toString(36);
        var seniorTag = SALES_SENIOR_PHONES.length ? (' (needs a senior: e.g. Umesh)') : '';
        var postText='\ud83d\uddd1 CANCELLATION for senior approval'+
          '\n\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500'+
          '\nUnit: '+f.unit+'\nCustomer: '+f.customer+'\nPaid to date: '+inrFull(f.paid)+
          '\nDisposition: '+human+'\nRaised by: '+(f.raiserName||'junior')+
          '\n\nReply to THIS message: yes / no'+seniorTag;
        var sent=await getClient().sendMessage(from, postText);
        var p=loadPending();
        p.items[itemId]={ kind:'cancel', msgId:sent&&sent.id&&sent.id._serialized, fields:f,
                          state:'await_senior', originChat:from, at:Date.now() };
        savePending(p);
        delete sessions[jidOf(msg)];
        await send('Sent for senior approval. A senior must reply "yes" on that message to commit.');
        return true;
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
