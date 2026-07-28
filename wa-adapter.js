// ─────────────────────────────────────────────────────────────────────────────
// wa-adapter.js — v1.0.0  (Fidato MIS migration, 28 Jul 2026)
//
// Presents the exact whatsapp-web.js surface server.js + sales.js use, backed
// by WPPConnect 2.2.4. server.js changes ONE line:
//   const { Client, LocalAuth, MessageMedia } = require('./wa-adapter');
//
// Surface covered (measured from the real code, nothing speculative):
//   Client: initialize, destroy, getState, info.wid, sendMessage(text|media),
//           getChats, getChatById(→name,fetchMessages), getContactById,
//           getMessageById, on(qr|ready|authenticated|auth_failure|disconnected|message)
//   Message: id._serialized, from, to, author, body, type, timestamp, fromMe,
//            hasMedia, hasQuotedMsg, _data (with quotedStanzaID/quotedParticipant/
//            quotedMsg/notifyName normalised), reply, getQuotedMessage,
//            downloadMedia, delete, getContact
//   MessageMedia: {mimetype,data,filename} constructor
//   LocalAuth: accepted for compatibility; token store handled here instead
//
// Design facts this build relies on (proven tonight, 28 Jul):
//   • WPPConnect message ids are ALREADY the event-store format
//     true_<group>_<stanza>_<participant> — no translation layer.
//   • Quoted data must come from the message payload, not a store lookup
//     (quotedId is null in list reads; payload carries the quoted content).
// ─────────────────────────────────────────────────────────────────────────────
'use strict';
const { EventEmitter } = require('events');
const wppconnect = require('@wppconnect-team/wppconnect');

// ── helpers ───────────────────────────────────────────────────────────────────
function jstr(v){ if(v===null||v===undefined) return null;
  if(typeof v==='string') return v;
  if(v._serialized) return String(v._serialized);
  if(v.user&&v.server) return v.user+'@'+v.server;
  return String(v); }

function msgIdString(raw){
  var id = raw && raw.id;
  if(typeof id==='string') return id;                       // proof: arrives serialized
  if(id && id._serialized) return String(id._serialized);
  if(id && id.id){                                          // object form → rebuild canonical
    var fromMe = (id.fromMe!==undefined?id.fromMe:raw.fromMe) ? 'true':'false';
    var remote = jstr(id.remote)||jstr(raw.from)||jstr(raw.chatId)||'';
    var part   = jstr(id.participant)||jstr(raw.author)||'';
    return fromMe+'_'+remote+'_'+id.id+(part?('_'+part):'');
  }
  return null; }

// ── MessageMedia: exactly the 3 fields server.js constructs/reads ─────────────
class MessageMedia {
  constructor(mimetype, data, filename){ this.mimetype=mimetype; this.data=data; this.filename=filename||null; }
}
// LocalAuth accepted for API compatibility; wppconnect's token store is
// configured in Client.initialize (kept under ./wa_auth so /api/wa-reset and
// the Railway volume behave exactly as before).
class LocalAuth { constructor(_o){ this.dataPath=(_o&&_o.dataPath)||'./wa_auth'; } }

// ── Message wrapper ───────────────────────────────────────────────────────────
class AdaptedMessage {
  constructor(client, raw){
    this._client=client; this._raw=raw||{};
    raw=this._raw;
    var idStr = msgIdString(raw);
    this.id = { _serialized:idStr, fromMe:!!raw.fromMe, id:(raw.id&&raw.id.id)||idStr };
    this.from = jstr(raw.from)||jstr(raw.chatId)||null;
    this.to = jstr(raw.to)||null;
    this.author = jstr(raw.author)|| (raw.sender?jstr(raw.sender.id):null) || null;
    this.body = raw.body!==undefined&&raw.body!==null ? String(raw.body) : (raw.caption?String(raw.caption):'');
    this.type = raw.type||'chat';
    this.timestamp = raw.timestamp||raw.t||Math.floor(Date.now()/1000);
    this.fromMe = !!raw.fromMe;
    this.hasMedia = !!(raw.isMedia||raw.isMMS||raw.mimetype||(raw.mediaData&&raw.mediaData.type));
    this.deviceType = raw.deviceType||null;
    // quoted detection: any of the payload markers (proof: list reads had
    // hasQuoted true with quotedId null — payload is the source of truth)
    var q = raw.quotedMsgObj||raw.quotedMsg||null;
    this.hasQuotedMsg = !!(q||raw.quotedMsgId||raw.quotedStanzaID||raw.quotedParticipant);
    // _data: what rawcapture + the s6.15 verdict fix read. Normalise wpp field
    // names onto the wwebjs ones so that code keeps working unchanged.
    this._data = raw;
    if(q && !raw.quotedMsg) raw.quotedMsg = q;
    if(!raw.quotedStanzaID){
      var qid = raw.quotedMsgId || (q&&q.id?msgIdString({id:q.id,fromMe:q.fromMe,from:raw.from,author:q.author}):null);
      if(qid && typeof qid==='string'){ var p=qid.split('_'); if(p.length>=3) raw.quotedStanzaID=p[2]; }
    }
    if(!raw.quotedParticipant){
      raw.quotedParticipant = (q&&(jstr(q.author)||(q.id&&jstr(q.id.participant)))) || null;
    }
    if(!raw.notifyName){
      raw.notifyName = raw.notifyName || (raw.sender&&(raw.sender.pushname||raw.sender.name||raw.sender.formattedName)) || null;
    }
  }
  async getQuotedMessage(){
    if(!this.hasQuotedMsg) return undefined;
    var raw=this._raw;
    // 1) payload first — the whole lesson of this outage
    var q = raw.quotedMsgObj||raw.quotedMsg;
    if(q && (q.body!==undefined||q.caption!==undefined||q.type)){
      var qraw = Object.assign({}, q);
      if(!qraw.from) qraw.from = raw.from;
      if(!qraw.id && raw.quotedStanzaID){
        qraw.id = { fromMe:!!qraw.fromMe, remote:jstr(raw.from), id:raw.quotedStanzaID,
                    participant:jstr(raw.quotedParticipant)||undefined };
      }
      return new AdaptedMessage(this._client, qraw);
    }
    // 2) network second, best-effort
    var qid = raw.quotedMsgId ||
      (raw.quotedStanzaID ? ['false',jstr(raw.from),raw.quotedStanzaID,jstr(raw.quotedParticipant)||''].filter(Boolean).join('_') : null);
    if(!qid) return undefined;
    try{ var m=await this._client._wpp.getMessageById(qid); return m?new AdaptedMessage(this._client,m):undefined; }
    catch(e){
      if(raw.quotedMsgId) return undefined;
      // retry the true_ variant (fromMe unknown from stanza alone)
      try{ var m2=await this._client._wpp.getMessageById(qid.replace(/^false_/,'true_'));
           return m2?new AdaptedMessage(this._client,m2):undefined; }catch(e2){ return undefined; }
    }
  }
  async downloadMedia(){
    try{
      var buf = await this._client._wpp.decryptFile(this._raw);
      var b64 = Buffer.isBuffer(buf)?buf.toString('base64'):String(buf);
      return new MessageMedia(this._raw.mimetype||'application/octet-stream', b64, this._raw.filename||null);
    }catch(e){ console.log('[adapter] downloadMedia failed:', e.message); return undefined; }
  }
  async reply(text){
    try{ return await this._client._wpp.sendText(this.from, String(text), { quotedMsg: this.id._serialized }); }
    catch(e){ return this._client.sendMessage(this.from, String(text)); }   // degrade to plain send
  }
  async delete(everyone){
    try{ return await this._client._wpp.deleteMessage(this.from, this.id._serialized, !everyone); }
    catch(e){ console.log('[adapter] delete failed:', e.message); }
  }
  async getContact(){ return this._client.getContactById(this.author||this.from); }
}

// ── Client ────────────────────────────────────────────────────────────────────
class Client extends EventEmitter {
  constructor(opts={}){ super(); this._opts=opts; this._wpp=null; this.info=null; this._ready=false; }

  async initialize(){
    var self=this;
    if(this._wpp){                      // already have a live session — re-announce, don't re-create
      console.log('[adapter] initialize() called again; reusing existing session');
      this._ready=true; self.emit('ready'); return this;
    }
    // wwebjs fires qr→authenticated→ready; reproduce that ordering.
    this._wpp = await wppconnect.create({
      session: 'fidato-mis',
      folderNameToken: './wa_auth/wpp-tokens',     // on the Railway volume; /api/wa-reset wipes it too
      catchQR: function(_b64, _ascii, _att, urlCode){
        // server.js renders this. Prefer the RAW payload (urlCode) which encodes to a
        // proper QR; fall back to the pre-rendered PNG, which server.js now detects
        // and shows directly rather than trying to re-encode.
        var payload = urlCode || _b64;
        console.log('[adapter] QR captured (attempt '+(_att||1)+') via '+(urlCode?'urlCode':'base64 image')+', len '+String(payload||'').length);
        self.emit('qr', payload);
      },
      statusFind: function(st){
        if(st==='qrReadSuccess'||st==='isLogged') self.emit('authenticated');
        if(st==='autocloseCalled'||st==='qrReadFail') self.emit('auth_failure', st);
        if(st==='desconnectedMobile'||st==='serverClose'||st==='browserClose'){
          if(self._ready) self.emit('disconnected', st==='desconnectedMobile'?'LOGOUT':st);
        }
      },
      headless: true,
      // Docker image ships system Chromium at PUPPETEER_EXECUTABLE_PATH; use it
      // explicitly so wppconnect never tries to download its own build.
      puppeteerOptions: Object.assign(
        { args:['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage',
                '--no-first-run','--disable-gpu','--disable-extensions'] },
        process.env.PUPPETEER_EXECUTABLE_PATH ? { executablePath: process.env.PUPPETEER_EXECUTABLE_PATH } : {}
      ),
      autoClose: 0,
      logQR: false, disableWelcome: true, updatesLog: false,
    });

    // own identity for /health + clientInfoWid probe step
    try{
      var wid = await this._wpp.getWid().catch(function(){return null;});
      if(!wid){ var hd=await this._wpp.getHostDevice().catch(function(){return null;});
                wid = hd && (hd.wid&&(hd.wid._serialized||hd.wid) || hd.id&&(hd.id._serialized||hd.id)); }
      this.info = { wid:{ _serialized: jstr(wid)||'unknown@c.us' } };
    }catch(e){ this.info = { wid:{ _serialized:'unknown@c.us' } }; }

    // incoming messages (excludes own sends, matching wwebjs 'message')
    this._wpp.onMessage(function(raw){
      try{ self.emit('message', new AdaptedMessage(self, raw)); }
      catch(e){ console.log('[adapter] message wrap failed:', e.message); }
    });
    // conflict/logout while running
    if(this._wpp.onStateChange){
      this._wpp.onStateChange(function(st){
        // v2.12.3: only a genuine unpair/conflict counts. Transient states used to
        // flip waReady false on a perfectly live session, which silenced every
        // handler and stuck /api/pair on "Waiting for QR".
        if(st==='CONFLICT'||st==='UNPAIRED'||st==='UNPAIRED_IDLE'){
          console.log('[adapter] genuine disconnect state:', st);
          self.emit('disconnected', st);
        } else {
          console.log('[adapter] state change (not a disconnect):', st);
        }
      });
    }

    this._ready=true;
    self.emit('ready');
    return this;
  }

  // CRITICAL: server.js and sales.js both read `sent.id._serialized` off the
  // return value and store it to match later swipe-replies (approvals, bookings,
  // digest maps, paid_posted). WPPConnect returns a different shape, so every
  // send result is normalised to guarantee that field exists.
  _normalizeSendResult(r){
    var idStr=null;
    if(typeof r==='string') idStr=r;
    else if(r){
      if(typeof r.id==='string') idStr=r.id;
      else if(r.id&&r.id._serialized) idStr=String(r.id._serialized);
      else if(r.to&&r.id&&r.id.id) idStr=msgIdString(r);
      else if(r.messageId) idStr=String(r.messageId);
    }
    var stanza=null;
    if(idStr){ var p=String(idStr).split('_'); stanza=p.length>=3?p[2]:idStr; }
    var out=(r&&typeof r==='object')?r:{};
    out.id={ _serialized:idStr, id:stanza, fromMe:true };
    return out;
  }
  async sendMessage(jid, content, opts){
    opts=opts||{};
    var r;
    if(content instanceof MessageMedia || (content&&content.mimetype&&content.data)){
      var dataUrl='data:'+content.mimetype+';base64,'+content.data;
      var fname=content.filename||'file';
      r = /^image\//.test(content.mimetype)
        ? await this._wpp.sendImageFromBase64(jid, dataUrl, fname, opts.caption||'')
        : await this._wpp.sendFileFromBase64(jid, dataUrl, fname, opts.caption||'');
    } else {
      r = await this._wpp.sendText(jid, String(content));
    }
    return this._normalizeSendResult(r);
  }

  async getChats(){
    var self=this;
    var list=await this._wpp.listChats();
    return (list||[]).map(function(c){
      var id=jstr(c.id)||String(c.id||'');
      return { id:{_serialized:id}, name:c.name||c.formattedTitle||c.contact&&c.contact.name||null,
               isGroup:/@g\.us$/.test(id),
               fetchMessages:function(o){ return self.getChatById(id).then(function(ch){return ch.fetchMessages(o);}); } };
    });
  }

  async getChatById(jid){
    var self=this, name=null;
    try{ var list=await this._wpp.listChats();
         var hit=(list||[]).find(function(c){ return (jstr(c.id)||String(c.id))===jid; });
         if(hit) name=hit.name||hit.formattedTitle||null;
    }catch(e){}
    return {
      id:{_serialized:jid}, name:name, isGroup:/@g\.us$/.test(jid),
      fetchMessages: async function(o){
        var lim=(o&&o.limit)||50;
        var raw=await self._wpp.getMessages(jid,{count:lim});
        return (raw||[]).map(function(r){ return new AdaptedMessage(self,r); });
      },
      sendMessage: function(content,o){ return self.sendMessage(jid,content,o); },
    };
  }

  async getContactById(id){
    try{
      var c=await this._wpp.getContact(id);
      if(!c) return null;
      var num=null;
      if(c.id&&c.id.user) num=c.id.user;
      else { var s=jstr(c.id)||String(id); var mm=s.match(/^(\d+)@c\.us$/); if(mm) num=mm[1]; }
      return { id:{_serialized:jstr(c.id)||String(id)},
               pushname:c.pushname||c.name||c.formattedName||null,
               name:c.name||c.formattedName||null,
               number:num, isMyContact:!!c.isMyContact };
    }catch(e){ return null; }   // identifySender treats null as unknown — same as today
  }

  async getMessageById(idStr){
    var self=this;
    try{ var m=await this._wpp.getMessageById(idStr); return m?new AdaptedMessage(self,m):null; }
    catch(e){ return null; }
  }

  async getState(){ try{ return await this._wpp.getConnectionState(); }catch(e){ return null; } }
  async destroy(){ try{ await this._wpp.close(); }catch(e){} }
}

module.exports = { Client, LocalAuth, MessageMedia };
