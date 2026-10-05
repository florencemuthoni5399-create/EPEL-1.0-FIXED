'use strict';

const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const C = {
  appId: process.env.DERIV_APP_ID || '1089',
  token: process.env.DERIV_TOKEN || '',
  asset: process.env.ASSET || 'R_75',
  horizons: (process.env.HORIZONS_TICKS || '3,5,7,10,15').split(',').map(Number).filter(x => Number.isInteger(x) && x > 0),
  stake: +(process.env.STAKE || 1),
  minSamples: +(process.env.EPEL_MIN_SAMPLES || 50),
  lambda: +(process.env.EPEL_LAMBDA || 0.5),
  z: +(process.env.WILSON_Z || 1.959964),
  signalEvery: +(process.env.SIGNAL_EVERY_TICKS || 5),
  lookback: +(process.env.LOOKBACK_TICKS || 20),
  logFile: process.env.LOG_FILE || './data/r75_tick_epel_ledger.csv',
  connectTimeoutMs: +(process.env.WS_CONNECT_TIMEOUT_MS || 15000),
  openTimeoutMs: +(process.env.WS_OPEN_TIMEOUT_MS || 12000),
  reconnectBaseMs: +(process.env.RECONNECT_BASE_MS || 3000),
  reconnectMaxMs: +(process.env.RECONNECT_MAX_MS || 60000),
  pingIntervalMs: +(process.env.PING_INTERVAL_MS || 30000),
  diagnostics: !['0','false','no'].includes(String(process.env.DIAGNOSTICS || 'true').toLowerCase())
};

const URL = `wss://ws.derivws.com/websockets/v3?app_id=${encodeURIComponent(C.appId)}`;
let ws = null;
let reconnectTimer = null;
let reconnectAttempt = 0;
let openTimer = null;
let pingTimer = null;
let firstTickTimer = null;
let connectStartedAt = 0;
let shuttingDown = false;
let reqId = 0;
let ticks = [];
let seq = 0;
let pending = [];
let signalId = 0;
let firstTickSeen = false;

const COL = ['signal_id','signal_tick','direction','entry_price','horizon_ticks','expiry_tick','expiry_price','result','pnl','samples_before','wins_before','probability','wilson_lower','epel_pass','features'];

function log(...args) { console.log(new Date().toISOString(), ...args); }
function init() {
  fs.mkdirSync(path.dirname(C.logFile), { recursive: true });
  if (!fs.existsSync(C.logFile)) fs.writeFileSync(C.logFile, COL.join(',') + '\n');
}
function q(v) { return JSON.stringify(String(v ?? '')); }
function append(r) { fs.appendFileSync(C.logFile, COL.map(k => q(r[k])).join(',') + '\n'); }
function wilson(w, n) {
  if (!n) return 0;
  const p = w / n, z = C.z, d = 1 + z*z/n, c = p + z*z/(2*n);
  const m = z * Math.sqrt((p*(1-p) + z*z/(4*n))/n);
  return (c - m) / d;
}
function prior(dir, h) {
  if (!fs.existsSync(C.logFile)) return { n:0, w:0, p:0.5, lower:0 };
  let n=0,w=0;
  for (const line of fs.readFileSync(C.logFile,'utf8').split('\n').slice(1)) {
    if (!line) continue;
    // Ledger fields are JSON strings, so this parser is intentionally limited to the fixed columns.
    let x; try { x = JSON.parse(`[${line}]`); } catch { continue; }
    if (x[2] === dir && +x[4] === h && ['WIN','LOSS'].includes(x[7])) { n++; if (x[7] === 'WIN') w++; }
  }
  return { n, w, p:n ? (w+1)/(n+2) : 0.5, lower:n ? wilson(w,n) : 0 };
}
function epel(lower,n) { return n >= C.minSamples && (1-lower) <= C.lambda * 0.78 * lower; }
function features() {
  if (ticks.length < C.lookback + 1) return null;
  const p = ticks.map(x => x.quote), last = p.at(-1), old = p.at(-(C.lookback+1));
  const deltas=[];
  for (let i=Math.max(1,p.length-20); i<p.length; i++) deltas.push(p[i]-p[i-1]);
  const mean=deltas.reduce((a,b)=>a+b,0)/(deltas.length||1);
  const sd=Math.sqrt(deltas.reduce((a,b)=>a+(b-mean)**2,0)/(Math.max(1,deltas.length-1)));
  const recent=p.slice(-5), move=recent.at(-1)-recent[0];
  let streak=0;
  if (p.length>1) { const s=Math.sign(p.at(-1)-p.at(-2)); for(let i=p.length-1;i>0&&Math.sign(p[i]-p[i-1])===s;i--) streak++; }
  const z=sd ? (last-old)/(sd*Math.sqrt(C.lookback)) : 0;
  let direction;
  if (z <= -1.0) direction='CALL'; else if (z >= 1.0) direction='PUT'; else return null;
  return { direction, move20:last-old, z20:+z.toFixed(4), recent5:move, streak, tickVol:+sd.toFixed(6) };
}
function signal() {
  if (!C.signalEvery || seq % C.signalEvery !== 0) return;
  const f=features(); if(!f) return;
  const id=++signalId;
  for(const h of C.horizons){
    const pr=prior(f.direction,h);
    pending.push({id,entryTick:seq,entryPrice:ticks.at(-1).quote,horizon:h,targetTick:seq+h,direction:f.direction,f,pr});
  }
}
function settle() {
  const now=seq;
  for(const p of [...pending]){
    if(now < p.targetTick) continue;
    const target=ticks[p.targetTick]; if(!target) continue;
    const win=p.direction==='CALL' ? target.quote>p.entryPrice : target.quote<p.entryPrice;
    const result=win?'WIN':'LOSS';
    const passed=epel(p.pr.lower,p.pr.n);
    append({signal_id:p.id,signal_tick:p.entryTick,direction:p.direction,entry_price:p.entryPrice,horizon_ticks:p.horizon,expiry_tick:p.targetTick,expiry_price:target.quote,result,pnl:win?.78:-1,samples_before:p.pr.n,wins_before:p.pr.w,probability:p.pr.p.toFixed(6),wilson_lower:p.pr.lower.toFixed(6),epel_pass:passed,features:JSON.stringify(p.f)});
    log(`[${result}] #${p.id} ${p.direction} ${p.horizon}t entry=${p.entryPrice} expiry=${target.quote} prior=${p.pr.n} p=${p.pr.p.toFixed(3)} lower=${p.pr.lower.toFixed(3)} EPEL=${passed}`);
    pending.splice(pending.indexOf(p),1);
  }
}
function onTick(t){
  const quote=+t.quote; if(!Number.isFinite(quote)) return;
  ticks.push({seq:seq++,epoch:+t.epoch,quote});
  if(ticks.length>5000) ticks.shift();
  if(!firstTickSeen){
    firstTickSeen=true; clearTimeout(firstTickTimer); log(`[STREAM] first ${C.asset} tick received: quote=${quote} epoch=${t.epoch}`); log('[HEALTH] R_75 tick stream healthy');
  }
  settle(); signal();
}
function safeSend(obj){
  if(!ws || ws.readyState !== WebSocket.OPEN){ log('[SEND] skipped: socket not OPEN'); return false; }
  try { ws.send(JSON.stringify(obj)); return true; } catch(e){ log('[SEND] failed:',e.message); return false; }
}
function startHeartbeat(){
  clearInterval(pingTimer);
  pingTimer=setInterval(()=>safeSend({ping:1,req_id:++reqId}),C.pingIntervalMs);
}
function clearTimers(){ clearTimeout(openTimer); clearTimeout(firstTickTimer); clearInterval(pingTimer); openTimer=null; firstTickTimer=null; pingTimer=null; }
function scheduleReconnect(reason){
  if(shuttingDown || reconnectTimer) return;
  const delay=Math.min(C.reconnectMaxMs,C.reconnectBaseMs*Math.pow(2,Math.min(reconnectAttempt,6)));
  reconnectAttempt++;
  log(`[RECONNECT] attempt=${reconnectAttempt} in ${delay}ms reason=${reason}`);
  reconnectTimer=setTimeout(()=>{reconnectTimer=null; connect();},delay);
}
function connect(){
  if(shuttingDown) return;
  if(ws && (ws.readyState===WebSocket.OPEN || ws.readyState===WebSocket.CONNECTING)) return;
  firstTickSeen=false; connectStartedAt=Date.now();
  log(`[WS] connecting endpoint=${URL}`);
  try { ws=new WebSocket(URL,{handshakeTimeout:C.connectTimeoutMs}); }
  catch(e){ log('[WS] constructor error:',e.message); scheduleReconnect('constructor_error'); return; }
  openTimer=setTimeout(()=>{
    if(ws && ws.readyState===WebSocket.CONNECTING){ log(`[WS] open timeout after ${C.openTimeoutMs}ms`); try{ws.terminate();}catch{} }
  },C.openTimeoutMs);
  ws.on('open',()=>{
    clearTimeout(openTimer); openTimer=null; reconnectAttempt=0;
    log(`[WS] open in ${Date.now()-connectStartedAt}ms`);
    log('[API] sending ping/time/active_symbols/ticks subscription');
    safeSend({ping:1,req_id:++reqId});
    safeSend({time:1,req_id:++reqId});
    safeSend({active_symbols:'brief',product_type:'basic',req_id:++reqId});
    safeSend({ticks:C.asset,subscribe:1,req_id:++reqId});
    startHeartbeat();
    firstTickTimer=setTimeout(()=>{ if(!firstTickSeen) log(`[HEALTH] WARNING: no ${C.asset} tick received within 10000ms after socket open`); },10000);
  });
  ws.on('message',(b)=>{
    let m; try{m=JSON.parse(b.toString());}catch(e){log('[API] invalid JSON:',e.message);return;}
    if(m.error){log(`[API] ERROR code=${m.error.code||'unknown'} message=${m.error.message||'unknown'}`,JSON.stringify(m.error));return;}
    if(m.msg_type==='tick'&&m.tick) onTick(m.tick);
    else if(m.msg_type==='ping') log('[API] pong/ping response received');
    else if(m.msg_type==='time') log(`[API] time OK epoch=${m.time}`);
    else if(m.msg_type==='active_symbols'){
      const found=Array.isArray(m.active_symbols)&&m.active_symbols.some(s=>s.symbol===C.asset);
      log(`[API] active_symbols OK asset=${C.asset} found=${found}`);
    } else if(m.msg_type==='tick'){ log('[API] tick message received'); }
  });
  ws.on('unexpected-response',(request,response)=>{
    const chunks=[];
    response.on('data',d=>{if(chunks.reduce((n,b)=>n+b.length,0)<8192)chunks.push(Buffer.from(d));});
    response.on('end',()=>{
      const body=Buffer.concat(chunks).toString('utf8').replace(/\s+/g,' ').slice(0,2000);
      log(`[WS] UNEXPECTED HTTP RESPONSE status=${response.statusCode} ${response.statusMessage||''}`);
      log('[WS] response headers:',JSON.stringify(response.headers));
      if(body) log('[WS] response body:',body);
      scheduleReconnect(`http_${response.statusCode}`);
    });
  });
  ws.on('error',(e)=>log(`[WS] error name=${e.name||''} code=${e.code||''} message=${e.message||''}`));
  ws.on('close',(code,reason)=>{
    clearTimers();
    const r=reason?reason.toString():'(none)';
    log(`[WS] close code=${code} reason=${r} uptimeMs=${Date.now()-connectStartedAt}`);
    ws=null;
    scheduleReconnect(`close_${code}`);
  });
}
function shutdown(sig){
  shuttingDown=true; log(`[SHUTDOWN] ${sig}`); clearTimers(); if(reconnectTimer)clearTimeout(reconnectTimer); if(ws)try{ws.close(1000,'shutdown');}catch{} setTimeout(()=>process.exit(0),250);
}
process.on('SIGTERM',()=>shutdown('SIGTERM')); process.on('SIGINT',()=>shutdown('SIGINT'));

init();
log(JSON.stringify({project:'SynthEPEL-R75',version:'2.1.0',asset:C.asset,mode:'TICK_RESEARCH',horizons:C.horizons,endpoint:URL,appId:C.appId,tokenConfigured:Boolean(C.token),signalEvery:C.signalEvery,lookback:C.lookback,stake:C.stake,epelLambda:C.lambda,minSamples:C.minSamples,ledger:C.logFile},null,2));
if(C.token) log('[AUTH] DERIV_TOKEN is configured, but v2.1 does not authorize because public tick streaming requires no token.');
else log('[AUTH] no DERIV_TOKEN configured — OK for public tick streaming.');
connect();
