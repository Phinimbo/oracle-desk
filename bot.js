// Oracle Desk — cloud autopilot
// Runs once per invocation (GitHub Actions calls it every 15 minutes).
// For each watched market: fetch recent candles, evaluate the ICT engine on the last CLOSED bar,
// and if it signals, open a Deriv multiplier contract with stop loss and take profit attached.
// Deriv enforces the stop and target on its own servers, so nothing needs to stay running.
//
// Environment (GitHub repo → Settings → Secrets and variables → Actions):
//   DERIV_TOKEN       personal access token (pat_…), trade scope        [secret, required]
//   DERIV_APP_ID      App ID from developers.deriv.com                   [secret, required]
//   DERIV_ACCOUNT_ID  account to trade (e.g. VRTC…/DOT…); default: first demo account
//   MODE              "trade" to place orders, anything else = dry run (logs only). Default: dry
//   SYMBOLS           comma list, default frxXAUUSD
//   TF                candle seconds, default 900 (15m). Must match the schedule cadence.
//   RISK_PCT 1 · RR 2 · THRESHOLD 70 · LOOKBACK 8 · MULT 100 · MAX_TRADES 6 · DAILY_LOSS 3 · KZ_ONLY true · BIAS_ONLY true · REAL_OK false

const env=(k,d)=>process.env[k]==null||process.env[k]===''?d:process.env[k];
const CFG={
  token:env('DERIV_TOKEN',''), appId:env('DERIV_APP_ID',''), accountId:env('DERIV_ACCOUNT_ID',''),
  mode:env('MODE','dry'), symbols:env('SYMBOLS','frxXAUUSD').split(',').map(s=>s.trim()).filter(Boolean), tf:+env('TF',900),
  riskPct:+env('RISK_PCT',1), rr:+env('RR',2), threshold:+env('THRESHOLD',70), lookback:+env('LOOKBACK',8), mult:+env('MULT',100),
  maxTrades:+env('MAX_TRADES',6), dailyLoss:+env('DAILY_LOSS',3), kzOnly:env('KZ_ONLY','true')==='true', biasOnly:env('BIAS_ONLY','true')==='true', realOk:env('REAL_OK','false')==='true'
};
const PUBLIC_WS='wss://api.derivws.com/trading/v1/options/ws/public';
const REST_BASE='https://api.derivws.com/trading/v1/options';
const log=(...a)=>console.log(new Date().toISOString().slice(11,19),...a);

/* ---------- Deriv socket (same protocol as the desk) ---------- */
class Deriv{
  constructor(){ this.id=1; this.pending={}; this.ws=null; }
  connect(url){ return new Promise((res,rej)=>{ const ws=new WebSocket(url); this.ws=ws; const t=setTimeout(()=>rej(new Error('socket timeout')),15000);
    ws.onopen=()=>{ clearTimeout(t); res(); }; ws.onerror=e=>{ clearTimeout(t); rej(new Error('socket error')); };
    ws.onmessage=e=>{ const m=JSON.parse(e.data); const p=this.pending[m.req_id]; if(!p) return; delete this.pending[m.req_id]; m.error?p.rej(new Error(m.error.message||m.error.code)):p.res(m); }; }); }
  send(req){ return new Promise((res,rej)=>{ const id=this.id++; req.req_id=id; this.pending[id]={res,rej}; this.ws.send(JSON.stringify(req)); setTimeout(()=>{ if(this.pending[id]){ delete this.pending[id]; rej(new Error('timeout: '+Object.keys(req)[0])); } },20000); }); }
  close(){ try{ this.ws.close(); }catch(e){} }
}
async function rest(path,method='GET',body){
  const r=await fetch(REST_BASE+path,{method,headers:{'Authorization':'Bearer '+CFG.token,'Deriv-App-ID':CFG.appId,'Content-Type':'application/json'},body:body?JSON.stringify(body):undefined});
  let j={}; try{ j=await r.json(); }catch(e){}
  if(!r.ok){ const e=(j.errors&&j.errors[0])||{}; throw new Error((e.code?e.code+': ':'')+(e.message||('HTTP '+r.status))); }
  return j;
}

/* ---------- ICT engine (identical to the desk) ---------- */
function ema(vals,n){ const k=2/(n+1); const out=new Array(vals.length).fill(null); let e=null; for(let i=0;i<vals.length;i++){ e=e==null?vals[i]:vals[i]*k+e*(1-k); if(i>=n-1) out[i]=e; } return out; }
function atr(c,n=14){ const out=new Array(c.length).fill(null); let a=null; for(let i=1;i<c.length;i++){ const tr=Math.max(c[i].high-c[i].low,Math.abs(c[i].high-c[i-1].close),Math.abs(c[i].low-c[i-1].close)); a=a==null?tr:(a*(n-1)+tr)/n; if(i>=n) out[i]=a; } return out; }
function inKillzone(epoch){ const h=new Date(epoch*1000).getUTCHours(); return (h>=7&&h<10)||(h>=12&&h<15); }
function analyzeAll(c){
  const n=c.length, L=3; const closes=c.map(x=>x.close); const e50=ema(closes,50), e200=ema(closes,200), A=atr(c,14); const swH=[], swL=[];
  for(let i=L;i<n-L;i++){ let hi=true, lo=true; for(let k=1;k<=L;k++){ if(c[i].high<=c[i-k].high||c[i].high<=c[i+k].high) hi=false; if(c[i].low>=c[i-k].low||c[i].low>=c[i+k].low) lo=false; } if(hi) swH.push(i); if(lo) swL.push(i); }
  const lastSH=new Array(n).fill(-1), lastSL=new Array(n).fill(-1); let hi=0, li=0;
  for(let i=0;i<n;i++){ while(hi<swH.length&&swH[hi]+L<=i){ lastSH[i]=swH[hi]; hi++; } if(i>0&&lastSH[i]===-1) lastSH[i]=lastSH[i-1]; while(li<swL.length&&swL[li]+L<=i){ lastSL[i]=swL[li]; li++; } if(i>0&&lastSL[i]===-1) lastSL[i]=lastSL[i-1]; }
  const trend=new Array(n).fill(0); let t=0, refH=null, refL=null, hp=0, lp=0;
  for(let i=0;i<n;i++){ while(hp<swH.length&&swH[hp]+L<=i){ refH=swH[hp]; hp++; } while(lp<swL.length&&swL[lp]+L<=i){ refL=swL[lp]; lp++; } if(refH!=null&&c[i].close>c[refH].high){ t=1; refH=null; } if(refL!=null&&c[i].close<c[refL].low){ t=-1; refL=null; } trend[i]=t; }
  const fvgs=[]; for(let i=2;i<n;i++){ if(c[i].low>c[i-2].high) fvgs.push({i,dir:1}); if(c[i].high<c[i-2].low) fvgs.push({i,dir:-1}); }
  return {e50,e200,atr:A,lastSH,lastSL,trend,fvgs};
}
function evaluate(c,A,i,st){
  const K=st.lookback, a=A.atr[i]; const bar=c[i]; const out={i,score:0,dir:0,signal:false,reasons:[]};
  if(i<210||a==null){ out.reasons.push('warming up'); return out; }
  let bias=0; if(A.e50[i]!=null&&A.e200[i]!=null) bias=A.e50[i]>A.e200[i]?1:-1; const sTrend=A.trend[i];
  function sweep(dir){ for(let j=i;j>=i-K&&j>=1;j--){ const ref=dir===1?A.lastSL[j]:A.lastSH[j]; if(ref<0||j-ref>80) continue; const lvl=dir===1?c[ref].low:c[ref].high; let clean=true; for(let k=ref+1;k<j;k++){ if(dir===1?c[k].close<lvl:c[k].close>lvl){ clean=false; break; } } if(!clean) continue; if(dir===1&&c[j].low<lvl&&c[j].close>lvl) return {j,lvl}; if(dir===-1&&c[j].high>lvl&&c[j].close<lvl) return {j,lvl}; } return null; }
  const isDisp=(b,dir,at)=>{ const r=b.high-b.low, bd=Math.abs(b.close-b.open); return (b.close>b.open?1:-1)===dir&&r>=1.1*at&&bd>=0.55*r; };
  let best=null; for(const dir of [1,-1]){ const sw=sweep(dir); if(!sw) continue; const disp=isDisp(bar,dir,a)||(c[i-1]&&A.atr[i-1]&&isDisp(c[i-1],dir,A.atr[i-1])); if(disp){ best={dir,sw,disp:true}; break; } if(!best) best={dir,sw,disp:false}; }
  if(!best){ out.reasons.push('no sweep'); return out; }
  const dir=best.dir, sw=best.sw; const fvg=A.fvgs.find(f=>f.dir===dir&&(f.i===i||f.i===i-1)); let ob=null; for(let k=i-1;k>=sw.j&&k>=i-6;k--){ if(dir===1?c[k].close<c[k].open:c[k].close>c[k].open){ ob=k; break; } }
  const kz=inKillzone(bar.epoch), biasOk=bias===dir||sTrend===dir, biasStrong=bias===dir&&sTrend===dir;
  let atrMean=0,cnt=0; for(let k=i-100;k<i;k++){ if(k>=0&&A.atr[k]!=null){ atrMean+=A.atr[k]; cnt++; } } atrMean=cnt?atrMean/cnt:a; const vol=a>=0.6*atrMean;
  const score=Math.round(30+(best.disp?15:0)+(fvg?15:0)+(ob!=null?10:0)+(biasStrong?15:(biasOk?9:0))+(kz?10:0)+(vol?5:0));
  let ext=dir===1?Infinity:-Infinity; for(let k=sw.j;k<=i;k++){ ext=dir===1?Math.min(ext,c[k].low):Math.max(ext,c[k].high); }
  const sl=dir===1?ext-0.15*a:ext+0.15*a; const R=Math.abs(bar.close-sl); const tp=dir===1?bar.close+st.rr*R:bar.close-st.rr*R; const rOk=R>=0.3*a&&R<=3*a;
  Object.assign(out,{dir,score,sl,tp,R,entry:bar.close});
  out.reasons.push(`sweep@${sw.lvl} disp:${best.disp} fvg:${!!fvg} ob:${ob!=null} bias:${biasOk} kz:${kz} R/ATR:${(R/a).toFixed(2)}`);
  out.signal=best.disp&&rOk&&(!st.kzOnly||kz)&&(!st.biasOnly||biasOk)&&score>=st.threshold;
  return out;
}

/* ---------- main ---------- */
(async()=>{
  log(`Oracle autopilot · ${CFG.mode==='trade'?'LIVE ORDERS':'DRY RUN'} · ${CFG.symbols.join(',')} · ${CFG.tf}s`);
  const pub=new Deriv(); await pub.connect(PUBLIC_WS);
  const signals=[];
  for(const sym of CFG.symbols){
    try{ const r=await pub.send({ticks_history:sym,style:'candles',granularity:CFG.tf,count:600,end:'latest',adjust_start_time:1});
      const c=r.candles.map(x=>({epoch:+x.epoch,open:+x.open,high:+x.high,low:+x.low,close:+x.close}));
      const ageSec=Date.now()/1000-c[c.length-1].epoch; if(ageSec>CFG.tf*2){ log(`${sym}: last candle is ${Math.round(ageSec/60)} min old, market likely closed, skip`); continue; }
      const A=analyzeAll(c); const d=evaluate(c,A,c.length-2,CFG);
      log(`${sym}: close ${c[c.length-2].close} · score ${d.score} · ${d.signal?(d.dir===1?'BUY':'SELL'):'wait'} · ${d.reasons.join(' ')}`);
      if(d.signal) signals.push({sym,d,px:c[c.length-1].close});
    }catch(e){ log(`${sym}: ${e.message}`); }
  }
  pub.close();
  if(!signals.length){ log('No signals this run.'); return; }
  if(CFG.mode!=='trade'){ log('Dry run: would trade', signals.map(s=>`${s.d.dir===1?'BUY':'SELL'} ${s.sym} @${s.px} stop ${s.d.sl.toFixed(3)} target ${s.d.tp.toFixed(3)}`).join('; ')); return; }
  if(!CFG.token||!CFG.appId){ log('MODE=trade but DERIV_TOKEN / DERIV_APP_ID missing. Nothing placed.'); process.exitCode=1; return; }

  // account + trading link
  const ar=await rest('/accounts'); const list=Array.isArray(ar)?ar:(ar.data||ar.accounts||[]);
  const accounts=list.map(a=>({id:a.account_id||a.loginid||a.id,type:(a.account_type||a.group||'').toLowerCase(),currency:a.currency||'USD',balance:+a.balance||0}));
  const isDemo=a=>/demo|virtual/i.test(a.type)||/^VRT/i.test(a.id);
  const acct=accounts.find(a=>a.id===CFG.accountId)||accounts.find(isDemo)||accounts[0]; if(!acct) throw new Error('no options accounts on this token');
  if(!isDemo(acct)&&!CFG.realOk){ log(`Account ${acct.id} is REAL and REAL_OK is not true. Nothing placed.`); return; }
  const otp=await rest('/accounts/'+encodeURIComponent(acct.id)+'/otp','POST'); const url=(otp.data&&otp.data.url)||otp.url;
  const t=new Deriv(); await t.connect(url); log(`Logged in: ${acct.id} (${isDemo(acct)?'demo':'REAL'}) · ${acct.currency} ${acct.balance}`);
  const bal=(await t.send({balance:1})).balance; const balance=+bal.balance, currency=bal.currency||acct.currency;

  // risk brakes from Deriv's own records
  const pf=await t.send({portfolio:1}); const open=pf.portfolio.contracts||[];
  const pt=await t.send({profit_table:1,description:1,limit:100,sort:'DESC'}); const tx=pt.profit_table.transactions||[];
  const day0=new Date(); day0.setUTCHours(0,0,0,0); const d0=day0.getTime()/1000;
  const today=tx.filter(x=>(+x.sell_time||+x.purchase_time)>=d0); const todayPnl=today.reduce((s,x)=>s+((+x.sell_price||0)-(+x.buy_price||0)),0);
  const openedRecently=tx.concat(open).some(x=>Date.now()/1000-(+x.purchase_time||0)<CFG.tf*1.5);
  log(`Open: ${open.length} · today: ${today.length} trades, P&L ${todayPnl.toFixed(2)} ${currency}`);
  if(today.length+open.length>=CFG.maxTrades){ log('Brake: max trades for today'); t.close(); return; }
  if(todayPnl<=-(balance-todayPnl)*CFG.dailyLoss/100){ log('Brake: daily loss limit'); t.close(); return; }
  if(openedRecently){ log('Brake: a contract was opened within the last candle, skip'); t.close(); return; }
  const lastLoss=tx.find(x=>(+x.sell_price||0)-(+x.buy_price||0)<0); if(lastLoss&&Date.now()/1000-(+lastLoss.sell_time||0)<1800){ log('Brake: cooling down after a loss'); t.close(); return; }

  for(const s of signals){
    if(open.some(o=>(o.symbol||o.underlying_symbol)===s.sym)){ log(`${s.sym}: already in this market`); continue; }
    const risk=balance*CFG.riskPct/100, R=s.d.R, px=s.px; let stake=risk/(CFG.mult*R/px); stake=Math.max(1,Math.round(stake*100)/100);
    const lossAmt=Math.round(Math.min(risk,stake*0.99)*100)/100, tpAmt=Math.round(stake*CFG.mult*Math.abs(s.d.tp-px)/px*100)/100;
    try{ const r=await t.send({buy:'1',price:stake,parameters:{amount:stake,basis:'stake',contract_type:s.d.dir===1?'MULTUP':'MULTDOWN',currency,multiplier:CFG.mult,underlying_symbol:s.sym,limit_order:{stop_loss:lossAmt,take_profit:Math.max(0.01,tpAmt)}}});
      log(`PLACED ${s.d.dir===1?'BUY':'SELL'} ${s.sym} · stake ${stake} ×${CFG.mult} · stop -${lossAmt} · target +${tpAmt} · #${r.buy.contract_id}`);
      open.push({symbol:s.sym}); if(open.length>=1) break; // one position at a time
    }catch(e){ log(`${s.sym}: order rejected: ${e.message}`); }
  }
  t.close();
})().catch(e=>{ console.error('FATAL', e.message||e); process.exitCode=1; });
