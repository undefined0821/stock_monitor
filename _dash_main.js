
const fmt=(n,d=2)=>n==null||isNaN(n)?'--':Number(n).toLocaleString('zh-CN',{minimumFractionDigits:d,maximumFractionDigits:d});
const cls=p=>p>0?'up':(p<0?'down':'flat');
const sign=p=>(p>0?'+':'')+fmt(p);
const probcls=p=>p>=60?'up':(p<=40?'down':'flat');
let showMoney=true;
function toggleMoney(){showMoney=!showMoney;
  document.getElementById('eyeBtn').textContent=showMoney?'👁':'🙈';
  document.getElementById('privacyTip').style.display=showMoney?'none':'block';
  load();}
document.getElementById('eyeBtn').addEventListener('click',toggleMoney);
// 主题切换：dark / light，默认跟随系统；选择持久化到 localStorage
const THEME_KEY='wb_theme';
function applyTheme(t){
  const real=(t==='light')?'light':'dark';
  document.documentElement.setAttribute('data-theme',real);
  document.getElementById('themeBtn').textContent=(real==='light')?'☀️':'🌙';
  document.getElementById('themeBtn').title=(real==='light')?'切换到夜间主题':'切换到白天主题';
}
(function(){
  let saved=null;
  try{saved=localStorage.getItem(THEME_KEY);}catch(e){}
  if(saved==='light'||saved==='dark'){applyTheme(saved);}
  else{
    const mq=window.matchMedia&&window.matchMedia('(prefers-color-scheme: light)');
    applyTheme(mq&&mq.matches?'light':'dark');
  }
})();
function toggleTheme(){
  const cur=document.documentElement.getAttribute('data-theme')==='light'?'light':'dark';
  const next=(cur==='light')?'dark':'light';
  applyTheme(next);
  try{localStorage.setItem(THEME_KEY,next);}catch(e){}
}
document.getElementById('themeBtn').addEventListener('click',toggleTheme);
function drawMinute(chart,pct){
  const c=document.getElementById('idxChart');
  if(!c||!chart||chart.length<2){if(c){c.parentNode.removeChild(c);}return;}
  const ctx=c.getContext('2d');
  const W=c.width=Math.max(c.clientWidth||600,300),H=c.height=130;
  const pts=chart.map(x=>x.p);
  const mn=Math.min(...pts),mx=Math.max(...pts),rg=(mx-mn)||1;
  ctx.clearRect(0,0,W,H);
  // 网格
  ctx.strokeStyle='#21262d';ctx.lineWidth=1;
  for(let i=1;i<4;i++){ctx.beginPath();ctx.moveTo(0,H*i/4);ctx.lineTo(W,H*i/4);ctx.stroke();}
  // 中线(昨收参考=首点与末点均值近似)
  ctx.strokeStyle='#30363d';ctx.beginPath();ctx.moveTo(0,H/2);ctx.lineTo(W,H/2);ctx.stroke();
  // 分时线
  ctx.beginPath();
  const step=W/(pts.length-1);
  pts.forEach((p,i)=>{const x=i*step,y=H-((p-mn)/rg)*(H-10)-5;i?ctx.lineTo(x,y):ctx.moveTo(x,y);});
  ctx.strokeStyle=(pct>=0?'#ef5350':'#26a69a');ctx.lineWidth=2;ctx.stroke();
  // 填充
  ctx.lineTo(W,H);ctx.lineTo(0,H);ctx.closePath();
  ctx.fillStyle=(pct>=0?'rgba(239,83,80,.08)':'rgba(38,166,154,.08)');ctx.fill();
  // 标注
  ctx.fillStyle='#8b949e';ctx.font='10px sans-serif';
  ctx.fillText('高 '+fmt(mx,1),6,12);ctx.fillText('低 '+fmt(mn,1),6,H-4);
}
function render(d){
  document.getElementById('clock').textContent=d.beijing+' '+['一','二','三','四','五','六','日'][d.weekday];
  const st=document.getElementById('status');
  if(!d.is_weekday){st.textContent='○ 周末/休市(不运行)';st.className='tag off';}
  else {st.textContent=d.trading?'● 交易中':'○ 非交易时段';st.className='tag '+(d.trading?'live':'off');}
  document.getElementById('phase').textContent=d.phase||'--';
  document.getElementById('tv').textContent=showMoney?fmt(d.total_value)+'元':'***';
  document.getElementById('tv').className=showMoney?'':'masked';
  const tp=document.getElementById('tp');
  tp.textContent=showMoney?sign(d.total_pnl)+'元':'***';tp.className=showMoney?cls(d.total_pnl):'masked';
  const tpp=document.getElementById('tpp');
  tpp.textContent=showMoney?sign(d.total_pnl_pct)+'%':'***';tpp.className=showMoney?cls(d.total_pnl_pct):'masked';
  const td=document.getElementById('td');
  td.textContent=showMoney?sign(d.total_day_pnl)+'元 ('+sign(d.total_day_pnl_pct)+'%)':'***';
  td.className=showMoney?cls(d.total_day_pnl):'masked';
  document.getElementById('sb').textContent=d.sector_bias||'--';

  // 异动提醒(置顶)
  const F=document.getElementById('feed');
  if(d.alerts&&d.alerts.length){F.innerHTML=d.alerts.slice().reverse().slice(0,10).map(a=>`<div class="it ${a.level}"><span class="t">${a.time}</span> <b>${a.name}</b> ${a.text}</div>`).join('');}
  else F.innerHTML='<div class="note">暂无预警</div>';

  // 指数1小时预测 + 分时图
  const ifEl=document.getElementById('idxforecast');
  if(d.idx_forecast){const f=d.idx_forecast;
    const conf = f.confidence!=null ? f.confidence : 0;
    const confPct = Math.round(conf*100);
    const confBadge = conf>=0.65 ? 'b-up' : (conf>=0.4 ? 'b-info' : 'b-muted');
    // v3.11.7: 三态着色 —— 旧版 up=(verdict==='看涨') 把"震荡"也一并染成了看跌色
    const dv = f.display_verdict || f.verdict || '震荡';
    const vcls = dv==='看涨' ? 'up' : (dv==='看跌' ? 'down' : 'flat');
    const act = f.actionable === true;
    const needPct = Math.round((f.min_conf!=null?f.min_conf:0.35)*100);
    const noSig = act ? '' : ` <span class="badge b-muted" title="置信度需≥${needPct}% 才给出方向判定">信号不足</span>`;
    const breadth = f.breadth!=null ? f.breadth : 0.5;
    const late = f.late!=null ? f.late : 0;
    const retail = f.retail!=null ? f.retail : 0;
    ifEl.innerHTML=`<div style="display:flex;align-items:baseline;gap:16px;flex-wrap:wrap">
      <span>现价 <b class="px ${cls(f.pct)}">${fmt(f.price)}</b> <span class="pct ${cls(f.pct)}">${sign(f.pct)}%</span></span>
      <span>1小时后<b class="gauge ${vcls}"> ${fmt(f.prob,1)}%</b> <b class="${vcls}">${dv}</b>${noSig}</span>
      <span class="note">更新 ${f.time} ｜ 日内高${fmt(f.high)} 低${fmt(f.low)} ｜ 置信度<span class="badge ${confBadge}">${confPct}%</span></span></div>
      <div style="display:flex;gap:14px;flex-wrap:wrap;margin:6px 0;font-size:13px;opacity:0.85">
        <span>📊 宽度(上涨板块占比): <b>${(breadth*100).toFixed(0)}%</b></span>
        <span>🚀 尾盘动向: <b class="${cls(late)}">${late>=0?'+':''}${late.toFixed(2)}%</b></span>
        <span>🧩 小盘(国证2000): <b class="${cls(retail)}">${retail>=0?'+':''}${retail.toFixed(2)}%</b></span>
      </div>
      <canvas id="idxChart" class="minichart"></canvas>
      <div class="note">${f.note}${f.ai_used?' ｜ <b style="color:var(--info)">AI模型已参与</b>':''}</div>`;
    drawMinute(f.chart, f.pct);
  } else ifEl.innerHTML='<div class="note">指数预测未生成(9:15后每2分钟更新)</div>';

  // 9:25:02 涨停扫描
  const po=document.getElementById('preopen');
  if(d.preopen){    let r='<div class="note">'+d.preopen.time+' 更新 ｜ '+d.preopen.note+'</div><table><tr><th>代码</th><th>名称</th><th>现价</th><th>涨停价</th><th>距涨停</th><th>涨停概率</th><th>模型</th><th>状态</th></tr>';
    d.preopen.rows.forEach(x=>{
      let yao = x.yao ? `<span class="badge b-yao">🔥妖·${x.yao_days}连板</span> ` : '';
      let badge='<span class="badge b-muted">观察</span>';
      if(x.broken){badge='<span class="badge b-down">⚠️ 炸板</span>';}
      else if(x.red_open){badge='<span class="badge b-up">🔥 可买</span>';}
      else if(x.is_succesor){badge='<span class="badge b-info">继位</span>';}
      r+=`<tr><td>${x.code}</td><td>${x.name}</td><td>${fmt(x.price)}</td><td>${fmt(x.limit_up)}</td><td class="${cls(-x.dist_limit_up)}">${fmt(x.dist_limit_up)}%</td><td class="prob ${probcls(x.prob)}">${fmt(x.prob,1)}%</td><td><span class="badge b-${x.model==='AI'?'info':'muted'}">${x.model||'启发式'}</span></td><td>${yao}${badge}</td></tr>`;
    });
    r+='</table>';po.innerHTML=r;}
  // 持仓 (支持隐私隐藏金额)
  const H=document.getElementById('holdings');H.innerHTML='';
  const mask=v=>showMoney?v:'***';
  const mval=v=>showMoney?fmt(v)+'元':'***';
  // 已清仓统计模块(按用户要求不展示): 数据保留在 snapshot 中, 仅不渲染卡片
  (d.holdings||[]).forEach(h=>{
    if(h.error){H.innerHTML+='<div class="card"><h3>'+h.name+' <span class="code">'+h.code+'</span></h3><div class="note">'+h.error+'</div></div>';return;}
    const pc=cls(h.pnl);      // 卡片主色: 以成本价 vs 实时价(盈亏)为基准, 不基于昨日价
    const pcd=cls(h.pct);     // 当日涨跌%: 以昨收为基准(独立于卡片主色)
    // 角落题材芯片: 所属细分题材名 + 题材平均涨跌幅 + 题材强弱排名(命中题材即自动显示)
    const secChip = h.theme_name ? `<span class="sector-chip" title="所属题材涨跌(成分股平均)">${h.theme_name} <span class="sp ${cls(h.theme_pct)}">${sign(h.theme_pct)}%</span>${h.theme_rank?'<span class="rk">'+h.theme_rank+'/'+(h.theme_total||'')+'</span>':''}</span>` : '';
    let badges=(h.anomalies||[]).map(a=>'<span class="badge b-'+a.level+'">'+a.text+'</span>').join('');
    H.innerHTML+=`<div class="card ${pc}">
      <h3><span>${h.name} <span class="code">${h.market.toUpperCase()}${h.code}</span></span>${secChip}</h3>
      <div style="margin:2px 0 4px"><span class="px ${pc}">${fmt(h.price)}</span><span class="pct ${pcd}">${sign(h.pct)}%</span></div>
      <div class="grp">盈亏概览</div>
      <div class="row"><span class="lbl">持仓市值</span><span class="${showMoney?'':'masked'}">${mval(h.value)}</span></div>
      <div class="row"><span class="lbl">浮动盈亏</span><span class="${showMoney?'':'masked'} ${cls(h.pnl)}">${showMoney?sign(h.pnl)+'元':mask(0)} (${showMoney?sign(h.pnl_pct)+'%':mask(0)})</span></div>
      <div class="row"><span class="lbl">当日盈亏</span><span class="${showMoney?'':'masked'} ${cls(h.day_pnl)}">${showMoney?sign(h.day_pnl)+'元':mask(0)} (${showMoney?sign(h.day_pnl_pct)+'%':mask(0)})<span class="note" style="margin-left:6px">·基${h.day_basis}</span></span></div>
      <div class="row"><span class="lbl">成本 / 股数</span><span class="${showMoney?'':'masked'}">${showMoney?fmt(h.cost,3):'***'} / ${h.shares}股</span></div>
      <div class="grp">风控线</div>
      <div class="row"><span class="lbl">🛑 止损线</span><span class="down">≤ ${fmt(h.stop_price)}</span></div>
      <div class="row"><span class="lbl">🟢 补仓区</span><span class="up">≤ ${fmt(h.add1_price)} / ${fmt(h.add2_price)}</span></div>
      <div class="row"><span class="lbl">✅ 止盈线</span><span class="up">≥ ${fmt(h.take_price)}</span></div>
      <div class="row"><span class="lbl">⛰ 压力位</span><span class="up">≈ ${fmt(h.pressure)}</span></div>
      <div class="grp">盘面</div>
      <div class="row"><span class="lbl">涨停 / 跌停</span><span>${fmt(h.limit_up)} / ${fmt(h.limit_down)}</span></div>
      <div class="row"><span class="lbl">换手 / 振幅</span><span>${fmt(h.turnover)}% / ${fmt(h.amplitude)}%</span></div>
      <div style="margin-top:10px">${badges||'<span class="badge b-muted">正常</span>'}</div>
    </div>`;
  });
  // 指数
  let it='<tr><th>指数</th><th>点位</th><th>涨跌幅</th></tr>';
  (d.indices||[]).forEach(i=>{it+=`<tr><td>${i.name}</td><td>${fmt(i.price)}</td><td class="${cls(i.pct)}">${sign(i.pct)}%</td></tr>`;});
  document.getElementById('idx').innerHTML=it;
  // 题材(细分)涨跌: 取代宽泛的 12 个中证行业, 更贴近个股关联主线
  const ths=d.themes||[];
  const tavg=ths.length?ths.reduce((a,b)=>a+b.pct,0)/ths.length:0;
  document.getElementById('sb2').textContent='↑'+ths.filter(x=>x.pct>0).length+' ↓'+ths.filter(x=>x.pct<0).length+' 均值'+sign(Math.round(tavg*100)/100)+'%';
  let s='';
  ths.forEach(x=>{const w=Math.min(Math.abs(x.pct)*6,100);s+=`<div style="margin:5px 0"><span style="display:inline-block;width:72px">${x.name}</span><span class="bar ${x.pct>=0?'up':''}" style="width:${w}px"></span> <span class="${cls(x.pct)}">${sign(x.pct)}%</span><span class="note" style="margin-left:4px">${x.n}股</span></div>`;});
  document.getElementById('sectors').innerHTML=s;
  document.getElementById('bias').textContent=d.sector_bias;
  // 散户今日平均盈亏(国证2000近似) —— 紧凑版
  const rt=document.getElementById('retail');
  if(d.retail_pnl){const r=d.retail_pnl;const rc=cls(r.pct);
    rt.innerHTML=`<div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">
      <span class="pct ${rc}" style="font-size:20px;font-weight:700">${sign(r.pct)}%</span>
      <span class="badge b-${r.pct>=0?'up':'down'}">小盘(国证2000) ${r.pct>=0?'涨':'跌'} ≈ ${sign(r.pct)}%</span>
      <span class="note" style="margin:0">${r.name} ${fmt(r.price)}</span>
    </div>
    <div class="note">以国证2000(小盘股)当日涨跌幅近似散户平均盈亏, 仅供参考</div>`;
  } else rt.innerHTML='<div class="note">散户平均盈亏数据未获取</div>';
  // 主题材拉/踩指数
  const dv=document.getElementById('drivers');
  if(d.sector_drivers){const dd=d.sector_drivers;
    const pull=(dd.pullers&&dd.pullers.length)?dd.pullers.map(s=>`<span class="badge b-up">▲ ${s.name} ${sign(s.pct)}% ·偏离+${fmt(s.dev)}%</span>`).join(' '):'<span class="badge b-muted">暂无明显拉动板块</span>';
    const pres=(dd.pressers&&dd.pressers.length)?dd.pressers.map(s=>`<span class="badge b-down">▼ ${s.name} ${sign(s.pct)}% ·偏离${fmt(s.dev)}%</span>`).join(' '):'<span class="badge b-muted">暂无明显压制板块</span>';
    dv.innerHTML=`<div class="note">${dd.index_name} ${sign(dd.index_pct)}% ｜ ${dd.move} ｜ ${dd.time} 更新</div>
      <div style="margin:8px 0"><b class="up">🟢 拉指数</b><br>${pull}</div>
      <div style="margin:8px 0"><b class="down">🔴 踩指数</b><br>${pres}</div>
      <div class="note">${dd.note}</div>`;
  } else dv.innerHTML='<div class="note">暂未检测(9:15后每10分钟更新, 可点按钮立即检测)</div>';
  // 尾盘
  if(d.close){let r=`<div class="note">${d.close.time} 生成 ｜ ${d.close.note}${d.close.ai_used?' ｜ <b style="color:var(--info)">AI模型已参与</b>':''}</div>`;
    const m=d.close.market;
    const cm = m.confidence!=null ? m.confidence : 0;
    const cmPct = Math.round(cm*100);
    const cmBadge = cm>=0.65 ? 'b-up' : (cm>=0.4 ? 'b-info' : 'b-muted');
    const mBr = m.breadth!=null ? m.breadth : 0.5;
    const mLt = m.late!=null ? m.late : 0;
    const mRt = m.retail!=null ? m.retail : 0;
    r+=`<div class="row" style="border:none"><span>大盘明日看涨概率</span><span class="prob ${probcls(m.prob)}">${fmt(m.prob,1)}% (${m.verdict}) <span class="badge ${cmBadge}" style="margin-left:6px">置信度${cmPct}%</span></span></div>`;
    r+=`<div style="display:flex;gap:14px;flex-wrap:wrap;margin:4px 0 8px;font-size:13px;opacity:0.85">
        <span>📊 宽度: <b>${(mBr*100).toFixed(0)}%</b></span>
        <span>🚀 尾盘动向: <b class="${cls(mLt)}">${mLt>=0?'+':''}${mLt.toFixed(2)}%</b></span>
        <span>🧩 小盘(国证2000): <b class="${cls(mRt)}">${mRt>=0?'+':''}${mRt.toFixed(2)}%</b></span>
      </div>`;
    r+='<table><tr><th>持仓</th><th>概率</th><th>倾向</th></tr>';
    d.close.stocks.forEach(x=>{r+=`<tr><td>${x.name}</td><td class="prob ${probcls(x.prob)}">${fmt(x.prob,1)}%</td><td>${x.verdict}</td></tr>`;});
    r+='</table>';document.getElementById('close').innerHTML=r;}
  // 尾盘高开潜力
  const gu=document.getElementById('gapup');
  if(d.gapup){const g=d.gapup;
    let r=`<div class="note">${g.time} 更新 ｜ ${g.ai_used?'已用AI模型精修':'启发式模型'} ｜ ${g.note}</div><table><tr><th>排名</th><th>代码</th><th>名称</th><th>现价</th><th>当日%</th><th>委比</th><th>尾盘拉升</th><th>高开概率</th><th>置信度</th><th>模型</th></tr>`;
    (g.rows||[]).forEach((x,i)=>{
      const lp = x.late_pull!=null ? x.late_pull : 0;
      const conf = x.confidence!=null ? x.confidence : 0;
      const cPct = Math.round(conf*100);
      const cBadge = conf>=0.65 ? 'b-up' : (conf>=0.4 ? 'b-info' : 'b-muted');
      r+=`<tr><td>${i+1}</td><td>${x.code}</td><td>${x.name}</td><td>${fmt(x.price)}</td><td class="${cls(x.pct)}">${sign(x.pct)}%</td><td class="${cls(x.weibi)}">${fmt(x.weibi)}%</td><td class="${cls(lp)}">${lp>=0?'+':''}${lp.toFixed(2)}%</td><td class="prob ${probcls(x.prob)}">${fmt(x.prob,1)}%</td><td><span class="badge ${cBadge}">${cPct}%</span></td><td><span class="badge b-${x.model==='AI'?'info':'muted'}">${x.model}</span></td></tr>`;
    });
    r+='</table>';gu.innerHTML=r;
  } else gu.innerHTML='<div class="note">尚未生成（交易日 14:52 起自动检测, 可点按钮立即检测）</div>';
}

// 高开回测(v3.4): 仅展示最近一次(昨天)推荐的 5 只开盘实测结果, 版面紧凑(非核心内容)
function renderGapVerify(d){
  const box=document.getElementById('gapverify');
  if(!box) return;
  const recent=(d.stats&&d.stats.recent)||[];
  const day=recent[0];  // recent[0] 即最近一次已验证(昨天推荐→今开实测)
  if(!day||!day.stocks||!day.stocks.length){
    box.innerHTML='<div class="note">暂无昨日的回测结果。下一交易日 09:30 后自动验证当日推荐的 5 只是否高开。</div>';
    return;
  }
  const stocks=day.stocks;
  const hits=stocks.filter(x=>x.is_gap_up).length;
  const rdate=day.date?day.date.slice(5):'--';          // 推荐日(昨天)
  const vdate=day.verified_at?day.verified_at.slice(5,10):rdate;  // 实测日
  let r=`<div class="gv-head"><span>推荐 <b>${rdate}</b> · 今开实测 <b>${vdate}</b></span>`
       +`<span class="gv-hit">命中 <b>${hits}</b>/${stocks.length}</span></div>`;
  r+='<div class="gv-list">';
  stocks.forEach(x=>{
    const gp=x.gap_pct;
    const hit=x.is_gap_up;
    r+=`<div class="gv-item">`
      +`<span class="gv-name">${x.code} ${x.name}</span>`
      +`<span class="gv-prob ${probcls(x.prob)}">预${fmt(x.prob,0)}%</span>`
      +`<span class="gv-gap ${cls(gp)}">${gp==null?'--':(gp>=0?'+':'')+fmt(gp,2)+'%'}</span>`
      +`<span class="gv-res ${hit?'up':'down'}">${hit?'✅':'❌'}</span>`
      +`</div>`;
  });
  r+='</div>';
  // 近5次自动调参的变化值(紧凑, 非核心): 展示 AUC 趋势 + 变动较大的权重
  const opts=(d.stats&&d.stats.optimizations)||[];
  if(opts.length){
    r+='<div class="gv-opt"><div class="gv-opt-h">近5次自动调参变化</div>';
    opts.slice(-5).reverse().forEach(o=>{
      const before=o.before||{}, after=o.after||{};
      const deltas=[];
      for(const k in after){
        const dv=after[k]-before[k];
        if(Math.abs(dv)>=0.1) deltas.push(`${k.replace('gu_','')} ${before[k].toFixed(1)}→${after[k].toFixed(1)}`);
      }
      const aucUp=(o.auc_after||0)>=(o.auc_before||0);
      r+=`<div class="gv-opt-row">`
        +`<span class="gv-opt-date">${o.at?o.at.slice(5,10):''}</span>`
        +`<span class="gv-opt-auc ${aucUp?'up':'down'}">AUC ${(o.auc_before||0).toFixed(3)}→${(o.auc_after||0).toFixed(3)}</span>`
        +`<span class="gv-opt-delta">${deltas.length?deltas.join(' · '):'微调'}</span>`
        +`</div>`;
    });
    r+='</div>';
  }
  box.innerHTML=r;
}

// 预测回测总览(v3.10): 四个预测模块的命中率/平均预测概率/校准偏差 + 日线库状态
const PRED_LABELS={idx_1h:'上证1小时方向',close_market:'尾盘大盘次日',close_stock:'尾盘个股次日',preopen_limitup:'盘前涨停预测',gapup:'尾盘高开潜力'};
const PRED_MIN_CALIB=30;   // 概率校准自动启用的样本阈值(与后端 PRED_MIN_CALIB_SAMPLES 同步)
function renderPredStats(s){
  const box=document.getElementById('predstats');
  if(!box) return;
  const mods=(s&&s.modules)||{};
  const tune=(s&&s.tune)||{};
  const tmeta=(s&&s.tune_meta)||{};
  const keys=Object.keys(PRED_LABELS).filter(k=>mods[k]);
  // v3.11.0: 顶部工具条(自动调参操作 + 门控说明)
  const head='<div class="gv-toolbar" style="display:flex;gap:8px;align-items:center;margin-bottom:6px;flex-wrap:wrap">'
    +'<button class="gv-btn" onclick="tuneNow(this)">🔧 重新调参</button>'
    +'<button class="gv-btn" onclick="tuneReset()">↺ 恢复默认</button>'
    +`<span class="note" style="margin:0">自动调参随样本累积生效：阈值≥${tmeta.min_thr||15}，权重≥${tmeta.min_w||50}（权重多、需更多样本防过拟合）</span>`
    +'</div>';
  if(!keys.length){
    box.innerHTML=head+'<div class="note">暂无回测样本。预测落盘后按到期时刻自动回填真实结果（上证1小时当日验证，尾盘/涨停次日或当日收盘验证），样本会随交易日积累。</div>';
    return;
  }
  let r='<div class="gv-list">';
  keys.forEach(k=>{
    const e=mods[k];
    const rate=e.hit_rate==null?null:e.hit_rate*100;
    const bias=e.bias_pp;
    const biasTxt=bias==null?'--':(bias>1?'高估'+fmt(bias,1)+'pp':bias<-1?'低估'+fmt(-bias,1)+'pp':'贴合±1pp');
    const biasCls=bias==null?'flat':(Math.abs(bias)<=1?'flat':(bias>0?'down':'up')); // 高估=橙红提示, 低估=偏绿
    r+=`<div class="gv-item">`
      +`<span class="gv-name">${PRED_LABELS[k]}</span>`
      +`<span class="gv-prob flat">样本${e.n}</span>`
      +`<span class="gv-gap ${rate==null?'flat':(rate>=50?'up':'down')}">命中率 ${rate==null?'--':fmt(rate,0)+'%'}</span>`
      +`<span class="gv-res ${biasCls}" style="min-width:96px;text-align:right;font-size:12px">偏差 ${biasTxt}</span>`
      +`</div>`;
    // v3.10.1: 概率自动校准徽章(样本≥12 自动拟合, 实时输出与面板概率同步对齐真实命中率)
    const cb=e.calib||{};
    const calibBadge = cb.applied
      ? `<span class="gv-prob" style="color:var(--green,#2ecc71)">✓已校准(n=${cb.n})</span>`
      : `<span class="gv-prob flat">未校准(${cb.n||0}/${PRED_MIN_CALIB||12})</span>`;
    r+=`<div class="gv-opt-row" style="padding-left:10px">${calibBadge}</div>`;
    // v3.11.0: 自动调参(阈值+权重)徽章
    const tn=tune[k]||{};
    const thr=tn.threshold||{}; const wt=tn.weights||{};
    let tuneBadge;
    if(k==='gapup'){   // v3.11.1: gapup 是排名问题, 以 AUC 衡量判别力, 无二元阈值
      if(wt.auc_after!=null){
        const up=wt.auc_after>=wt.auc_before;
        tuneBadge='<span class="gv-prob" style="color:var(--green,#2ecc71)">✓已调参 AUC '
          +fmt(wt.auc_before,3)+'→'+fmt(wt.auc_after,3)+(up?' ▲':' ▼')+' (n='+wt.n+')</span>';
      } else {
        tuneBadge='<span class="gv-prob flat">待激活('+(wt.n||0)+'/'+(wt.need||(tmeta.gapup_min||10))+')</span>';
      }
    } else if(thr.threshold!=null){
      tuneBadge='<span class="gv-prob" style="color:var(--green,#2ecc71)">✓已调参 阈值'+thr.threshold
        +' F1 '+thr.def_f1+'→'+thr.f1;
      if(wt.values&&Object.keys(wt.values).length) tuneBadge+=' 权重漂移'+wt.drift;
      tuneBadge+=' (n='+thr.n+')</span>';
    } else {
      const need=Math.max(thr.need||0, wt.need||0);
      const have=Math.max(thr.n||0, wt.n||0);
      tuneBadge='<span class="gv-prob flat">待激活('+have+'/'+need+')</span>';
    }
    r+=`<div class="gv-opt-row" style="padding-left:10px">${tuneBadge}</div>`;
    // 分方向明细(紧凑一行)
    const bv=e.by_verdict||{};
    const detail=Object.keys(bv).map(v=>{
      const b=bv[v];
      return `${v} ${b.hit}/${b.n}`;
    }).join(' · ');
    if(detail) r+=`<div class="gv-opt-row" style="padding-left:10px"><span class="gv-opt-date">${e.avg_pred!=null?'均预测 '+fmt(e.avg_pred,1)+'%':''}</span><span class="gv-opt-delta">${detail}</span></div>`;
  });
  r+='</div>';
  // 校准说明: 偏差>0 表示模型宣称的概率系统性高于实际发生率(过度自信)
  const anyBias=keys.some(k=>mods[k].bias_pp!=null&&Math.abs(mods[k].bias_pp)>5);
  if(anyBias) r+='<div class="note" style="margin-top:6px">提示: 偏差为"平均预测概率 − 实际命中率"。偏差>5pp 说明该模块概率过度自信，样本≥12 后可启用概率校准自动修正。</div>';
  box.innerHTML=head+r;
}
function tuneNow(b){if(b){b.disabled=true;b.textContent='⏳ 调参中…';} fetch('/api/tune',{method:'POST'}).then(()=>loadPredStats()).finally(()=>{if(b){b.disabled=false;b.textContent='🔧 重新调参';}});}
function tuneReset(){fetch('/api/tune_reset',{method:'POST'}).then(()=>loadPredStats()).catch(()=>{});}
function loadPredStats(){fetch('/api/pred_stats').then(r=>r.json()).then(renderPredStats).catch(()=>{});}
function load(){fetch('/api/snapshot').then(r=>r.json()).then(render).catch(()=>{});fetch('/api/gapup/log').then(r=>r.json()).then(renderGapVerify).catch(()=>{});loadPredStats();}
function manual(t){
  if(t==='close'){
    const b=document.getElementById('closeRefreshBtn');
    const tip=document.getElementById('closeRefreshTip');
    if(b&&!b.disabled){
      b.disabled=true;b.style.opacity=.6;b.textContent='⏳ 预测中…';
      if(tip)tip.textContent='正在立即重新计算大盘+各持仓方向（含AI融合），请稍候…';
      fetch('/api/'+t).then(r=>r.json()).then(()=>{
        setTimeout(()=>{load();if(b){b.disabled=false;b.style.opacity=1;b.textContent='🎯 立即预测';}
          if(tip)tip.textContent='点击立即重新计算一次尾盘预测，一般需 10~30 秒';},800);
      }).catch(()=>{if(b){b.disabled=false;b.style.opacity=1;b.textContent='🎯 立即预测';}});
    }
  }else if(t==='gapup'){
    const b=document.getElementById('gapupBtn');
    const tip=document.getElementById('gapupTip');
    if(b&&!b.disabled){
      b.disabled=true;b.style.opacity=.6;b.textContent='⏳ 扫描中…';
      if(tip)tip.textContent='正在立即全市场扫描主板（约需 1~3 分钟），请稍候自动更新…';
      fetch('/api/gapup?force=1').then(r=>r.json()).then(()=>{
        setTimeout(()=>{load();if(b){b.disabled=false;b.style.opacity=1;b.textContent='🎯 立即检测';}
          if(tip)tip.textContent='点击立即全市场扫描主板，约需 1~3 分钟';},800);
      }).catch(()=>{if(b){b.disabled=false;b.style.opacity=1;b.textContent='🎯 立即检测';}});
    }
  }else{fetch('/api/'+t).then(r=>r.json()).then(()=>load());}
}
// ---- 持仓前端编辑(仅 code/cost/shares, 保存走 /api/portfolio) ----
let editing=false, pfEdit=[];
function enterEdit(){
  editing=true;
  document.getElementById('pfEditor').style.display='block';
  const msg=document.getElementById('pfMsg'); if(msg) msg.textContent='';
  fetch('/api/portfolio').then(r=>r.json()).then(d=>{
    pfEdit=(d.holdings||[]).map(h=>({code:String(h.code), cost:h.cost, shares:h.shares}));
    renderPfRows();
  }).catch(()=>{pfEdit=[];renderPfRows();});
}
function renderPfRows(){
  const box=document.getElementById('pfRows');
  if(!box) return;
  if(!pfEdit.length){box.innerHTML='<div class="note">暂无持仓，点「➕ 新增一行」添加</div>';return;}
  let h='<table style="width:100%;border-collapse:collapse"><tr><th style="text-align:left;padding:4px 8px">股票代码</th><th style="text-align:left;padding:4px 8px">成本</th><th style="text-align:left;padding:4px 8px">股数</th><th style="padding:4px 8px"></th></tr>';
  pfEdit.forEach((x,i)=>{
    h+=`<tr>
      <td style="padding:4px 8px"><input id="pfCode${i}" value="${x.code}" style="width:96px;background:var(--bg);color:var(--txt);border:1px solid var(--line);border-radius:5px;padding:5px 7px;font-size:13px" oninput="pfUpd(${i},'code',this.value)"></td>
      <td style="padding:4px 8px"><input id="pfCost${i}" type="number" step="0.001" value="${Number(x.cost||0).toFixed(3)}" style="width:96px;background:var(--bg);color:var(--txt);border:1px solid var(--line);border-radius:5px;padding:5px 7px;font-size:13px" oninput="pfUpd(${i},'cost',this.value)"></td>
      <td style="padding:4px 8px"><input id="pfShares${i}" type="number" step="1" value="${x.shares}" style="width:96px;background:var(--bg);color:var(--txt);border:1px solid var(--line);border-radius:5px;padding:5px 7px;font-size:13px" oninput="pfUpd(${i},'shares',this.value)"></td>
      <td style="padding:4px 8px"><button onclick="pfDel(${i})" style="background:transparent;color:var(--down);border:1px solid var(--down);border-radius:5px;padding:4px 9px;cursor:pointer;font-size:12px">🗑 删除</button></td>
    </tr>`;
  });
  h+='</table>';
  box.innerHTML=h;
}
function pfUpd(i,f,v){ if(pfEdit[i]) pfEdit[i][f]=v; }
function pfAdd(){ pfEdit.push({code:'',cost:0,shares:0}); renderPfRows(); }
function pfDel(i){ pfEdit.splice(i,1); renderPfRows(); }
function pfCancel(){ editing=false; const e=document.getElementById('pfEditor'); if(e) e.style.display='none'; const m=document.getElementById('pfMsg'); if(m) m.textContent=''; }
function pfSave(){
  // 从输入框读取最新值(防止 oninput 漏抓)
  pfEdit.forEach((x,i)=>{
    const c=document.getElementById('pfCode'+i), co=document.getElementById('pfCost'+i), s=document.getElementById('pfShares'+i);
    if(c) x.code=c.value; if(co) x.cost=parseFloat(co.value)||0; if(s) x.shares=parseFloat(s.value)||0;
  });
  const rows=pfEdit.map(x=>({code:(x.code||'').trim(), cost:parseFloat(x.cost)||0, shares:parseFloat(x.shares)||0})).filter(x=>x.code);
  const msg=document.getElementById('pfMsg');
  if(!rows.length){ if(msg) msg.textContent='请至少保留一只持仓，或填写股票代码'; return; }
  if(msg) msg.textContent='保存中…';
  fetch('/api/portfolio',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({holdings:rows})})
    .then(r=>r.json()).then(d=>{
      if(d.ok){ if(msg) msg.textContent='✅ 已保存（'+d.count+' 只），约 5 秒内刷新行情'; editing=false; const e=document.getElementById('pfEditor'); if(e) e.style.display='none'; load(); }
      else { if(msg) msg.textContent='❌ 保存失败：'+(d.error||'未知错误'); }
    }).catch(e=>{ if(msg) msg.textContent='❌ 保存失败：'+e; });
}
load();setInterval(load,5000);
