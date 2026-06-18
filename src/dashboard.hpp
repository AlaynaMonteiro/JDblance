// The control plane dashboard: one self-contained page, no external assets,
// no build step.  It polls /stats once a second.
#pragma once

namespace jdblance {

inline constexpr const char* kDashboardHtml = R"HTML(<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>JDblance :: control plane</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{--bg:#0b0e14;--panel:#141922;--line:#222b39;--fg:#d6deeb;--dim:#7c8aa5;
--ok:#4ade80;--warn:#fbbf24;--bad:#f87171;--accent:#38bdf8}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
header{padding:18px 24px;border-bottom:1px solid var(--line);display:flex;
gap:24px;align-items:baseline;flex-wrap:wrap}
h1{margin:0;font-size:16px;letter-spacing:.16em;text-transform:uppercase;color:var(--accent)}
.meta{color:var(--dim)}
main{padding:24px;display:grid;gap:18px;max-width:1400px}
.row{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.stat{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:12px 14px}
.stat .k{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.08em}
.stat .v{font-size:22px;margin-top:4px}
table{width:100%;border-collapse:collapse;background:var(--panel);
border:1px solid var(--line);border-radius:8px;overflow:hidden}
th,td{text-align:left;padding:9px 12px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--dim);font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:.08em}
tr:last-child td{border-bottom:none}
.pill{padding:2px 8px;border-radius:99px;font-size:11px;border:1px solid}
.up{color:var(--ok);border-color:#1c4532;background:#0f2a1d}
.down{color:var(--bad);border-color:#4c1d1d;background:#2a0f0f}
.half{color:var(--warn);border-color:#4c3a0f;background:#2a220f}
.bar{height:6px;background:#1d2530;border-radius:3px;overflow:hidden;margin-top:5px}
.bar>i{display:block;height:100%;background:var(--accent)}
button{background:#1b2431;color:var(--fg);border:1px solid var(--line);
border-radius:5px;padding:3px 9px;cursor:pointer;font:inherit;font-size:11px}
button:hover{border-color:var(--accent);color:var(--accent)}
.panic{color:var(--bad);font-weight:600}
canvas{width:100%;height:70px;display:block}
</style></head><body>
<header>
  <h1>JDblance</h1>
  <span class="meta" id="policy">policy: -</span>
  <span class="meta" id="uptime">uptime: -</span>
  <span class="meta" id="worker">worker: -</span>
  <span class="meta" id="panic"></span>
</header>
<main>
  <div class="row" id="stats"></div>
  <div class="stat"><div class="k">requests / sec</div><canvas id="spark"></canvas></div>
  <table id="endpoints"><thead><tr>
    <th>endpoint</th><th>upstream</th><th>address</th><th>state</th><th>breaker</th><th>in flight</th>
    <th>ewma</th><th>idle conns</th><th>requests</th><th>failures</th><th>weight</th><th></th>
  </tr></thead><tbody></tbody></table>
</main>
<script>
const history = [];
const fmt = n => n === null || n === undefined ? '-' : (typeof n === 'number' ? n.toLocaleString(undefined,{maximumFractionDigits:2}) : n);
let previous = null, previousAt = 0;

function card(k, v){ return `<div class="stat"><div class="k">${k}</div><div class="v">${v}</div></div>`; }

function draw(){
  const c = document.getElementById('spark'), ctx = c.getContext('2d');
  const w = c.width = c.clientWidth * devicePixelRatio, h = c.height = 70 * devicePixelRatio;
  ctx.clearRect(0,0,w,h);
  if (history.length < 2) return;
  const max = Math.max(1, ...history);
  ctx.beginPath();
  history.forEach((v,i)=>{
    const x = i/(history.length-1)*w, y = h - (v/max)*(h-6) - 3;
    i ? ctx.lineTo(x,y) : ctx.moveTo(x,y);
  });
  ctx.strokeStyle = '#38bdf8'; ctx.lineWidth = 2*devicePixelRatio; ctx.stroke();
  ctx.lineTo(w,h); ctx.lineTo(0,h); ctx.closePath();
  ctx.fillStyle = 'rgba(56,189,248,.12)'; ctx.fill();
  ctx.fillStyle = '#7c8aa5'; ctx.font = `${11*devicePixelRatio}px monospace`;
  ctx.fillText(max.toFixed(0) + ' rps', 6*devicePixelRatio, 14*devicePixelRatio);
}

async function act(name, path){
  await fetch(`/endpoints/${name}/${path}`, {method:'POST'});
  refresh();
}

async function refresh(){
  let s;
  try { s = await (await fetch('/stats')).json(); } catch(e) { return; }
  const now = Date.now();
  if (previous) {
    const rps = (s.totals.requests - previous) / ((now - previousAt)/1000);
    history.push(Math.max(0, rps));
    if (history.length > 120) history.shift();
    draw();
  }
  previous = s.totals.requests; previousAt = now;

  document.getElementById('policy').textContent = 'policy: ' + s.policy;
  document.getElementById('uptime').textContent = 'uptime: ' + s.uptime_seconds + 's';
  document.getElementById('worker').textContent = 'worker: ' + s.worker;
  document.getElementById('panic').innerHTML = s.panic_mode ? '<span class="panic">PANIC MODE</span>' : '';

  const cls = s.status_classes || {};
  document.getElementById('stats').innerHTML = [
    card('healthy', `${s.healthy}/${s.total}`),
    card('connections', fmt(s.connections)),
    card('in flight', fmt(s.admission.in_flight)),
    card('queued', fmt(s.admission.queued)),
    card('conc. limit', fmt(s.admission.limit)),
    card('p50 / p99', `${fmt(s.latency_ms.p50)} / ${fmt(s.latency_ms.p99)} ms`),
    card('requests', fmt(s.totals.requests)),
    card('2xx / 5xx', `${fmt(cls['2xx']||0)} / ${fmt(cls['5xx']||0)}`),
    card('shed', fmt(s.totals.shed)),
    card('retries', fmt(s.totals.retries)),
    card('hedges', `${fmt(s.totals.hedges)} (${fmt(s.totals.hedge_wins)} won)`),
    card('codel', s.admission.codel_dropping ? 'DROPPING' : 'idle'),
  ].join('');

  const maxLoad = Math.max(1, ...s.endpoints.map(e=>e.in_flight));
  document.querySelector('#endpoints tbody').innerHTML = s.endpoints.map(e=>{
    const state = !e.enabled ? ['down','disabled'] : e.draining ? ['half','draining']
                : e.healthy ? ['up','up'] : ['down','down'];
    const bs = e.breaker.state, bcls = bs==='closed'?'up':bs==='open'?'down':'half';
    return `<tr>
      <td>${e.name}</td><td>${e.upstream}</td><td>${e.address}</td>
      <td><span class="pill ${state[0]}">${state[1]}</span></td>
      <td><span class="pill ${bcls}">${bs}</span></td>
      <td>${e.in_flight}<div class="bar"><i style="width:${e.in_flight/maxLoad*100}%"></i></div></td>
      <td>${fmt(e.ewma_ms)} ms</td><td>${e.idle_connections}</td>
      <td>${fmt(e.requests)}</td><td>${fmt(e.failures)}</td><td>${e.weight}</td>
      <td>
        <button onclick="act('${e.name}','drain')">drain</button>
        <button onclick="act('${e.name}','enable')">enable</button>
        <button onclick="act('${e.name}','disable')">disable</button>
      </td></tr>`;
  }).join('');
}
refresh(); setInterval(refresh, 1000);
</script></body></html>
)HTML";

}  // namespace jdblance
