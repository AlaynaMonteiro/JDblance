// The origin's browser page (formerly the Flask backend's template and static
// files).  @@NAME@@ placeholders are substituted, HTML-escaped, per request.
#pragma once

namespace jdblance::origin_page {

inline constexpr const char* kIndexHtml = R"PAGE(<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>@@CONTAINER@@ :: backend</title>
    <link rel="stylesheet" href="/static/css/style.css">
</head>
<body>
<div class="wrap">
    <h1>backend</h1>
    <div class="card">
        <div class="row"><span class="k">container</span>
            <span class="v container-name" id="container-name">@@CONTAINER@@</span></div>
        <div class="row"><span class="k">request id</span><span class="v">@@REQUEST_ID@@</span></div>
        <div class="row"><span class="k">forwarded for</span><span class="v">@@FORWARDED_FOR@@</span></div>
        <div class="row"><span class="k">served at</span><span class="v" id="clock">-</span></div>
        <button onclick="refreshContainer()">reload</button>
    </div>

    <div class="card">
        <h2>fault injection</h2>
        <p class="hint">Break this backend and watch the balancer route around it.</p>
        <div class="controls">
            <button onclick="chaos({fail_rate: 1.0})">fail 100%</button>
            <button onclick="chaos({fail_rate: 0.3})">fail 30%</button>
            <button onclick="chaos({latency_ms: 500})">+500 ms</button>
            <button onclick="chaos({healthy: false})">fail health checks</button>
            <button onclick="chaos({reset: true})">reset</button>
        </div>
        <pre id="chaos-state">-</pre>
    </div>

    <p class="footnote">
        Load balancer control plane: <a href="http://localhost:8001/">localhost:8001</a>
        &middot; Grafana: <a href="http://localhost:3001/">localhost:3001</a>
    </p>
</div>
<script src="/static/js/script.js"></script>
</body>
</html>
)PAGE";

inline constexpr const char* kStyleCss = R"PAGE(:root {
    --bg: #0b0e14;
    --panel: #141922;
    --line: #222b39;
    --fg: #d6deeb;
    --dim: #7c8aa5;
    --accent: #38bdf8;
}

* { box-sizing: border-box; }

body {
    margin: 0;
    min-height: 100vh;
    background: var(--bg);
    color: var(--fg);
    font: 14px/1.6 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}

.wrap { max-width: 720px; margin: 0 auto; padding: 48px 24px; }

h1 {
    margin: 0 0 24px;
    font-size: 16px;
    letter-spacing: .18em;
    text-transform: uppercase;
    color: var(--accent);
}

h2 { margin: 0 0 4px; font-size: 13px; color: var(--fg); font-weight: 600; }

.card {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 18px 20px;
    margin-bottom: 18px;
}

.row {
    display: flex;
    justify-content: space-between;
    gap: 16px;
    padding: 5px 0;
    border-bottom: 1px dashed var(--line);
}

.row:last-of-type { border-bottom: none; }

.k { color: var(--dim); }
.v { text-align: right; word-break: break-all; }
.container-name { color: var(--accent); font-weight: 600; }

.hint { color: var(--dim); margin: 0 0 12px; }
.controls { display: flex; flex-wrap: wrap; gap: 8px; }

button {
    background: #1b2431;
    color: var(--fg);
    border: 1px solid var(--line);
    border-radius: 6px;
    padding: 6px 12px;
    margin-top: 12px;
    cursor: pointer;
    font: inherit;
    font-size: 12px;
}

button:hover { border-color: var(--accent); color: var(--accent); }
.controls button { margin-top: 0; }

pre {
    margin: 14px 0 0;
    padding: 12px;
    background: #0e131b;
    border: 1px solid var(--line);
    border-radius: 6px;
    color: var(--dim);
    overflow-x: auto;
}

.footnote { color: var(--dim); font-size: 12px; text-align: center; }
a { color: var(--accent); }
)PAGE";

inline constexpr const char* kScriptJs = R"PAGE(function refreshContainer() {
    fetch('/', {headers: {'Accept': 'application/json'}})
        .then(r => r.json())
        .then(data => {
            document.getElementById('container-name').textContent = data.container;
            document.getElementById('clock').textContent = new Date().toLocaleTimeString();
        })
        .catch(err => console.error('refresh failed:', err));
}

function chaos(payload) {
    fetch('/chaos', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(payload)
    })
        .then(r => r.json())
        .then(state => {
            document.getElementById('chaos-state').textContent = JSON.stringify(state, null, 2);
        })
        .catch(err => console.error('chaos failed:', err));
}

function pollChaos() {
    fetch('/info')
        .then(r => r.json())
        .then(data => {
            document.getElementById('chaos-state').textContent = JSON.stringify(data.chaos, null, 2);
            document.getElementById('clock').textContent = new Date().toLocaleTimeString();
        })
        .catch(() => {});
}

pollChaos();
setInterval(pollChaos, 2000);
)PAGE";

}  // namespace jdblance::origin_page
