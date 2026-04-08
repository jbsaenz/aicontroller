/* AI Controller — Frontend Application */
'use strict';

const API = '';  // same origin
let TOKEN = localStorage.getItem('aic_token') || '';
let FLEET_DATA = [];
let ALERT_COUNT = 0;
let refreshTimer = null;
let activeCharts = {};
let SOURCE_URL_VALIDATION = null;
let SOURCE_ALLOWLIST_INFO = null;

// ── Helpers ────────────────────────────────────────────────────────────────
async function api(path, opts = {}) {
  const res = await fetch(API + path, {
    ...opts,
    headers: {
      'Content-Type': 'application/json',
      ...(TOKEN ? { Authorization: `Bearer ${TOKEN}` } : {}),
      ...(opts.headers || {}),
    },
  });
  if (res.status === 401) { logout(); return null; }
  if (!res.ok) { const t = await res.text(); throw new Error(t); }
  return res.json();
}

function fmt(val, decimals = 1, suffix = '') {
  if (val == null || isNaN(val)) return '—';
  return Number(val).toFixed(decimals) + suffix;
}
function fmtPct(val) { return val == null ? '—' : (val * 100).toFixed(1) + '%'; }
function timeAgo(dt) {
  if (!dt) return '—';
  const s = Math.floor((Date.now() - new Date(dt)) / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s/60)}m ago`;
  if (s < 86400) return `${Math.floor(s/3600)}h ago`;
  return `${Math.floor(s/86400)}d ago`;
}
function riskClass(band) { return `risk-${(band || 'low').toLowerCase()}`; }

// ── Auth ───────────────────────────────────────────────────────────────────
function logout() {
  TOKEN = '';
  localStorage.removeItem('aic_token');
  showLogin();
}

function showLogin() {
  document.getElementById('login-page').style.display = 'flex';
  document.getElementById('app').style.display = 'none';
  if (refreshTimer) clearInterval(refreshTimer);
}

function showApp() {
  document.getElementById('login-page').style.display = 'none';
  document.getElementById('app').style.display = 'block';
  navigate('fleet');
  startRefresh();
}

document.getElementById('login-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const err = document.getElementById('login-error');
  err.style.display = 'none';
  const user = document.getElementById('login-user').value;
  const pass = document.getElementById('login-pass').value;
  try {
    const data = await fetch('/api/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: user, password: pass }),
    });
    if (!data.ok) throw new Error('Invalid credentials');
    const json = await data.json();
    TOKEN = json.access_token;
    localStorage.setItem('aic_token', TOKEN);
    document.getElementById('sidebar-user').textContent = json.username;
    showApp();
  } catch {
    err.textContent = 'Invalid username or password.';
    err.style.display = 'block';
  }
});

document.getElementById('logout-btn').addEventListener('click', logout);

// ── Navigation ─────────────────────────────────────────────────────────────
function navigate(page) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById(`page-${page}`)?.classList.add('active');
  document.querySelector(`[data-page="${page}"]`)?.classList.add('active');
  document.querySelector('.topbar-title').textContent = {
    fleet: 'Fleet Overview', analytics: 'Analytics', alerts: 'Alert Center',
    ingest: 'Data Ingestion', settings: 'Settings'
  }[page] || page;
  if (page === 'fleet') loadFleet();
  if (page === 'analytics') loadAnalytics();
  if (page === 'alerts') loadAlerts();
  if (page === 'ingest') loadSources();
  if (page === 'settings') loadSettings();
}

document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', () => navigate(item.dataset.page));
});

// ── Auto-refresh ───────────────────────────────────────────────────────────
function startRefresh() {
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = setInterval(() => {
    const activePage = document.querySelector('.page.active')?.id?.replace('page-', '');
    if (activePage === 'fleet') loadFleet();
    if (activePage === 'alerts') loadAlerts();
  }, 30000);
}

// ── Fleet Page ─────────────────────────────────────────────────────────────
async function loadFleet() {
  try {
    const [fleet, summary] = await Promise.all([api('/api/fleet'), api('/api/fleet/summary')]);
    if (!fleet) return;
    FLEET_DATA = fleet;
    renderSummary(summary);
    renderFleetGrid(fleet);
  } catch (e) { console.error('Fleet load failed', e); }
}

function renderSummary(s) {
  if (!s) return;
  document.getElementById('stat-total').textContent = s.total_miners ?? '—';
  document.getElementById('stat-hash').textContent = s.total_hashrate ? fmt(s.total_hashrate, 1, ' TH/s') : '—';
  document.getElementById('stat-temp').textContent = s.avg_temperature ? fmt(s.avg_temperature, 1, '°C') : '—';
  document.getElementById('stat-critical').textContent = s.critical_count ?? 0;
  document.getElementById('stat-high').textContent = s.high_risk_count ?? 0;
  document.getElementById('stat-healthy').textContent = s.healthy_count ?? 0;
  // Update alert badge
  ALERT_COUNT = (s.critical_count || 0) + (s.high_risk_count || 0);
  const badge = document.getElementById('alert-badge');
  if (ALERT_COUNT > 0) { badge.textContent = ALERT_COUNT; badge.style.display = ''; }
  else { badge.style.display = 'none'; }
}

function renderFleetGrid(fleet) {
  const search = document.getElementById('fleet-search').value.toLowerCase();
  const filter = document.getElementById('fleet-filter').value;
  const grid = document.getElementById('miner-grid');

  const filtered = fleet.filter(m => {
    if (search && !m.miner_id.toLowerCase().includes(search)) return false;
    if (filter && filter !== 'all' && m.risk_band !== filter) return false;
    return true;
  });

  if (!filtered.length) {
    grid.innerHTML = '<div class="empty-state"><div class="empty-icon">📡</div><div>No miners match the current filter</div></div>';
    return;
  }

  grid.innerHTML = filtered.map(m => {
    const rc = riskClass(m.risk_band);
    const score = (m.risk_score || 0);
    const pct = (score * 100).toFixed(1);
    const mode = m.operating_mode || 'normal';
    return `
      <div class="miner-card ${rc}" onclick="openMinerModal('${m.miner_id}')">
        <div class="miner-id">⛏ ${m.miner_id}</div>
        <div class="miner-metrics">
          <div class="metric"><div class="metric-label">Hashrate</div><div class="metric-value">${fmt(m.asic_hashrate_ths, 1)} TH/s</div></div>
          <div class="metric"><div class="metric-label">Temp</div><div class="metric-value">${fmt(m.asic_temperature_c, 1)}°C</div></div>
          <div class="metric"><div class="metric-label">Power</div><div class="metric-value">${fmt(m.asic_power_w, 0)} W</div></div>
          <div class="metric"><div class="metric-label">Clock</div><div class="metric-value">${fmt(m.asic_clock_mhz, 0)} MHz</div></div>
        </div>
        <div class="risk-score-bar">
          <div class="risk-label-row">
            <span>Risk Score</span>
            <span class="risk-pct ${rc}">${pct}%</span>
          </div>
          <div class="risk-bar"><div class="risk-bar-fill ${rc}" style="width:${pct}%"></div></div>
        </div>
        <div class="miner-mode mode-${mode}">${mode.toUpperCase()}</div>
        <div class="text-muted text-sm mt-16">${timeAgo(m.last_seen)}</div>
      </div>`;
  }).join('');
}

document.getElementById('fleet-search').addEventListener('input', () => renderFleetGrid(FLEET_DATA));
document.getElementById('fleet-filter').addEventListener('change', () => renderFleetGrid(FLEET_DATA));

// ── Miner Detail Modal ─────────────────────────────────────────────────────
async function openMinerModal(minerId) {
  document.getElementById('modal-miner-id').textContent = minerId;
  document.getElementById('miner-modal').classList.add('open');
  // Destroy old charts
  Object.values(activeCharts).forEach(c => c.destroy());
  activeCharts = {};

  try {
    const [history, kpi, risk] = await Promise.all([
      api(`/api/miners/${minerId}?hours=168`),
      api(`/api/miners/${minerId}/kpi?hours=168`),
      api(`/api/miners/${minerId}/risk?hours=168`),
    ]);
    renderMinerCharts(minerId, history, kpi, risk);
  } catch(e) { console.error('Modal load failed', e); }
}

function renderMinerCharts(id, history, kpi, risk) {
  const labels = (history || []).map(r => new Date(r.timestamp).toLocaleTimeString());
  const lineOpts = (color) => ({
    type: 'line', data: {},
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#64748b', maxTicksLimit: 6 }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { ticks: { color: '#64748b' }, grid: { color: 'rgba(255,255,255,0.04)' } },
      },
    }
  });

  const makeChart = (canvasId, label, data, color) => {
    const ctx = document.getElementById(canvasId)?.getContext('2d');
    if (!ctx) return;
    const cfg = lineOpts(color);
    cfg.data = { labels, datasets: [{ label, data, borderColor: color, backgroundColor: color + '20', fill: true, tension: 0.3, pointRadius: 0 }] };
    activeCharts[canvasId] = new Chart(ctx, cfg);
  };

  makeChart('chart-temp', 'Temperature (°C)', (history||[]).map(r => r.asic_temperature_c), '#ef4444');
  makeChart('chart-hash', 'Hashrate (TH/s)', (history||[]).map(r => r.asic_hashrate_ths), '#3b82f6');
  makeChart('chart-power', 'Power (W)', (history||[]).map(r => r.asic_power_w), '#f59e0b');
  const riskLabels = (risk||[]).map(r => new Date(r.predicted_at).toLocaleTimeString());
  const ctx = document.getElementById('chart-risk')?.getContext('2d');
  if (ctx && risk?.length) {
    activeCharts['chart-risk'] = new Chart(ctx, {
      type: 'line',
      data: { labels: riskLabels, datasets: [{ label: 'Risk', data: risk.map(r => (r.risk_score*100).toFixed(1)), borderColor: '#f97316', backgroundColor: '#f9731620', fill: true, tension: 0.3, pointRadius: 0 }] },
      options: { responsive: true, maintainAspectRatio: false, animation: false, plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#64748b', maxTicksLimit: 6 }, grid: { color: 'rgba(255,255,255,0.04)' } }, y: { min: 0, max: 100, ticks: { color: '#64748b' }, grid: { color: 'rgba(255,255,255,0.04)' } } } }
    });
  }
}

document.getElementById('modal-close').addEventListener('click', () => {
  document.getElementById('miner-modal').classList.remove('open');
  Object.values(activeCharts).forEach(c => c.destroy());
  activeCharts = {};
});
document.getElementById('miner-modal').addEventListener('click', (e) => {
  if (e.target === e.currentTarget) document.getElementById('modal-close').click();
});

// ── Analytics Page ─────────────────────────────────────────────────────────
let ANALYTICS_HOURS = 720;

async function loadAnalytics(hours) {
  if (hours) ANALYTICS_HOURS = hours;
  const h = ANALYTICS_HOURS;
  // Update selector if present
  const sel = document.getElementById('analytics-range');
  if (sel) sel.value = h;
  try {
    const [corr, tradeoffs, anomalies] = await Promise.all([
      api(`/api/analytics/correlations?hours=${h}`),
      api(`/api/analytics/tradeoffs?hours=${h}`),
      api(`/api/analytics/anomalies?hours=${h}`),
    ]);
    renderCorrelationHeatmap(corr);
    renderTradeoffCharts(tradeoffs);
    renderAnomalies(anomalies, h);
  } catch(e) { console.error('Analytics fail', e); }
}

function renderCorrelationHeatmap(data) {
  if (!data?.columns?.length) return;
  const canvas = document.getElementById('corr-canvas');
  const ctx = canvas.getContext('2d');
  const cols = data.columns.map(c => c.replace('asic_', '').replace('_', ' '));
  const n = cols.length;
  const cell = Math.floor(Math.min(canvas.width, canvas.height) / n);
  canvas.width = cell * n; canvas.height = cell * n;

  data.matrix.forEach((row, i) => {
    row.forEach((val, j) => {
      const v = val ?? 0;
      const r = v > 0 ? Math.round(59 + v * 176) : 239;
      const g = v > 0 ? Math.round(130 + v * 50) : Math.round(68 + (1+v) * 100);
      const b = v > 0 ? Math.round(246 - v * 100) : Math.round(68 + (1+v) * 50);
      ctx.fillStyle = `rgba(${r},${g},${b},${Math.abs(v) * 0.8 + 0.1})`;
      ctx.fillRect(j * cell, i * cell, cell - 1, cell - 1);
      ctx.fillStyle = '#e2e8f0';
      ctx.font = `${Math.max(10, cell/4.5)}px Inter`;
      ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
      ctx.fillText(v.toFixed(2), j * cell + cell/2, i * cell + cell/2);
    });
  });
  // Labels
  ctx.fillStyle = '#94a3b8'; ctx.font = `${Math.max(9, cell/5)}px Inter`;
  cols.forEach((label, i) => {
    ctx.save(); ctx.translate(i * cell + cell/2, canvas.height + 16);
    ctx.fillText(label, 0, 0); ctx.restore();
    ctx.save(); ctx.translate(-4, i * cell + cell/2);
    ctx.rotate(-Math.PI/2); ctx.textAlign = 'center';
    ctx.fillText(label, 0, 0); ctx.restore();
  });
}

function renderTradeoffCharts(data) {
  if (!data?.length) return;
  const COLORS = { eco: '#10b981', normal: '#3b82f6', turbo: '#ef4444' };
  const datasets = (xKey, yKey) => {
    const byMode = {};
    data.forEach(r => { const m = r.operating_mode || 'normal'; if (!byMode[m]) byMode[m] = []; byMode[m].push({ x: r[xKey], y: r[yKey] }); });
    return Object.entries(byMode).map(([m, pts]) => ({ label: m, data: pts, backgroundColor: (COLORS[m] || '#94a3b8') + '99', pointRadius: 2, }));
  };
  const scatterOpts = (xl, yl) => ({
    type: 'scatter',
    options: { responsive: true, maintainAspectRatio: false, animation: false,
      plugins: { legend: { labels: { color: '#94a3b8', font: { size: 11 } } } },
      scales: {
        x: { title: { display: true, text: xl, color: '#64748b' }, ticks: { color: '#64748b' }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { title: { display: true, text: yl, color: '#64748b' }, ticks: { color: '#64748b' }, grid: { color: 'rgba(255,255,255,0.04)' } },
      }
    }
  });

  [['to-power-hash', 'asic_power_w', 'asic_hashrate_ths', 'Power (W)', 'Hashrate (TH/s)'],
   ['to-temp-hash', 'asic_temperature_c', 'asic_hashrate_ths', 'Temp (°C)', 'Hashrate (TH/s)'],
   ['to-clock-hash', 'asic_clock_mhz', 'asic_hashrate_ths', 'Clock (MHz)', 'Hashrate (TH/s)'],
   ['to-voltage-temp', 'asic_voltage_v', 'asic_temperature_c', 'Voltage (V)', 'Temp (°C)']
  ].forEach(([id, x, y, xl, yl]) => {
    const ctx = document.getElementById(id)?.getContext('2d');
    if (!ctx) return;
    if (activeCharts[id]) activeCharts[id].destroy();
    const cfg = scatterOpts(xl, yl);
    cfg.data = { datasets: datasets(x, y) };
    activeCharts[id] = new Chart(ctx, cfg);
  });
}

function renderAnomalies(anomalies, hours) {
  const tbody = document.getElementById('anomaly-tbody');
  if (!anomalies?.length) {
    const label = hours >= 720 ? '30 days' : hours >= 168 ? '7 days' : '24 hours';
    tbody.innerHTML = `<tr><td colspan="6" class="text-muted" style="text-align:center;padding:32px">No anomalies detected in the last ${label}</td></tr>`;
    return;
  }
  tbody.innerHTML = anomalies.slice(0,50).map(a => `
    <tr>
      <td>${a.miner_id}</td>
      <td class="text-muted">${new Date(a.timestamp).toLocaleString()}</td>
      <td>${a.field.replace('asic_','')}</td>
      <td>${fmt(a.value, 2)}</td>
      <td>${fmt(a.z_score, 2)}σ</td>
      <td><span class="badge badge-${a.severity}">${a.severity}</span></td>
    </tr>`).join('');
}

// ── Alerts Page ────────────────────────────────────────────────────────────
async function loadAlerts() {
  try {
    const alerts = await api('/api/alerts');
    renderAlertsTable(alerts);
  } catch(e) { console.error('Alerts load failed', e); }
}

function renderAlertsTable(alerts) {
  const tbody = document.getElementById('alerts-tbody');
  if (!alerts?.length) {
    tbody.innerHTML = '<tr><td colspan="8" class="text-muted" style="text-align:center;padding:48px">✅ No active alerts</td></tr>';
    return;
  }
  tbody.innerHTML = alerts.map(a => `
    <tr>
      <td>${new Date(a.created_at).toLocaleString()}</td>
      <td><b>${a.miner_id}</b></td>
      <td><span class="badge badge-${a.severity}">${a.severity}</span></td>
      <td>${a.risk_score ? (a.risk_score*100).toFixed(1)+'%' : '—'}</td>
      <td class="text-muted">${a.trigger_reason || '—'}</td>
      <td><span class="badge ${a.recommended_action !== 'CONTINUE' ? 'badge-warn' : ''}">${a.recommended_action || 'CONTINUE'}</span> ${a.automation_triggered ? '🤖':''}</td>
      <td>${a.email_sent?'✉️':'—'} ${a.telegram_sent?'📨':'—'}</td>
      <td><button class="btn btn-sm btn-ghost" onclick="resolveAlert(${a.id}, this)">Resolve</button></td>
    </tr>`).join('');
}

async function resolveAlert(id, btn) {
  btn.disabled = true; btn.textContent = '...';
  await api(`/api/alerts/${id}/resolve`, { method: 'POST' });
  loadAlerts();
}

// ── Ingestion Page ─────────────────────────────────────────────────────────
const dropzone = document.getElementById('dropzone');
const fileInput = document.getElementById('file-input');

dropzone.addEventListener('click', () => fileInput.click());
dropzone.addEventListener('dragover', e => { e.preventDefault(); dropzone.classList.add('drag-over'); });
dropzone.addEventListener('dragleave', () => dropzone.classList.remove('drag-over'));
dropzone.addEventListener('drop', e => { e.preventDefault(); dropzone.classList.remove('drag-over'); handleUpload(e.dataTransfer.files[0]); });
fileInput.addEventListener('change', e => handleUpload(e.target.files[0]));

async function handleUpload(file) {
  if (!file) return;
  const result = document.getElementById('upload-result');
  const error = document.getElementById('upload-error');
  const progress = document.getElementById('upload-progress');
  result.style.display = 'none'; error.style.display = 'none';
  progress.style.display = 'block';
  document.querySelector('.progress-bar-fill').style.width = '30%';

  try {
    const form = new FormData();
    form.append('file', file);
    document.querySelector('.progress-bar-fill').style.width = '70%';
    const res = await fetch('/api/ingest/csv', {
      method: 'POST',
      headers: { Authorization: `Bearer ${TOKEN}` },
      body: form,
    });
    document.querySelector('.progress-bar-fill').style.width = '100%';
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
    result.innerHTML = `✅ <b>${data.rows_inserted}</b> rows inserted | <b>${data.miners_found.length}</b> miners | ${data.errors.length ? '⚠️ '+data.errors.join(', ') : 'No errors'}`;
    result.style.display = 'block';
    setTimeout(() => loadFleet(), 3000);
  } catch(e) {
    error.textContent = '❌ Upload failed: ' + e.message;
    error.style.display = 'block';
  } finally {
    setTimeout(() => { progress.style.display = 'none'; document.querySelector('.progress-bar-fill').style.width = '0%'; }, 1500);
  }
}

async function loadSources() {
  const [sources, allowlistInfo] = await Promise.all([
    api('/api/ingest/sources'),
    api('/api/ingest/sources/allowlist'),
  ]);
  SOURCE_ALLOWLIST_INFO = allowlistInfo || null;
  renderSourceAllowlistInfo(SOURCE_ALLOWLIST_INFO);
  renderSourcesTable(sources);
}

function renderSourcesTable(sources) {
  const tbody = document.getElementById('sources-tbody');
  if (!sources?.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="text-muted" style="text-align:center;padding:32px">No API sources configured</td></tr>';
    return;
  }
  tbody.innerHTML = sources.map(s => `
    <tr>
      <td><b>${s.name}</b></td>
      <td class="text-muted text-sm" style="max-width:200px;overflow:hidden;text-overflow:ellipsis">${s.url_template}</td>
      <td>${s.polling_interval_minutes}m</td>
      <td>${s.last_fetched_at ? timeAgo(s.last_fetched_at) : 'Never'}</td>
      <td><span class="badge ${s.enabled ? 'badge-ok' : ''}" style="${!s.enabled?'background:rgba(100,116,139,0.15);color:#64748b':''}">${s.enabled?'Active':'Paused'}</span></td>
      <td>
        <button class="btn btn-sm btn-ghost" onclick="toggleSource(${s.id})">${s.enabled?'Pause':'Resume'}</button>
        <button class="btn btn-sm btn-danger" onclick="deleteSource(${s.id})" style="margin-left:6px">Delete</button>
      </td>
    </tr>`).join('');
}

async function toggleSource(id) { await api(`/api/ingest/sources/${id}/toggle`, { method: 'POST' }); loadSources(); }
async function deleteSource(id) { if (!confirm('Delete this source?')) return; await api(`/api/ingest/sources/${id}`, { method: 'DELETE' }); loadSources(); }

function renderSourceAllowlistInfo(info) {
  const el = document.getElementById('source-allowlist-info');
  if (!el) return;
  if (!info) {
    el.style.display = 'none';
    return;
  }
  const allowlist = info.allowlist || [];
  const configured = !!info.allowlist_configured;
  if (configured) {
    el.className = 'source-validation source-validation-ok';
    el.innerHTML = `<div class="source-validation-title">Egress allowlist enabled</div><div>${allowlist.join(', ')}</div>`;
  } else {
    el.className = 'source-validation source-validation-warn';
    el.innerHTML = '<div class="source-validation-title">Egress allowlist is not configured</div><div>Set <code>API_SOURCE_ALLOWLIST</code> to enable external source ingestion.</div>';
  }
  el.style.display = 'block';
}

function clearSourceValidationMessage() {
  const el = document.getElementById('source-validate-status');
  if (!el) return;
  el.style.display = 'none';
  el.textContent = '';
  el.className = 'source-validation source-validation-neutral';
}

function renderSourceValidationReport(report) {
  const el = document.getElementById('source-validate-status');
  if (!el || !report) return;

  const host = report.hostname || 'unknown-host';
  const resolved = (report.resolved_ips || []).length ? report.resolved_ips.join(', ') : 'none';
  const blocked = (report.blocked_ips || []).length ? report.blocked_ips.join(', ') : 'none';
  const errors = (report.errors || []).map(e => `• ${e}`).join('<br/>');

  if (report.valid) {
    el.className = 'source-validation source-validation-ok';
    el.innerHTML = `<div class="source-validation-title">URL validation passed</div><div>Host: <b>${host}</b> | Resolved IPs: ${resolved}</div>`;
  } else {
    el.className = 'source-validation source-validation-bad';
    el.innerHTML = `<div class="source-validation-title">URL validation failed</div><div>${errors || 'Unknown validation error'}</div><div style="margin-top:4px">Host: <b>${host}</b> | Resolved IPs: ${resolved} | Blocked: ${blocked}</div>`;
  }
  el.style.display = 'block';
}

async function validateSourceUrlCandidate(url) {
  const report = await api('/api/ingest/sources/validate-url', {
    method: 'POST',
    body: JSON.stringify({ url_template: url }),
  });
  SOURCE_URL_VALIDATION = {
    url,
    report,
    validatedAt: Date.now(),
  };
  renderSourceValidationReport(report);
  return report;
}

document.getElementById('validate-source-btn')?.addEventListener('click', async () => {
  const form = document.getElementById('add-source-form');
  if (!form) return;
  const url = (form.source_url.value || '').trim();
  if (!url) {
    clearSourceValidationMessage();
    const el = document.getElementById('source-validate-status');
    el.className = 'source-validation source-validation-bad';
    el.innerHTML = '<div class="source-validation-title">URL validation failed</div><div>Please enter a source URL first.</div>';
    el.style.display = 'block';
    return;
  }
  const btn = document.getElementById('validate-source-btn');
  btn.disabled = true;
  const prev = btn.textContent;
  btn.textContent = 'Validating...';
  try {
    await validateSourceUrlCandidate(url);
  } catch (e) {
    SOURCE_URL_VALIDATION = null;
    const el = document.getElementById('source-validate-status');
    el.className = 'source-validation source-validation-bad';
    el.innerHTML = `<div class="source-validation-title">Validation request failed</div><div>${e.message}</div>`;
    el.style.display = 'block';
  } finally {
    btn.disabled = false;
    btn.textContent = prev;
  }
});

document.querySelector('#add-source-form [name="source_url"]')?.addEventListener('input', () => {
  SOURCE_URL_VALIDATION = null;
  clearSourceValidationMessage();
});

document.getElementById('add-source-form')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = e.target;
  const url = (f.source_url.value || '').trim();
  const submitBtn = f.querySelector('button[type="submit"]');
  submitBtn.disabled = true;

  try {
    let report = null;
    const isFreshValidation =
      SOURCE_URL_VALIDATION &&
      SOURCE_URL_VALIDATION.url === url &&
      SOURCE_URL_VALIDATION.report &&
      (Date.now() - SOURCE_URL_VALIDATION.validatedAt) <= 5 * 60 * 1000;

    if (isFreshValidation) {
      report = SOURCE_URL_VALIDATION.report;
    } else {
      report = await validateSourceUrlCandidate(url);
    }

    if (!report?.valid) {
      throw new Error('Source URL did not pass validation. Fix issues and validate again.');
    }

    await api('/api/ingest/sources', { method: 'POST', body: JSON.stringify({
      name: f.source_name.value, url_template: url,
      polling_interval_minutes: parseInt(f.source_interval.value || 10),
    }) });
    f.reset();
    SOURCE_URL_VALIDATION = null;
    clearSourceValidationMessage();
    await loadSources();
  } catch (err) {
    const el = document.getElementById('source-validate-status');
    el.className = 'source-validation source-validation-bad';
    el.innerHTML = `<div class="source-validation-title">Save failed</div><div>${err.message}</div>`;
    el.style.display = 'block';
  } finally {
    submitBtn.disabled = false;
  }
});

// ── Settings Page ──────────────────────────────────────────────────────────
async function loadSettings() {
  const data = await api('/api/settings');
  if (!data) return;
  const s = data.settings || {};
  const fields = [
    'smtp_host','smtp_port','smtp_user','smtp_password','alert_from_email','alert_to_emails',
    'telegram_bot_token','telegram_chat_id','risk_threshold','alert_cooldown_hours',
    'policy_optimizer_enabled','automation_require_policy_backtest','policy_min_uplift_usd_per_miner',
    'hashprice_usd_per_ph_day','opex_usd_per_mwh','capex_usd_per_mwh',
    'energy_price_usd_per_kwh','energy_price_schedule_json','curtailment_windows_json',
    'curtailment_penalty_multiplier','policy_reward_per_th_hour_usd','policy_failure_cost_usd',
    'policy_horizon_hours','risk_probability_horizon_hours','policy_timezone'
  ];
  fields.forEach(k => {
    const el = document.getElementById(`setting-${k.replace(/_/g,'-')}`);
    if (el) el.value = s[k] || '';
  });
}

document.getElementById('settings-form')?.addEventListener('submit', async (e) => {
  e.preventDefault();
  const fields = [
    'smtp_host','smtp_port','smtp_user','smtp_password','alert_from_email','alert_to_emails',
    'telegram_bot_token','telegram_chat_id','risk_threshold','alert_cooldown_hours',
    'policy_optimizer_enabled','automation_require_policy_backtest','policy_min_uplift_usd_per_miner',
    'hashprice_usd_per_ph_day','opex_usd_per_mwh','capex_usd_per_mwh',
    'energy_price_usd_per_kwh','energy_price_schedule_json','curtailment_windows_json',
    'curtailment_penalty_multiplier','policy_reward_per_th_hour_usd','policy_failure_cost_usd',
    'policy_horizon_hours','risk_probability_horizon_hours','policy_timezone'
  ];
  const settings = {};
  fields.forEach(k => {
    const el = document.getElementById(`setting-${k.replace(/_/g,'-')}`);
    if (el) settings[k] = el.value;
  });
  await api('/api/settings', { method: 'PUT', body: JSON.stringify({ settings }) });
  alert('Settings saved!');
});

// ── Init ───────────────────────────────────────────────────────────────────
if (TOKEN) {
  showApp();
} else {
  showLogin();
}
