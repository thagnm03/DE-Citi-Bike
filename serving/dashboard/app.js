"use strict";

const state = {
  alerts: [],
  stations: [],
  selectedAlertId: null,
  actionFilter: "ALL",
  query: "",
  activeTab: "queue",
};

const elements = {
  systemState: document.querySelector("#system-state"),
  lastUpdated: document.querySelector("#last-updated"),
  refreshButton: document.querySelector("#refresh-button"),
  searchInput: document.querySelector("#search-input"),
  alertRows: document.querySelector("#alert-rows"),
  stationRows: document.querySelector("#station-rows"),
  alertsEmpty: document.querySelector("#alerts-empty"),
  stationsEmpty: document.querySelector("#stations-empty"),
  queueTab: document.querySelector("#queue-tab"),
  stationsTab: document.querySelector("#stations-tab"),
  queuePanel: document.querySelector("#queue-panel"),
  stationsPanel: document.querySelector("#stations-panel"),
  dialog: document.querySelector("#alert-dialog"),
  dialogContent: document.querySelector("#dialog-content"),
  dialogClose: document.querySelector("#dialog-close"),
  toast: document.querySelector("#toast"),
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function titleCase(value) {
  return String(value ?? "").toLowerCase().replaceAll("_", " ").replace(/\b\w/g, letter => letter.toUpperCase());
}

function formatTime(value) {
  if (!value) return "Unknown";
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value));
}

function formatDuration(seconds) {
  const value = Number(seconds || 0);
  if (value < 60) return `${value}s`;
  const minutes = Math.floor(value / 60);
  if (minutes < 60) return `${minutes}m`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

function showToast(message) {
  elements.toast.textContent = message;
  elements.toast.classList.remove("hidden");
  window.setTimeout(() => elements.toast.classList.add("hidden"), 3200);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.detail || `Request failed (${response.status})`);
  return payload;
}

function setConnection(online, label) {
  elements.systemState.classList.toggle("online", online);
  elements.systemState.classList.toggle("offline", !online);
  elements.systemState.querySelector("span:last-child").textContent = label;
}

function updateMetrics() {
  const critical = state.alerts.filter(item => item.severity === "CRITICAL").length;
  const acknowledged = state.alerts.filter(item => item.lifecycle_status === "ACKNOWLEDGED").length;
  document.querySelector("#metric-active").textContent = state.alerts.length;
  document.querySelector("#metric-critical").textContent = critical;
  document.querySelector("#metric-acknowledged").textContent = acknowledged;
  document.querySelector("#metric-stations").textContent = state.stations.length;
}

function severityBadge(alert) {
  const css = alert.severity === "CRITICAL" ? "badge-critical" : "badge-warning";
  return `<span class="badge ${css}">${escapeHtml(titleCase(alert.alert_type))}</span>`;
}

function lifecycleBadge(status) {
  const css = status === "ACKNOWLEDGED" ? "badge-acknowledged" : "badge-open";
  return `<span class="badge ${css}">${escapeHtml(titleCase(status))}</span>`;
}

function filteredAlerts() {
  const query = state.query.trim().toLowerCase();
  return state.alerts.filter(alert => {
    const actionMatch = state.actionFilter === "ALL" || alert.recommended_action === state.actionFilter;
    const haystack = [alert.station.name, alert.station.station_id, alert.alert_type, alert.recommended_action]
      .join(" ").toLowerCase();
    return actionMatch && (!query || haystack.includes(query));
  });
}

function renderAlerts() {
  const alerts = filteredAlerts();
  elements.alertRows.innerHTML = alerts.map((alert, index) => `
    <tr>
      <td><div class="rank"><span class="score">${escapeHtml(Math.round(alert.priority.score))}</span><small>#${index + 1}</small></div></td>
      <td><span class="station-name">${escapeHtml(alert.station.name || "Unnamed station")}</span><span class="station-id">${escapeHtml(alert.station.station_id)}</span></td>
      <td>${severityBadge(alert)}</td>
      <td><span class="action">${escapeHtml(titleCase(alert.recommended_action))}</span></td>
      <td>${escapeHtml(formatDuration(alert.evidence.risk_duration_seconds))}</td>
      <td>${lifecycleBadge(alert.lifecycle_status)}</td>
      <td><button class="row-button" type="button" data-alert-id="${escapeHtml(alert.alert_id)}" aria-label="Open ${escapeHtml(alert.station.name || alert.station.station_id)} alert">→</button></td>
    </tr>`).join("");
  elements.alertsEmpty.classList.toggle("hidden", alerts.length > 0);
}

function filteredStations() {
  const query = state.query.trim().toLowerCase();
  return state.stations.filter(item => !query || [item.station.name, item.station_id, item.station.short_name].join(" ").toLowerCase().includes(query));
}

function renderStations() {
  const stations = filteredStations();
  elements.stationRows.innerHTML = stations.map(item => {
    const bikes = Number(item.availability.bikes_available || 0);
    const docks = Number(item.availability.docks_available || 0);
    const total = Math.max(bikes + docks, 1);
    const serviceOk = item.service.is_installed && item.service.is_renting && item.service.is_returning;
    return `<tr>
      <td><span class="station-name">${escapeHtml(item.station.name || "Unnamed station")}</span><span class="station-id">${escapeHtml(item.station_id)}</span></td>
      <td><div class="inventory"><div class="inventory-track"><span class="inventory-bikes" style="width:${(bikes / total) * 100}%"></span><span class="inventory-docks" style="width:${(docks / total) * 100}%"></span></div><div class="inventory-label"><span>${bikes} bikes</span><span>${docks} docks</span></div></div></td>
      <td><strong>${bikes}</strong></td><td><strong>${docks}</strong></td>
      <td><span class="badge ${serviceOk ? "badge-acknowledged" : "badge-critical"}">${serviceOk ? "Operational" : "Unavailable"}</span></td>
      <td>${item.source_age_seconds == null ? "Unknown" : `${escapeHtml(item.source_age_seconds)}s`}</td>
    </tr>`;
  }).join("");
  elements.stationsEmpty.classList.toggle("hidden", stations.length > 0);
}

function render() {
  updateMetrics();
  renderAlerts();
  renderStations();
}

function componentRows(components) {
  return Object.entries(components).map(([name, value]) => `
    <div class="component"><span>${escapeHtml(titleCase(name))}</span><div class="component-track"><div class="component-fill" style="width:${Math.max(0, Math.min(100, Number(value)))}%"></div></div><strong>${escapeHtml(Math.round(value))}</strong></div>
  `).join("");
}

async function openAlert(alertId) {
  state.selectedAlertId = alertId;
  elements.dialogContent.innerHTML = "<p>Loading alert evidence…</p>";
  if (!elements.dialog.open) elements.dialog.showModal();
  try {
    const [alert, history] = await Promise.all([
      api(`/api/v1/alerts/${encodeURIComponent(alertId)}`),
      api(`/api/v1/alerts/${encodeURIComponent(alertId)}/history`),
    ]);
    const canAcknowledge = alert.lifecycle_status === "OPEN";
    elements.dialogContent.innerHTML = `
      <section class="detail-lead">
        <div>${severityBadge(alert)}<h3>${escapeHtml(alert.station.name || "Unnamed station")}</h3><span class="station-id">${escapeHtml(alert.station.station_id)}</span></div>
        <div class="detail-score"><strong>${escapeHtml(Math.round(alert.priority.score))}</strong><span>priority score</span></div>
      </section>
      <div class="detail-grid">
        <div class="detail-card"><p>Recommended action</p><strong>${escapeHtml(titleCase(alert.recommended_action))}</strong></div>
        <div class="detail-card"><p>Lifecycle</p><strong>${escapeHtml(titleCase(alert.lifecycle_status))}</strong></div>
        <div class="detail-card"><p>Available inventory</p><strong>${escapeHtml(alert.evidence.bikes_available)} bikes · ${escapeHtml(alert.evidence.docks_available)} docks</strong></div>
        <div class="detail-card"><p>Risk duration</p><strong>${escapeHtml(formatDuration(alert.evidence.risk_duration_seconds))}</strong></div>
      </div>
      <h4 class="section-title">Why this is prioritized</h4>
      <div class="reason-list">${alert.evidence.reason_codes.map(code => `<span class="reason">${escapeHtml(code)}</span>`).join("")}</div>
      <div>${componentRows(alert.priority.components)}</div>
      <h4 class="section-title">Lifecycle history</h4>
      <ol class="timeline">${history.items.map(event => `<li><strong>${escapeHtml(titleCase(event.event_type))}</strong><span>${escapeHtml(formatTime(event.time.status_changed_at_utc))} · score ${escapeHtml(event.priority.score)}</span></li>`).join("")}</ol>
      ${canAcknowledge ? `<form class="ack-form" id="ack-form"><label>Operator name<input name="requested_by" required maxlength="128" autocomplete="name" placeholder="e.g. control-room-a"></label><button class="button button-primary" type="submit">Acknowledge task</button></form>` : `<div class="detail-card"><p>Operator state</p><strong>${alert.lifecycle_status === "ACKNOWLEDGED" ? `Acknowledged by ${escapeHtml(alert.acknowledgement?.acknowledged_by || "operator")}` : "Episode resolved"}</strong></div>`}
    `;
    document.querySelector("#ack-form")?.addEventListener("submit", acknowledgeSelected);
  } catch (error) {
    elements.dialogContent.innerHTML = `<div class="error-banner"><strong>Could not load alert.</strong><br>${escapeHtml(error.message)}</div>`;
  }
}

async function acknowledgeSelected(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const button = form.querySelector("button");
  button.disabled = true;
  try {
    await api(`/api/v1/alerts/${encodeURIComponent(state.selectedAlertId)}/acknowledge`, {
      method: "POST",
      body: JSON.stringify({
        idempotency_key: `dashboard-${crypto.randomUUID()}`,
        requested_by: new FormData(form).get("requested_by"),
        acknowledged_at_utc: new Date().toISOString(),
      }),
    });
    showToast("Task acknowledged. The queue has been updated.");
    await refreshData();
    await openAlert(state.selectedAlertId);
  } catch (error) {
    showToast(error.message);
    button.disabled = false;
  }
}

async function refreshData() {
  elements.refreshButton.disabled = true;
  try {
    const [ready, alerts, stations] = await Promise.all([
      api("/health/ready"), api("/api/v1/alerts?active_only=true&limit=500"), api("/api/v1/stations?limit=500"),
    ]);
    state.alerts = alerts.items;
    state.stations = stations.items;
    setConnection(true, ready.database === "healthy" ? "Systems healthy" : "API online");
    elements.lastUpdated.textContent = `Updated ${new Intl.DateTimeFormat(undefined, { timeStyle: "medium" }).format(new Date())}`;
    render();
  } catch (error) {
    setConnection(false, "Data unavailable");
    showToast(`Refresh failed: ${error.message}`);
  } finally {
    elements.refreshButton.disabled = false;
  }
}

function switchTab(tab) {
  state.activeTab = tab;
  const queueActive = tab === "queue";
  elements.queueTab.classList.toggle("active", queueActive);
  elements.queueTab.setAttribute("aria-selected", String(queueActive));
  elements.stationsTab.classList.toggle("active", !queueActive);
  elements.stationsTab.setAttribute("aria-selected", String(!queueActive));
  elements.queuePanel.classList.toggle("hidden", !queueActive);
  elements.stationsPanel.classList.toggle("hidden", queueActive);
}

elements.refreshButton.addEventListener("click", refreshData);
elements.searchInput.addEventListener("input", event => { state.query = event.target.value; render(); });
elements.queueTab.addEventListener("click", () => switchTab("queue"));
elements.stationsTab.addEventListener("click", () => switchTab("stations"));
elements.dialogClose.addEventListener("click", () => elements.dialog.close());
elements.dialog.addEventListener("click", event => { if (event.target === elements.dialog) elements.dialog.close(); });
elements.alertRows.addEventListener("click", event => {
  const button = event.target.closest("[data-alert-id]");
  if (button) openAlert(button.dataset.alertId);
});
document.querySelectorAll(".filter-chip").forEach(button => button.addEventListener("click", () => {
  state.actionFilter = button.dataset.action;
  document.querySelectorAll(".filter-chip").forEach(item => item.classList.toggle("active", item === button));
  renderAlerts();
}));

refreshData();
window.setInterval(refreshData, 30_000);
