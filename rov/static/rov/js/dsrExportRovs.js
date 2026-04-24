import { getCSRFToken } from "../../baseproject/js/csrf.js";

function byId(id) { return document.getElementById(id); }

function showError(msg) {
  const el = byId("sm-export-error");
  if (!el) return;
  el.textContent = msg || "";
  el.classList.toggle("d-none", !msg);
}

function setHelp(text) {
  const el = byId("sm-export-help");
  if (!el) return;
  el.textContent = text || "";
}

function getSelectedMode() {
  return byId("sm-export-mode")?.value || "day"; // day|interval
}

function getStatusMode() {
  // deployed|recovered (your radio buttons)
  return document.querySelector("input[name='sm-status']:checked")?.value || "deployed";
}

function getTimePayload() {
  const mode = getSelectedMode();
  const payload = { mode };

  if (mode === "day") {
    payload.day = byId("sm-day")?.value || "";
  } else {
    payload.from = byId("sm-from")?.value || "";
    payload.to   = byId("sm-to")?.value || "";
  }
  return payload;
}

function isTimePayloadComplete(payload) {
  if (payload.mode === "day") return !!payload.day;
  return !!payload.from && !!payload.to;
}

function getCheckedRovsSet() {
  const set = new Set();
  document.querySelectorAll(".sm-rov-cb:checked").forEach(cb => set.add(cb.value));
  return set;
}

function renderRovList(rovs, keepCheckedSet = new Set()) {
  const wrap = byId("sm-rov-list");
  if (!wrap) return;

  wrap.innerHTML = "";

  if (!rovs || rovs.length === 0) {
    wrap.innerHTML = `<div class="text-muted small p-2">No ROVs found for selected filters.</div>`;
    return;
  }

  for (const name of rovs) {
    const col = document.createElement("div");
    col.className = "col-6 col-md-4";
    const checked = keepCheckedSet.has(name) ? "checked" : "";
    col.innerHTML = `
      <div class="form-check">
        <input class="form-check-input sm-rov-cb" type="checkbox" value="${name}" id="rov_${CSS.escape(name)}" ${checked}>
        <label class="form-check-label" for="rov_${CSS.escape(name)}">${name}</label>
      </div>
    `;
    wrap.appendChild(col);
  }
}

function updateSelectAllState() {
  const selectAll = byId("sm-rov-select-all");
  if (!selectAll) return;

  const all = Array.from(document.querySelectorAll(".sm-rov-cb"));
  const checked = all.filter(x => x.checked).length;

  if (all.length === 0) {
    selectAll.checked = false;
    selectAll.indeterminate = false;
    return;
  }

  selectAll.checked = (checked === all.length);
  selectAll.indeterminate = (checked > 0 && checked < all.length);
}

function setAllRovs(checked) {
  document.querySelectorAll(".sm-rov-cb").forEach(cb => { cb.checked = checked; });
  updateSelectAllState();
}

/**
 * Calls Django endpoint and returns list:
 *  - deployed: only from ROV/TimeStamp
 *  - recovered: only from ROV1/TimeStamp1
 *  - both: union (if you enable it later)
 *
 * IMPORTANT: this requires your Django view to accept `status` too.
 * If your current view doesn't accept status yet, see note below.
 */
async function fetchRovs({ url, payload }) {
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-CSRFToken": getCSRFToken(),
    },
    body: JSON.stringify(payload),
  });

  const data = await res.json();
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);

  return data.rovs || [];
}

export function initDsrRovListAutoReload() {
  // Controls (match your modal IDs)
  const modeEl = byId("sm-export-mode");
  const dayEl  = byId("sm-day");
  const fromEl = byId("sm-from");
  const toEl   = byId("sm-to");

  const selectAllEl = byId("sm-rov-select-all");
  const btn = byId("sm-export-btn"); // we store data-rovs-url here

  if (!btn) return;

  const rovsUrl = btn.dataset.rovsUrl;
  if (!rovsUrl) {
    console.warn("Missing data-rovs-url on #sm-export-btn");
    return;
  }

  let lastRequestId = 0;

  async function reloadRovs() {
    showError("");
    const keepChecked = getCheckedRovsSet();

    const timePayload = getTimePayload();
    if (!isTimePayloadComplete(timePayload)) {
      renderRovList([], keepChecked);
      updateSelectAllState();
      setHelp("Select a day or set FROM/TO to load ROVs.");
      return;
    }

    // Add what-to-export filter
    // deployed|recovered (if you later add "both", it will work too)
    const status = getStatusMode();
    const payload = { ...timePayload, status };

    const reqId = ++lastRequestId;
    setHelp("Loading ROV list...");

    try {
      const rovs = await fetchRovs({ url: rovsUrl, payload });

      // If a newer request happened, ignore this response
      if (reqId !== lastRequestId) return;

      renderRovList(rovs, keepChecked);
      updateSelectAllState();
      setHelp(`${rovs.length} ROV(s) found for ${status} in selected timeframe.`);
    } catch (e) {
      if (reqId !== lastRequestId) return;
      renderRovList([], keepChecked);
      updateSelectAllState();
      showError(`Failed to load ROV list: ${e.message}`);
      setHelp("");
    }
  }

  // events that trigger reload
  [modeEl, dayEl, fromEl, toEl].forEach((el) => {
    if (!el) return;
    el.addEventListener("change", reloadRovs);
  });

  // radio buttons deployed/recovered
  document.querySelectorAll("input[name='sm-status']").forEach((el) => {
    el.addEventListener("change", reloadRovs);
  });

  // select all
  if (selectAllEl) {
    selectAllEl.addEventListener("change", () => setAllRovs(selectAllEl.checked));
  }

  // checkbox changes -> update select all state
  document.addEventListener("change", (e) => {
    if (e.target && e.target.classList && e.target.classList.contains("sm-rov-cb")) {
      updateSelectAllState();
    }
  });

  // optional: when modal opens, try load immediately
  const modalEl = byId("dsrExportSmModal");
  if (modalEl) {
    modalEl.addEventListener("shown.bs.modal", reloadRovs);
  }
}
