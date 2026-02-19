import { getCSRFToken } from "../../baseproject/js/csrf.js";

function qs(id) { return document.getElementById(id); }

function setModeUI(mode) {
  const dayWrap = qs("sm-day-wrap");
  const intWrap = qs("sm-interval-wrap");
  if (mode === "day") {
    dayWrap.classList.remove("d-none");
    intWrap.classList.add("d-none");
  } else {
    dayWrap.classList.add("d-none");
    intWrap.classList.remove("d-none");
  }
}

function renderRovCheckboxes(rovNames) {
  const container = qs("sm-rov-list");
  container.innerHTML = "";
  rovNames.forEach((name) => {
    const col = document.createElement("div");
    col.className = "col-6 col-md-4";
    col.innerHTML = `
      <div class="form-check">
        <input class="form-check-input sm-rov-cb" type="checkbox" value="${name}" id="rov_${CSS.escape(name)}">
        <label class="form-check-label" for="rov_${CSS.escape(name)}">${name}</label>
      </div>`;
    container.appendChild(col);
  });
}

function getSelectedRovs() {
  return Array.from(document.querySelectorAll(".sm-rov-cb:checked")).map(cb => cb.value);
}

function setAllRovs(checked) {
  document.querySelectorAll(".sm-rov-cb").forEach(cb => { cb.checked = checked; });
}

async function downloadBlobAsFile(blob, filename) {
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename || "dsr_export.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.URL.revokeObjectURL(url);
}
function showToast(html) {
  const toastEl = qs("sm-export-toast");
  const bodyEl  = qs("sm-export-toast-body");
  if (!toastEl || !bodyEl || !window.bootstrap) return;

  bodyEl.innerHTML = html;
  const toast = bootstrap.Toast.getOrCreateInstance(toastEl, { delay: 5000 });
  toast.show();
}

export function initDsrExportSmModal({
  rovNames = [],   // pass list from Django context OR load via another endpoint
} = {}) {

  const modeSel = qs("sm-export-mode");
  const selectAll = qs("sm-rov-select-all");
  const btn = qs("sm2file-export-btn");
  const err = qs("sm-export-error");
  const help = qs("sm-export-help");

  if (!modeSel || !btn) return;

  // initial UI
  setModeUI(modeSel.value);
  renderRovCheckboxes(rovNames);
  help.textContent = `${rovNames.length} ROV(s) available.`;

  modeSel.addEventListener("change", () => setModeUI(modeSel.value));

  selectAll.addEventListener("change", () => setAllRovs(selectAll.checked));

  // if user manually changes, update select-all
  document.addEventListener("change", (e) => {
    if (!e.target.classList.contains("sm-rov-cb")) return;
    const all = Array.from(document.querySelectorAll(".sm-rov-cb"));
    const checked = all.filter(x => x.checked).length;
    selectAll.checked = (all.length > 0 && checked === all.length);
    selectAll.indeterminate = (checked > 0 && checked < all.length);
  });

  btn.addEventListener("click", async () => {
    err.classList.add("d-none");
    err.textContent = "";

    const exportUrl = btn.dataset.exportUrl;
    const mode = modeSel.value;

    const status = document.querySelector("input[name='sm-status']:checked")?.value || "deployed";
    const depthMode = qs("sm-depth-mode").value; // neg|abs
    const format = qs("sm-format").value;        // z_nodes|mass_nodes
    const filename = (qs("sm-filename").value || "").trim();
    const rovs = getSelectedRovs();

    if (rovs.length === 0) {
      err.textContent = "Select at least one ROV.";
      err.classList.remove("d-none");
      return;
    }

    let payload = { mode, status, depth_mode: depthMode, format, rovs, filename };

    if (mode === "day") {
      const day = qs("sm-day").value;
      if (!day) {
        err.textContent = "Select a day.";
        err.classList.remove("d-none");
        return;
      }
      payload.day = day; // YYYY-MM-DD
    } else {
      const from = qs("sm-from").value;
      const to = qs("sm-to").value;
      if (!from || !to) {
        err.textContent = "Select both FROM AND TO.";
        err.classList.remove("d-none");
        return;
      }
      payload.from = from; // datetime-local
      payload.to = to;
    }

    btn.disabled = true;
    btn.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Exporting...`;

    try {
      const res = await fetch(exportUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        let msg = "";
              try {
                const j = await res.json();
                msg = j.error || JSON.stringify(j);
              } catch {
                msg = await res.text();
              }
              throw new Error(msg || `HTTP ${res.status}`);
      }

      const data = await res.json();
      if (!res.ok || !data.ok) {
        throw new Error(data?.error || `HTTP ${res.status}`);
      }

      // show toast with filename + exported nodes
      showToast(`
        <div><b>Export done</b></div>
        <div>File: <code>${data.filename || ""}</code></div>
        <div>Exported nodes: <b>${data.rows ?? 0}</b></div>
      `);

      // close modal
      const modalEl = qs("dsrExportSmModal");
      const modal = bootstrap.Modal.getInstance(modalEl);
      modal?.hide();

    } catch (e) {
      err.textContent = `Export failed: ${e.message}`;
      err.classList.remove("d-none");
    } finally {
      btn.disabled = false;
      btn.innerHTML = `<i class="fas fa-download me-2"></i>Export CSV`;
    }
  });
}
