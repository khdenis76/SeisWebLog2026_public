import { getCSRFToken } from "../../baseproject/js/csrf.js";

function qs(id) {
  return document.getElementById(id);
}

function getOptionalValue(id) {
  return (qs(id)?.value || "").trim();
}

function debounce(fn, delay = 500) {
  let timer = null;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

function setModeUI(mode) {
  const dayWrap = qs("sm-day-wrap");
  const intWrap = qs("sm-interval-wrap");

  if (!dayWrap || !intWrap) return;

  if (mode === "day") {
    dayWrap.classList.remove("d-none");
    intWrap.classList.add("d-none");
  } else {
    dayWrap.classList.add("d-none");
    intWrap.classList.remove("d-none");
  }
}

function safeId(value) {
  return String(value)
    .replaceAll("\\", "_")
    .replaceAll("/", "_")
    .replaceAll(" ", "_")
    .replaceAll(".", "_")
    .replaceAll("#", "_")
    .replaceAll(":", "_");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderRovCheckboxes(rovNames) {
  const container = qs("sm-rov-list");
  const selectAll = qs("sm-rov-select-all");

  if (!container) return;

  container.innerHTML = "";

  if (selectAll) {
    selectAll.checked = false;
    selectAll.indeterminate = false;
  }

  if (!rovNames || rovNames.length === 0) {
    container.innerHTML = `
      <div class="col-12">
        <div class="text-muted small border rounded p-3">
          No ROVs found for selected filters.
        </div>
      </div>
    `;
    return;
  }

  rovNames.forEach((name) => {
    const safeName = escapeHtml(name);
    const id = `rov_${safeId(name)}`;

    const col = document.createElement("div");
    col.className = "col-6 col-md-4";

    col.innerHTML = `
      <div class="form-check">
        <input class="form-check-input sm-rov-cb" type="checkbox" value="${safeName}" id="${id}">
        <label class="form-check-label" for="${id}">${safeName}</label>
      </div>
    `;

    container.appendChild(col);
  });
}

function getSelectedRovs() {
  return Array.from(document.querySelectorAll(".sm-rov-cb:checked"))
    .map((cb) => cb.value);
}

function setAllRovs(checked) {
  document.querySelectorAll(".sm-rov-cb").forEach((cb) => {
    cb.checked = checked;
  });
}

function showToast(html) {
  const toastEl = qs("sm-export-toast");
  const bodyEl = qs("sm-export-toast-body");

  if (!toastEl || !bodyEl || !window.bootstrap) return;

  bodyEl.innerHTML = html;

  const toast = bootstrap.Toast.getOrCreateInstance(toastEl, {
    delay: 5000,
  });

  toast.show();
}

function validateRangeInputs(err) {
  const lineFrom = getOptionalValue("sm-line-from");
  const lineTo = getOptionalValue("sm-line-to");
  const stationFrom = getOptionalValue("sm-station-from");
  const stationTo = getOptionalValue("sm-station-to");

  const numericFields = [
    ["Line from", lineFrom],
    ["Line to", lineTo],
    ["Station from", stationFrom],
    ["Station to", stationTo],
  ];

  for (const [label, value] of numericFields) {
    if (value && !/^\d+$/.test(value)) {
      err.textContent = `${label} must be a number.`;
      err.classList.remove("d-none");
      return null;
    }
  }

  return {
    line_from: lineFrom,
    line_to: lineTo,
    station_from: stationFrom,
    station_to: stationTo,
  };
}

function hasLineStationFilter(rangePayload) {
  return Boolean(
    rangePayload.line_from ||
    rangePayload.line_to ||
    rangePayload.station_from ||
    rangePayload.station_to
  );
}

function getSmFilterPayload() {
  const mode = qs("sm-export-mode")?.value || "day";

  const status =
    document.querySelector("input[name='sm-status']:checked")?.value ||
    "deployed";

  const payload = {
    mode,
    status,
    line_from: getOptionalValue("sm-line-from"),
    line_to: getOptionalValue("sm-line-to"),
    station_from: getOptionalValue("sm-station-from"),
    station_to: getOptionalValue("sm-station-to"),
  };

  if (mode === "day") {
    payload.day = qs("sm-day")?.value || "";
  } else {
    payload.from = qs("sm-from")?.value || "";
    payload.to = qs("sm-to")?.value || "";
  }

  return payload;
}

async function refreshSmRovs() {
  const btn = qs("sm2file-export-btn");
  const help = qs("sm-export-help");
  const err = qs("sm-export-error");
  const rovsUrl = btn?.dataset.rovsUrl;

  if (!rovsUrl) return;

  if (err) {
    err.classList.add("d-none");
    err.textContent = "";
  }

  const payload = getSmFilterPayload();

  const hasLineOrStation = Boolean(
    payload.line_from ||
    payload.line_to ||
    payload.station_from ||
    payload.station_to
  );

  const hasTime =
    payload.mode === "day"
      ? Boolean(payload.day)
      : Boolean(payload.from && payload.to);

  if (!hasTime && !hasLineOrStation) {
    renderRovCheckboxes([]);
    if (help) {
      help.textContent = "Select a day, FROM/TO, Line, or Station to load ROVs.";
    }
    return;
  }

  if (help) {
    help.textContent = "Loading ROV list...";
  }

  try {
    const res = await fetch(rovsUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": getCSRFToken(),
      },
      body: JSON.stringify(payload),
    });

    const data = await res.json();

    if (!res.ok || data.error) {
      renderRovCheckboxes([]);
      if (help) {
        help.textContent = data.error || "Failed to load ROVs.";
      }
      return;
    }

    renderRovCheckboxes(data.rovs || []);

    if (help) {
      help.textContent = `${data.count || 0} ROV(s) found for selected filters.`;
    }

  } catch (e) {
    renderRovCheckboxes([]);
    if (help) {
      help.textContent = `Failed to load ROV list: ${e.message}`;
    }
  }
}

export function initDsrExportSmModal({
  rovNames = [],
} = {}) {
  const modeSel = qs("sm-export-mode");
  const selectAll = qs("sm-rov-select-all");
  const btn = qs("sm2file-export-btn");
  const err = qs("sm-export-error");
  const help = qs("sm-export-help");

  if (!modeSel || !btn || !err) return;

  setModeUI(modeSel.value);
  renderRovCheckboxes(rovNames);

  if (help) {
    help.textContent = "Select a day, FROM/TO, Line, or Station to load ROVs.";
  }

  modeSel.addEventListener("change", () => {
    setModeUI(modeSel.value);
    refreshSmRovs();
  });

  if (selectAll) {
    selectAll.addEventListener("change", () => {
      setAllRovs(selectAll.checked);
    });
  }

  document.addEventListener("change", (e) => {
    if (!e.target.classList.contains("sm-rov-cb")) return;
    if (!selectAll) return;

    const all = Array.from(document.querySelectorAll(".sm-rov-cb"));
    const checked = all.filter((x) => x.checked).length;

    selectAll.checked = all.length > 0 && checked === all.length;
    selectAll.indeterminate = checked > 0 && checked < all.length;
  });

  const delayedRefreshRovs = debounce(refreshSmRovs, 500);

  [
    "sm-day",
    "sm-from",
    "sm-to",
    "sm-line-from",
    "sm-line-to",
    "sm-station-from",
    "sm-station-to",
  ].forEach((id) => {
    qs(id)?.addEventListener("change", refreshSmRovs);
    qs(id)?.addEventListener("input", delayedRefreshRovs);
  });

  document.querySelectorAll("input[name='sm-status']").forEach((el) => {
    el.addEventListener("change", refreshSmRovs);
  });

  btn.addEventListener("click", async () => {
    err.classList.add("d-none");
    err.textContent = "";

    const exportUrl = btn.dataset.exportUrl;
    const mode = modeSel.value;

    const status =
      document.querySelector("input[name='sm-status']:checked")?.value ||
      "deployed";

    const depthMode = qs("sm-depth-mode")?.value || "neg";
    const format = qs("sm-format")?.value || "z_nodes";
    const filename = getOptionalValue("sm-filename");
    const rovs = getSelectedRovs();

    const alwaysPrimaryDeployment =
      qs("sm-always-primary-deployment")?.checked ?? true;

    const rangePayload = validateRangeInputs(err);
    if (!rangePayload) return;

    const hasLineOrStation = hasLineStationFilter(rangePayload);

    if (rovs.length === 0) {
      err.textContent = "Select at least one ROV.";
      err.classList.remove("d-none");
      return;
    }

    const payload = {
      mode,
      status,
      depth_mode: depthMode,
      format,
      rovs,
      filename,
      always_primary_deployment: alwaysPrimaryDeployment,
      ...rangePayload,
    };

    if (mode === "day") {
      const day = qs("sm-day")?.value || "";

      if (!day && !hasLineOrStation) {
        err.textContent = "Select a day or use Line/Station filter.";
        err.classList.remove("d-none");
        return;
      }

      payload.day = day;

    } else {
      const from = qs("sm-from")?.value || "";
      const to = qs("sm-to")?.value || "";

      if ((!from || !to) && !hasLineOrStation) {
        err.textContent = "Select FROM/TO or use Line/Station filter.";
        err.classList.remove("d-none");
        return;
      }

      payload.from = from;
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

      if (!data.ok) {
        throw new Error(data?.error || "Export failed");
      }

      showToast(`
        <div><b>Export done</b></div>
        <div>File: <code>${escapeHtml(data.filename || "")}</code></div>
        <div>Exported nodes: <b>${data.rows ?? 0}</b></div>
      `);

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