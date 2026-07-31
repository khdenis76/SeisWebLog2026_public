export function initSVPTable() {
  const tbody = document.getElementById("svpTableBody");
  const countBadge = document.getElementById("svpProfileCount");
  const detailsBox = document.getElementById("svpDetailsBox");
  const selectedLabel = document.getElementById("svpSelectedLabel");
  const plotBox = document.getElementById("svpPlotBox");
  const mapBox = document.getElementById("svpMapBox");
  const dataBody = document.getElementById("svpDataTableBody");
  const statisticsBox = document.getElementById("svpStatisticsBox");
  const selectAll = document.getElementById("svpSelectAll");
  const deleteBtn = document.getElementById("btnDeleteSelectedSVP");
  if (!tbody) return;

  const refresh = () => loadTable(tbody, countBadge, detailsBox, selectedLabel, plotBox, dataBody, statisticsBox, selectAll, deleteBtn);
  const refreshGlobalViews = () => {
    refresh();
    loadSVPMap(mapBox);
    loadSVPStatistics(statisticsBox);
  };
  loadSVPMap(mapBox);
  loadSVPStatistics(statisticsBox);
  window.addEventListener("svp:profiles-changed", refreshGlobalViews);

  selectAll?.addEventListener("change", () => {
    tbody.querySelectorAll(".svp-row-check").forEach((cb) => { cb.checked = selectAll.checked; });
    updateDeleteButton(tbody, deleteBtn, selectAll);
  });
  deleteBtn?.addEventListener("click", () => deleteSelected(tbody, deleteBtn, selectAll, detailsBox, selectedLabel, plotBox, dataBody, statisticsBox, refresh, mapBox));
  refresh();
}

function loadTable(tbody, countBadge, detailsBox, selectedLabel, plotBox, dataBody, statisticsBox, selectAll, deleteBtn) {
  fetch("/svp/api/list/", { headers: { "X-Requested-With": "XMLHttpRequest" } })
    .then((r) => r.json())
    .then((data) => {
      const rows = Array.isArray(data?.rows) ? data.rows : [];
      if (countBadge) countBadge.textContent = rows.length;
      if (selectAll) selectAll.checked = false;
      if (deleteBtn) deleteBtn.disabled = true;
      if (!rows.length) {
        tbody.innerHTML = `<tr><td colspan="6" class="text-center text-muted py-4">No profiles found</td></tr>`;
        return;
      }
      tbody.innerHTML = rows.map((r) => `
        <tr data-id="${escapeHtml(r.id)}">
          <td class="text-center svp-check-col"><input class="form-check-input svp-row-check" type="checkbox" value="${escapeHtml(r.id)}"></td>
          <td class="fw-semibold">${escapeHtml(r.name || "")}</td>
          <td>${escapeHtml(formatDateTime(r.timestamp || r.created_at || ""))}</td>
          <td>${escapeHtml(r.rov || "")}</td>
          <td class="text-truncate" style="max-width:220px" title="${escapeHtml(r.source_file_name || r.source_svp_file || r.source_000_file || "")}">${escapeHtml(r.source_svp_file || r.source_000_file || r.source_file_name || "")}</td>
          <td class="text-end">${Number(r.points_count || 0)}</td>
        </tr>`).join("");

      tbody.querySelectorAll(".svp-row-check").forEach((cb) => {
        cb.addEventListener("click", (e) => e.stopPropagation());
        cb.addEventListener("change", () => updateDeleteButton(tbody, deleteBtn, selectAll));
      });
      tbody.querySelectorAll("tr[data-id]").forEach((tr) => tr.addEventListener("click", () => {
        tbody.querySelectorAll("tr").forEach((row) => row.classList.remove("table-active"));
        tr.classList.add("table-active");
        loadProfileDetails(tr.dataset.id, detailsBox, selectedLabel, plotBox, dataBody, statisticsBox);
      }));
    })
    .catch(() => { tbody.innerHTML = `<tr><td colspan="6" class="text-center text-danger py-4">Failed to load profiles</td></tr>`; });
}

function loadProfileDetails(id, detailsBox, selectedLabel, plotBox, dataBody, statisticsBox) {
  plotBox.innerHTML = `<div class="text-muted small p-3">Loading plot...</div>`;
  dataBody.innerHTML = `<tr><td colspan="7" class="text-center text-muted py-4">Loading data...</td></tr>`;
  fetch(`/svp/api/details/${id}/`, { headers: { "X-Requested-With": "XMLHttpRequest" } })
    .then((r) => r.json())
    .then((data) => {
      const p = data?.profile || {};
      const points = Array.isArray(data?.points) ? data.points : [];
      if (selectedLabel) selectedLabel.textContent = p.name || `Profile ${id}`;
      detailsBox.innerHTML = `
        <div class="row g-3">
          <div class="col-md-4"><div class="small text-muted">Name</div><div class="fw-semibold">${escapeHtml(p.name || "")}</div></div>
          <div class="col-md-4"><div class="small text-muted">Start Date/Time</div><div class="fw-semibold">${escapeHtml(formatDateTime(p.timestamp || p.created_at || ""))}</div></div>
          <div class="col-md-4"><div class="small text-muted">ROV</div><div class="fw-semibold">${escapeHtml(p.rov || "")}</div></div>
          <div class="col-md-4"><div class="small text-muted">Instrument Model</div><div class="fw-semibold">${escapeHtml(p.instrument_model || "")}</div></div>
          <div class="col-md-4"><div class="small text-muted">Easting</div><div class="fw-semibold">${formatNumber(p.coord_e)}</div></div>
          <div class="col-md-4"><div class="small text-muted">Northing</div><div class="fw-semibold">${formatNumber(p.coord_n)}</div></div>
          <div class="col-md-4"><div class="small text-muted">.000 File</div><div class="fw-semibold text-break">${escapeHtml(p.source_000_file || "")}</div></div>
          <div class="col-md-4"><div class="small text-muted">.svp File</div><div class="fw-semibold text-break">${escapeHtml(p.source_svp_file || "")}</div></div>
          <div class="col-md-4"><div class="small text-muted">Points</div><div class="fw-semibold">${Number(p.points_count || points.length || 0)}</div></div>
        </div>`;
      renderDataTable(points, dataBody);
      // Statistics is an all-profile aggregate view and is loaded separately.
      loadProfilePlot(id, plotBox);
    })
    .catch(() => {
      detailsBox.innerHTML = `<div class="text-danger">Failed to load profile details.</div>`;
      plotBox.innerHTML = `<div class="text-danger small p-3">Failed to load plot.</div>`;
      dataBody.innerHTML = `<tr><td colspan="7" class="text-center text-danger py-4">Failed to load data.</td></tr>`;
    });
}

function renderDataTable(points, dataBody) {
  if (!points.length) {
    dataBody.innerHTML = `<tr><td colspan="7" class="text-center text-muted py-4">No data points.</td></tr>`;
    return;
  }
  dataBody.innerHTML = points.map((p, i) => `<tr>
    <td>${i + 1}</td><td>${formatValue(p.depth_m, 3)}</td><td>${formatValue(p.velocity_mps, 3)}</td>
    <td>${formatValue(p.salinity_psu, 4)}</td><td>${formatValue(p.temperature_c, 4)}</td>
    <td>${formatValue(p.conductivity_mscm, 4)}</td><td>${formatValue(p.density_kgm3, 4)}</td>
  </tr>`).join("");
}


function loadProfilePlot(id, plotBox) {
  fetch(`/svp/api/plot/${id}/`, { headers: { "X-Requested-With": "XMLHttpRequest" } })
    .then((r) => r.json()).then((data) => {
      if (!data?.success) throw new Error(data?.error || "Failed to load plot");
      setIframe(plotBox, data.html, "SVP profile plot");
    }).catch((err) => { plotBox.innerHTML = `<div class="text-danger small p-3">${escapeHtml(err.message)}</div>`; });
}

function loadSVPStatistics(statisticsBox) {
  if (!statisticsBox) return;
  statisticsBox.innerHTML = `<div class="text-muted small p-3">Loading average curves for all SVP profiles...</div>`;
  fetch("/svp/api/statistics/", { headers: { "X-Requested-With": "XMLHttpRequest" } })
    .then((r) => r.json()).then((data) => {
      if (!data?.success) throw new Error(data?.error || "Failed to load statistics");
      setIframe(statisticsBox, data.html, "SVP aggregate statistics");
    }).catch((err) => {
      statisticsBox.innerHTML = `<div class="text-danger small p-3">${escapeHtml(err.message)}</div>`;
    });
}

function loadSVPMap(mapBox) {
  if (!mapBox) return;
  fetch("/svp/api/map/", { headers: { "X-Requested-With": "XMLHttpRequest" } })
    .then((r) => r.json()).then((data) => {
      if (!data?.success) throw new Error(data?.error || "Failed to load map");
      setIframe(mapBox, data.html, "SVP map");
    }).catch((err) => { mapBox.innerHTML = `<div class="text-danger small p-3">${escapeHtml(err.message)}</div>`; });
}

function setIframe(box, html, title) {
  const iframe = document.createElement("iframe");
  iframe.className = "svp-plot-frame"; iframe.title = title; iframe.srcdoc = html || "";
  box.innerHTML = ""; box.appendChild(iframe);
}

function deleteSelected(tbody, deleteBtn, selectAll, detailsBox, selectedLabel, plotBox, dataBody, statisticsBox, refresh, mapBox) {
  const ids = Array.from(tbody.querySelectorAll(".svp-row-check:checked")).map((cb) => cb.value);
  if (!ids.length || !window.confirm(`Delete ${ids.length === 1 ? "this SVP profile" : `${ids.length} SVP profiles`}?`)) return;
  const fd = new FormData(); ids.forEach((id) => fd.append("ids[]", id)); deleteBtn.disabled = true;
  fetch("/svp/api/delete-selected/", { method:"POST", headers:{"X-Requested-With":"XMLHttpRequest","X-CSRFToken":getCsrfToken()}, body:fd })
    .then((r) => r.json()).then((data) => {
      if (!data?.success) throw new Error(data?.error || "Delete failed");
      detailsBox.innerHTML = `<div class="text-muted">Select an SVP profile from the left table.</div>`;
      selectedLabel.textContent = "Nothing selected";
      plotBox.innerHTML = `<div class="text-muted small p-3">Select an SVP profile to load the plot.</div>`;
      dataBody.innerHTML = `<tr><td colspan="7" class="text-center text-muted py-4">Select an SVP profile.</td></tr>`;
      if (statisticsBox) statisticsBox.innerHTML = `<div class="text-muted small">Refreshing average curves...</div>`;
      window.dispatchEvent(new CustomEvent("svp:profiles-changed", {detail: data}));
    }).catch((e) => { window.alert(e.message); updateDeleteButton(tbody, deleteBtn, selectAll); });
}

function updateDeleteButton(tbody, deleteBtn, selectAll) {
  const checks = Array.from(tbody.querySelectorAll(".svp-row-check")); const checked = checks.filter((cb) => cb.checked);
  if (deleteBtn) { deleteBtn.disabled = !checked.length; deleteBtn.innerHTML = `<i class="fa-solid fa-trash-can me-1"></i> Delete${checked.length ? ` (${checked.length})` : ""}`; }
  if (selectAll) { selectAll.checked = checks.length > 0 && checked.length === checks.length; selectAll.indeterminate = checked.length > 0 && checked.length < checks.length; }
}
function getCsrfToken(){const i=document.querySelector("input[name='csrfmiddlewaretoken']");if(i?.value)return i.value;const m=document.cookie.match(/(?:^|; )csrftoken=([^;]+)/);return m?decodeURIComponent(m[1]):"";}
function formatDateTime(v){return v?String(v).replace("T"," ").replace(/\.\d+$/,""):"";}
function formatNumber(v){return v===null||v===undefined||v===""?"":Number(v).toFixed(2);}
function formatValue(v,d){if(v===null||v===undefined||v==="")return "";const n=Number(v);return Number.isNaN(n)?escapeHtml(v):n.toFixed(d);}
function escapeHtml(v){return String(v).replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;").replaceAll('"',"&quot;").replaceAll("'","&#039;");}
