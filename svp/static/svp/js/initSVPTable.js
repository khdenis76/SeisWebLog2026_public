export function initSVPTable() {
  const tbody = document.getElementById("svpTableBody");
  const countBadge = document.getElementById("svpProfileCount");
  const detailsBox = document.getElementById("svpDetailsBox");
  const selectedLabel = document.getElementById("svpSelectedLabel");
  const plotBox = document.getElementById("svpPlotBox");
  const selectAll = document.getElementById("svpSelectAll");
  const deleteBtn = document.getElementById("btnDeleteSelectedSVP");

  if (!tbody) return;

  const refresh = () => loadTable(tbody, countBadge, detailsBox, selectedLabel, plotBox, selectAll, deleteBtn);

  if (selectAll) {
    selectAll.addEventListener("change", () => {
      tbody.querySelectorAll(".svp-row-check").forEach((cb) => {
        cb.checked = selectAll.checked;
      });
      updateDeleteButton(tbody, deleteBtn, selectAll);
    });
  }

  if (deleteBtn) {
    deleteBtn.addEventListener("click", () => deleteSelected(tbody, deleteBtn, selectAll, detailsBox, selectedLabel, plotBox, refresh));
  }

  refresh();
}

function loadTable(tbody, countBadge, detailsBox, selectedLabel, plotBox, selectAll, deleteBtn) {
  fetch("/svp/api/list/", {
    headers: { "X-Requested-With": "XMLHttpRequest" },
  })
    .then((r) => r.json())
    .then((data) => {
      const rows = Array.isArray(data?.rows) ? data.rows : [];
      if (countBadge) countBadge.textContent = rows.length;
      if (selectAll) selectAll.checked = false;
      if (deleteBtn) deleteBtn.disabled = true;

      if (!rows.length) {
        tbody.innerHTML = `
          <tr>
            <td colspan="6" class="text-center text-muted py-4">No profiles found</td>
          </tr>
        `;
        return;
      }

      tbody.innerHTML = rows.map((r) => `
        <tr data-id="${escapeHtml(r.id)}">
          <td class="text-center svp-check-col">
            <input class="form-check-input svp-row-check" type="checkbox" value="${escapeHtml(r.id)}" title="Select profile">
          </td>
          <td class="fw-semibold">${escapeHtml(r.name || "")}</td>
          <td>${escapeHtml(formatDateTime(r.timestamp || r.created_at || ""))}</td>
          <td>${escapeHtml(r.rov || "")}</td>
          <td class="text-truncate" style="max-width: 220px;" title="${escapeHtml(r.source_file_name || r.source_000_file || "")}">
            ${escapeHtml(r.source_000_file || r.source_file_name || "")}
          </td>
          <td class="text-end">${Number(r.points_count || 0)}</td>
        </tr>
      `).join("");

      tbody.querySelectorAll(".svp-row-check").forEach((cb) => {
        cb.addEventListener("click", (event) => event.stopPropagation());
        cb.addEventListener("change", () => updateDeleteButton(tbody, deleteBtn, selectAll));
      });

      tbody.querySelectorAll("tr[data-id]").forEach((tr) => {
        tr.addEventListener("click", () => {
          tbody.querySelectorAll("tr").forEach((row) => row.classList.remove("table-active"));
          tr.classList.add("table-active");
          loadProfileDetails(tr.dataset.id, detailsBox, selectedLabel, plotBox);
        });
      });
    })
    .catch((err) => {
      console.error("Failed to load SVP profiles:", err);
      tbody.innerHTML = `
        <tr>
          <td colspan="6" class="text-center text-danger py-4">Failed to load profiles</td>
        </tr>
      `;
    });
}

function loadProfileDetails(id, detailsBox, selectedLabel, plotBox) {
  if (plotBox) {
    plotBox.innerHTML = `<div class="text-muted small p-3">Loading plot...</div>`;
  }

  fetch(`/svp/api/details/${id}/`, {
    headers: { "X-Requested-With": "XMLHttpRequest" },
  })
    .then((r) => r.json())
    .then((data) => {
      const p = data?.profile || {};
      if (selectedLabel) selectedLabel.textContent = p.name || `Profile ${id}`;

      detailsBox.innerHTML = `
        <div class="row g-3">
          <div class="col-md-4">
            <div class="small text-muted">Name</div>
            <div class="fw-semibold">${escapeHtml(p.name || "")}</div>
          </div>
          <div class="col-md-4">
            <div class="small text-muted">Start Date/Time</div>
            <div class="fw-semibold">${escapeHtml(formatDateTime(p.timestamp || p.created_at || ""))}</div>
          </div>
          <div class="col-md-4">
            <div class="small text-muted">ROV</div>
            <div class="fw-semibold">${escapeHtml(p.rov || "")}</div>
          </div>

          <div class="col-md-4">
            <div class="small text-muted">Instrument Model</div>
            <div class="fw-semibold">${escapeHtml(p.instrument_model || "")}</div>
          </div>
          <div class="col-md-4">
            <div class="small text-muted">Easting</div>
            <div class="fw-semibold">${formatNumber(p.coord_e)}</div>
          </div>
          <div class="col-md-4">
            <div class="small text-muted">Northing</div>
            <div class="fw-semibold">${formatNumber(p.coord_n)}</div>
          </div>

          <div class="col-md-4">
            <div class="small text-muted">.000 File</div>
            <div class="fw-semibold text-break">${escapeHtml(p.source_000_file || "")}</div>
          </div>
          <div class="col-md-4">
            <div class="small text-muted">.svp File</div>
            <div class="fw-semibold text-break">${escapeHtml(p.source_svp_file || "")}</div>
          </div>
          <div class="col-md-4">
            <div class="small text-muted">Points</div>
            <div class="fw-semibold">${Number(p.points_count || 0)}</div>
          </div>
        </div>
      `;

      loadProfilePlot(id, plotBox);
    })
    .catch((err) => {
      console.error("Failed to load SVP profile details:", err);
      detailsBox.innerHTML = `<div class="text-danger">Failed to load profile details.</div>`;
      if (plotBox) plotBox.innerHTML = `<div class="text-danger small p-3">Failed to load plot.</div>`;
    });
}

function loadProfilePlot(id, plotBox) {
  if (!plotBox) return;

  fetch(`/svp/api/plot/${id}/`, {
    headers: { "X-Requested-With": "XMLHttpRequest" },
  })
    .then((r) => r.json())
    .then((data) => {
      if (!data?.success) {
        plotBox.innerHTML = `<div class="text-danger small p-3">${escapeHtml(data?.error || "Failed to load plot.")}</div>`;
        return;
      }

      const iframe = document.createElement("iframe");
      iframe.className = "svp-plot-frame";
      iframe.setAttribute("title", "SVP profile plot");
      iframe.srcdoc = data.html || "";

      plotBox.innerHTML = "";
      plotBox.appendChild(iframe);
    })
    .catch((err) => {
      console.error("Failed to load SVP plot:", err);
      plotBox.innerHTML = `<div class="text-danger small p-3">Failed to load plot.</div>`;
    });
}

function deleteSelected(tbody, deleteBtn, selectAll, detailsBox, selectedLabel, plotBox, refresh) {
  const ids = Array.from(tbody.querySelectorAll(".svp-row-check:checked")).map((cb) => cb.value);
  if (!ids.length) return;

  const label = ids.length === 1 ? "this SVP profile" : `${ids.length} SVP profiles`;
  if (!window.confirm(`Delete ${label}? This cannot be undone.`)) return;

  const formData = new FormData();
  ids.forEach((id) => formData.append("ids[]", id));

  deleteBtn.disabled = true;

  fetch("/svp/api/delete-selected/", {
    method: "POST",
    headers: {
      "X-Requested-With": "XMLHttpRequest",
      "X-CSRFToken": getCsrfToken(),
    },
    body: formData,
  })
    .then((r) => r.json())
    .then((data) => {
      if (!data?.success) {
        window.alert(data?.error || "Delete failed.");
        updateDeleteButton(tbody, deleteBtn, selectAll);
        return;
      }

      if (detailsBox) detailsBox.innerHTML = `<div class="text-muted">Select an SVP profile from the left table.</div>`;
      if (selectedLabel) selectedLabel.textContent = "Nothing selected";
      if (plotBox) plotBox.innerHTML = `<div class="text-muted small p-3">Click an SVP row to load the profile plot.</div>`;
      refresh();
    })
    .catch((err) => {
      console.error("Failed to delete SVP profiles:", err);
      window.alert("Delete failed.");
      updateDeleteButton(tbody, deleteBtn, selectAll);
    });
}

function updateDeleteButton(tbody, deleteBtn, selectAll) {
  const checks = Array.from(tbody.querySelectorAll(".svp-row-check"));
  const checked = checks.filter((cb) => cb.checked);

  if (deleteBtn) {
    deleteBtn.disabled = checked.length === 0;
    deleteBtn.innerHTML = checked.length
      ? `<i class="fa-solid fa-trash-can me-1"></i> Delete (${checked.length})`
      : `<i class="fa-solid fa-trash-can me-1"></i> Delete`;
  }

  if (selectAll) {
    selectAll.checked = checks.length > 0 && checked.length === checks.length;
    selectAll.indeterminate = checked.length > 0 && checked.length < checks.length;
  }
}

function getCsrfToken() {
  const input = document.querySelector("input[name='csrfmiddlewaretoken']");
  if (input?.value) return input.value;

  const match = document.cookie.match(/(?:^|; )csrftoken=([^;]+)/);
  return match ? decodeURIComponent(match[1]) : "";
}

function formatDateTime(value) {
  if (!value) return "";
  return String(value).replace("T", " ").replace(/\.\d+$/, "");
}

function formatNumber(value) {
  if (value === null || value === undefined || value === "") return "";
  const n = Number(value);
  if (Number.isNaN(n)) return escapeHtml(value);
  return n.toFixed(2);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}
