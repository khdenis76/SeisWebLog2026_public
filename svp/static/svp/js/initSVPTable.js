export function initSVPTable() {
  const tbody = document.getElementById("svpTableBody");
  const countBadge = document.getElementById("svpProfileCount");
  const detailsBox = document.getElementById("svpDetailsBox");
  const selectedLabel = document.getElementById("svpSelectedLabel");

  if (!tbody) return;

  fetch("/svp/api/list/")
    .then((r) => r.json())
    .then((data) => {
      const rows = Array.isArray(data?.rows) ? data.rows : [];
      countBadge.textContent = rows.length;

      if (!rows.length) {
        tbody.innerHTML = `
          <tr>
            <td colspan="4" class="text-center text-muted py-4">No profiles found</td>
          </tr>
        `;
        return;
      }

      tbody.innerHTML = rows.map((r) => `
        <tr data-id="${r.id}">
          <td>${escapeHtml(r.name || "")}</td>
          <td>${escapeHtml(r.created_at || "")}</td>
          <td>${escapeHtml(r.source || "")}</td>
          <td class="text-end">${Number(r.points_count || 0)}</td>
        </tr>
      `).join("");

      tbody.querySelectorAll("tr[data-id]").forEach((tr) => {
        tr.addEventListener("click", () => {
          tbody.querySelectorAll("tr").forEach((row) => row.classList.remove("table-active"));
          tr.classList.add("table-active");

          const id = tr.dataset.id;
          loadProfileDetails(id, detailsBox, selectedLabel);
        });
      });
    })
    .catch(() => {
      tbody.innerHTML = `
        <tr>
          <td colspan="4" class="text-center text-danger py-4">Failed to load profiles</td>
        </tr>
      `;
    });
}

function loadProfileDetails(id, detailsBox, selectedLabel) {
  fetch(`/svp/api/details/${id}/`)
    .then((r) => r.json())
    .then((data) => {
      const p = data?.profile || {};
      selectedLabel.textContent = p.name || `Profile ${id}`;

      detailsBox.innerHTML = `
        <div class="row g-3">
          <div class="col-md-6">
            <div class="small text-muted">Name</div>
            <div class="fw-semibold">${escapeHtml(p.name || "")}</div>
          </div>
          <div class="col-md-6">
            <div class="small text-muted">Date</div>
            <div class="fw-semibold">${escapeHtml(p.created_at || "")}</div>
          </div>
          <div class="col-md-6">
            <div class="small text-muted">Source</div>
            <div class="fw-semibold">${escapeHtml(p.source || "")}</div>
          </div>
          <div class="col-md-6">
            <div class="small text-muted">Points</div>
            <div class="fw-semibold">${Number(p.points_count || 0)}</div>
          </div>
        </div>
      `;
    })
    .catch(() => {
      detailsBox.innerHTML = `<div class="text-danger">Failed to load profile details.</div>`;
    });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}