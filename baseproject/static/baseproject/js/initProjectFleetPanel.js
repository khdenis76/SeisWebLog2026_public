import { getCSRFToken } from "./csrf.js";
export function initProjectFleetPanel() {
  const cfg = document.getElementById("project-fleet-panel");
  if (!cfg) return;

  const pfTbody = document.getElementById("pf-tbody");
  const dfTbody = document.getElementById("df-tbody");
  const pfCount = document.getElementById("pf-count");
  const dfCount = document.getElementById("df-count");

  const dfSearch = document.getElementById("df-search");
  const dfSearchBtn = document.getElementById("df-search-btn");

  const pfListUrl = cfg.dataset.pfListUrl;
  const dfListUrl = cfg.dataset.dfListUrl;
  const addUrl = cfg.dataset.addUrl;
  const removeUrl = cfg.dataset.removeUrl;

  function esc(s) {
    return String(s ?? "").replace(/[&<>"']/g, c => ({
      "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"
    }[c]));
  }

  async function loadProjectFleet() {
    pfTbody.innerHTML = `<tr><td colspan="4" class="text-muted">Loading...</td></tr>`;
    const res = await fetch(pfListUrl, { headers: { "Accept":"application/json" } });
    const data = await res.json();
    if (!data.ok) {
      pfTbody.innerHTML = `<tr><td colspan="4" class="text-danger">Failed</td></tr>`;
      return;
    }
    pfCount.textContent = data.items.length;

    if (!data.items.length) {
      pfTbody.innerHTML = `<tr><td colspan="4" class="text-muted">No project vessels</td></tr>`;
      return;
    }

    pfTbody.innerHTML = data.items.map(v => `
      <tr data-project-fleet-id="${v.id}">
        <td class="fw-semibold">${esc(v.vessel_name)}</td>
        <td>${esc(v.vessel_type || "")}</td>
        <td>${esc(v.imo || "")}</td>
        <td class="text-end">
          <button class="btn btn-sm btn-outline-danger pf-remove" title="Remove from project">
            <i class="fas fa-trash"></i>
          </button>
        </td>
      </tr>
    `).join("");

    pfTbody.querySelectorAll(".pf-remove").forEach(btn => {
      btn.addEventListener("click", async () => {
        const tr = btn.closest("tr");
        const id = tr.dataset.projectFleetId;
        await fetch(removeUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": getCSRFToken(),   // your global helper
            "Accept":"application/json",
          },
          body: JSON.stringify({ project_fleet_id: id }),
        });
        await loadProjectFleet();
      });
    });
  }

  async function loadDjangoFleet() {
    dfTbody.innerHTML = `<tr><td colspan="4" class="text-muted">Loading...</td></tr>`;
    const q = (dfSearch?.value || "").trim();
    const url = q ? `${dfListUrl}?q=${encodeURIComponent(q)}` : dfListUrl;

    const res = await fetch(url, { headers: { "Accept":"application/json" } });
    const data = await res.json();
    if (!data.ok) {
      dfTbody.innerHTML = `<tr><td colspan="4" class="text-danger">Failed</td></tr>`;
      return;
    }
    dfCount.textContent = data.items.length;

    if (!data.items.length) {
      dfTbody.innerHTML = `<tr><td colspan="4" class="text-muted">No vessels</td></tr>`;
      return;
    }

    dfTbody.innerHTML = data.items.map(v => `
      <tr data-vessel-id="${v.id}">
        <td class="fw-semibold">${esc(v.name)}</td>
        <td>${esc(v.vessel_type || "")}</td>
        <td>${esc(v.imo || "")}</td>
        <td class="text-end">
          <button class="btn btn-sm btn-outline-warning df-add" title="Add to project">
            <i class="fas fa-plus"></i>
          </button>
        </td>
      </tr>
    `).join("");

    dfTbody.querySelectorAll(".df-add").forEach(btn => {
      btn.addEventListener("click", async () => {
        const tr = btn.closest("tr");
        const id = tr.dataset.vesselId;
        await fetch(addUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": getCSRFToken(),
            "Accept":"application/json",
          },
          body: JSON.stringify({ vessel_id: id }),
        });
        await loadProjectFleet(); // reflect new project vessel
      });
    });
  }

  dfSearchBtn?.addEventListener("click", loadDjangoFleet);
  dfSearch?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") loadDjangoFleet();
  });

  // initial
  loadProjectFleet();
  loadDjangoFleet();
}