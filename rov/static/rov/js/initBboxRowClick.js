// initBboxRowClick.js

import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { renderBokehInto } from "../../baseproject/js/renderBokeh.js";

export function initBboxPlotClick() {
  const tbody = document.getElementById("bbox-list-tbody");
  if (!tbody) return;

  let currentController = null;
  let selectedFilePayload = null;
  let loadedPlots = new Set();

  const plots = [
  {
    key: "gnss_qc",
    divId: "gnss-qc-plot",
    paneId: "pane-gnss",
    label: "GNSS QC",
  },
  {
    key: "gnss_delta",
    divId: "gnss-delta-plot",
    paneId: "pane-gnss-delta",
    label: "GNSS ΔE/ΔN",
  },
  {
    key: "rov1_ins_usbl",
    divId: "rov1-ins-usbl-plot",
    paneId: "pane-rov1-ins-usbl",
    label: "ROV1 INS/USBL",
  },
  {
    key: "rov2_ins_usbl",
    divId: "rov2-ins-usbl-plot",
    paneId: "pane-rov2-ins-usbl",
    label: "ROV2 INS/USBL",
  },
  {
    key: "hdop",
    divId: "gnss-hdop-plot",
    paneId: "pane-hdop",
    label: "HDOP",
  },
  {
    key: "rovs_depths",
    divId: "rov-depth-qc-plot",
    paneId: "pane-depths",
    label: "DEPTH",
  },
  {
    key: "vessel_sog",
    divId: "vessel-sog-plot",
    paneId: "pane-sog",
    label: "SOG",
  },
  {
    key: "cog_vs_hdg",
    divId: "hdg-cog-plot",
    paneId: "pane-hdg",
    label: "HDG / COG",
  },
];

  function getActivePlot() {
    const activePane = document.querySelector("#bboxPlotsTabsContent .tab-pane.active");
    if (!activePane) return plots[0];

    return plots.find((p) => p.paneId === activePane.id) || plots[0];
  }

  function resetPlotSlots() {
    for (const p of plots) {
      const el = document.getElementById(p.divId);
      if (!el) continue;

      const isActive = document.getElementById(p.paneId)?.classList.contains("active");

      el.innerHTML = isActive
        ? `<div class="text-muted p-2">Loading ${p.label}…</div>`
        : `<div class="text-muted p-2">Open tab to load…</div>`;
    }

    const driftEl = document.getElementById("drift-plot");
    if (driftEl) {
      driftEl.innerHTML = `<div class="text-muted p-2">DRIFT plot is not configured yet…</div>`;
    }
  }

  async function loadPlot(plot) {
    if (!plot || !selectedFilePayload) return;
    if (loadedPlots.has(plot.key)) return;

    const url = tbody.dataset.plotItemUrl;
    const el = document.getElementById(plot.divId);

    if (!url) {
      console.warn("Missing data-plot-item-url on #bbox-list-tbody");
      return;
    }

    if (!el) return;

    loadedPlots.add(plot.key);

    el.innerHTML = `<div class="text-muted p-2">Loading ${plot.label}…</div>`;

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({
          ...selectedFilePayload,
          plot_key: plot.key,
        }),
        signal: currentController?.signal,
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        loadedPlots.delete(plot.key);
        el.innerHTML = `
          <div class="text-danger p-2">
            Failed to load ${plot.label}: ${data.error || "Unknown error"}
          </div>
        `;
        console.error(data.error || "Failed to load plot", plot.key);
        return;
      }

      if (!data.item) {
        loadedPlots.delete(plot.key);
        el.innerHTML = `
          <div class="text-danger p-2">
            Server did not return Bokeh item for ${plot.label}.
          </div>
        `;
        console.error("Server did not return json_item", plot.key);
        return;
      }

      el.innerHTML = "";
      renderBokehInto(plot.divId, data.item);
    } catch (err) {
      if (err?.name === "AbortError") return;

      loadedPlots.delete(plot.key);
      console.error(err);

      el.innerHTML = `
        <div class="text-danger p-2">
          Error loading ${plot.label}.
        </div>
      `;
    }
  }

  tbody.addEventListener("click", (e) => {
    if (e.target.closest(".bbox-file-checkbox")) return;

    const tr = e.target.closest("tr[data-file-id][data-file-name]");
    if (!tr) return;

    const fileId = tr.dataset.fileId;
    const fileName = tr.dataset.fileName;

    tbody
      .querySelectorAll("tr.table-active")
      .forEach((r) => r.classList.remove("table-active"));

    tr.classList.add("table-active");

    if (currentController) currentController.abort();
    currentController = new AbortController();

    loadedPlots = new Set();

    selectedFilePayload = {
      file_id: fileId ? Number(fileId) : null,
      file_name: fileName || null,
    };

    resetPlotSlots();

    loadPlot(getActivePlot());
  });

  document.querySelectorAll('#bboxPlotsTabs button[data-bs-toggle="tab"]').forEach((btn) => {
    btn.addEventListener("shown.bs.tab", (e) => {
      if (!selectedFilePayload) return;

      const targetPaneId = (e.target.dataset.bsTarget || "").replace("#", "");
      const plot = plots.find((p) => p.paneId === targetPaneId);

      if (plot) {
        loadPlot(plot);
      }
    });
  });
}