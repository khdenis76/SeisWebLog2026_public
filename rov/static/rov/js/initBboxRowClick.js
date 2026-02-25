// bboxPlotClick.js
import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { renderBokehInto } from "../../baseproject/js/renderBokeh.js";

export function initBboxPlotClick() {
  let currentController = null;

  document.addEventListener("click", (e) => {
    const tbody = document.getElementById("bbox-list-tbody");
    if (!tbody) return;
    if (!tbody.contains(e.target)) return;
    if (e.target.closest(".bbox-file-checkbox")) return;

    const tr = e.target.closest("tr[data-file-id][data-file-name]");
    if (!tr) return;

    const url = tbody.dataset.plotItemUrl; // NEW data attribute (see HTML below)
    if (!url) {
      console.warn("Missing data-plot-item-url on #bbox-list-tbody");
      return;
    }

    const fileId = tr.dataset.fileId;
    const fileName = tr.dataset.fileName;

    // highlight
    tbody.querySelectorAll("tr.table-active").forEach(r => r.classList.remove("table-active"));
    tr.classList.add("table-active");

    // cancel previous batch
    if (currentController) currentController.abort();
    currentController = new AbortController();

    const plots = [
      { key: "gnss_qc",     divId: "gnss-qc-plot" },
      { key: "rovs_depths", divId: "rov-depth-qc-plot" },
      { key: "vessel_sog",  divId: "vessel-sog-plot" },
      { key: "hdop",        divId: "gnss-hdop-plot" },
      { key: "cog_vs_hdg",  divId: "hdg-cog-plot" },
    ];

    // placeholders
    for (const p of plots) {
      const el = document.getElementById(p.divId);
      if (el) el.innerHTML = `<div class="text-muted p-2">Loading ${p.key}…</div>`;
    }

    // common payload
    const basePayload = {
      file_id: fileId ? Number(fileId) : null,
      file_name: fileName || null,
    };

    // load sequentially (true "step by step")
    (async () => {
      for (const p of plots) {
        const el = document.getElementById(p.divId);
        if (!el) continue;

        try {
          const resp = await fetch(url, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-CSRFToken": getCSRFToken(),
            },
            body: JSON.stringify({ ...basePayload, plot_key: p.key }),
            signal: currentController.signal,
          });

          const data = await resp.json();

          if (!resp.ok || !data.ok) {
            el.innerHTML = "";
            console.error(data.error || "Failed to load plot", p.key);
            continue; // keep going for other plots
          }

          if (!data.item) {
            el.innerHTML = "";
            console.error("Server did not return json_item", p.key);
            continue;
          }

          renderBokehInto(p.divId, data.item);

        } catch (err) {
          // aborted -> stop quietly
          if (err?.name === "AbortError") return;
          console.error(err);
          el.innerHTML = "";
        }
      }
    })();
  });
}
