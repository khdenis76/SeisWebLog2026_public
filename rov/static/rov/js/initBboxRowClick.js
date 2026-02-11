// bboxPlotClick.js
import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { renderBokehInto } from "../../baseproject/js/renderBokeh.js";

export function initBboxPlotClick() {
  // Event delegation (works even if tbody is replaced later)
  document.addEventListener("click", async (e) => {
    const tbody = document.getElementById("bbox-list-tbody");
    if (!tbody) return;

    // Only handle clicks inside the bbox tbody
    if (!tbody.contains(e.target)) return;

    // Ignore checkbox clicks
    if (e.target.closest(".bbox-file-checkbox")) return;

    const tr = e.target.closest("tr[data-file-id][data-file-name]");
    if (!tr) return;

    const url = tbody.dataset.plotUrl;
    if (!url) {
      console.warn("Missing data-plot-url on #bbox-list-tbody");
      return;
    }

    const fileId = tr.dataset.fileId;
    const fileName = tr.dataset.fileName;

    if (!fileId && !fileName) {
      console.warn("Row has neither file_id nor file_name");
      return;
    }

    // ✅ Send BOTH
    const payload = {
      file_id: fileId ? Number(fileId) : null,
      file_name: fileName || null,
    };

    // Highlight selected row
    tbody.querySelectorAll("tr.table-active")
         .forEach(r => r.classList.remove("table-active"));
    tr.classList.add("table-active");

    // Loading indicator
    const plotDivId = "gnss-qc-plot";
    const plotDiv = document.getElementById(plotDivId);
    if (plotDiv) {
      plotDiv.innerHTML = `<div class="text-muted p-2">Loading plot…</div>`;
    }

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify(payload),
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        if (plotDiv) plotDiv.innerHTML = "";
        alert(data.error || "Failed to load plot");
        return;
      }

      if (!data.gnss_qc_plot) {
        if (plotDiv) plotDiv.innerHTML = "";
        alert("Server did not return json_item");
        return;
      }

      // Render using your helper
      renderBokehInto(plotDivId, data.gnss_qc_plot);

    } catch (err) {
      console.error(err);
      if (plotDiv) plotDiv.innerHTML = "";
      alert("Network error while loading plot");
    }
  });
}
