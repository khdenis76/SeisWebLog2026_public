import { getCSRFToken } from "./csrf.js";
import {initSPPointCheckboxes} from "./initMainSPCheckbox.js";
import {activateTab} from "./initTabActivator.js";
export function initSLLineClick() {
  const table = document.getElementById("slpreplotTable");
  const rldiv = document.getElementById("sl-points-div")
  if (!table) return;

  const url = table.dataset.lineClickUrl;

  table.addEventListener("click", async (e) => {
    const td = e.target.closest("td");
    if (!td) return;

    // ignore checkbox clicks
    if (e.target.closest("input[type='checkbox']")) return;

    const tr = td.closest("tr");
    if (!tr) return;

    const lineId = tr.dataset.lineId;
    if (!lineId) return;

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({ line_id: lineId }),
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        alert(data.error || "Failed to load line data");
        return;
      }
      rldiv.innerHTML = data.point_table
      initSPPointCheckboxes();
      activateTab("sl-points-tab")
      // ✅ Use returned JSON
      console.log("Line data:", data);

      // Example: put HTML into a panel
      if (data.html) {
        const panel = document.getElementById("lineDetailPanel");
        if (panel) panel.innerHTML = data.html;
      }

      // Example: if you return bokeh json_item
      // if (data.bokeh_item) {
      //   const el = document.getElementById("linePlot");
      //   el.innerHTML = "";
      //   Bokeh.embed.embed_item(data.bokeh_item, "linePlot");
      // }

    } catch (err) {
      console.error(err);
      alert("Network error");
    }
  });
}
