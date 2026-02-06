import { getCSRFToken } from "./csrf.js";
import { renderBokehInto } from "./renderBokeh.js"
export function initDeleteShapesButton() {
  const btn = document.getElementById("btnDeleteShapes");
  const tbody = document.getElementById("shape-folder-body");
  const prj_shapes_body = document.getElementById("prj-shp-body")
  if (!btn) return;

  btn.addEventListener("click", async () => {
    const table = document.getElementById("shp-in-db-table");
    if (!table) return;

    // Select only the row-selection checkboxes (switches) in column 2
    const checked = Array.from(
      table.querySelectorAll('tbody input.form-check-input[type="checkbox"]:checked')
    );

    const fullNames = checked
      .map(cb => cb.value)
      .filter(v => typeof v === "string" && v.trim().length > 0);

    if (fullNames.length === 0) {
      alert("Select at least one row to delete.");
      return;
    }

    if (!confirm(`Delete ${fullNames.length} shape(s) from database?`)) return;

    const url = btn.dataset.postUrl;

    try {
      btn.disabled = true;

      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({ full_names: fullNames }),
      });

      const data = await resp.json();

      if (!resp.ok) {
        alert(data.error || "Failed to delete shapes.");
        return;
      }
      if (data.preplot_map) {
          renderBokehInto("preplot-map-div", data.preplot_map);
      }
      tbody.innerHTML=data.shapes_in_folder
      prj_shapes_body.innerHTML=data.prj_shp_body
      // Remove rows from DOM
      checked.forEach(cb => cb.closest("tr")?.remove());

      // Optional: reset main checkbox
      const main = document.getElementById("MainCheckbox");
      if (main) main.checked = false;

      alert(`Deleted: ${data.deleted} shape(s)`);

    } catch (err) {
      console.error(err);
      alert("Network error while deleting shapes.");
    } finally {
      btn.disabled = false;
    }
  });
}
export function forceInputsFromHtmlDefaults() {
  document.querySelectorAll("#shp-in-db-table input[type='color'], #shp-in-db-table input[type='number']").forEach(el => {
    const attr = el.getAttribute("value");
    if (attr !== null) el.value = attr; // override restored value
  });

  // also restore checkboxes that you render using checked attribute
  document.querySelectorAll("#shp-in-db-table input[type='checkbox']").forEach(el => {
    const shouldBeChecked = el.hasAttribute("checked");
    el.checked = shouldBeChecked;
  });
}
