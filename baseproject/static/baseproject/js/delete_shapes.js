import { getCSRFToken } from "./csrf.js";
import { renderBokehInto } from "./renderBokeh.js";
import { showAppToast } from "./toast.js";
import { showConfirmModal } from "./modalConfirm.js";

export function initDeleteShapesButton() {
  const btn = document.getElementById("btnDeleteShapes");
  const tbody = document.getElementById("shape-folder-body");
  const prjShapesBody = document.getElementById("prj-shp-body");
  if (!btn) return;

  btn.addEventListener("click", async () => {
    const table = document.getElementById("shp-in-db-table");
    if (!table) return;

    const checked = Array.from(table.querySelectorAll('tbody input.form-check-input[type="checkbox"]:checked'));
    const fullNames = checked.map(cb => cb.value).filter(v => typeof v === "string" && v.trim().length > 0);

    if (fullNames.length === 0) {
      showAppToast("Select at least one shape row.", { title: "Nothing selected", variant: "warning" });
      return;
    }

    const confirmed = await showConfirmModal({
      title: "Delete project shapes",
      message: `Delete ${fullNames.length} shape(s) from database?`,
      details: "The files will stay in the source folder. Only the project records will be removed.",
      confirmText: "Delete shapes",
      confirmClass: "btn btn-danger seis-btn-danger",
      iconClass: "fa-shapes",
    });
    if (!confirmed) return;

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
        showAppToast(data.error || "Failed to delete shapes.", { title: "Delete failed", variant: "danger" });
        return;
      }
      if (data.preplot_map) renderBokehInto("preplot-map-div", data.preplot_map);
      tbody.innerHTML = data.shapes_in_folder;
      prjShapesBody.innerHTML = data.prj_shp_body;
      checked.forEach(cb => cb.closest("tr")?.remove());

      const main = document.getElementById("MainCheckbox");
      if (main) main.checked = false;

      showAppToast(`Deleted: ${data.deleted} shape(s).`, { title: "Shapes removed", variant: "success" });
    } catch (err) {
      console.error(err);
      showAppToast("Network error while deleting shapes.", { title: "Request failed", variant: "danger" });
    } finally {
      btn.disabled = false;
    }
  });
}

export function forceInputsFromHtmlDefaults() {
  document.querySelectorAll("#shp-in-db-table input[type='color'], #shp-in-db-table input[type='number']").forEach(el => {
    const attr = el.getAttribute("value");
    if (attr !== null) el.value = attr;
  });

  document.querySelectorAll("#shp-in-db-table input[type='checkbox']").forEach(el => {
    const shouldBeChecked = el.hasAttribute("checked");
    el.checked = shouldBeChecked;
  });
}
