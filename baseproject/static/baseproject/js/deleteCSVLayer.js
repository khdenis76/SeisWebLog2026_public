import { getCSRFToken } from "./csrf.js";
import { renderBokehInto } from "./renderBokeh.js";
import { showAppToast } from "./toast.js";
import { showConfirmModal } from "./modalConfirm.js";

export function initDeleteLayersBtn() {
  const btn = document.getElementById("deleteLayersBtn");
  const tbody = document.getElementById("layersTbody");
  if (!btn || !tbody) return;

  btn.addEventListener("click", async () => {
    const checkboxes = tbody.querySelectorAll(".layers-checkbox:checked");
    if (checkboxes.length === 0) {
      showAppToast("Select layer rows first.", { title: "Nothing selected", variant: "warning" });
      return;
    }

    const ids = Array.from(checkboxes).map(cb => cb.dataset.layerid);
    const confirmed = await showConfirmModal({
      title: "Delete selected layers",
      message: `Delete ${ids.length} layer(s) from the project?`,
      details: "This action removes the selected CSV layers from the current project view.",
      confirmText: "Delete layers",
      confirmClass: "btn btn-danger seis-btn-danger",
      iconClass: "fa-layer-group",
    });
    if (!confirmed) return;

    const url = btn.dataset.deleteUrl;

    try {
      btn.disabled = true;

      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": getCSRFToken(),
        },
        body: JSON.stringify({ ids }),
      });

      const data = await resp.json();

      if (!resp.ok || !data.ok) {
        showAppToast(data.error || "Delete failed.", { title: "Layers not deleted", variant: "danger" });
        return;
      }
      if (data.preplot_map) {
        renderBokehInto("preplot-map-div", data.preplot_map);
      }

      ids.forEach(id => {
        const row = tbody.querySelector(`tr[data-id="${id}"]`);
        if (row) row.remove();
      });

      if (tbody.querySelectorAll("tr").length === 0) {
        tbody.innerHTML = `
          <tr>
            <td colspan="6" class="text-center text-muted py-4">No layers loaded</td>
          </tr>
        `;
      }

      showAppToast(`${ids.length} layer(s) deleted.`, { title: "Layers updated", variant: "success" });
    } catch (err) {
      console.error(err);
      showAppToast("Network error while deleting layers.", { title: "Request failed", variant: "danger" });
    } finally {
      btn.disabled = false;
    }
  });
}
