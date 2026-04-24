import { getCSRFToken } from "./csrf.js";
import { renderBokehInto } from "./renderBokeh.js";
import { showAppToast } from "./toast.js";

export function initCsvLayerUpload() {
  const form = document.getElementById("csv-load-form");
  const btn = document.getElementById("btnSaveCsv");
  const tbody = document.getElementById("layersTbody");

  if (!form || !btn || !tbody) return;

  btn.addEventListener("click", async () => {
    const fd = new FormData(form);
    const fileInput = document.getElementById("csvFileInput");

    if (!fileInput?.files?.length) {
      showAppToast("Choose CSV file first.", { title: "CSV layer", variant: "warning" });
      return;
    }

    try {
      btn.disabled = true;

      const res = await fetch(form.action, {
        method: "POST",
        headers: { "X-CSRFToken": getCSRFToken() },
        body: fd,
      });

      const data = await res.json();

      if (!res.ok || !data.ok) {
        showAppToast(data.error || "Upload failed.", { title: "CSV layer", variant: "danger" });
        return;
      }
      if (data.preplot_map) renderBokehInto("preplot-map-div", data.preplot_map);
      tbody.innerHTML = data.layers_body;

      const modalEl = document.getElementById("csvuploadModal");
      const modal = bootstrap.Modal.getInstance(modalEl) || bootstrap.Modal.getOrCreateInstance(modalEl);
      modal.hide();

      form.reset();
      showAppToast("CSV layer imported successfully.", { title: "Layer added", variant: "success" });
    } catch (e) {
      showAppToast("Network error while uploading CSV layer.", { title: "CSV layer", variant: "danger" });
    } finally {
      btn.disabled = false;
    }
  });
}
