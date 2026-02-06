import {getCSRFToken} from "./csrf.js";
import { renderBokehInto } from "./renderBokeh.js"
export function initCsvLayerUpload() {
  const form = document.getElementById("csv-load-form");
  const btn = document.getElementById("btnSaveCsv");
  const err = document.getElementById("csvErr");
  const tbody = document.getElementById("layersTbody");

  if (!form || !btn || !tbody) return;

  function showErr(msg) {
    err.textContent = msg;
    err.classList.remove("d-none");
  }
  function clearErr() {
    err.textContent = "";
    err.classList.add("d-none");
  }

  btn.addEventListener("click", async () => {
    clearErr();

    const fd = new FormData(form);

    // quick client checks
    const fileInput = document.getElementById("csvFileInput");
    if (!fileInput?.files?.length) {
      showErr("Choose CSV file first.");
      return;
    }

    try {
      const res = await fetch(form.action, {
        method: "POST",
        headers: { "X-CSRFToken": getCSRFToken() },
        body: fd,
      });

      const data = await res.json();

      if (!res.ok || !data.ok) {
        showErr(data.error || "Upload failed.");
        return;
      }
      if (data.preplot_map) {
           console.log("update preplot")
          renderBokehInto("preplot-map-div", data.preplot_map);
      }
      // ✅ update table body HTML (no page reboot)
      console.log("update layersTbody")
      tbody.innerHTML = data.layers_body;

      // close modal (your button already has data-bs-dismiss, but this is safer)
      const modalEl = document.getElementById("csvuploadModal");
      const modal = bootstrap.Modal.getInstance(modalEl) || bootstrap.Modal.getOrCreateInstance(modalEl);
      modal.hide();

      // reset form for next upload
      form.reset();

    } catch (e) {
      showErr("Network error while uploading.");
    }
  });
}
