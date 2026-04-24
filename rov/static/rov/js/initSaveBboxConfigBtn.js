import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed

export function initBBoxConfigSave() {
  const btn = document.getElementById("btnSaveBBoxCfg");
  const form = document.getElementById("bbox-config-form");
  const errBox = document.getElementById("csvErr");

  if (!btn || !form) return;

  // prevent double-binding
  if (btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";

  btn.addEventListener("click", async () => {
    errBox?.classList.add("d-none");
    if (errBox) errBox.textContent = "";

    try {
      // 1) base form data
      const fd = new FormData(form);

      // 2) collect DB-field → CSV-column mapping
      const mapping = {};
      document.querySelectorAll(".bbox-config-selector").forEach(el => {
        const field = el.dataset.fieldname;
        if (field) {
          mapping[field] = (el.value || "").trim();
        }
      });

      fd.append("mapping_json", JSON.stringify(mapping));

      // 3) POST
      const resp = await fetch(form.action, {
        method: "POST",
        headers: {
          "X-CSRFToken": getCSRFToken(),
        },
        body: fd,
      });

      const data = await resp.json().catch(() => ({}));

      if (!resp.ok || data.error) {
        throw new Error(data.error || `HTTP ${resp.status}`);
      }

      // 4) success → close modal
      const modalEl = document.getElementById("bbox-config-modal");
      bootstrap.Modal.getInstance(modalEl)?.hide();

      // optional: notify / refresh config list
      // refreshBBoxConfigs();
      // showToast(data.message || "Config saved");

    } catch (e) {
      if (errBox) {
        errBox.textContent = e.message || String(e);
        errBox.classList.remove("d-none");
      }
    }
  });
}
