import { getCSRFToken } from "../../baseproject/js/csrf.js";
import { showToast } from "./toast.js";

export function initSetDefaultBBoxConfig() {
  const btn = document.getElementById("set-default-bbox");
  const sel = document.getElementById("bblog-config-select");

  if (!btn || !sel) return;

  if (btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";

  btn.addEventListener("click", async () => {
    const cfgId = sel.value;
    if (!cfgId) {
      showToast({
        title: "Default BBox config",
        message: "Select a configuration first.",
        type: "warning",
      });
      return;
    }

    const url = btn.dataset.postUrl;
    if (!url) {
      console.error("set-default-bbox: missing data-post-url");
      return;
    }

    try {
      btn.disabled = true;

      const fd = new FormData();
      fd.append("id", cfgId);

      const resp = await fetch(url, {
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

      [...sel.options].forEach((o) => {
        o.textContent = o.textContent.replace(/^⭐\s*/, "");
      });
      const opt = sel.options[sel.selectedIndex];
      if (opt) opt.textContent = `⭐ ${opt.textContent}`;

      showToast({
        title: "Default BBox config",
        message: data.toast?.message || data.message || "Default configuration updated.",
        type: "success",
      });
    } catch (e) {
      console.error(e);
      showToast({
        title: "Default BBox config",
        message: e.message || "Failed to set default config.",
        type: "danger",
      });
    } finally {
      btn.disabled = false;
    }
  });
}
