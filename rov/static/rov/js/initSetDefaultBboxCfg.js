import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed

export function initSetDefaultBBoxConfig() {
  const btn = document.getElementById("set-default-bbox");
  const sel = document.getElementById("bblog-config-select");

  if (!btn || !sel) return;

  // prevent double binding
  if (btn.dataset.bound === "1") return;
  btn.dataset.bound = "1";

  btn.addEventListener("click", async () => {
    const cfgId = sel.value;
    if (!cfgId) return;

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

      // ✅ Option A: quick UI update (mark selected option visually)
      [...sel.options].forEach(o => (o.textContent = o.textContent.replace(/^⭐\s*/, "")));
      const opt = sel.options[sel.selectedIndex];
      opt.textContent = `⭐ ${opt.textContent}`;

      // ✅ Option B (better): reload config list from server (if you have endpoint)
      // await loadBBoxConfigs();

    } catch (e) {
      console.error(e);
      alert(e.message || "Failed to set default config");
    } finally {
      btn.disabled = false;
    }
  });
}
