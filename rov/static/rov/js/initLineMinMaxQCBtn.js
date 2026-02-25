// initLineMinMaxQc.js
// Expects backend to return JSON like: { ok: true, line_qc_plot: <bokeh json_item dict> }
// Renders into: #lines-min-max-qc-container
import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path if needed

export function initLineMinMaxQc() {
  const btn = document.getElementById("line-qc-load-button");
  const container = document.getElementById("lines-min-max-qc-container");
  if (!btn || !container) return;

  const url = btn.dataset.url;
  if (!url) return;

  const getCookie = (name) => {
    const v = `; ${document.cookie}`;
    const parts = v.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(";").shift();
    return "";
  };

  const postJson = async (u, body) => {

    const res = await fetch(u, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": getCSRFToken(),
      },
      body: JSON.stringify(body || {}),
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data?.error || `HTTP ${res.status}`);
    }
    return data;
  };

  const clearContainer = () => {
    // Remove previous Bokeh roots cleanly
    container.innerHTML = "";
  };

  const ensureBokeh = () => {
    // Requires BokehJS to be loaded globally.
    // In templates, include:
    // <script src="https://cdn.bokeh.org/bokeh/release/bokeh-3.4.3.min.js"></script>
    // <script src="https://cdn.bokeh.org/bokeh/release/bokeh-widgets-3.4.3.min.js"></script>
    // (version must match your server json_item)
    if (!window.Bokeh || !window.Bokeh.embed || !window.Bokeh.embed.embed_item) {
      throw new Error("BokehJS is not loaded. Include bokeh.js before calling initLineMinMaxQc().");
    }
  };

  const renderJsonItem = (jsonItem) => {
    ensureBokeh();
    clearContainer();

    // bokeh.embed.embed_item accepts (item, target)
    // item already has target_id inside sometimes, but we pass our container id.
    const targetId = container.id || "lines-min-max-qc-container";
    window.Bokeh.embed.embed_item(jsonItem, targetId);
  };

  const setLoading = (isLoading) => {
    btn.disabled = isLoading;
    btn.classList.toggle("disabled", isLoading);
    btn.innerHTML = isLoading
      ? `<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>Loading QC…`
      : `<i class="fas fa-image me-2"></i>Load map`;
  };

  btn.addEventListener("click", async () => {
    try {
      setLoading(true);

      // If you need to send filters/selected lines, put them here:
      const payload = {}; // e.g. { lines: selectedLines }

      const resp = await postJson(url, payload);

      // Accept either {line_qc_plot: ...} or {data:{line_qc_plot:...}}
      const item = resp?.line_qc_plot ?? resp?.data?.line_qc_plot;

      if (!item) {
        throw new Error("Server did not return 'line_qc_plot' json_item.");
      }

      renderJsonItem(item);
    } catch (err) {
      clearContainer();
      const msg = (err && err.message) ? err.message : String(err);
      container.innerHTML = `
        <div class="alert alert-danger m-2">
          <div class="fw-bold mb-1">QC plot load failed</div>
          <div class="small">${msg}</div>
        </div>
      `;
      // restore button label
    } finally {
      setLoading(false);
    }
  });
}