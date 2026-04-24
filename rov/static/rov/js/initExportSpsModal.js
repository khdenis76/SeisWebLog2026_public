// exportSpsModal.js
import { getCSRFToken } from "../../baseproject/js/csrf.js"; // adjust path

export function initExportSpsModal() {
  const modalEl = document.getElementById("export-sps-modal");
  if (!modalEl) return;

  const form = document.getElementById("export-sps-form");
  if (!form) return;

  const exportBtn = modalEl.querySelector("button[data-url]");
  if (!exportBtn) return;

  const statusEl = document.getElementById("export-sps-status");

  const setStatus = (html) => {
    if (!statusEl) return;
    statusEl.innerHTML = html;
  };

  const setBtnLoading = (isLoading) => {
    exportBtn.disabled = isLoading;
    exportBtn.innerHTML = isLoading
      ? `<span class="spinner-border spinner-border-sm me-2"></span>Exporting...`
      : `<i class="fas fa-file-export me-2"></i>Export SPS`;
  };

  exportBtn.addEventListener("click", async () => {
    const url = exportBtn.dataset.url;
    if (!url) return;

    // 1) Selected lines from table
    const selectedLines = Array.from(
      document.querySelectorAll(".dsr-line-checkbox:checked")
    ).map((cb) => cb.value);

    if (selectedLines.length === 0) {
      setStatus(`<div class="alert alert-warning mb-0">
        <i class="fas fa-triangle-exclamation me-2"></i>Select at least one line.
      </div>`);
      return;
    }

    // 2) Modal params
    const fd = new FormData(form);

    // Ensure unchecked checkboxes are sent as 0 (optional but convenient)
    // If you prefer Django default None/absent -> remove this block.
    ["export_header", "use_seq", "use_line_seq","use_line_fn"].forEach((name) => {
      if (!fd.has(name)) fd.append(name, "0");
    });

    // Add selected lines (JSON string or send multiple values)
    fd.set("selected_lines", JSON.stringify(selectedLines));

    // 3) POST fetch expecting JSON
    try {
      setStatus("");
      setBtnLoading(true);

      const resp = await fetch(url, {
        method: "POST",
        headers: {
          "X-CSRFToken": getCSRFToken(),
          "X-Requested-With": "XMLHttpRequest",
        },
        body: fd,
      });

      let data = null;
      const ct = resp.headers.get("content-type") || "";
      if (ct.includes("application/json")) {
        data = await resp.json();
      } else {
        // If backend returned HTML error page etc.
        const text = await resp.text();
        throw new Error(`Server did not return JSON. First 200 chars: ${text.slice(0, 200)}`);
      }

      if (!resp.ok || !data?.ok) {
        const msg = data?.message || `Export failed (HTTP ${resp.status})`;
        const details = data?.errors ? `<pre class="mb-0 mt-2">${JSON.stringify(data.errors, null, 2)}</pre>` : "";
        setStatus(`<div class="alert alert-danger">
          <i class="fas fa-circle-xmark me-2"></i>${msg}${details}
        </div>`);
        return;
      }

      // ✅ Success UI (customize)
      const filesHtml = Array.isArray(data.files) && data.files.length
        ? `<ul class="mb-0 mt-2">${data.files.map(f => `<li>${f}</li>`).join("")}</ul>`
        : "";

      setStatus(`<div class="alert alert-success">
        <i class="fas fa-circle-check me-2"></i>${data.message || "Export completed."}
        ${filesHtml}
      </div>`);

      // Optionally close modal on success:
      // bootstrap.Modal.getInstance(modalEl)?.hide();

      // Optionally refresh some table / badge count:
      // updateExportsList(data);

    } catch (err) {
      console.error(err);
      setStatus(`<div class="alert alert-danger mb-0">
        <i class="fas fa-bug me-2"></i>${err.message || "Unexpected error"}
      </div>`);
    } finally {
      setBtnLoading(false);
    }
  });
}
